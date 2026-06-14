use anyhow::Result;
use datachannel_socket_common::{
    ChannelRef, DataChannelMessage, SignalMessage, WebRTCCommunicationHandle, WebRTCNotification,
};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};
use str0m::{
    Candidate, Event, IceConnectionState, Input, Output, Rtc, RtcConfig,
    change::{SdpAnswer, SdpOffer, SdpPendingOffer},
    channel::ChannelId,
    net::{Protocol, Receive},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tokio::{net::UdpSocket, sync::oneshot};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::{WebSocketStream, tungstenite};

// either return the advertise ip if its correct, or else generate a good one
// this is especially useful for local testing since IP addresses and ports might be in use
fn validate_advertised_addr(advertise_ip: IpAddr, udp_port: u16) -> Option<SocketAddr> {
    let advertised_addr = if advertise_ip.is_loopback() {
        // Discover the preferred outbound local interface without sending traffic.
        let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        socket.connect("1.1.1.1:80").ok()?;
        socket.local_addr().ok()?
    } else {
        SocketAddr::new(advertise_ip, udp_port)
    };

    if std::net::UdpSocket::bind(advertised_addr).is_ok() {
        return Some(advertised_addr);
    }

    // If the requested port is already in use, keep the same advertised IP but let the OS
    // choose a free port so the session can still establish.
    // (Binding to port 0 generates a fresh random port we can use)
    let fallback_socket =
        std::net::UdpSocket::bind(SocketAddr::new(advertised_addr.ip(), 0)).ok()?;
    Some(fallback_socket.local_addr().ok()?)
}

async fn write_msg<S>(
    sink: &mut SplitSink<WebSocketStream<S>, tungstenite::Message>,
    msg: &SignalMessage,
) -> Result<(), std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let json = serde_json::to_string(msg)?;
    let send_message = tungstenite::Message::Text(json.into());
    sink.send(send_message)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(())
}

async fn read_msg<S>(
    stream: &mut SplitStream<WebSocketStream<S>>,
) -> Result<SignalMessage, std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let msg = stream
            .next()
            .await
            .ok_or_else(|| std::io::Error::other("no signal message"))?
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        match msg {
            tungstenite::Message::Text(text) => {
                let parsed = serde_json::from_str::<SignalMessage>(&text)?;
                return Ok(parsed);
            }
            tungstenite::Message::Binary(bytes) => {
                let parsed = serde_json::from_slice::<SignalMessage>(&bytes)?;
                return Ok(parsed);
            }
            tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {
                continue;
            }
            tungstenite::Message::Close(_) => {
                return Err(std::io::Error::other("websocket closed"));
            }
            _ => {
                continue;
            }
        }
    }
}

// double-sided hashmap
struct ChannelMap {
    id_to_ref: HashMap<ChannelId, ChannelRef>,
    ref_to_id: HashMap<ChannelRef, ChannelId>,
}

impl ChannelMap {
    fn new() -> ChannelMap {
        Self {
            id_to_ref: HashMap::new(),
            ref_to_id: HashMap::new(),
        }
    }

    fn get_ref_of_id(&self, id: &ChannelId) -> Option<&ChannelRef> {
        self.id_to_ref.get(id)
    }

    fn get_id_of_ref(&self, channel_ref: &ChannelRef) -> Option<&ChannelId> {
        self.ref_to_id.get(channel_ref)
    }

    fn add_channel(&mut self, rtc: &mut Rtc, channel_id: ChannelId) -> Option<ChannelRef> {
        if let Some(ch) = rtc.channel(channel_id)
            && let Some(config) = ch.config()
        {
            let channel_ref = ChannelRef {
                label: config.label.clone(),
                id_hint: config.negotiated,
            };
            self.id_to_ref.insert(channel_id, channel_ref.clone());
            self.ref_to_id.insert(channel_ref.clone(), channel_id);
            Some(channel_ref)
        } else {
            None
        }
    }

    fn remove_channel(&mut self, channel_id: ChannelId) {
        let channel_ref = self.id_to_ref.remove(&channel_id);
        if let Some(channel_ref) = channel_ref {
            self.ref_to_id.remove(&channel_ref);
        }
    }
}

pub struct NativePeer {
    pub rtc: Rtc,
    pub socket: UdpSocket,
    pub bound_addr: SocketAddr,      // wildcard or actual socket bind
    pub advertised_addr: SocketAddr, // ICE candidate address exposed to peer
    pending_offer: Option<SdpPendingOffer>,
    channel_map: ChannelMap,
    webrtc_notification_sender: UnboundedSender<WebRTCNotification>,
    // we want to let client api take the receiver so clients can read what's come in
    webrtc_notification_receiver: Option<UnboundedReceiver<WebRTCNotification>>,
    incoming_datachannel_message_sender: UnboundedSender<(ChannelRef, DataChannelMessage)>,
    // we want to let client api take the receiver so clients can read what's come in
    incoming_datachannel_message_receiver:
        Option<UnboundedReceiver<(ChannelRef, DataChannelMessage)>>,
    // we can clone this and give it to the user before the native peer starts running for real
    outgoing_datachannel_message_sender: UnboundedSender<(ChannelRef, DataChannelMessage)>,
    outgoing_datachannel_message_receiver: UnboundedReceiver<(ChannelRef, DataChannelMessage)>,
}

impl NativePeer {
    pub async fn new(advertised_addr: SocketAddr) -> Result<Self, std::io::Error> {
        let bind_ip = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

        let std_socket =
            std::net::UdpSocket::bind(SocketAddr::new(bind_ip, advertised_addr.port()))?;
        std_socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_socket)?;

        let bound_addr = socket.local_addr()?;

        let rtc = RtcConfig::new().build(Instant::now());
        let candidate = Candidate::host(advertised_addr, "udp")
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let (webrtc_notification_sender, webrtc_notification_receiver) = unbounded();
        let (incoming_datachannel_message_sender, incoming_datachannel_message_receiver) =
            unbounded();
        let (outgoing_datachannel_message_sender, outgoing_datachannel_message_receiver) =
            unbounded();

        let mut peer = Self {
            rtc,
            socket,
            bound_addr,
            advertised_addr,
            pending_offer: None,
            channel_map: ChannelMap::new(),
            webrtc_notification_sender,
            webrtc_notification_receiver: Some(webrtc_notification_receiver),
            incoming_datachannel_message_sender,
            incoming_datachannel_message_receiver: Some(incoming_datachannel_message_receiver),
            outgoing_datachannel_message_sender,
            outgoing_datachannel_message_receiver,
        };

        peer.rtc.add_local_candidate(candidate);

        Ok(peer)
    }

    pub fn get_communication_handle(
        &mut self,
    ) -> Result<WebRTCCommunicationHandle, std::io::Error> {
        let webrtc_notification_receiver =
            self.webrtc_notification_receiver
                .take()
                .ok_or(std::io::Error::new(
                    ErrorKind::AlreadyExists,
                    "WebRTC Channel receiver already sent out",
                ))?;
        let incoming_datachannel_message_receiver = self
            .incoming_datachannel_message_receiver
            .take()
            .ok_or(std::io::Error::new(
                ErrorKind::AlreadyExists,
                "Incoming Channel receiver already sent out",
            ))?;
        Ok(WebRTCCommunicationHandle::new(
            webrtc_notification_receiver,
            incoming_datachannel_message_receiver,
            self.outgoing_datachannel_message_sender.clone(),
        ))
    }

    // once this runs, we won't be able to access any channel receivers or channel data, so we need to get it beforehand
    pub async fn run(&mut self, peer_name: &str, done_tx: oneshot::Sender<()>) -> Result<()> {
        let mut buf = vec![0u8; 65535];

        'rtc_loop: loop {
            let next_timeout = loop {
                match self.rtc.poll_output()? {
                    Output::Timeout(t) => break t,
                    Output::Transmit(t) => {
                        self.socket.send_to(&t.contents, t.destination).await?;
                    }
                    Output::Event(event) => match event {
                        Event::Connected => {
                            println!("{peer_name}: connected");
                        }
                        Event::IceConnectionStateChange(state) => {
                            println!("{peer_name}: event: IceConnectionStateChange({state:?})");

                            if state == IceConnectionState::Disconnected {
                                println!(
                                    "{peer_name}: ending session after terminal ICE state {state:?}"
                                );
                                let _ = done_tx.send(());
                                return Ok(());
                            }
                        }
                        Event::ChannelOpen(cid, label) => {
                            println!("{peer_name}: channel open: {label:?}");
                            let channel_ref = self
                                .channel_map
                                .add_channel(&mut self.rtc, cid)
                                .expect("channel should be open");
                            let _ = self
                                .webrtc_notification_sender
                                .send(WebRTCNotification::ChannelOpen(channel_ref))
                                .await;
                        }
                        Event::ChannelData(data) => {
                            let data_as_string = String::from_utf8_lossy(&data.data);
                            //println!("{peer_name}: got data: {data_as_string:?}");
                            let channel_ref = self
                                .channel_map
                                .get_ref_of_id(&data.id)
                                .expect("channel should exist")
                                .clone();
                            if data.binary {
                                let _ = self
                                    .incoming_datachannel_message_sender
                                    .send((channel_ref, DataChannelMessage::Binary(data.data)))
                                    .await;
                            } else {
                                let _ = self
                                    .incoming_datachannel_message_sender
                                    .send((
                                        channel_ref,
                                        DataChannelMessage::Text(data_as_string.parse()?),
                                    ))
                                    .await;
                            }
                        }
                        Event::ChannelClose(cid) => {
                            println!("{peer_name}: channel close: {cid:?}");
                            self.channel_map.remove_channel(cid);
                        }
                        _other => {
                            //println!("{peer_name}: event: {other:?}");
                        }
                    },
                }
                // since this is a loop, lets allow for yielding
                //tokio::task::yield_now().await;
            };

            let wait = next_timeout.saturating_duration_since(Instant::now());
            let sleep = tokio::time::sleep(wait);

            tokio::select! {
                result = self.socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, source)) => {
                            let receive = Receive::new(
                                Protocol::Udp,
                                source,
                                 // this should actually be the addr that is publicly available (the advertised addr).
                                self.advertised_addr,
                                &buf[..n],
                            )?;
                            self.rtc.handle_input(Input::Receive(Instant::now(), receive))?;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                            self.rtc.handle_input(Input::Timeout(Instant::now()))?;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                // TODO: Is this fine? Or should we put this in a while loop before the select?
                msg = self.outgoing_datachannel_message_receiver.recv() => {
                    if let Ok((channel_ref, msg)) = msg {
                        if let Some(cid) = self.channel_map.get_id_of_ref(&channel_ref) && let Some(mut ch) = self.rtc.channel(*cid) {
                            match msg {
                                DataChannelMessage::Text(text) => {
                                    ch.write(false, text.as_bytes())?;
                                }
                                DataChannelMessage::Binary(binary) => {
                                    ch.write(true, binary.as_slice())?;
                                }
                            }
                        }
                    }
                }
                _ = sleep => {
                    self.rtc.handle_input(Input::Timeout(Instant::now()))?;
                }
            }
        }
    }

    pub async fn create_offer(
        &mut self,
        channel_label: &str,
    ) -> std::result::Result<String, std::io::Error> {
        let mut api = self.rtc.sdp_api();
        let _cid = api.add_channel(channel_label.into());
        let (offer, pending) = api
            .apply()
            .ok_or_else(|| std::io::Error::other("no SDP changes to apply"))?;
        self.pending_offer = Some(pending);
        Ok(offer.to_sdp_string())
    }

    pub async fn accept_offer(
        &mut self,
        sdp_offer: &str,
    ) -> std::result::Result<String, std::io::Error> {
        let offer = SdpOffer::from_sdp_string(sdp_offer)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let answer = self
            .rtc
            .sdp_api()
            .accept_offer(offer)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(answer.to_sdp_string())
    }

    pub async fn accept_answer(
        &mut self,
        sdp_answer: &str,
    ) -> std::result::Result<(), std::io::Error> {
        let pending = self
            .pending_offer
            .take()
            .ok_or_else(|| std::io::Error::other("no pending offer"))?;
        let answer = SdpAnswer::from_sdp_string(sdp_answer)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        self.rtc
            .sdp_api()
            .accept_answer(pending, answer)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(())
    }
}

pub struct NativeClientPeerFactory {}

// native
impl NativeClientPeerFactory {
    pub fn new() -> Self {
        str0m::crypto::from_feature_flags().install_process_default();
        Self {}
    }

    pub async fn create_peer(
        &self,
        signaling_server_addr: String,
        udp_port: u16,
    ) -> Result<NativePeer, std::io::Error> {
        // because the server is already advertising it's public IP, we don't actually need to put in the work to find our own IP
        // so we can put whatever we want here since the "server" peer will be able to directly connect anyways.
        let advertised_addr = validate_advertised_addr(IpAddr::V4(Ipv4Addr::LOCALHOST), udp_port)
            .ok_or(std::io::Error::other("Failed to generate address"))?;

        let mut peer = NativePeer::new(advertised_addr).await?;
        println!("client: UDP bound on {}", peer.bound_addr);
        println!("client: advertising ICE candidate {advertised_addr}",);

        println!("connecting to websocket signaling server {signaling_server_addr:?}");
        let (stream, response) = connect_async(signaling_server_addr)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        println!("client: connected to server, initial response {response:?}");

        let offer_sdp = peer.create_offer("chat").await?;
        let (mut write_half, mut read_half) = stream.split();
        write_msg(&mut write_half, &SignalMessage::Offer { sdp: offer_sdp }).await?;

        let answer = read_msg(&mut read_half).await;
        if let Ok(SignalMessage::Answer { sdp }) = answer {
            peer.accept_answer(&sdp).await?;
        } else {
            return Err(std::io::Error::other("expected answer"));
        }
        drop(read_half);
        drop(write_half);

        println!("We're connected, so no need for websocket connection");

        Ok(peer)
    }
}

pub struct NativeServerPeerFactory {
    tcp_listener: TcpListener,
}

impl NativeServerPeerFactory {
    pub fn new(tcp_listener: TcpListener) -> Self {
        Self { tcp_listener }
    }

    pub async fn create_peer(
        &self,
        advertised_ip: IpAddr,
        port: u16,
    ) -> Result<NativePeer, std::io::Error> {
        let (raw_stream, addr) = self.tcp_listener.accept().await?;
        // this is only really necessary if you are testing server and client on same machine
        let advertise_addr = validate_advertised_addr(advertised_ip, port)
            .ok_or(std::io::Error::other("Failed to generate address"))?;
        println!("Advertising server on '{advertise_addr}'");
        println!(
            "Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip."
        );
        if addr.ip().is_loopback() {
            eprintln!(
                "server: info: signaling peer is loopback ({addr}), advertising non-loopback ICE IP {advertise_addr} for browser compatibility"
            );
        }

        let mut peer = NativePeer::new(advertise_addr).await?;
        println!("server: UDP bound on {}", peer.bound_addr);
        println!("server: advertising ICE candidate {}", peer.advertised_addr);

        let ws_stream_result = tokio_tungstenite::accept_async(raw_stream).await;
        if let Err(err) = ws_stream_result {
            return Err(std::io::Error::other(format!(
                "WebSocket handshake failed for {}: {:?}",
                addr, err
            )));
        }
        let ws_stream = ws_stream_result.unwrap();
        println!("server: signaling connected from {addr}");

        let (mut write_stream, mut read_stream) = ws_stream.split();

        let offer = read_msg(&mut read_stream).await?;

        if let SignalMessage::Offer { sdp } = offer {
            let answer_sdp = peer.accept_offer(&sdp).await?;
            write_msg(
                &mut write_stream,
                &SignalMessage::Answer { sdp: answer_sdp },
            )
            .await?;
        } else {
            return Err(std::io::Error::other("expected offer"));
        }

        println!("Closing stream, don't need it anymore, client should be connected.");
        Ok(peer)
    }
}
