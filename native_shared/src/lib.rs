use anyhow::{Result, anyhow};
use common::{Peer, PeerFactory, SignalMessage};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};
use str0m::{
    Candidate, Event, Input, Output, Rtc, RtcConfig,
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
pub fn validate_advertised_addr(advertise_ip: IpAddr, udp_port: u16) -> Option<SocketAddr> {
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

pub async fn write_msg<S>(
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

pub async fn read_msg<S>(
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

pub enum RoleAction {
    EchoServer,
    ClientSendAndWait { message: Vec<u8> },
}

pub struct NativePeer {
    pub rtc: Rtc,
    pub socket: UdpSocket,
    pub bound_addr: SocketAddr,      // wildcard or actual socket bind
    pub advertised_addr: SocketAddr, // ICE candidate address exposed to peer
    pending_offer: Option<SdpPendingOffer>,
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

        let mut peer = Self {
            rtc,
            socket,
            bound_addr,
            advertised_addr,
            pending_offer: None,
        };

        peer.rtc.add_local_candidate(candidate);

        Ok(peer)
    }

    pub async fn run(
        &mut self,
        peer_name: &str,
        action: RoleAction,
        done_tx: oneshot::Sender<Vec<u8>>,
    ) -> Result<()> {
        let mut buf = vec![0u8; 65535];
        let mut channel_id: Option<ChannelId> = None;
        let mut sent = false;
        let mut done_tx = Some(done_tx);
        let mut buffered_echo: Vec<(ChannelId, bool, Vec<u8>)> = Vec::new();

        loop {
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

                            if matches!(&action, RoleAction::EchoServer)
                                && matches!(
                                    format!("{state:?}").as_str(),
                                    "Disconnected" | "Failed" | "Closed"
                                )
                            {
                                println!(
                                    "{peer_name}: ending session after terminal ICE state {state:?}"
                                );
                                return Ok(());
                            }
                        }
                        Event::ChannelOpen(cid, label) => {
                            println!("{peer_name}: channel open: {label:?}");
                            channel_id = Some(cid);

                            if let RoleAction::EchoServer = action {
                                for (id, binary, data) in std::mem::take(&mut buffered_echo) {
                                    if let Some(mut ch) = self.rtc.channel(id) {
                                        ch.write(binary, &data)?;
                                    }
                                }
                            }
                        }
                        Event::ChannelData(data) => {
                            println!(
                                "{peer_name}: got data: {:?}",
                                String::from_utf8_lossy(&data.data)
                            );

                            match &action {
                                RoleAction::EchoServer => {
                                    if channel_id == Some(data.id) {
                                        if let Some(mut ch) = self.rtc.channel(data.id) {
                                            ch.write(data.binary, &data.data)?;
                                        }
                                    } else {
                                        buffered_echo.push((
                                            data.id,
                                            data.binary,
                                            data.data.to_vec(),
                                        ));
                                    }
                                }
                                RoleAction::ClientSendAndWait { message } => {
                                    if data.data == message.as_slice() {
                                        if let Some(tx) = done_tx.take() {
                                            let _ = tx.send(data.data.to_vec());
                                        }
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        other => {
                            println!("{peer_name}: event: {other:?}");
                        }
                    },
                }
            };

            if let RoleAction::ClientSendAndWait { message } = &action {
                if !sent {
                    if let Some(cid) = channel_id {
                        if let Some(mut ch) = self.rtc.channel(cid) {
                            ch.write(false, message)?;
                            println!(
                                "{peer_name}: sent data: {:?}",
                                String::from_utf8_lossy(message)
                            );
                            sent = true;
                            continue;
                        }
                    }
                }
            }

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
                _ = sleep => {
                    self.rtc.handle_input(Input::Timeout(Instant::now()))?;
                }
            }
        }
    }
}

impl Peer for NativePeer {
    type Error = std::io::Error;

    async fn create_offer(
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

    async fn accept_offer(
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

    async fn accept_answer(&mut self, sdp_answer: &str) -> std::result::Result<(), std::io::Error> {
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
impl PeerFactory for NativeClientPeerFactory {
    type Error = std::io::Error;
    type PeerType = NativePeer;
    // String is url to signaling server, u16 is what port the client is connecting to the server peer with.
    type CreateArgs = (String, u16);
    type FactoryArgs = ();

    fn new(_: Self::FactoryArgs) -> Self {
        str0m::crypto::from_feature_flags().install_process_default();
        Self {}
    }

    async fn create_peer(&self, args: Self::CreateArgs) -> Result<Self::PeerType, Self::Error> {
        let signaling_server_addr = args.0;
        let udp_port = args.1;
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

impl PeerFactory for NativeServerPeerFactory {
    type Error = std::io::Error;
    type PeerType = NativePeer;
    type FactoryArgs = TcpListener;
    // The IpAddr is the advertised IP (which should be the server's public internet IP), and the u16 is the port it will try to listen on.
    type CreateArgs = (IpAddr, u16);

    fn new(args: Self::FactoryArgs) -> Self {
        Self { tcp_listener: args }
    }

    async fn create_peer(&self, args: Self::CreateArgs) -> Result<Self::PeerType, Self::Error> {
        let (raw_stream, addr) = self.tcp_listener.accept().await?;
        // this is only really necessary if you are testing server and client on same machine
        let advertise_addr = validate_advertised_addr(args.0, args.1)
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
