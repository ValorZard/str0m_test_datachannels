use anyhow::{Result, anyhow};
use std::{
    net::{IpAddr, SocketAddr},
    time::Instant,
};
use str0m::{
    Candidate, Event, Input, Output, Rtc, RtcConfig,
    change::{SdpAnswer, SdpOffer, SdpPendingOffer},
    channel::ChannelId,
    net::{Protocol, Receive},
};
use tokio::{net::UdpSocket, sync::oneshot};

pub enum RoleAction {
    EchoServer,
    ClientSendAndWait { message: Vec<u8> },
}

pub struct Peer {
    pub rtc: Rtc,
    pub socket: UdpSocket,
    pub bound_addr: SocketAddr,      // wildcard or actual socket bind
    pub advertised_addr: SocketAddr, // ICE candidate address exposed to peer
    pending_offer: Option<SdpPendingOffer>,
}

impl Peer {
    pub async fn new(bind_ip: IpAddr, advertise_ip: IpAddr, udp_port: u16) -> Result<Self> {
        str0m::crypto::from_feature_flags().install_process_default();

        let std_socket = std::net::UdpSocket::bind(SocketAddr::new(bind_ip, udp_port))?;
        std_socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_socket)?;

        let bound_addr = socket.local_addr()?;
        let advertised_addr = SocketAddr::new(advertise_ip, bound_addr.port());

        let rtc = RtcConfig::new().build(Instant::now());
        let candidate = Candidate::host(advertised_addr, "udp")?;

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

    pub fn create_offer(&mut self, channel_label: &str) -> Result<String> {
        let mut api = self.rtc.sdp_api();
        let _cid = api.add_channel(channel_label.into());
        let (offer, pending) = api
            .apply()
            .ok_or_else(|| anyhow!("no SDP changes to apply"))?;
        self.pending_offer = Some(pending);
        Ok(offer.to_sdp_string())
    }

    pub fn accept_offer(&mut self, sdp_offer: &str) -> Result<String> {
        let offer = SdpOffer::from_sdp_string(sdp_offer)?;
        let answer = self.rtc.sdp_api().accept_offer(offer)?;
        Ok(answer.to_sdp_string())
    }

    pub fn accept_answer(&mut self, sdp_answer: &str) -> Result<()> {
        let pending = self
            .pending_offer
            .take()
            .ok_or_else(|| anyhow!("no pending offer"))?;
        let answer = SdpAnswer::from_sdp_string(sdp_answer)?;
        self.rtc.sdp_api().accept_answer(pending, answer)?;
        Ok(())
    }

    pub fn add_remote_ice_candidate(
    &mut self,
    candidate: String,
    ) -> Result<()> {
        let candidate = Candidate::from_sdp_string(&candidate)?;
        self.rtc.add_remote_candidate(candidate);
        Ok(())
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
