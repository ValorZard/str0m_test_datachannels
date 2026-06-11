use std::net::{IpAddr, SocketAddr};

use anyhow::{Result, anyhow};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use common::SignalMessage;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{WebSocketStream, tungstenite};
pub mod peer;

// IMPORTANT! THIS HAS TO BE CALLED BEFORE ALL STR0M WEBRTC STUFF
pub fn install_str0m_process() {
    str0m::crypto::from_feature_flags().install_process_default();
}

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
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let json = serde_json::to_string(msg)?;
    let send_message = tungstenite::Message::Text(json.into());
    sink.send(send_message).await?;
    Ok(())
}

pub async fn read_msg<S>(stream: &mut SplitStream<WebSocketStream<S>>) -> Result<SignalMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let msg = stream
            .next()
            .await
            .ok_or_else(|| anyhow!("no signal message"))??;

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
                return Err(anyhow!("websocket closed"));
            }
            _ => {
                continue;
            }
        }
    }
}
