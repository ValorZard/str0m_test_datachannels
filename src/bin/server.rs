use anyhow::Result;
use serde_json::Deserializer;
use std::{
    io::{BufReader, Write},
    net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
};
use tokio::sync::oneshot;

#[path = "../common.rs"]
mod common;
#[path = "../peer.rs"]
mod peer;

use common::SignalMessage;
use peer::{Peer, RoleAction};

fn write_msg(stream: &mut TcpStream, msg: &SignalMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    stream.write_all(json.as_bytes())?;
    stream.write_all(b"\n")?;
    Ok(())
}

fn read_msg(stream: TcpStream) -> Result<SignalMessage> {
    let reader = BufReader::new(stream);
    let mut de = Deserializer::from_reader(reader).into_iter::<SignalMessage>();
    de.next().ok_or_else(|| anyhow::anyhow!("no signal message"))?
        .map_err(Into::into)
}

#[tokio::main]
async fn main() -> Result<()> {
    str0m::crypto::from_feature_flags().install_process_default();

    let bind_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let mut peer = Peer::new(bind_ip, 5000).await?;
    println!("server: UDP on {}", peer.local_addr);

    let listener = TcpListener::bind("127.0.0.1:7000")?;
    println!("server: signaling on 127.0.0.1:7000");

    let (mut stream, addr) = listener.accept()?;
    println!("server: signaling connected from {addr}");

    let offer = read_msg(stream.try_clone()?)?;
    let answer_sdp = match offer {
        SignalMessage::Offer { sdp } => peer.accept_offer(&sdp)?,
        _ => anyhow::bail!("expected offer"),
    };

    write_msg(&mut stream, &SignalMessage::Answer { sdp: answer_sdp })?;

    let (_tx, rx) = oneshot::channel::<Vec<u8>>();
    peer.run("server", RoleAction::EchoServer, _tx).await?;
    drop(rx);

    Ok(())
}