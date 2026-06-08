use anyhow::Result;
use serde_json::Deserializer;
use std::{
    io::{BufReader, Write},
    net::{IpAddr, Ipv4Addr, TcpStream},
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

    let mut peer = Peer::new(bind_ip, 5001).await?;
    println!("client: UDP on {}", peer.local_addr);

    let mut stream = TcpStream::connect("127.0.0.1:7000")?;
    println!("client: signaling connected");

    let offer_sdp = peer.create_offer("chat")?;
    write_msg(&mut stream, &SignalMessage::Offer { sdp: offer_sdp })?;

    let answer = read_msg(stream.try_clone()?)?;
    match answer {
        SignalMessage::Answer { sdp } => peer.accept_answer(&sdp)?,
        _ => anyhow::bail!("expected answer"),
    }

    let (tx, rx) = oneshot::channel::<Vec<u8>>();
    let msg = b"hello from client".to_vec();

    let run = peer.run(
        "client",
        RoleAction::ClientSendAndWait { message: msg.clone() },
        tx,
    );

    tokio::spawn(async move {
        match rx.await {
            Ok(reply) => {
                println!("client: echo reply = {:?}", String::from_utf8_lossy(&reply));
            }
            Err(e) => {
                eprintln!("client: wait failed: {e}");
            }
        }
    });

    run.await?;
    Ok(())
}