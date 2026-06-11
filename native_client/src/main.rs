use anyhow::{Result, anyhow, bail};
use clap::Parser;
use common::{Peer, SignalMessage};
use futures_util::StreamExt;
use native_shared::{
    peer::{NativePeer, RoleAction},
    read_msg, validate_advertised_addr, write_msg,
};
use std::net::{IpAddr, Ipv4Addr};
use tokio::sync::oneshot;
use tokio_tungstenite::connect_async;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    server_addr: String,

    #[arg(long, default_value_t = 5001)]
    udp_port: u16,

    #[arg(long, default_value = "hello from client")]
    message: String,
}

async fn run_client(args: &Args) -> Result<()> {
    // HAS TO BE RUN BEFORE WEBRTC STUFF RUNS
    let server_addr = &args.server_addr;

    // because the server is already advertising it's public IP, we don't actually need to put in the work to find our own IP
    // so we can put whatever we want here since the "server" peer will be able to directly connect anyways.
    let advertised_addr = validate_advertised_addr(IpAddr::V4(Ipv4Addr::LOCALHOST), args.udp_port)
        .ok_or(anyhow!("Failed to generate address"))?;

    let mut peer = NativePeer::new(advertised_addr).await?;
    println!("client: UDP bound on {}", peer.bound_addr);
    println!("client: advertising ICE candidate {advertised_addr}",);

    println!("connecting to websocket signaling server {server_addr}");
    let (stream, response) = connect_async(server_addr).await?;
    println!("client: signaling connected to {server_addr}");
    println!("client: initial response from server {response:?}");

    let offer_sdp = peer.create_offer("chat").await?;
    let (mut write_half, mut read_half) = stream.split();
    write_msg(&mut write_half, &SignalMessage::Offer { sdp: offer_sdp }).await?;

    let answer = read_msg(&mut read_half).await?;
    match answer {
        SignalMessage::Answer { sdp } => peer.accept_answer(&sdp).await?,
        _ => bail!("expected answer"),
    }

    drop(read_half);
    drop(write_half);

    println!("We're connected, so no need for websocket connection");

    let (tx, rx) = oneshot::channel::<Vec<u8>>();
    let msg = args.message.as_bytes().to_vec();

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

    peer.run("client", RoleAction::ClientSendAndWait { message: msg }, tx)
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_client(&args).await?;

    Ok(())
}
