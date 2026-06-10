use anyhow::{Result, bail, anyhow};
use clap::Parser;
use futures_util::StreamExt;
use native_shared::{
    install_str0m_process, peer::{Peer, RoleAction}, read_msg, validate_advertised_addr, write_msg
};
use signaling_shared::SignalMessage;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::{net::TcpStream, sync::oneshot};
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
    install_str0m_process();
    let server_addr = &args.server_addr;

    // because the server is already advertising it's public IP, we don't actually need to put in the work to find our own IP
    // so we can put whatever we want here since the "server" peer will be able to directly connect anyways.
    let advertised_addr = validate_advertised_addr(IpAddr::V4(Ipv4Addr::LOCALHOST), args.udp_port).ok_or(anyhow!("Failed to generate address"))?;

    let mut peer = Peer::new(advertised_addr).await?;
    println!("client: UDP bound on {}", peer.bound_addr);
    println!(
        "client: advertising ICE candidate {advertised_addr}",
    );

    println!("connecting to websocket signaling server {server_addr}");
    let (stream, response) = connect_async(server_addr).await?;
    println!("client: signaling connected to {server_addr}");
    println!("client: initial response from server {response:?}");

    let offer_sdp = peer.create_offer("chat")?;
    let (mut write_half, mut read_half) = stream.split();
    write_msg(&mut write_half, &SignalMessage::Offer { sdp: offer_sdp }).await?;

    let answer = read_msg(&mut read_half).await?;
    match answer {
        SignalMessage::Answer { sdp } => peer.accept_answer(&sdp)?,
        _ => bail!("expected answer"),
    }

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

    peer.run(
        "client",
        RoleAction::ClientSendAndWait { message: msg },
        None,
        tx,
    )
    .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_client(&args).await?;

    Ok(())
}
