use anyhow::{Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use native_shared::{
    SignalMessage,
    peer::{Peer, RoleAction},
    read_msg, write_msg,
};
use serde_json::Deserializer;
use std::{
    net::{IpAddr},
};
use tokio::{net::TcpStream, sync::oneshot};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    bind_ip: IpAddr,

    #[arg(long)]
    advertise_ip: Option<IpAddr>,

    #[arg(long)]
    server_ip: Option<IpAddr>,

    #[arg(long, default_value_t = 7000)]
    signal_port: u16,

    #[arg(long, default_value_t = 5000)]
    server_udp_port: u16,

    #[arg(long, default_value_t = 5001)]
    client_udp_port: u16,

    #[arg(long, default_value = "hello from client")]
    message: String,
}

async fn run_client(args: &Args) -> Result<()> {
    let server_ip = args
        .server_ip
        .ok_or_else(|| anyhow!("--server-ip is required in client mode"))?;

    let advertise_ip = args.advertise_ip.unwrap_or(args.bind_ip);

    let mut peer = Peer::new(args.bind_ip, advertise_ip, args.client_udp_port).await?;
    println!("client: UDP bound on {}", peer.local_addr);
    println!(
        "client: advertising ICE candidate {}:{}",
        advertise_ip, args.client_udp_port
    );

    let mut stream = TcpStream::connect((server_ip, args.signal_port)).await?;
    println!(
        "client: signaling connected to {}:{}",
        server_ip, args.signal_port
    );

    let offer_sdp = peer.create_offer("chat")?;
    let (mut read_half, mut write_half) = stream.split();
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

    peer.run("client", RoleAction::ClientSendAndWait { message: msg }, tx)
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_client(&args).await?;

    Ok(())
}
