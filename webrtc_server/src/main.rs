use std::net::IpAddr;

use anyhow::{Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use native_shared::{
    SignalMessage,
    peer::{Peer, RoleAction},
    read_msg, write_msg,
};
use serde_json::Deserializer;

use tokio::{net::TcpListener, sync::oneshot, task::JoinSet};

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

async fn run_server(args: &Args) -> Result<()> {
    let advertise_ip = args
        .advertise_ip
        .ok_or_else(|| anyhow!("--advertise-ip is required in server mode"))?;

    let mut peer = Peer::new(args.bind_ip, advertise_ip, args.server_udp_port).await?;
    println!("server: UDP bound on {}", peer.local_addr);
    println!(
        "server: advertising ICE candidate {}:{}",
        advertise_ip, args.server_udp_port
    );

    let listener = TcpListener::bind((args.bind_ip, args.signal_port)).await?;
    println!("server: signaling on {}:{}", args.bind_ip, args.signal_port);

    while let Ok((mut stream, addr)) = listener.accept().await {
        println!("server: signaling connected from {addr}");

        let (mut read_stream, mut write_stream) = stream.split();

        let offer = read_msg(&mut read_stream).await?;
        let answer_sdp = match offer {
            SignalMessage::Offer { sdp } => peer.accept_offer(&sdp)?,
            _ => bail!("expected offer"),
        };

        write_msg(&mut write_stream, &SignalMessage::Answer { sdp: answer_sdp }).await?;

        let (tx, _rx) = oneshot::channel::<Vec<u8>>();
        peer.run("server", RoleAction::EchoServer, tx).await?
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_server(&args).await?;

    Ok(())
}
