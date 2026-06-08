use anyhow::{Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use serde_json::Deserializer;
use std::{
    io::{BufReader, Write},
    net::{IpAddr, TcpListener, TcpStream},
};
use tokio::sync::oneshot;

mod common;
mod peer;

use common::SignalMessage;
use peer::{Peer, RoleAction};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Server,
    Client,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_enum)]
    mode: Mode,

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

fn write_msg(stream: &mut TcpStream, msg: &SignalMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    stream.write_all(json.as_bytes())?;
    stream.write_all(b"\n")?;
    Ok(())
}

fn read_msg(stream: TcpStream) -> Result<SignalMessage> {
    let reader = BufReader::new(stream);
    let mut de = Deserializer::from_reader(reader).into_iter::<SignalMessage>();
    de.next()
        .ok_or_else(|| anyhow!("no signal message"))?
        .map_err(Into::into)
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

    let listener = TcpListener::bind((args.bind_ip, args.signal_port))?;
    println!("server: signaling on {}:{}", args.bind_ip, args.signal_port);

    let (mut stream, addr) = listener.accept()?;
    println!("server: signaling connected from {addr}");

    let offer = read_msg(stream.try_clone()?)?;
    let answer_sdp = match offer {
        SignalMessage::Offer { sdp } => peer.accept_offer(&sdp)?,
        _ => bail!("expected offer"),
    };

    write_msg(&mut stream, &SignalMessage::Answer { sdp: answer_sdp })?;

    let (tx, _rx) = oneshot::channel::<Vec<u8>>();
    peer.run("server", RoleAction::EchoServer, tx).await
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

    let mut stream = TcpStream::connect((server_ip, args.signal_port))?;
    println!(
        "client: signaling connected to {}:{}",
        server_ip, args.signal_port
    );

    let offer_sdp = peer.create_offer("chat")?;
    write_msg(&mut stream, &SignalMessage::Offer { sdp: offer_sdp })?;

    let answer = read_msg(stream.try_clone()?)?;
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
    str0m::crypto::from_feature_flags().install_process_default();

    let args = Args::parse();

    match args.mode {
        Mode::Server => run_server(&args).await,
        Mode::Client => run_client(&args).await,
    }
}