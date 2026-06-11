use anyhow::{Result, anyhow, bail};
use clap::Parser;
use common::{Peer, PeerFactory, SignalMessage};
use futures_util::StreamExt;
use native_shared::{
    NativeClientPeerFactory, NativePeer, RoleAction, read_msg, validate_advertised_addr, write_msg,
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
    let peer_factory = NativeClientPeerFactory::new(());

    let mut peer = peer_factory
        .create_peer((server_addr.clone(), args.udp_port))
        .await?;

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
