use std::net::IpAddr;

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use common::{Peer, PeerFactory, SignalMessage};
use native_peer::{NativePeer, NativeServerPeerFactory, RoleAction};

use tokio::{net::TcpListener, sync::oneshot, task::JoinSet};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    bind_ip: IpAddr,

    #[arg(long, default_value = "127.0.0.1")]
    advertise_ip: IpAddr,

    #[arg(long, default_value_t = 7000)]
    signal_port: u16,

    #[arg(long, default_value_t = 5000)]
    udp_port: u16,
}

async fn run_server(args: Args) -> Result<()> {
    // HAS TO BE RUN BEFORE WEBRTC STUFF RUNS
    let listener = TcpListener::bind((args.bind_ip, args.signal_port)).await?;
    println!("server: signaling on {}:{}", args.bind_ip, args.signal_port);
    let factory = NativeServerPeerFactory::new(listener);

    let mut join_set = JoinSet::new();
    while let Ok(mut peer) = factory
        .create_peer((args.advertise_ip, args.udp_port))
        .await
    {
        join_set.spawn(async move {
            let (tx, _rx) = oneshot::channel::<Vec<u8>>();
            if let Err(e) = peer.run("server", RoleAction::EchoServer, tx).await {
                println!("Peer failed with error {e}");
            }
        });
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_server(args).await?;

    Ok(())
}
