use std::net::IpAddr;

use anyhow::Result;
use clap::Parser;
use datachannel_socket_native_peer::NativeServerPeerFactory;

use tokio::{net::TcpListener, sync::oneshot, task::JoinSet};
use datachannel_socket_common::DataChannelMessage;

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

    let mut join_set: JoinSet<Result<_>> = JoinSet::new();
    while let Ok(mut peer) = factory.create_peer(args.advertise_ip, args.udp_port).await {
        join_set.spawn(async move {
            let (tx, _rx) = oneshot::channel::<()>();
            let mut communication_handle = peer.get_communication_handle()?;
            tokio::spawn(async move {
                if let Err(e) = peer.run("server", tx).await {
                    println!("Peer failed with error {e}");
                }
            });
            while let Ok((channel_ref, message)) = communication_handle.recv_datachannel_message().await {
                println!("From {channel_ref:?} Received incoming datachannel message: {message:?}");
                let _ = communication_handle.send_datachannel_message(channel_ref,  message);
            }
            Ok(())
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
