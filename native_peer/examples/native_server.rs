use std::net::IpAddr;

use anyhow::Result;
use clap::Parser;
use datachannel_socket_native_peer::{NativeServerPeerFactory};

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

    let mut join_set: JoinSet<Result<_>> = JoinSet::new();
    while let Ok(mut peer) = factory.create_peer(args.advertise_ip, args.udp_port).await {
        join_set.spawn(async move {
            let (tx, _rx) = oneshot::channel::<()>();
            let (channel_id_db, mut incoming_datachannel_message_receiver, outgoing_datachannel_message_sender) = peer.get_communication_data()?;
            tokio::spawn(async move {
                if let Err(e) = peer.run("server", tx).await {
                    println!("Peer failed with error {e}");
                }
            });
            let mut echo_messages = Vec::new();
            for message in incoming_datachannel_message_receiver.recv().await {
                println!("Received incoming datachannel message: {:?}", message);
                echo_messages.push(message);
            }
            for message in echo_messages {
                outgoing_datachannel_message_sender.unbounded_send(message)?;
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
