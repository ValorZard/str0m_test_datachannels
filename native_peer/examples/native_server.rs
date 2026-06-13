use anyhow::Result;
use clap::Parser;
use datachannel_socket_native_peer::{NativePeer, NativeServerPeerFactory};
use std::net::IpAddr;
use std::sync::Arc;

use datachannel_socket_common::{
    DataChannelMessage, OutgoingDataChannelMessageSender, WebRTCCommunicationHandle,
};
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
    let peer_senders = Arc::new(tokio::sync::Mutex::new(Vec::<
        OutgoingDataChannelMessageSender,
    >::new()));
    while let Ok(mut peer) = factory.create_peer(args.advertise_ip, args.udp_port).await {
        let mut peers_lock = peer_senders.lock().await;
        let mut peer_communication_handle = peer.get_communication_handle()?;
        peers_lock.push(peer_communication_handle.clone_datachannel_message_sender());
        drop(peers_lock);
        let peer_senders = peer_senders.clone();
        join_set.spawn(async move {
            let (tx, _rx) = oneshot::channel::<()>();
            tokio::spawn(async move {
                if let Err(e) = peer.run("server", tx).await {
                    println!("Peer failed with error {e}");
                }
            });
            while let Ok((channel_ref, message)) =
                peer_communication_handle.recv_datachannel_message().await
            {
                println!("From {channel_ref:?} Received incoming datachannel message: {message:?}");
                let peer_senders = peer_senders.lock().await;
                for sender in peer_senders.iter() {
                    // TODO: Add channel id safety here, or some way to get the list of channels available in CommunicationHandle
                    // we can just send to another channel with the exact same ref in another peer, but watch out!
                    // this could totally break, so honestly we need a better of sending messages safely
                    let _ = sender.unbounded_send((channel_ref.clone(), message.clone()));
                }
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
