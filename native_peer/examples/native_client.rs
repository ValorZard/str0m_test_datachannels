use anyhow::Result;
use clap::Parser;
use datachannel_socket_common::{DataChannelMessage, WebRTCNotification};
use datachannel_socket_native_peer::NativeClientPeerFactory;
use std::time::Duration;
use tokio::sync::oneshot;

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
    let peer_factory = NativeClientPeerFactory::new();

    let mut peer = peer_factory
        .create_peer(server_addr.clone(), args.udp_port)
        .await?;

    let (tx, rx) = oneshot::channel::<()>();
    let msg = args.message.as_bytes().to_vec();

    let mut communication_handle = peer.get_communication_handle()?;

    tokio::spawn(async move { peer.run("client", tx).await });

    // TODO: Find a better way to tell if the client has started running
    let mut channels = Vec::new();
    // TODO: just take one channel for now, figure out something better later
    while let Ok(notification) = communication_handle.recv_notification().await {
        if let WebRTCNotification::ChannelOpen(channel_ref) = notification {
            channels.push(channel_ref);
            break;
        }
    }

    for channel_ref in channels {
        let _ = communication_handle.send_datachannel_message(
            channel_ref.clone(),
            DataChannelMessage::Text("Hello from native client!".into()),
        );
        let _ = communication_handle.send_datachannel_message(
            channel_ref,
            DataChannelMessage::Binary("Hello from native client in binary!".into()),
        );
    }

    while let Ok((channel_ref, message)) = communication_handle.recv_datachannel_message().await {
        println!("From {channel_ref:?} Received incoming datachannel message: {message:?}");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_client(&args).await?;

    Ok(())
}
