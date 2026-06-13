use std::time::Duration;
use anyhow::Result;
use clap::Parser;
use datachannel_socket_native_peer::{NativeClientPeerFactory};
use tokio::sync::oneshot;
use datachannel_socket_common::DataChannelMessage;

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

    let (channel_id_db, mut incoming_datachannel_message_receiver, outgoing_datachannel_message_sender) = peer.get_communication_data()?;

    tokio::spawn(async move {
        peer.run("client",  tx)
            .await
    });

    // TODO: Find a better way to tell if the client has started running
    tokio::time::sleep(Duration::from_secs(1)).await;

    for channel in channel_id_db.get_all().await {
        outgoing_datachannel_message_sender.unbounded_send((channel, DataChannelMessage::Text("Hello from native client!".into())))?;
    }

    for message in incoming_datachannel_message_receiver.recv().await {
        println!("Received incoming datachannel message: {:?}", message);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_client(&args).await?;

    Ok(())
}
