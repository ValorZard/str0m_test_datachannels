use std::net::IpAddr;

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use futures_util::StreamExt;
use native_shared::{
    install_str0m_process,
    peer::{Peer, RoleAction},
    read_msg, validate_advertised_addr, write_msg,
};
use signaling_shared::SignalMessage;

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
    install_str0m_process();
    let listener = TcpListener::bind((args.bind_ip, args.signal_port)).await?;
    println!("server: signaling on {}:{}", args.bind_ip, args.signal_port);

    let mut join_set = JoinSet::new();
    while let Ok((raw_stream, addr)) = listener.accept().await {
        // this is only really necessary if you are testing server and client on same machine
        let advertise_addr = validate_advertised_addr(args.advertise_ip, args.udp_port)
            .ok_or(anyhow!("Failed to generate address"))?;
        println!("Advertising server on '{advertise_addr}'");
        println!(
            "Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip."
        );
        if addr.ip().is_loopback() {
            eprintln!(
                "server: info: signaling peer is loopback ({addr}), advertising non-loopback ICE IP {advertise_addr} for browser compatibility"
            );
        }

        join_set.spawn(async move {
            let result: Result<()> = async {
                let mut peer = Peer::new(advertise_addr).await?;
                println!("server: UDP bound on {}", peer.bound_addr);
                println!("server: advertising ICE candidate {}", peer.advertised_addr);

                let ws_stream = match tokio_tungstenite::accept_async(raw_stream).await {
                    Ok(ws) => ws,
                    Err(err) => {
                        eprintln!("WebSocket handshake failed for {}: {:?}", addr, err);
                        return Ok(());
                    }
                };
                println!("server: signaling connected from {addr}");

                let (mut write_stream, mut read_stream) = ws_stream.split();

                let offer = read_msg(&mut read_stream).await?;
                let answer_sdp = match offer {
                    SignalMessage::Offer { sdp } => peer.accept_offer(&sdp)?,
                    _ => bail!("expected offer"),
                };

                write_msg(
                    &mut write_stream,
                    &SignalMessage::Answer { sdp: answer_sdp },
                )
                .await?;

                let (ice_sender, ice_receiver) = tokio::sync::mpsc::unbounded_channel::<String>();

                let (tx, _rx) = oneshot::channel::<Vec<u8>>();
                peer.run("server", RoleAction::EchoServer, tx).await?;
                Ok(())
            }
            .await;

            if let Err(err) = result {
                eprintln!("server: session {addr} ended with error: {err:#}");
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
