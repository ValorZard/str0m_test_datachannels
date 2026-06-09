use std::net::IpAddr;

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use futures_util::StreamExt;
use native_shared::{
    peer::{Peer, RoleAction},
    read_msg, write_msg,
};
use signaling_shared::SignalMessage;

use tokio::{net::TcpListener, sync::oneshot};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    bind_ip: IpAddr,

    #[arg(long)]
    advertise_ip: Option<IpAddr>,

    #[arg(long, default_value_t = 7000)]
    signal_port: u16,

    #[arg(long, default_value_t = 5000)]
    udp_port: u16,
}

async fn run_server(args: &Args) -> Result<()> {
    let advertise_ip = args
        .advertise_ip
        .ok_or_else(|| anyhow!("--advertise-ip is required in server mode"))?;

    let mut peer = Peer::new(args.bind_ip, advertise_ip, args.udp_port).await?;
    println!("server: UDP bound on {}", peer.bound_addr);
    println!("server: advertising ICE candidate {}", peer.advertised_addr);

    let listener = TcpListener::bind((args.bind_ip, args.signal_port)).await?;
    println!("server: signaling on {}:{}", args.bind_ip, args.signal_port);

    while let Ok((raw_stream, addr)) = listener.accept().await {
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

        let (ice_tx, ice_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            loop {
                let msg = match read_msg(&mut read_stream).await {
                    Ok(m) => m,
                    Err(err) => {
                        eprintln!("server: signaling read ended: {err}");
                        break;
                    }
                };

                match msg {
                    SignalMessage::IceCandidate { candidate } => {
                        if ice_tx.send(candidate).is_err() {
                            break;
                        }
                    }
                    SignalMessage::Offer { .. } | SignalMessage::Answer { .. } => {
                        eprintln!("server: unexpected signaling message after answer");
                    }
                }
            }
        });

        let (tx, _rx) = oneshot::channel::<Vec<u8>>();
        peer.run("server", RoleAction::EchoServer, Some(ice_rx), tx)
            .await?
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    run_server(&args).await?;

    Ok(())
}
