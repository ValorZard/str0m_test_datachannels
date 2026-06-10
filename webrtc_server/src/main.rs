use std::net::IpAddr;

use anyhow::{Result, bail};
use clap::Parser;
use futures_util::StreamExt;
use native_shared::{
    install_str0m_process, peer::{Peer, RoleAction}, read_msg, write_msg
};
use signaling_shared::SignalMessage;

use tokio::{net::TcpListener, sync::oneshot};

fn detect_primary_local_ip() -> Option<IpAddr> {
    // Discover the preferred outbound local interface without sending traffic.
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("1.1.1.1:80").ok()?;
    Some(socket.local_addr().ok()?.ip())
}

// Attempt to generate a non-loopback advertising ip for local testing
fn choose_advertise_ip(args: &Args) -> Option<IpAddr> {
    match args.advertise_ip {
        None => {
            if let Some(ip) = detect_primary_local_ip() {
                if !ip.is_loopback() {
                    return Some(ip);
                }
            }
            None
        }
        Some(ip) => {
            if ip.is_loopback() {
                if let Some(new_ip) = detect_primary_local_ip() {
                    if !new_ip.is_loopback() {
                        return Some(new_ip);
                    }
                }
            }
            Some(ip)
        }
    }
}

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
    // HAS TO BE RUN BEFORE WEBRTC STUFF RUNS
    install_str0m_process();
    let listener = TcpListener::bind((args.bind_ip, args.signal_port)).await?;
    println!("server: signaling on {}:{}", args.bind_ip, args.signal_port);
    // this is only really necessary if you are testing server and client on same machine
    let advertise_ip =
        choose_advertise_ip(args).expect("a proper advertising_address should be generated");
    println!("Advertising server on '{advertise_ip}'");
    println!(
        "Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip."
    );

    while let Ok((raw_stream, addr)) = listener.accept().await {
        if addr.ip().is_loopback() && !advertise_ip.is_loopback() {
            eprintln!(
                "server: info: signaling peer is loopback ({addr}), advertising non-loopback ICE IP {advertise_ip} for browser compatibility"
            );
        }

        let mut peer = Peer::new(advertise_ip, args.udp_port).await?;
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
