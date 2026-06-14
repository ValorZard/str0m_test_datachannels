use anyhow::{Context, Result};
use clap::Parser;
use datachannel_socket_common::{DataChannelMessage, WebRTCNotification};
use datachannel_socket_native_peer::{NativeClientPeerFactory, NativeServerPeerFactory};
use futures_util::{SinkExt, StreamExt};
use std::net::IpAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind_ip: IpAddr,

    #[arg(long, default_value = "127.0.0.1")]
    advertise_ip: IpAddr,

    #[arg(long, default_value_t = 7000)]
    signal_port: u16,

    #[arg(long, default_value_t = 5000)]
    server_udp_port: u16,

    #[arg(long, default_value_t = 5001)]
    client_udp_port: u16,

    #[arg(long, default_value_t = 7100)]
    ws_port: u16,

    #[arg(long, default_value_t = 200)]
    warmup_messages: usize,

    #[arg(long, default_value_t = 2_000)]
    message_amount: usize,

    #[arg(long, default_value = "Hello World")]
    message: String,

    #[arg(long, default_value_t = 5)]
    recv_timeout_secs: u64,
}

#[derive(Debug)]
struct BenchResult {
    name: &'static str,
    messages: usize,
    payload_bytes: usize,
    total_duration: Duration,
    rtt_us: Vec<u128>,
}

impl BenchResult {
    fn print(&self) {
        let secs = self.total_duration.as_secs_f64();
        let total_bytes = self.messages as f64 * self.payload_bytes as f64;
        let throughput_mib_s = if secs > 0.0 {
            (total_bytes / secs) / (1024.0 * 1024.0)
        } else {
            0.0
        };
        let msgs_s = if secs > 0.0 {
            self.messages as f64 / secs
        } else {
            0.0
        };

        let mut sorted = self.rtt_us.clone();
        sorted.sort_unstable();

        let p50 = percentile_us(&sorted, 0.50);
        let p95 = percentile_us(&sorted, 0.95);
        let p99 = percentile_us(&sorted, 0.99);
        let mean = if sorted.is_empty() {
            0.0
        } else {
            sorted.iter().sum::<u128>() as f64 / sorted.len() as f64
        };

        println!("\n=== {} ===", self.name);
        println!("messages           : {}", self.messages);
        println!("payload bytes      : {}", self.payload_bytes);
        println!("total duration     : {:.3}s", secs);
        println!("messages/sec       : {:.2}", msgs_s);
        println!("throughput MiB/sec : {:.3}", throughput_mib_s);
        println!("RTT mean (us)      : {:.1}", mean);
        println!("RTT p50  (us)      : {}", p50);
        println!("RTT p95  (us)      : {}", p95);
        println!("RTT p99  (us)      : {}", p99);
    }
}

fn percentile_us(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * pct).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

async fn run_webrtc_echo_server(
    bind_ip: IpAddr,
    advertise_ip: IpAddr,
    signal_port: u16,
    udp_port: u16,
    ready_tx: oneshot::Sender<()>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let listener = TcpListener::bind((bind_ip, signal_port)).await?;
    let factory = NativeServerPeerFactory::new(listener);

    let _ = ready_tx.send(());

    let mut peer = factory.create_peer(advertise_ip, udp_port).await?;
    let mut handle = peer.get_communication_handle()?;

    let (done_tx, _done_rx) = oneshot::channel::<()>();
    let peer_task = tokio::spawn(async move {
        if let Err(e) = peer.run("bench-webrtc-server", done_tx).await {
            eprintln!("webrtc server peer failed: {e}");
        }
    });

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            msg = handle.recv_datachannel_message() => {
                match msg {
                    Ok((channel_ref, message)) => {
                        let _ = handle.send_datachannel_message(channel_ref, message);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }
    }

    peer_task.abort();

    Ok(())
}

async fn run_ws_echo_server(
    bind_ip: IpAddr,
    ws_port: u16,
    ready_tx: oneshot::Sender<()>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let listener = TcpListener::bind((bind_ip, ws_port)).await?;
    let _ = ready_tx.send(());

    let (stream, _) = listener.accept().await?;
    run_ws_echo_connection(stream, &mut shutdown_rx).await
}

async fn run_ws_echo_connection(
    stream: TcpStream,
    shutdown_rx: &mut oneshot::Receiver<()>,
) -> Result<()> {
    let mut ws = accept_async(stream).await?;

    loop {
        tokio::select! {
            _ = &mut *shutdown_rx => {
                break;
            }
            next_item = ws.next() => {
                let Some(item) = next_item else {
                    break;
                };
                let msg = item?;
                match msg {
                    Message::Binary(payload) => {
                        ws.send(Message::Binary(payload)).await?;
                    }
                    Message::Text(text) => {
                        ws.send(Message::Text(text)).await?;
                    }
                    Message::Ping(payload) => {
                        ws.send(Message::Pong(payload)).await?;
                    }
                    Message::Close(_) => {
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn benchmark_webrtc(args: &Args) -> Result<BenchResult> {
    let server_addr = format!("ws://{}:{}", args.bind_ip, args.signal_port);
    let factory = NativeClientPeerFactory::new();

    let mut peer = factory
        .create_peer(server_addr, args.client_udp_port)
        .await
        .context("failed creating webrtc client peer")?;

    let mut handle = peer.get_communication_handle()?;

    let (done_tx, _done_rx) = oneshot::channel::<()>();
    let client_task = tokio::spawn(async move {
        if let Err(e) = peer.run("bench-webrtc-client", done_tx).await {
            eprintln!("webrtc client peer failed: {e}");
        }
    });

    let channel_ref = loop {
        match timeout(
            Duration::from_secs(args.recv_timeout_secs),
            handle.recv_notification(),
        )
        .await
        {
            Ok(Ok(WebRTCNotification::ChannelOpen(channel_ref))) => break channel_ref,
            Ok(Ok(_)) => continue,
            Ok(Err(_)) => anyhow::bail!("webrtc notification stream closed before channel opened"),
            Err(_) => anyhow::bail!("timed out waiting for webrtc channel open"),
        }
    };

    let payload = args.message.clone().into_bytes();
    for i in 0..args.warmup_messages {
        handle.send_datachannel_message(channel_ref.clone(), DataChannelMessage::Binary(payload.clone()))?;
        let _ = timeout(
            Duration::from_secs(args.recv_timeout_secs),
            handle.recv_datachannel_message(),
        )
        .await
        .context("timeout during webrtc warmup")??;
    }

    let mut rtt_us = Vec::with_capacity(args.message_amount);
    let start = Instant::now();

    for i in 0..args.message_amount {
        let t0 = Instant::now();

        handle.send_datachannel_message(channel_ref.clone(), DataChannelMessage::Binary(payload.clone()))?;

        let (_, echo_msg) = timeout(
            Duration::from_secs(args.recv_timeout_secs),
            handle.recv_datachannel_message(),
        )
        .await
        .context("timeout while waiting for webrtc echo")??;

        let got = match echo_msg {
            DataChannelMessage::Binary(bytes) => bytes,
            DataChannelMessage::Text(text) => text.into_bytes(),
        };

        if got != payload {
            anyhow::bail!("unexpected message received!");
        }

        rtt_us.push(t0.elapsed().as_micros());
    }

    client_task.abort();

    Ok(BenchResult {
        name: "datachannel-socket (str0m)",
        messages: args.message_amount,
        payload_bytes: payload.len(),
        total_duration: start.elapsed(),
        rtt_us,
    })
}

async fn benchmark_ws(args: &Args) -> Result<BenchResult> {
    let url = format!("ws://{}:{}", args.bind_ip, args.ws_port);
    let (mut ws, _) = connect_async(url).await?;

    let payload = args.message.clone().into_bytes();

    for i in 0..args.warmup_messages {
        ws.send(Message::Binary(payload.clone().into())).await?;
        let _ = timeout(Duration::from_secs(args.recv_timeout_secs), ws.next())
            .await
            .context("timeout during ws warmup")?
            .ok_or_else(|| anyhow::anyhow!("ws stream closed during warmup"))??;
    }

    let mut rtt_us = Vec::with_capacity(args.message_amount);
    let start = Instant::now();

    for i in 0..args.message_amount {
        let t0 = Instant::now();

        ws.send(Message::Binary(payload.clone().into())).await?;

        let msg = timeout(Duration::from_secs(args.recv_timeout_secs), ws.next())
            .await
            .context("timeout while waiting for ws echo")?
            .ok_or_else(|| anyhow::anyhow!("ws stream closed while benchmarking"))??;

        let got = match msg {
            Message::Binary(bytes) => bytes.to_vec(),
            Message::Text(text) => text.bytes().collect(),
            other => anyhow::bail!("unexpected ws message during benchmark: {other:?}"),
        };

        if got != payload {
            anyhow::bail!("unexpected message received!");
        }

        rtt_us.push(t0.elapsed().as_micros());
    }

    let _ = ws.send(Message::Close(None)).await;

    Ok(BenchResult {
        name: "tokio-tungstenite",
        messages: args.message_amount,
        payload_bytes: payload.len(),
        total_duration: start.elapsed(),
        rtt_us,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    console_subscriber::init();
    let args = Args::parse();

    println!(
        "running benchmark: message_amount={}, payload={}, warmup_messages={}",
        args.message_amount,
        args.message,
        args.warmup_messages
    );

    println!("\nphase 1/2: starting WebRTC server...");
    let (webrtc_ready_tx, webrtc_ready_rx) = oneshot::channel();
    let (webrtc_shutdown_tx, webrtc_shutdown_rx) = oneshot::channel();
    let webrtc_server_task = tokio::spawn(run_webrtc_echo_server(
        args.bind_ip,
        args.advertise_ip,
        args.signal_port,
        args.server_udp_port,
        webrtc_ready_tx,
        webrtc_shutdown_rx,
    ));
    let _ = webrtc_ready_rx.await;

    println!("phase 1/2: running WebRTC benchmark...");
    let webrtc = benchmark_webrtc(&args).await?;

    println!("phase 1/2: shutting down WebRTC server/clients...");
    let _ = webrtc_shutdown_tx.send(());
    let _ = webrtc_server_task.await;

    println!("\nphase 2/2: starting WebSocket server...");
    let (ws_ready_tx, ws_ready_rx) = oneshot::channel();
    let (ws_shutdown_tx, ws_shutdown_rx) = oneshot::channel();
    let ws_server_task = tokio::spawn(run_ws_echo_server(
        args.bind_ip,
        args.ws_port,
        ws_ready_tx,
        ws_shutdown_rx,
    ));
    let _ = ws_ready_rx.await;

    println!("phase 2/2: running WebSocket benchmark...");
    let websocket = benchmark_ws(&args).await?;

    println!("phase 2/2: shutting down WebSocket server/clients...");
    let _ = ws_shutdown_tx.send(());
    let _ = ws_server_task.await;

    webrtc.print();
    websocket.print();

    println!("\nrelative comparison (higher is better):");
    let ws_msgs_s = websocket.messages as f64 / websocket.total_duration.as_secs_f64();
    let webrtc_msgs_s = webrtc.messages as f64 / webrtc.total_duration.as_secs_f64();
    println!("datachannel / websocket messages/sec ratio: {:.3}", webrtc_msgs_s / ws_msgs_s);

    Ok(())
}
