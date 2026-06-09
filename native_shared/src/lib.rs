use anyhow::{Result, anyhow};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use signaling_shared::SignalMessage;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{WebSocketStream, tungstenite};
pub mod peer;

pub async fn write_msg<S>(
    sink: &mut SplitSink<WebSocketStream<S>, tungstenite::Message>,
    msg: &SignalMessage,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let json = serde_json::to_string(msg)?;
    let send_message = tungstenite::Message::Text(json.into());
    sink.send(send_message).await?;
    Ok(())
}

pub async fn read_msg<S>(stream: &mut SplitStream<WebSocketStream<S>>) -> Result<SignalMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let msg = stream
            .next()
            .await
            .ok_or_else(|| anyhow!("no signal message"))??;

        match msg {
            tungstenite::Message::Text(text) => {
                let parsed = serde_json::from_str::<SignalMessage>(&text)?;
                return Ok(parsed);
            }
            tungstenite::Message::Binary(bytes) => {
                let parsed = serde_json::from_slice::<SignalMessage>(&bytes)?;
                return Ok(parsed);
            }
            tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {
                continue;
            }
            tungstenite::Message::Close(_) => {
                return Err(anyhow!("websocket closed"));
            }
            _ => {
                continue;
            }
        }
    }
}
