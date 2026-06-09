use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{TcpStream, tcp::{ReadHalf, WriteHalf}}};
pub mod peer;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignalMessage {
    Offer { sdp: String },
    Answer { sdp: String },
}

pub async fn write_msg(stream: &mut WriteHalf<'_>, msg: &SignalMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    stream.write_all(json.as_bytes()).await?;
    stream.write_all(b"\n").await?;
    Ok(())
}

pub async fn read_msg(stream: &mut ReadHalf<'_>) -> Result<SignalMessage> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Err(anyhow!("no signal message"));
    }

    let msg = serde_json::from_str::<SignalMessage>(line.trim_end())?;
    Ok(msg)
}