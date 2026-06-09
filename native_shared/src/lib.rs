use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    io::{BufReader, Write},
    net::{IpAddr, TcpListener, TcpStream},
};
pub mod peer;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignalMessage {
    Offer { sdp: String },
    Answer { sdp: String },
}

pub fn write_msg(stream: &mut TcpStream, msg: &SignalMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    stream.write_all(json.as_bytes())?;
    stream.write_all(b"\n")?;
    Ok(())
}

pub fn read_msg(stream: TcpStream) -> Result<SignalMessage> {
    let reader = BufReader::new(stream);
    let mut de = Deserializer::from_reader(reader).into_iter::<SignalMessage>();
    de.next()
        .ok_or_else(|| anyhow!("no signal message"))?
        .map_err(Into::into)
}
