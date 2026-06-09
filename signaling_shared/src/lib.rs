use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignalMessage {
    Offer { sdp: String },
    Answer { sdp: String },
}

impl SignalMessage {
    pub fn sdp(&self) -> &String {
        match self {
            SignalMessage::Offer { sdp } => sdp,
            SignalMessage::Answer { sdp } => sdp,
        }
    }
}
