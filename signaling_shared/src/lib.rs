use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignalMessage {
    Offer { sdp: String },
    Answer { sdp: String },
    IceCandidate { candidate: String},
}

impl SignalMessage {
    pub fn sdp(&self) -> Option<&String> {
        match self {
            Self::Offer { sdp } => Some(sdp),
            Self::Answer { sdp } => Some(sdp),
            Self::IceCandidate {..} => None,
        }
    }
}
