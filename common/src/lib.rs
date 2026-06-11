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
            Self::Offer { sdp } => sdp,
            Self::Answer { sdp } => sdp,
        }
    }
}

pub trait Peer {
    type Error;

    fn create_offer(&mut self, channel_label: &str) -> impl Future<Output = Result<String, Self::Error>>;

    fn accept_offer(&mut self, sdp_offer: &str) -> impl Future<Output = Result<String, Self::Error>>;

    fn accept_answer(&mut self, sdp_answer: &str) -> impl Future<Output = Result<(), Self::Error>>;
}
