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

#[allow(async_fn_in_trait)]
pub trait Peer {
    type Error;

    async fn create_offer(&mut self, channel_label: &str) -> Result<String, Self::Error>;

    async fn accept_offer(&mut self, sdp_offer: &str) -> Result<String, Self::Error>;

    async fn accept_answer(&mut self, sdp_answer: &str) -> Result<(), Self::Error>;
}
