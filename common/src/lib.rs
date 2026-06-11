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

    fn create_offer(
        &mut self,
        channel_label: &str,
    ) -> impl Future<Output = Result<String, Self::Error>>;

    fn accept_offer(
        &mut self,
        sdp_offer: &str,
    ) -> impl Future<Output = Result<String, Self::Error>>;

    fn accept_answer(&mut self, sdp_answer: &str) -> impl Future<Output = Result<(), Self::Error>>;
}

// WARNING: You can ONLY create one of these things in the entire lifetime of the program.
// This is because this needs to setup str0m crypto and some other stuff.
// If you create another one of these, it will error since you can't set it up again.
pub trait PeerFactory {
    type Error;
    type PeerType: Peer<Error = Self::Error>;
    type FactoryArgs;
    type CreateArgs;

    fn new(args: Self::FactoryArgs) -> Self;
    fn create_peer(
        &self,
        args: Self::CreateArgs,
    ) -> impl Future<Output = Result<Self::PeerType, Self::Error>>;
}
