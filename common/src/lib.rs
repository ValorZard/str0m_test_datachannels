use futures::SinkExt;
use futures::channel::mpsc::{Recv, TrySendError, UnboundedReceiver, UnboundedSender};
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

#[derive(Debug, Clone)]
pub enum DataChannelMessage {
    Text(String),
    Binary(Vec<u8>),
}

// there isn't a super good way of doing cross-platform channel handles, mostly because str0m does its own thing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelRef {
    pub label: String,
    pub id_hint: Option<u16>,
}

#[derive(Debug, Clone)]
pub enum WebRTCNotification {
    ChannelOpen(ChannelRef),
    ChannelClose(ChannelRef),
}

pub type OutgoingDataChannelMessageSender = UnboundedSender<(ChannelRef, DataChannelMessage)>;

// The general idea here is that we can recv messages on one task/thread, and send messages on another task.
// so, we need to allow users to clone the sender so we can move it somewhere else.
#[derive(Debug)]
pub struct WebRTCCommunicationHandle {
    notification_receiver: UnboundedReceiver<WebRTCNotification>,
    incoming_datachannel_message_receiver: UnboundedReceiver<(ChannelRef, DataChannelMessage)>,
    outgoing_datachannel_message_sender: OutgoingDataChannelMessageSender,
}

impl WebRTCCommunicationHandle {
    pub fn new(
        notification_receiver: UnboundedReceiver<WebRTCNotification>,
        incoming_datachannel_message_receiver: UnboundedReceiver<(ChannelRef, DataChannelMessage)>,
        outgoing_datachannel_message_sender: UnboundedSender<(ChannelRef, DataChannelMessage)>,
    ) -> Self {
        Self {
            notification_receiver,
            incoming_datachannel_message_receiver,
            outgoing_datachannel_message_sender,
        }
    }

    // returns a future, must await or figure out something else
    pub fn recv_notification(&mut self) -> Recv<'_, UnboundedReceiver<WebRTCNotification>> {
        self.notification_receiver.recv()
    }

    // returns a future, must await or figure out something else
    pub fn recv_datachannel_message(
        &mut self,
    ) -> Recv<'_, UnboundedReceiver<(ChannelRef, DataChannelMessage)>> {
        self.incoming_datachannel_message_receiver.recv()
    }

    pub fn send_datachannel_message(
        &mut self,
        channel_ref: ChannelRef,
        data: DataChannelMessage,
    ) -> Result<(), TrySendError<(ChannelRef, DataChannelMessage)>> {
        self.outgoing_datachannel_message_sender
            .unbounded_send((channel_ref, data))
    }

    // clone out sender so we can move it out to its own thread or task or whatever
    pub fn clone_datachannel_message_sender(
        &self,
    ) -> UnboundedSender<(ChannelRef, DataChannelMessage)> {
        self.outgoing_datachannel_message_sender.clone()
    }
}
