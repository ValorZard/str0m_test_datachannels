use anyhow::Result;
use datachannel_socket_common::{DataChannelMessage, SignalMessage};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_util::{SinkExt, StreamExt};
use gloo_timers::future::TimeoutFuture;
use js_sys::Uint8Array;
use js_sys::futures::spawn_local;
use serde::{Deserialize, Serialize};
use std::cell::OnceCell;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Event, MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelEvent,
    RtcIceConnectionState, RtcIceGatheringState, RtcPeerConnection, RtcPeerConnectionIceEvent,
    RtcSdpType, RtcSessionDescriptionInit,
};
use ws_stream_wasm::{WsMessage, WsMeta};

#[macro_export]
macro_rules! peer_log {
    ($($arg:tt)*) => {
        web_sys::console::log_1(&format!($($arg)*).into());
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceCandidateMessage {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u16>,
}

// See: https://developer.mozilla.org/en-US/docs/Web/API/RTCDataChannel/id
type ChannelId = u16;

struct Inner {
    pc: RtcPeerConnection,
    data_channels: RefCell<HashMap<ChannelId, RtcDataChannel>>,
    pending_local_ice: RefCell<Vec<IceCandidateMessage>>,
    incoming_datachannel_message_sender: UnboundedSender<(ChannelId, DataChannelMessage)>,
    // we want to let client api take the receiver so clients can read what's come in
    incoming_datachannel_message_receiver:
        RefCell<Option<UnboundedReceiver<(ChannelId, DataChannelMessage)>>>,
    // we can clone this and give it to the user before the native peer starts running for real
    outgoing_datachannel_message_sender: UnboundedSender<(ChannelId, DataChannelMessage)>,
    outgoing_datachannel_message_receiver:
        RefCell<UnboundedReceiver<(ChannelId, DataChannelMessage)>>,

    on_ice_candidate: OnceCell<Closure<dyn FnMut(RtcPeerConnectionIceEvent)>>,
    on_ice_connection_state_change: OnceCell<Closure<dyn FnMut(Event)>>,
    on_data_channel: OnceCell<Closure<dyn FnMut(RtcDataChannelEvent)>>,
    on_data_channel_open: RefCell<Option<Closure<dyn FnMut(Event)>>>,
    on_data_channel_message: RefCell<Option<Closure<dyn FnMut(MessageEvent)>>>,
}

#[wasm_bindgen]
pub struct WasmPeer {
    inner: Rc<Inner>,
}

#[wasm_bindgen]
impl WasmPeer {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WasmPeer, JsValue> {
        let config = make_rtc_config();
        let pc = RtcPeerConnection::new_with_configuration(&config)?;

        let (incoming_datachannel_message_sender, incoming_datachannel_message_receiver) =
            unbounded();
        let (outgoing_datachannel_message_sender, outgoing_datachannel_message_receiver) =
            unbounded();

        let inner = Rc::new(Inner {
            pc,
            data_channels: RefCell::new(HashMap::new()),
            pending_local_ice: RefCell::new(Vec::new()),
            incoming_datachannel_message_sender,
            incoming_datachannel_message_receiver: RefCell::new(Some(
                incoming_datachannel_message_receiver,
            )),
            outgoing_datachannel_message_sender,
            outgoing_datachannel_message_receiver: RefCell::new(
                outgoing_datachannel_message_receiver,
            ),
            on_ice_candidate: OnceCell::new(),
            on_ice_connection_state_change: OnceCell::new(),
            on_data_channel: OnceCell::new(),
            on_data_channel_open: RefCell::new(None),
            on_data_channel_message: RefCell::new(None),
        });

        install_peer_handlers(&inner)?;

        Ok(WasmPeer { inner })
    }

    pub async fn create_offer(&mut self, channel_label: &str) -> Result<String, JsValue> {
        // first create an internal data channel to initalize the peer connection
        let dc = self.inner.pc.create_data_channel(channel_label);
        install_data_channel_handlers(&self.inner, dc)?;

        let offer_val = JsFuture::from(self.inner.pc.create_offer()).await?;
        let offer: RtcSessionDescriptionInit = offer_val.unchecked_into();

        JsFuture::from(self.inner.pc.set_local_description(&offer)).await?;

        // since we are connected to a public IP, we don't need to actually send ICE candidates,
        // but we do it to make firefox happy
        loop {
            peer_log!("{:?}", self.inner.pc.ice_gathering_state());
            if self.inner.pc.ice_gathering_state() == web_sys::RtcIceGatheringState::Complete {
                break;
            }
            TimeoutFuture::new(50).await;
        }

        let local = self
            .inner
            .pc
            .local_description()
            .ok_or_else(|| JsValue::from_str("missing local description"))?;
        peer_log!("local description after gathering: {:?}", local.sdp());
        Ok(local.sdp())
    }

    pub async fn accept_offer(&mut self, sdp_offer: &str) -> Result<String, JsValue> {
        let remote = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
        remote.set_sdp(sdp_offer);

        JsFuture::from(self.inner.pc.set_remote_description(&remote)).await?;

        let answer_val = JsFuture::from(self.inner.pc.create_answer()).await?;
        let answer: RtcSessionDescriptionInit = answer_val.unchecked_into();

        JsFuture::from(self.inner.pc.set_local_description(&answer)).await?;

        answer
            .get_sdp()
            .ok_or_else(|| JsValue::from_str("missing answer SDP"))
    }

    pub async fn accept_answer(&mut self, sdp_answer: &str) -> Result<(), JsValue> {
        let remote = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        remote.set_sdp(sdp_answer);

        JsFuture::from(self.inner.pc.set_remote_description(&remote)).await?;
        Ok(())
    }

    pub fn send_text(&self, channel_id: ChannelId, text: String) -> Result<(), JsValue> {
        self.inner
            .outgoing_datachannel_message_sender
            .unbounded_send((channel_id, DataChannelMessage::Text(text)))
            .map_err(|e| e.to_string().into())
    }

    pub fn send_bytes(&self, channel_id: ChannelId, bytes: Vec<u8>) -> Result<(), JsValue> {
        self.inner
            .outgoing_datachannel_message_sender
            .unbounded_send((channel_id, DataChannelMessage::Binary(bytes)))
            .map_err(|e| e.to_string().into())
    }

    pub fn ice_connection_state(&self) -> RtcIceConnectionState {
        self.inner.pc.ice_connection_state()
    }

    pub fn ice_gathering_state(&self) -> RtcIceGatheringState {
        self.inner.pc.ice_gathering_state()
    }

    pub fn close(&self) {
        self.inner.pc.close();
    }
}

impl WasmPeer {
    pub fn get_communication_channels(
        &mut self,
    ) -> Result<
        (
            UnboundedReceiver<(ChannelId, DataChannelMessage)>,
            UnboundedSender<(ChannelId, DataChannelMessage)>,
        ),
        std::io::Error,
    > {
        let incoming_datachannel_message_receiver = self
            .inner
            .incoming_datachannel_message_receiver
            .take()
            .ok_or(std::io::Error::new(
                ErrorKind::NotConnected,
                "RTC not connected",
            ))?;
        Ok((
            incoming_datachannel_message_receiver,
            self.inner.outgoing_datachannel_message_sender.clone(),
        ))
    }

    pub fn get_channel_ids(&self) -> Vec<ChannelId> {
        self.inner.data_channels.borrow().keys().cloned().collect()
    }
}

fn make_rtc_config() -> RtcConfiguration {
    // since we are directly connected to a public server with a public IP, we don't need a STUN server
    let config = RtcConfiguration::new();
    config
}

fn install_peer_handlers(inner: &Rc<Inner>) -> Result<(), JsValue> {
    let inner_for_ice = Rc::clone(inner);
    let on_ice = Closure::wrap(Box::new(move |e: RtcPeerConnectionIceEvent| {
        if let Some(candidate) = e.candidate() {
            let candidate_str = candidate.candidate();
            if candidate_str.trim().is_empty() {
                return;
            }

            peer_log!("local candidate: {:?}", candidate_str);
            // since we are just doing datachannels, we don't care about media, so no need for mid or mline
            inner_for_ice
                .pending_local_ice
                .borrow_mut()
                .push(IceCandidateMessage {
                    candidate: candidate_str,
                    sdp_mid: None,
                    sdp_mline_index: None,
                });
        }
    }) as Box<dyn FnMut(_)>);
    inner
        .pc
        .set_onicecandidate(Some(on_ice.as_ref().unchecked_ref()));
    inner
        .on_ice_candidate
        .set(on_ice)
        .expect("Should only be init once");

    let inner_for_state = Rc::clone(inner);
    let on_ice_state_change = Closure::wrap(Box::new(move |_e: Event| {
        let state = inner_for_state.pc.ice_connection_state();
        peer_log!("ICE connection state: {:?}", state);
    }) as Box<dyn FnMut(_)>);
    inner
        .pc
        .set_oniceconnectionstatechange(Some(on_ice_state_change.as_ref().unchecked_ref()));
    inner
        .on_ice_connection_state_change
        .set(on_ice_state_change)
        .expect("Should only be init once");

    let inner_for_dc = Rc::clone(inner);
    let on_data_channel = Closure::wrap(Box::new(move |e: RtcDataChannelEvent| {
        let dc = e.channel();
        if let Err(err) = install_data_channel_handlers(&inner_for_dc, dc.clone()) {
            web_sys::console::error_1(&JsValue::from_str(&format!(
                "failed to install data channel handlers: {:?}",
                err
            )));
            return;
        }
    }) as Box<dyn FnMut(_)>);
    inner
        .pc
        .set_ondatachannel(Some(on_data_channel.as_ref().unchecked_ref()));
    inner
        .on_data_channel
        .set(on_data_channel)
        .expect("Should only be init once");

    // sender pump to send messages to the datachannels
    let inner_for_datachannel_sender = Rc::clone(&inner);
    spawn_local(async move {
        while let Ok((channel_id, msg)) = inner_for_datachannel_sender
            .outgoing_datachannel_message_receiver
            .borrow_mut()
            .recv()
            .await
        {
            if let Some(dc) = inner_for_datachannel_sender
                .data_channels
                .borrow()
                .get(&channel_id)
            {
                match msg {
                    DataChannelMessage::Text(text) => {
                        let _ = dc.send_with_str(&text);
                    }
                    DataChannelMessage::Binary(bytes) => {
                        let _ = dc.send_with_u8_array(&bytes);
                    }
                }
            }
        }
    });

    Ok(())
}

fn install_data_channel_handlers(inner: &Rc<Inner>, dc: RtcDataChannel) -> Result<(), JsValue> {
    let inner_for_open = Rc::clone(inner);
    let dc_for_open = dc.clone();
    let on_open = Closure::wrap(Box::new(move |_e: Event| {
        // add data_channel to hashmap
        let dc_id = dc_for_open.id().expect("There should be an ID here.");
        inner_for_open
            .data_channels
            .borrow_mut()
            .insert(dc_id, dc_for_open.clone());
        web_sys::console::log_1(&"data channel open".into());
    }) as Box<dyn FnMut(_)>);
    dc.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    inner.on_data_channel_open.replace(Some(on_open));

    let inner_for_msg = Rc::clone(inner);
    let dc_for_msg = dc.clone();
    let on_message = Closure::wrap(Box::new(move |e: MessageEvent| {
        let dc_id = dc_for_msg.id().expect("There should be an ID here.");
        if let Some(text) = e.data().as_string() {
            let _ = inner_for_msg
                .incoming_datachannel_message_sender
                .unbounded_send((dc_id, DataChannelMessage::Text(text)));
            return;
        }

        if let Ok(buf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let arr = Uint8Array::new(&buf);
            let out = arr.to_vec();
            let _ = inner_for_msg
                .incoming_datachannel_message_sender
                .unbounded_send((dc_id, DataChannelMessage::Binary(out)));
            return;
        }
        web_sys::console::warn_1(&"received unsupported message type".into());
    }) as Box<dyn FnMut(_)>);
    dc.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    inner.on_data_channel_message.replace(Some(on_message));

    Ok(())
}

pub struct WasmPeerFactory {}

impl WasmPeerFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn create_peer(&self, server_address: String) -> Result<WasmPeer, JsValue> {
        let mut wasm_peer = WasmPeer::new()?;

        let (ws, wsio) = match WsMeta::connect(server_address.clone(), None).await {
            Ok(parts) => parts,
            Err(e) => {
                return Err(
                    format!("WebSocket connect failed for {}: {:?}", server_address, e).into(),
                );
            }
        };

        let (mut send_stream, mut recv_stream) = wsio.split();
        let offer_sdp = wasm_peer.create_offer("chat").await?;
        let signaling_message = SignalMessage::Offer { sdp: offer_sdp };
        let signaling_message = serde_json::to_string(&signaling_message).unwrap();
        let send_message = WsMessage::Text(signaling_message);
        let _ = send_stream.send(send_message).await;

        // now wait for message to send back answer
        loop {
            if let Some(WsMessage::Text(answer_string)) = recv_stream.next().await {
                let parsed_answer = serde_json::from_str::<SignalMessage>(&answer_string).unwrap();
                let answer_sdp = parsed_answer.sdp();
                peer_log!("received answer sdp: {:?}", answer_sdp);
                wasm_peer.accept_answer(answer_sdp.as_str()).await?;
                break;
            }
        }
        let _ = ws.close();
        peer_log!("we can close the websocket now, webrtc connection should be bootstrapped");
        Ok(wasm_peer)
    }
}
