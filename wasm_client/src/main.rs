use anyhow::Result;
use common::{Peer, PeerFactory, SignalMessage};
use futures_util::{SinkExt, StreamExt};
use gloo_timers::future::TimeoutFuture;
use js_sys::{Array, Uint8Array};
use serde::{Deserialize, Serialize};
use std::cell::OnceCell;
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{
    Event, HtmlElement, HtmlInputElement, MessageEvent, RtcConfiguration, RtcDataChannel,
    RtcDataChannelEvent, RtcIceCandidate, RtcIceCandidateInit, RtcIceConnectionState,
    RtcIceGatheringState, RtcIceServer, RtcPeerConnection, RtcPeerConnectionIceEvent, RtcSdpType,
    RtcSessionDescriptionInit,
};
use ws_stream_wasm::{WsMessage, WsMeta};

#[macro_export]
macro_rules! log {
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

struct Inner {
    pc: RtcPeerConnection,
    data_channel: RefCell<Option<RtcDataChannel>>,
    is_data_channel_open: RefCell<bool>,
    pending_local_ice: RefCell<Vec<IceCandidateMessage>>,
    received_messages: RefCell<Vec<Vec<u8>>>,

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

        let inner = Rc::new(Inner {
            pc,
            data_channel: RefCell::new(None),
            is_data_channel_open: RefCell::new(false),
            pending_local_ice: RefCell::new(Vec::new()),
            received_messages: RefCell::new(Vec::new()),
            on_ice_candidate: OnceCell::new(),
            on_ice_connection_state_change: OnceCell::new(),
            on_data_channel: OnceCell::new(),
            on_data_channel_open: RefCell::new(None),
            on_data_channel_message: RefCell::new(None),
        });

        install_peer_handlers(&inner)?;

        Ok(WasmPeer { inner })
    }

    pub async fn add_ice_candidate(&self, candidate: String) -> Result<(), JsValue> {
        let init = RtcIceCandidateInit::new(&candidate);
        let candidate = RtcIceCandidate::new(&init)?;
        JsFuture::from(
            self.inner
                .pc
                .add_ice_candidate_with_opt_rtc_ice_candidate(Some(&candidate)),
        )
        .await?;

        Ok(())
    }

    pub fn send_text(&self, text: String) -> Result<(), JsValue> {
        if *self.inner.is_data_channel_open.borrow() {
            let dc = self
                .inner
                .data_channel
                .borrow()
                .as_ref()
                .cloned()
                .ok_or_else(|| JsValue::from_str("data channel not available"))?;

            return dc.send_with_str(&text);
        }
        Err(JsValue::from_str("Data channel hasn't initialized yet!"))
    }

    pub fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), JsValue> {
        if *self.inner.is_data_channel_open.borrow() {
            let dc = self
                .inner
                .data_channel
                .borrow()
                .as_ref()
                .cloned()
                .ok_or_else(|| JsValue::from_str("data channel not available"))?;

            return dc.send_with_u8_array(&bytes);
        }
        Err(JsValue::from_str("Data channel hasn't initialized yet!"))
    }

    pub fn take_received_messages(&self) -> Result<String, JsValue> {
        let mut msgs = self.inner.received_messages.borrow_mut();
        let rendered: Vec<String> = msgs
            .iter()
            .map(|m| String::from_utf8_lossy(m).to_string())
            .collect();

        let out = serde_json::to_string(&rendered)
            .map_err(|e| JsValue::from_str(&format!("serialize received messages: {e}")))?;
        msgs.clear();
        Ok(out)
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

impl Peer for WasmPeer {
    type Error = JsValue;

    async fn create_offer(&mut self, channel_label: &str) -> Result<String, JsValue> {
        // first create an internal data channel to initalize the peer connection
        let dc = self.inner.pc.create_data_channel(channel_label);
        install_data_channel_handlers(&self.inner, &dc)?;
        self.inner.data_channel.replace(Some(dc));

        let offer_val = JsFuture::from(self.inner.pc.create_offer()).await?;
        let offer: RtcSessionDescriptionInit = offer_val.unchecked_into();

        JsFuture::from(self.inner.pc.set_local_description(&offer)).await?;

        // since we are connected to a public IP, we don't need to actually send ICE candidates,
        // but we do it to make firefox happy
        loop {
            log!("{:?}", self.inner.pc.ice_gathering_state());
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
        log!("local description after gathering: {:?}", local.sdp());
        Ok(local.sdp())
    }

    async fn accept_offer(&mut self, sdp_offer: &str) -> Result<String, JsValue> {
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

    async fn accept_answer(&mut self, sdp_answer: &str) -> Result<(), JsValue> {
        let remote = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        remote.set_sdp(sdp_answer);

        JsFuture::from(self.inner.pc.set_remote_description(&remote)).await?;
        Ok(())
    }
}

fn make_rtc_config() -> RtcConfiguration {
    let stun = RtcIceServer::new();
    stun.set_urls(&JsValue::from_str("stun:stun.l.google.com:19302"));

    let servers = Array::new();
    servers.push(&stun);

    let config = RtcConfiguration::new();
    config.set_ice_servers(&servers);
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

            log!("local candidate: {:?}", candidate_str);
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
        log!("ICE connection state: {:?}", state);
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
        if let Err(err) = install_data_channel_handlers(&inner_for_dc, &dc) {
            web_sys::console::error_1(&JsValue::from_str(&format!(
                "failed to install data channel handlers: {:?}",
                err
            )));
            return;
        }
        inner_for_dc.data_channel.replace(Some(dc));
    }) as Box<dyn FnMut(_)>);
    inner
        .pc
        .set_ondatachannel(Some(on_data_channel.as_ref().unchecked_ref()));
    inner
        .on_data_channel
        .set(on_data_channel)
        .expect("Should only be init once");

    Ok(())
}

fn install_data_channel_handlers(inner: &Rc<Inner>, dc: &RtcDataChannel) -> Result<(), JsValue> {
    let inner_for_open = Rc::clone(inner);
    let on_open = Closure::wrap(Box::new(move |_e: Event| {
        *inner_for_open.is_data_channel_open.borrow_mut() = true;
        web_sys::console::log_1(&"data channel open".into());
    }) as Box<dyn FnMut(_)>);
    dc.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    inner.on_data_channel_open.replace(Some(on_open));

    let inner_for_msg = Rc::clone(inner);
    let on_message = Closure::wrap(Box::new(move |e: MessageEvent| {
        if let Some(text) = e.data().as_string() {
            inner_for_msg
                .received_messages
                .borrow_mut()
                .push(text.into_bytes());
            return;
        }

        if let Ok(buf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let arr = Uint8Array::new(&buf);
            let mut out = vec![0u8; arr.length() as usize];
            arr.copy_to(&mut out);
            inner_for_msg.received_messages.borrow_mut().push(out);
            return;
        }

        web_sys::console::warn_1(&"received unsupported message type".into());
    }) as Box<dyn FnMut(_)>);
    dc.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    inner.on_data_channel_message.replace(Some(on_message));

    Ok(())
}

pub fn drain_local_ice_candidates(peer: Rc<WasmPeer>) -> Vec<IceCandidateMessage> {
    peer.inner
        .pending_local_ice
        .borrow_mut()
        .drain(..)
        .collect()
}

struct WasmPeerFactory {}

impl PeerFactory for WasmPeerFactory {
    type Error = JsValue;

    type PeerType = WasmPeer;

    type CreateArgs = ();

    fn new() -> Self {
        Self {}
    }

    async fn create_peer(&self, _: Self::CreateArgs) -> Result<WasmPeer, Self::Error> {
        WasmPeer::new()
    }
}

fn connect_to_server(server_address: String) {
    spawn_local(async move {
        let factory = WasmPeerFactory::new();
        let mut wasm_peer = factory.create_peer(()).await.expect("should work");

        let (ws, wsio) = match WsMeta::connect(server_address.clone(), None).await {
            Ok(parts) => parts,
            Err(e) => {
                log!("WebSocket connect failed for {}: {:?}", server_address, e);
                return;
            }
        };

        let (mut send_stream, mut recv_stream) = wsio.split();
        let offer_sdp = wasm_peer.create_offer("chat").await.unwrap();
        let signaling_message = SignalMessage::Offer { sdp: offer_sdp };
        let signaling_message = serde_json::to_string(&signaling_message).unwrap();
        let send_message = WsMessage::Text(signaling_message);
        let _ = send_stream.send(send_message).await;

        // now wait for message to send back answer
        loop {
            if let Some(WsMessage::Text(answer_string)) = recv_stream.next().await {
                let parsed_answer = serde_json::from_str::<SignalMessage>(&answer_string).unwrap();
                let answer_sdp = parsed_answer.sdp();
                log!("received answer sdp: {:?}", answer_sdp);
                wasm_peer.accept_answer(answer_sdp.as_str()).await.unwrap();
                break;
            }
        }

        drop(send_stream);
        drop(recv_stream);
        let _ = ws.close();
        log!("we can close the websocket now, webrtc connection should be bootstrapped");

        // send until its open
        loop {
            if let Ok(_) = wasm_peer.send_text("Hello from WASM!".to_string()) {
                log!("Success");
                break;
            }
            TimeoutFuture::new(50).await;
        }

        // read messages coming in
        loop {
            if let Ok(message) = wasm_peer.take_received_messages() {
                log!("Message from data channel: {message}");
            }
            TimeoutFuture::new(50).await;
        }
    });
}
fn main() {
    console_error_panic_hook::set_once();

    let window = web_sys::window().expect("no global `window` exists");
    let document = window.document().expect("should have a document on window");
    let _body = document.body().expect("document should have a body");

    let server_searchbox = document
        .get_element_by_id("server-searchbox")
        .expect("should be here");

    let a = Closure::<dyn FnMut()>::new(move || {
        let server_searchbox: &HtmlInputElement = server_searchbox.dyn_ref().unwrap();
        connect_to_server(server_searchbox.value());
    });
    document
        .get_element_by_id("server-button")
        .expect("should be here")
        .dyn_ref::<HtmlElement>()
        .unwrap()
        .set_onclick(Some(a.as_ref().unchecked_ref()));
    a.forget();
}
