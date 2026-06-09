use anyhow::{Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use gloo_timers::future::TimeoutFuture;
use js_sys::{Array, Uint8Array};
use serde::{Deserialize, Serialize};
use signaling_shared::SignalMessage;
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{
    Element, Event, HtmlButtonElement, HtmlElement, HtmlInputElement, MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcIceCandidate, RtcIceCandidateInit, RtcIceConnectionState, RtcIceGatheringState, RtcIceServer, RtcPeerConnection, RtcPeerConnectionIceEvent, RtcSdpType, RtcSessionDescriptionInit
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
    data_channel_open: RefCell<bool>,
    pending_local_ice: RefCell<Vec<IceCandidateMessage>>,
    received_messages: RefCell<Vec<Vec<u8>>>,

    on_ice_candidate: RefCell<Option<Closure<dyn FnMut(RtcPeerConnectionIceEvent)>>>,
    on_data_channel: RefCell<Option<Closure<dyn FnMut(RtcDataChannelEvent)>>>,
    on_open: RefCell<Option<Closure<dyn FnMut(Event)>>>,
    on_message: RefCell<Option<Closure<dyn FnMut(MessageEvent)>>>,
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
            data_channel_open: RefCell::new(false),
            pending_local_ice: RefCell::new(Vec::new()),
            received_messages: RefCell::new(Vec::new()),
            on_ice_candidate: RefCell::new(None),
            on_data_channel: RefCell::new(None),
            on_open: RefCell::new(None),
            on_message: RefCell::new(None),
        });

        install_peer_handlers(&inner)?;

        Ok(WasmPeer { inner })
    }

    pub async fn create_offer(&self) -> Option<String> {
        // first create an internal data channel to initalize the peer connection
        let dc = self.inner.pc.create_data_channel("test");
        install_data_channel_handlers(&self.inner, &dc).ok()?;
        self.inner.data_channel.replace(Some(dc));

        let offer_val = JsFuture::from(self.inner.pc.create_offer()).await.ok()?;
        let offer: RtcSessionDescriptionInit = offer_val.unchecked_into();

        JsFuture::from(self.inner.pc.set_local_description(&offer))
            .await
            .ok()?;

        let local = self.inner.pc.local_description()?;
        log!("local description after gathering: {:?}", local.sdp());
        Some(local.sdp())
    }

    pub async fn accept_offer(&self, sdp_offer: String) -> Option<String> {
        let remote = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
        remote.set_sdp(&sdp_offer);

        JsFuture::from(self.inner.pc.set_remote_description(&remote))
            .await
            .unwrap();

        let answer_val = JsFuture::from(self.inner.pc.create_answer()).await.unwrap();
        let answer: RtcSessionDescriptionInit = answer_val.dyn_into().unwrap();

        JsFuture::from(self.inner.pc.set_local_description(&answer))
            .await
            .unwrap();

        answer.get_sdp()
    }

    pub async fn accept_answer(&self, sdp_answer: String) -> Result<(), JsValue> {
        let remote = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        remote.set_sdp(&sdp_answer);

        JsFuture::from(self.inner.pc.set_remote_description(&remote)).await?;
        Ok(())
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
        if *self.inner.data_channel_open.borrow() {
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
        if *self.inner.data_channel_open.borrow() {
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

fn make_rtc_config() -> RtcConfiguration {
    let mut stun = RtcIceServer::new();
    stun.set_urls(&JsValue::from_str("stun:stun.l.google.com:19302"));

    let servers = Array::new();
    servers.push(&stun);

    let mut config = RtcConfiguration::new();
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

            log!("{:?}", candidate);
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
    inner.on_ice_candidate.replace(Some(on_ice));

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
    inner.on_data_channel.replace(Some(on_data_channel));

    Ok(())
}

fn install_data_channel_handlers(inner: &Rc<Inner>, dc: &RtcDataChannel) -> Result<(), JsValue> {
    let inner_for_open = Rc::clone(inner);
    let on_open = Closure::wrap(Box::new(move |_e: Event| {
        *inner_for_open.data_channel_open.borrow_mut() = true;
        web_sys::console::log_1(&"data channel open".into());
    }) as Box<dyn FnMut(_)>);
    dc.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    inner.on_open.replace(Some(on_open));

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
    inner.on_message.replace(Some(on_message));

    Ok(())
}

pub fn drain_local_ice_candidates(peer: Rc<WasmPeer>) -> Vec<IceCandidateMessage> {
    peer.inner
        .pending_local_ice
        .borrow_mut()
        .drain(..)
        .collect()
}
// TODO: Make this not hardcoded at some point
const SERVER_ADDRESS: &str = "ws://127.0.0.1:7000";

fn connect_to_server(server_address: String) {
    spawn_local(async move {
        let wasm_peer = Rc::new(WasmPeer::new().expect("should work"));

        let (_ws, wsio) = match WsMeta::connect(server_address.clone(), None).await {
            Ok(parts) => parts,
            Err(e) => {
                log!("WebSocket connect failed for {}: {:?}", server_address, e);
                return;
            }
        };

        let (send_stream, mut recv_stream) = wsio.split();
        let send_stream = Rc::new(RefCell::new(send_stream));

        let offer_sdp = match wasm_peer.create_offer().await {
            Some(sdp) => sdp,
            None => {
                log!("failed to create offer");
                return;
            }
        };

        {
            let msg = SignalMessage::Offer { sdp: offer_sdp };
            let text = serde_json::to_string(&msg).unwrap();
            if let Err(e) = send_stream.borrow_mut().send(WsMessage::Text(text)).await {
                log!("failed to send offer: {:?}", e);
                return;
            }
        }

        let peer = wasm_peer.clone();
        spawn_local(async move {
            while let Some(msg) = recv_stream.next().await {
                match msg {
                    WsMessage::Text(text) => {
                        let parsed = match serde_json::from_str::<SignalMessage>(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                log!("failed to parse signaling message: {:?}", e);
                                continue;
                            }
                        };

                        match parsed {
                            SignalMessage::Answer { sdp } => {
                                log!("received answer sdp");
                                if let Err(e) = peer.accept_answer(sdp).await {
                                    log!("failed to accept answer: {:?}", e);
                                    return;
                                }
                            }
                            SignalMessage::IceCandidate { candidate } => {
                                if let Err(e) = peer.add_ice_candidate(candidate).await {
                                    log!("failed to add remote ICE candidate: {:?}", e);
                                }
                            }
                            SignalMessage::Offer { .. } => {
                                log!("unexpected offer");
                            }
                        }
                    }
                    other => {
                        log!("unexpected websocket message: {:?}", other);
                    }
                }
            }
        });

        // Send local ICE candidates as they are gathered.
        loop {
            if wasm_peer.ice_connection_state() == RtcIceConnectionState::Completed || wasm_peer.ice_gathering_state() == RtcIceGatheringState::Complete {
                log!("Ice is finished");
                break;
            }

            let candidates = drain_local_ice_candidates(wasm_peer.clone());

            for ice in candidates {
                let msg = SignalMessage::IceCandidate {
                    candidate: ice.candidate,
                };
                let text = serde_json::to_string(&msg).unwrap();
                if let Err(e) = send_stream.borrow_mut().send(WsMessage::Text(text)).await {
                    log!("failed to send ICE candidate: {:?}", e);
                    return;
                }
            }

            TimeoutFuture::new(25).await;
        }

        // send until its open
        loop {
            if let Ok(_) = wasm_peer.send_text("Hello from WASM!".to_string()) {
                log!("Success");
                break;
            }
            TimeoutFuture::new(50).await;
        }
    });
}
fn main() {
    console_error_panic_hook::set_once();

    let window = web_sys::window().expect("no global `window` exists");
    let document = window.document().expect("should have a document on window");
    let body = document.body().expect("document should have a body");

    let server_searchbox = document
        .get_element_by_id("server-searchbox")
        .expect("should be here");

    // set default address here
    {
        let server_searchbox: &HtmlInputElement = server_searchbox.dyn_ref().unwrap();
        server_searchbox.set_value(SERVER_ADDRESS);
    }

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
