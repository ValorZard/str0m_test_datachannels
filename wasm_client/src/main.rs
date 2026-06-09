use anyhow::{Result, anyhow};
use js_sys::{Array, Uint8Array};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Event, MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcIceCandidate,
    RtcIceCandidateInit, RtcIceServer, RtcPeerConnection, RtcPeerConnectionIceEvent, RtcSdpType,
    RtcSessionDescriptionInit,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceCandidateMessage {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u16>,
}

struct Inner {
    pc: RtcPeerConnection,
    data_channel: RefCell<Option<RtcDataChannel>>,
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

    pub async fn create_offer(&self, channel_label: String) -> Option<String> {
        let dc = self.inner.pc.create_data_channel(&channel_label);
        install_data_channel_handlers(&self.inner, &dc).unwrap();
        self.inner.data_channel.replace(Some(dc));

        let offer_val = JsFuture::from(self.inner.pc.create_offer()).await.unwrap();
        let offer: RtcSessionDescriptionInit = offer_val.dyn_into().unwrap();

        JsFuture::from(self.inner.pc.set_local_description(&offer))
            .await
            .unwrap();

        offer.get_sdp()
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

    pub async fn add_ice_candidate(
        &self,
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    ) -> Result<(), JsValue> {
        let mut init = RtcIceCandidateInit::new(&candidate);
        if let Some(mid) = sdp_mid.as_deref() {
            init.set_sdp_mid(Some(mid));
        }
        if let Some(index) = sdp_mline_index {
            init.set_sdp_m_line_index(Some(index));
        }

        let candidate = RtcIceCandidate::new(&init)?;
        JsFuture::from(
            self.inner
                .pc
                .add_ice_candidate_with_opt_rtc_ice_candidate(Some(&candidate)),
        )
        .await?;

        Ok(())
    }

    pub fn take_local_ice_candidates(&self) -> Result<String, JsValue> {
        let mut pending = self.inner.pending_local_ice.borrow_mut();
        let out = serde_json::to_string(&*pending)
            .map_err(|e| JsValue::from_str(&format!("serialize ICE candidates: {e}")))?;
        pending.clear();
        Ok(out)
    }

    pub fn send_text(&self, text: String) -> Result<(), JsValue> {
        let dc = self
            .inner
            .data_channel
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| JsValue::from_str("data channel not available"))?;

        dc.send_with_str(&text)
    }

    pub fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), JsValue> {
        let dc = self
            .inner
            .data_channel
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| JsValue::from_str("data channel not available"))?;

        dc.send_with_u8_array(&bytes)
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

    pub fn ice_connection_state(&self) -> String {
        format!("{:?}", self.inner.pc.ice_connection_state())
    }

    pub fn ice_gathering_state(&self) -> String {
        format!("{:?}", self.inner.pc.ice_gathering_state())
    }

    pub fn close(&self) {
        self.inner.pc.close();
    }
}

fn make_rtc_config() -> RtcConfiguration {
    let mut stun = RtcIceServer::new();
    stun.urls(&JsValue::from_str("stun:stun.l.google.com:19302"));

    let servers = Array::new();
    servers.push(&stun);

    let mut config = RtcConfiguration::new();
    config.ice_servers(&servers);
    config
}

fn install_peer_handlers(inner: &Rc<Inner>) -> Result<(), JsValue> {
    let inner_for_ice = Rc::clone(inner);
    let on_ice = Closure::wrap(Box::new(move |e: RtcPeerConnectionIceEvent| {
        if let Some(candidate) = e.candidate() {
            inner_for_ice
                .pending_local_ice
                .borrow_mut()
                .push(IceCandidateMessage {
                    candidate: candidate.candidate(),
                    sdp_mid: candidate.sdp_mid(),
                    sdp_mline_index: candidate.sdp_m_line_index(),
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
    let on_open = Closure::wrap(Box::new(move |_e: Event| {
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

fn main() {
    console_error_panic_hook::set_once();
    let wasm_peer = WasmPeer::new();
}
