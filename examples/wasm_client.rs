use datachannel_socket::common::{Peer, PeerFactory, SignalMessage};
use datachannel_socket::wasm_peer::{WasmPeer, WasmPeerFactory, peer_log};
use gloo_timers::future::TimeoutFuture;
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

fn connect_to_server(server_address: String) {
    spawn_local(async move {
        let factory = WasmPeerFactory::new(());
        let wasm_peer = factory
            .create_peer(server_address)
            .await
            .expect("should work");

        // send until its open
        loop {
            if let Ok(_) = wasm_peer.send_text("Hello from WASM!".to_string()) {
                peer_log!("Success");
                break;
            }
            TimeoutFuture::new(50).await;
        }

        // read messages coming in
        loop {
            if let Ok(message) = wasm_peer.take_received_messages() {
                peer_log!("Message from data channel: {message}");
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
