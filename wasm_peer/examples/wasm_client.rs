use anyhow::{Result, anyhow};
use datachannel_socket_common::{DataChannelMessage, WebRTCNotification};
use datachannel_socket_wasm_peer::{WasmPeerFactory, peer_log};
use gloo_timers::future::TimeoutFuture;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::spawn_local;
use web_sys::{HtmlElement, HtmlInputElement};

fn connect_to_server(server_address: String) {
    spawn_local(async move {
        let result: Result<_> = async {
            let factory = WasmPeerFactory::new();
            let mut wasm_peer = factory
                .create_peer(server_address)
                .await
                .expect("should work");

            let mut communication_handle = wasm_peer.get_communication_handle()?;

            // TODO: Find a better way to tell if the client has started running
            let mut channels = Vec::new();
            // TODO: just take one channel for now, figure out something better later
            while let Ok(notification) = communication_handle.recv_notification().await {
                if let WebRTCNotification::ChannelOpen(channel_ref) = notification {
                    channels.push(channel_ref);
                    break;
                }
            }

            for channel_ref in channels {
                let _ = communication_handle.send_datachannel_message(
                    channel_ref.clone(),
                    DataChannelMessage::Text("Hello from wasm client!".into()),
                );
                let _ = communication_handle.send_datachannel_message(
                    channel_ref,
                    DataChannelMessage::Binary("Hello from wasm client in binary!".into()),
                );
            }

            while let Ok((channel_ref, message)) = communication_handle.recv_datachannel_message().await {
                peer_log!("From {channel_ref:?} Received incoming datachannel message: {message:?}");
            }

            Ok(())
        }
        .await;

        if let Err(e) = result {
            peer_log!("Error! {e}");
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
