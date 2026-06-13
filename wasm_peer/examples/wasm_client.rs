use anyhow::{Result, anyhow};
use datachannel_socket_common::DataChannelMessage;
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

            // wait until we have channels
            let mut waited_ms = 0u32;
            let mut channel_ids = wasm_peer.get_channel_ids();
            peer_log!(
                "Checking for channels after signaling, initial_count={}",
                channel_ids.len()
            );

            while channel_ids.is_empty() {
                if waited_ms % 500 == 0 {
                    peer_log!(
                        "Waiting on channels to be available... waited={}ms",
                        waited_ms
                    );
                }

                TimeoutFuture::new(50).await;
                waited_ms += 50;
                channel_ids = wasm_peer.get_channel_ids();

                if waited_ms >= 10_000 {
                    return Err(anyhow!(
                        "Timed out waiting for data channel to open after {}ms",
                        waited_ms
                    ));
                }
            }

            peer_log!(
                "Channels available after {}ms, channel_ids={:?}",
                waited_ms,
                channel_ids
            );

            let (mut incoming_datachannel_message_receiver, outgoing_datachannel_message_sender) =
                wasm_peer.get_communication_channels()?;
            for channel in channel_ids {
                outgoing_datachannel_message_sender.unbounded_send((
                    channel,
                    DataChannelMessage::Text("Hello from wasm client!".into()),
                ))?;

                outgoing_datachannel_message_sender.unbounded_send((
                    channel,
                    DataChannelMessage::Binary("Hello from wasm client in binary!".into()),
                ))?;
            }

            while let Ok(message) = incoming_datachannel_message_receiver.recv().await {
                peer_log!("Received incoming datachannel message: {:?}", message);
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
