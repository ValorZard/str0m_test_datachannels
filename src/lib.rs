pub use datachannel_socket_common as common;

#[cfg(not(target_arch = "wasm32"))]
pub use datachannel_socket_native_peer as native_peer;

#[cfg(target_arch = "wasm32")]
pub use datachannel_socket_wasm_peer as wasm_peer;
