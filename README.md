# datachannel socket

Connect to a [str0m](https://github.com/algesten/str0m) webrtc peer through either a native client (using str0m itself) or a WASM client using a websocket signaling server.

Native str0m clients can bypass using STUN/TURN by just sending its own IP address directly.

The reason is that the entire point of STUN/TURN is for clients to discover their own public IP address and port number. But, that's only needed in the browser for security reasons. If we are launching from our own machine we can just ... look up our own IP ourselves.

The WASM Client is stuck having to use STUN/TURN/ICE trickling, though, and it's a little annoying if you want Trickle ICE. Just waiting for all ICE candidates to show up before connecting works, but adds extra startup time.

# How to run
## Localhost test

### Server:

```bash
cargo run --example native_server
```

Note: running the server locally will require the server to generate a non-loopback address for certain targets to connect to it properly.

for example:
```bash
PS C:\github\str0m_test_datachannels> cargo run --example native_server                                
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2.63s
     Running `target\debug\examples\native_server.exe`
server: signaling on 0.0.0.0:7000
Advertising server on '192.168.68.51:59471'
Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip.
server: info: signaling peer is loopback (127.0.0.1:53301), advertising non-loopback ICE IP 192.168.68.51:59471 for browser compatibility
```

This is because firefox doesn't like it when you connect to WebRTC using a loopback address.

### Native Client:

```bash
cargo run --example native_client  -- --server-addr "ws://127.0.0.1:7000"
```

### WASM Client:
You will need to have installed [trunk](https://github.com/trunk-rs/trunk).

Then, you can just do:
```bash
trunk serve --example wasm_client
```


## Real remote server

On the server machine:

```bash
cargo run --example native_server -- --advertise-ip YOUR_PUBLIC_SERVER_IP
```
On the client machine:

```bash
cargo run --example native_client -- --server-addr YOUR_SIGNALING_SERVER_ADDR
```

Note that the client peer doesn't need to advertise it's own IP address. This is because because the server is already advertising it's public IP, we don't actually need to put in the work to find our own IP. So, as long as we put in a somewhat valid IP address, the server peer will be able to connect to the client peer.

# How to build for Linux
You will need to have installed [cross](https://github.com/cross-rs/cross).

Then you can just do:
``cross build --target x86_64-unknown-linux-gnu # can do release version by adding --release``