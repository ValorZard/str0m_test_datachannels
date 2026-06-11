# datachannel socket

Connect to a [str0m](https://github.com/algesten/str0m) WebRTC "server" peer through either a native client (using str0m itself) or a WASM client.

This is done by "bootstrapping" the WebRTC connection with a WebSocket connection to a signaling server and then dropping the connection once the WebRTC session has started.

This is specifically intended for the usecase of people wanting access to UDP-style performance with WebRTC datachannels for a client-server application or game that can compile to both WASM or native. 

**If your interested in WebRTC for media streaming/telephony, please use something else.**

**If you only care about compiling something for native targets, please use something else.**

This is considered an alternative to [WebTransport](https://www.w3.org/TR/webtransport/), except unlike WebTransport this can work in all browsers today.

Native str0m peers hosted on a public server can bypass using a STUN server for external address discovery by just sending its own IP address directly.

The reason is that the entire point of STUN is for clients to discover their own public IP address and port number. But, that's only needed in the browser for security reasons. If we are launching from our own machine we can just ... look up our own IP ourselves.

We should NEVER need to use a TURN server for a relay. If we have to resort to using a TURN for a relay, that defeats the entire point of this library, since we want to have as little latency as possible compared to existing options like WebSockets.

We do still use ICE for setting up the connection and connectivity checks, but we never need to connect to any other external servers other than the signaling "bootstrap" server.


# Caveats

**Right now this can only work as an echo server. I'm releasing this library now to make sure this can work on different people's computer in real world situations before continuing development.**

# How to run
## Localhost test

### Server:

```bash
cargo run -p datachannel_socket_native_peer --example native_server
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
cargo run -p datachannel_socket_native_peer --example native_client  -- --server-addr "ws://127.0.0.1:7000"
```

### WASM Client:
You will need to have installed [trunk](https://github.com/trunk-rs/trunk).

Then, you can just do:
```bash
cd wasm_peer
trunk serve --example wasm_client
```


## Real remote server

On the server machine:

```bash
cargo run -p datachannel_socket_native_peer --example native_server -- --advertise-ip YOUR_PUBLIC_SERVER_IP
```
On the client machine:

```bash
cargo run -p datachannel_socket_native_peer -- --server-addr YOUR_SIGNALING_SERVER_ADDR
```

Note that the client peer doesn't need to advertise it's own IP address. This is because because the server is already advertising it's public IP, we don't actually need to put in the work to find our own IP. So, as long as we put in a somewhat valid IP address, the server peer will be able to connect to the client peer.

# How to build for Linux
You will need to have installed [cross](https://github.com/cross-rs/cross).

Then you can just do:
``cross build --target x86_64-unknown-linux-gnu # can do release version by adding --release``


# Alternatives

If you want to use WebRTC to make a peer to peer game, checkout [matchbox](https://github.com/johanhelsing/matchbox).

If you don't care about Firefox or Safari, and are okay with having something only work on Chrome, use WebTransport. I'm fond of [this library in particular](https://github.com/MOZGIII/xwt), but you can also use [web-transport](https://github.com/moq-dev/web-transport).

If you don't care about the browser but are interested in WebRTC for the P2P stuff, might I suggest you use [iroh](https://www.iroh.computer/) instead? I find the API VERY easy to use.