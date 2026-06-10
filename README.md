# str0m datachannel test
Connect to a [str0m](https://github.com/algesten/str0m) webrtc peer through either a native client (using str0m itself) or a WASM client using a websocket signaling server.

Native str0m clients can bypass using STUN/TURN by just sending its own IP address directly.

The reason is that the entire point of STUN/TURN is for clients to discover their own public IP address and port number. But, that's only needed in the browser for security reasons. If we are launching from our own machine we can just ... look up our own IP ourselves.

The WASM Client is stuck having to use STUN/TURN/ICE trickling, though, and it's a little annoying if you want Trickle ICE. Just waiting for all ICE candidates to show up before connecting works, but adds extra startup time.

# How to run
## Localhost test

### Server:

```bash
cargo run -p webrtc_server
```

Note: running the server locally will require the server to generate a non-loopback address for certain targets to connect to it properly.

for example:
```bash
PS C:\github\str0m_test_datachannels> cargo run -p webrtc_server -- --bind-ip 0.0.0.0 --signal-port 7000 --udp-port 5000
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.22s
     Running `target\debug\webrtc_server.exe --bind-ip 0.0.0.0 --signal-port 7000 --udp-port 5000`
server: signaling on 0.0.0.0:7000
Advertising server on '10.1.2.145'
Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip.
```

This is because firefox doesn't like it when you connect to WebRTC using a loopback address.

### Native Client:

```bash
cargo run -p client -- --server-addr "ws://127.0.0.1:7000"
```

### WASM Client:
You will need to have installed [trunk](https://github.com/trunk-rs/trunk).

Then, you can just do:
```bash
cd wasm_client
trunk serve
```


## Real remote server

On the server machine:

```bash
cargo run -p server -- --advertise-ip YOUR_PUBLIC_SERVER_IP
```
On the client machine:

```bash
cargo run -p client -- --server-addr YOUR_PUBLIC_SERVER_ADDR
```

Note that the client peer doesn't need to advertise it's own IP address. This is because because the server is already advertising it's public IP, we don't actually need to put in the work to find our own IP. So, as long as we put in a somewhat valid IP address, the server peer will be able to connect to the client peer.

# How to build for Linux
You will need to have installed [cross](https://github.com/cross-rs/cross).

Then you can just do:
``cross build --target x86_64-unknown-linux-gnu # can do release version by adding --release``