# str0m datachannel test
Connect to a str0m webrtc peer through either a native client (using str0m itself) or a WASM client.

native str0m client can bypass using STUN/TURN by just sending its own IP address directly.

WASM Client has to use STUN/TURN/ICE trickling. 

# How to build for Linux
``cross build --target x86_64-unknown-linux-gnu # can do release version by adding --release``

# How to run
## Localhost test

### Server:
```bash
cargo run -p server -- --advertise-ip 127.0.0.1
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
cargo run -p client -- --advertise-ip 127.0.0.1 --server-ip 127.0.0.1
```

### WASM Client:

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
cargo run -p client -- --server-ip YOUR_PUBLIC_SERVER_IP --advertise-ip YOUR_CLIENT_PUBLIC_IP
```
