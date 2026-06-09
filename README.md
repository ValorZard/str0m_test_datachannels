# str0m datachannel test
Connect to a str0m webrtc peer through either a native client (using str0m itself) or a WASM client.

native str0m client can bypass using STUN/TURN by just sending its own IP address directly.

WASM Client has to use STUN/TURN/ICE trickling. 

(Note: for some reason, this doesn't work on firefox for now.)

# How to build for Linux
``cross build --target x86_64-unknown-linux-gnu # can do release version by adding --release``

# How to run
## Localhost test

### Server:
```bash
cargo run -p server -- --advertise-ip 127.0.0.1
```
### Native Client:

```bash
cargo run -p client -- --advertise-ip 127.0.0.1 --server-ip 127.0.0.1
```

### WASM Client:

```bash
trunk serve # hardcoded server path for now
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