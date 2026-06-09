# How to build for Linux
``cross build --target x86_64-unknown-linux-gnu # can do release version by adding --release``

# How to run
## Localhost test

### Server:
```bash
cargo run -- --mode server --advertise-ip 127.0.0.1
```
### Client:

```bash
cargo run -- --mode client --advertise-ip 127.0.0.1 --server-ip 127.0.0.1
```

## Real remote server

On the server machine:

```bash
cargo run -- --mode server --advertise-ip YOUR_PUBLIC_SERVER_IP
```
On the client machine:

```bash
cargo run -- --mode client --server-ip YOUR_PUBLIC_SERVER_IP --advertise-ip YOUR_CLIENT_PUBLIC_IP
```