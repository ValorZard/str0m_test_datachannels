# How to run
## Localhost test

### Server:
```bash
cargo run -- --mode server --bind-ip 127.0.0.1 --advertise-ip 127.0.0.1
```
### Client:

```bash
cargo run -- --mode client --bind-ip 127.0.0.1 --advertise-ip 127.0.0.1 --server-ip 127.0.0.1
```

## Real remote server

On the server machine:

```bash
cargo run -- --mode server --bind-ip 0.0.0.0 --advertise-ip YOUR_PUBLIC_SERVER_IP
```
On the client machine:

```bash
cargo run -- --mode client --bind-ip 0.0.0.0 --server-ip 167.233.56.216 --advertise-ip YOUR_CLIENT_PUBLIC_IP
```