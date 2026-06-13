Current benchmarking against tokio-tungstenite

```bash
warning: `datachannel_socket_native_peer` (lib) generated 1 warning                                                                 
    Finished `release` profile [optimized] target(s) in 2m 11s                                                                      
     Running `target\release\examples\native_transport_benchmark.exe --messages 100000 --warmup-messages 5000 --payload-bytes 1024`
running benchmark: messages=100000, payload_bytes=1024, warmup_messages=5000

phase 1/2: starting WebRTC server...
phase 1/2: running WebRTC benchmark...
client: UDP bound on 0.0.0.0:64318
client: advertising ICE candidate 192.168.68.53:64318
connecting to websocket signaling server "ws://127.0.0.1:7000"
Advertising server on '192.168.68.53:64319'
Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip.
server: info: signaling peer is loopback (127.0.0.1:56617), advertising non-loopback ICE IP 192.168.68.53:64319 for browser compatibility
server: UDP bound on 0.0.0.0:64319
server: advertising ICE candidate 192.168.68.53:64319
server: signaling connected from 127.0.0.1:56617
client: connected to server, initial response Response { status: 101, version: HTTP/1.1, headers: {"connection": "Upgrade", "upgrade": "websocket", "sec-websocket-accept": "oy6iXWEksWTZv2P2cLRMEgzMobs="}, body: None }
Closing stream, don't need it anymore, client should be connected.
We're connected, so no need for websocket connection
bench-webrtc-client: event: IceConnectionStateChange(Checking)
bench-webrtc-server: event: IceConnectionStateChange(Checking)
bench-webrtc-client: event: IceConnectionStateChange(Completed)
bench-webrtc-server: event: IceConnectionStateChange(Completed)
bench-webrtc-server: connected
bench-webrtc-client: connected
bench-webrtc-server: channel open: "chat"
bench-webrtc-client: channel open: "chat"
phase 1/2: shutting down WebRTC server/clients...

phase 2/2: starting WebSocket server...
phase 2/2: running WebSocket benchmark...
phase 2/2: shutting down WebSocket server/clients...

=== datachannel-socket (str0m) ===
messages           : 100000
payload bytes      : 1024
total duration     : 18.831s
messages/sec       : 5310.26
throughput MiB/sec : 5.186
RTT mean (us)      : 185.8
RTT p50  (us)      : 90
RTT p95  (us)      : 579
RTT p99  (us)      : 944

=== tokio-tungstenite ===
messages           : 100000
payload bytes      : 1024
total duration     : 21.432s
messages/sec       : 4665.83
throughput MiB/sec : 4.556
RTT mean (us)      : 210.3
RTT p50  (us)      : 182
RTT p95  (us)      : 387
RTT p99  (us)      : 771

relative comparison (higher is better):
datachannel / websocket messages/sec ratio: 1.138
PS C:\github\str0m_test_datachannels> cargo run --release -p datachannel_socket_native_peer --example native_transport_benchmark -- --messages 100 --warmup-messages 5 --payload-bytes 1024      
warning: unused import: `futures::SinkExt`
 --> common\src\lib.rs:1:5
  |
1 | use futures::SinkExt;
  |     ^^^^^^^^^^^^^^^^
  |
  = note: `#[warn(unused_imports)]` (part of `#[warn(unused)]`) on by default

warning: `datachannel_socket_common` (lib) generated 1 warning (run `cargo fix --lib -p datachannel_socket_common` to apply 1 suggestion)
warning: unused label
   --> native_peer\src\lib.rs:238:9
    |
238 |         'rtc_loop: loop {
    |         ^^^^^^^^^
    |
    = note: `#[warn(unused_labels)]` (part of `#[warn(unused)]`) on by default

warning: `datachannel_socket_native_peer` (lib) generated 1 warning
    Finished `release` profile [optimized] target(s) in 0.22s
     Running `target\release\examples\native_transport_benchmark.exe --messages 100 --warmup-messages 5 --payload-bytes 1024`
running benchmark: messages=100, payload_bytes=1024, warmup_messages=5

phase 1/2: starting WebRTC server...
phase 1/2: running WebRTC benchmark...
client: UDP bound on 0.0.0.0:51709
client: advertising ICE candidate 192.168.68.53:51709
connecting to websocket signaling server "ws://127.0.0.1:7000"
Advertising server on '192.168.68.53:51710'
Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip.
server: info: signaling peer is loopback (127.0.0.1:51279), advertising non-loopback ICE IP 192.168.68.53:51710 for browser compatibility
server: UDP bound on 0.0.0.0:51710
server: advertising ICE candidate 192.168.68.53:51710
server: signaling connected from 127.0.0.1:51279
client: connected to server, initial response Response { status: 101, version: HTTP/1.1, headers: {"connection": "Upgrade", "upgrade": "websocket", "sec-websocket-accept": "w13LVixMsXH81JzFyTIzW6s82AY="}, body: None }
Closing stream, don't need it anymore, client should be connected.
We're connected, so no need for websocket connection
bench-webrtc-client: event: IceConnectionStateChange(Checking)
bench-webrtc-server: event: IceConnectionStateChange(Checking)
bench-webrtc-client: event: IceConnectionStateChange(Completed)
bench-webrtc-server: event: IceConnectionStateChange(Completed)
bench-webrtc-server: connected
bench-webrtc-client: connected
bench-webrtc-server: channel open: "chat"
bench-webrtc-client: channel open: "chat"
phase 1/2: shutting down WebRTC server/clients...

phase 2/2: starting WebSocket server...
phase 2/2: running WebSocket benchmark...
phase 2/2: shutting down WebSocket server/clients...

=== datachannel-socket (str0m) ===
messages           : 100
payload bytes      : 1024
total duration     : 0.008s
messages/sec       : 12677.00
throughput MiB/sec : 12.380
RTT mean (us)      : 77.5
RTT p50  (us)      : 72
RTT p95  (us)      : 107
RTT p99  (us)      : 122

=== tokio-tungstenite ===
messages           : 100
payload bytes      : 1024
total duration     : 0.004s
messages/sec       : 27184.99
throughput MiB/sec : 26.548
RTT mean (us)      : 35.5
RTT p50  (us)      : 32
RTT p95  (us)      : 47
RTT p99  (us)      : 91

relative comparison (higher is better):
datachannel / websocket messages/sec ratio: 0.466
PS C:\github\str0m_test_datachannels> cargo run --release -p datachannel_socket_native_peer --example native_transport_benchmark -- --messages 100000 --warmup-messages 5000 --payload-bytes 1024
warning: unused import: `futures::SinkExt`
 --> common\src\lib.rs:1:5
  |
1 | use futures::SinkExt;
  |     ^^^^^^^^^^^^^^^^
  |
  = note: `#[warn(unused_imports)]` (part of `#[warn(unused)]`) on by default

warning: `datachannel_socket_common` (lib) generated 1 warning (run `cargo fix --lib -p datachannel_socket_common` to apply 1 suggestion)
warning: unused label
   --> native_peer\src\lib.rs:238:9
    |
238 |         'rtc_loop: loop {
    |         ^^^^^^^^^
    |
    = note: `#[warn(unused_labels)]` (part of `#[warn(unused)]`) on by default

warning: `datachannel_socket_native_peer` (lib) generated 1 warning
    Finished `release` profile [optimized] target(s) in 0.23s
     Running `target\release\examples\native_transport_benchmark.exe --messages 100000 --warmup-messages 5000 --payload-bytes 1024`
running benchmark: messages=100000, payload_bytes=1024, warmup_messages=5000

phase 1/2: starting WebRTC server...
phase 1/2: running WebRTC benchmark...
client: UDP bound on 0.0.0.0:50201
client: advertising ICE candidate 192.168.68.53:50201
connecting to websocket signaling server "ws://127.0.0.1:7000"
Advertising server on '192.168.68.53:50202'
Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip.
server: info: signaling peer is loopback (127.0.0.1:33189), advertising non-loopback ICE IP 192.168.68.53:50202 for browser compatibility
server: UDP bound on 0.0.0.0:50202
server: advertising ICE candidate 192.168.68.53:50202
server: signaling connected from 127.0.0.1:33189
client: connected to server, initial response Response { status: 101, version: HTTP/1.1, headers: {"connection": "Upgrade", "upgrade": "websocket", "sec-websocket-accept": "0CDWC/pyaGOaAVumIqfk93Xi4+g="}, body: None }
Closing stream, don't need it anymore, client should be connected.
We're connected, so no need for websocket connection
bench-webrtc-client: event: IceConnectionStateChange(Checking)
bench-webrtc-server: event: IceConnectionStateChange(Checking)
bench-webrtc-client: event: IceConnectionStateChange(Completed)
bench-webrtc-server: event: IceConnectionStateChange(Completed)
bench-webrtc-server: connected
bench-webrtc-client: connected
bench-webrtc-server: channel open: "chat"
bench-webrtc-client: channel open: "chat"
phase 1/2: shutting down WebRTC server/clients...

phase 2/2: starting WebSocket server...
phase 2/2: running WebSocket benchmark...
phase 2/2: shutting down WebSocket server/clients...

=== datachannel-socket (str0m) ===
messages           : 100000
payload bytes      : 1024
total duration     : 7.144s
messages/sec       : 13997.36
throughput MiB/sec : 13.669
RTT mean (us)      : 70.0
RTT p50  (us)      : 66
RTT p95  (us)      : 98
RTT p99  (us)      : 127

=== tokio-tungstenite ===
messages           : 100000
payload bytes      : 1024
total duration     : 3.739s
messages/sec       : 26744.18
throughput MiB/sec : 26.117
RTT mean (us)      : 36.2
RTT p50  (us)      : 33
RTT p95  (us)      : 55
RTT p99  (us)      : 91

relative comparison (higher is better):
datachannel / websocket messages/sec ratio: 0.523
PS C:\github\str0m_test_datachannels> cargo run --release -p datachannel_socket_native_peer --example native_transport_benchmark -- --messages 100000 --warmup-messages 5000 --payload-bytes 1024
warning: unused import: `futures::SinkExt`
 --> common\src\lib.rs:1:5
  |
1 | use futures::SinkExt;
  |     ^^^^^^^^^^^^^^^^
  |
  = note: `#[warn(unused_imports)]` (part of `#[warn(unused)]`) on by default

warning: `datachannel_socket_common` (lib) generated 1 warning (run `cargo fix --lib -p datachannel_socket_common` to apply 1 suggestion)
warning: unused label
   --> native_peer\src\lib.rs:238:9
    |
238 |         'rtc_loop: loop {
    |         ^^^^^^^^^
    |
    = note: `#[warn(unused_labels)]` (part of `#[warn(unused)]`) on by default

warning: `datachannel_socket_native_peer` (lib) generated 1 warning
    Finished `release` profile [optimized] target(s) in 0.16s
     Running `target\release\examples\native_transport_benchmark.exe --messages 100000 --warmup-messages 5000 --payload-bytes 1024`
running benchmark: messages=100000, payload_bytes=1024, warmup_messages=5000

phase 1/2: starting WebRTC server...
phase 1/2: running WebRTC benchmark...
client: UDP bound on 0.0.0.0:59770
client: advertising ICE candidate 192.168.68.53:59770
connecting to websocket signaling server "ws://127.0.0.1:7000"
Advertising server on '192.168.68.53:59771'
Note that if you are running this over the internet proper, the ip of the remote machine you are running this one has to be passed through to the server process itself as the advertise_ip.
server: info: signaling peer is loopback (127.0.0.1:33218), advertising non-loopback ICE IP 192.168.68.53:59771 for browser compatibility
server: UDP bound on 0.0.0.0:59771
server: advertising ICE candidate 192.168.68.53:59771
server: signaling connected from 127.0.0.1:33218
client: connected to server, initial response Response { status: 101, version: HTTP/1.1, headers: {"connection": "Upgrade", "upgrade": "websocket", "sec-websocket-accept": "tqWr+z4xK+Q1nPhKUx0UdxX6A9Q="}, body: None }
Closing stream, don't need it anymore, client should be connected.
We're connected, so no need for websocket connection
bench-webrtc-client: event: IceConnectionStateChange(Checking)
bench-webrtc-server: event: IceConnectionStateChange(Checking)
bench-webrtc-client: event: IceConnectionStateChange(Completed)
bench-webrtc-server: event: IceConnectionStateChange(Completed)
bench-webrtc-server: connected
bench-webrtc-client: connected
bench-webrtc-server: channel open: "chat"
bench-webrtc-client: channel open: "chat"
phase 1/2: shutting down WebRTC server/clients...

phase 2/2: starting WebSocket server...
phase 2/2: running WebSocket benchmark...
phase 2/2: shutting down WebSocket server/clients...

=== datachannel-socket (str0m) ===
messages           : 100000
payload bytes      : 1024
total duration     : 7.787s
messages/sec       : 12842.45
throughput MiB/sec : 12.541
RTT mean (us)      : 76.4
RTT p50  (us)      : 68
RTT p95  (us)      : 129
RTT p99  (us)      : 178

=== tokio-tungstenite ===
messages           : 100000
payload bytes      : 1024
total duration     : 4.396s
messages/sec       : 22747.62
throughput MiB/sec : 22.214
RTT mean (us)      : 42.7
RTT p50  (us)      : 35
RTT p95  (us)      : 86
RTT p99  (us)      : 118

relative comparison (higher is better):
datachannel / websocket messages/sec ratio: 0.565
PS C:\github\str0m_test_datachannels> 
```

These results seem really inconsistent...