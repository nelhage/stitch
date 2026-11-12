`stitch` offers the abstraction of "a stable, persistent, in-order stream" between a client and a server; "stable" in the sense of being robust to network failures, one end going offline for a period of time, or network changes. This stream is backed by a series of `ssh` connections between the client and server.

The primary use case for a `stitch` stream is to run **another** "ssh" instance over it, creating the abstraction of "ssh, but robust across network changes." However, it is a generic primitive that can be used for other purposes, as well.

# Design goals
- Completely transparent to both ends of the connection
- Survive connection disconnection, and extended periods of downtime (e.g. "I suspend my laptop and then commute home")
    - Notably, this means we must be able to resume after the first channel has shutdown -- we can't rely on an online handoff to a new transport
- All authentication and encryption is handled by `ssh`
- In particular, a minimal change to the threat model or operational model relative to vanilla ssh:
    - If the underlying `ssh` connection drops, resuming requires a new `ssh` authentication -- we do not artificially extend the duration of any credential.
    - Connections are, to the extent possible, visible to any existing security tooling or monitoring that understands `ssh`

# Interface
- `stitch tunnel <HOST> [[<DEST>:]<PORT>]` will establish a reliable tunnel to `<DEST>:<PORT>`. It acts similarly to `netcat` -- i.e., its stdin and stdout will be connected to a connection to `<DEST>:<PORT>`  -- except that the connection is static and resilient to network failures or changes.
    - If only `PORT` is specified, `localhost` is assumed.
    - If neither `HOST` nor `PORT` is not specified, the port will be inferred based on `SSH_CONNECTION` to match the host's `ssh` server port.
    - Instead of a host/port, the user can also specify `stitch tunnel -- command...` to directly execute a command on the remote server and conect to that.
- The connection is proxied via one or more `ssh` connections to `<HOST>`. In common use, `<HOST>` and `<DEST>` will either be identical, or `<DEST>` may be `localhost`, such that you are connecting to `HOST` via itself.
    - However, setting `HOST` and `DEST` to distinct addresses can be useful in order to use `HOST` as a jump box or bastion to reach `DEST`
- In order to achieve a stable `ssh` connection, `stitch tunnel` can be used as an ssh `ProxyCommand`
    - e.g. `ssh HOST -o ProxyCommand="stitch tunnel '%h' 127.0.0.1:%p"`
    - `ssh` runs a `ProxyCommand` with the user's shell, so `%h` must be quoted, or the shell will split and glob-expand the host name. Separately, `stitch tunnel` refuses a `HOST` which is empty, starts with `-`, or contains whitespace, control characters or shell metacharacters.
- `stitch ssh HOST` will invoke `ssh` with `stitch` as an appropriate `ProxyCommand` and set various other `ssh` configuration options to establish a reliable tunnel.
- `stitch tunnel` and `stitch ssh` both accept `--remote-stitch=CMD`, giving the `stitch` command to run on the remote host (e.g. a path to the binary), instead of relying on the remote `PATH`. Like `rsync --rsync-path` or `xpra --remote-xpra`, it is interpreted by the remote shell.
- `stitch ssh-config` will set up `ssh` configuration such that `ssh HOST.stitch` will work for arbitrary `HOST`

# Implementation
## Connection initiation
`stitch tunnel` runs `ssh` to create an initial connection to `HOST`, and starts a pair of proxy processes, which are initially communicating with each other over the original `ssh` channel.

The server's reply in the setup handshake (see "Tunnel setup", below) contains the path to a rendezvous socket (located in `$XDG_RUNTIME_DIR` on the server, falling back to `/tmp`).

## Proxying data
The proxies communicate over the `ssh` channel. They speak a minimal "transport" protocol, which encapsulates data and acknowledges received data; its wire format is specified in `framing.md`. Each proxy retains a buffer of sent-but-unacknowledged data for use during a resumption. The buffer is sized larger than a typical TCP send buffer to absorb flow during an extended offline window, but is fixed-size; if the buffer fills up, the proxy stops reading from the corresponding fd and relies on the underlying transport to eventually backpressure the sender.

Each proxy also holds a bounded buffer of data it has received but not yet delivered to its local application, and advertises the free space in that buffer to its peer, which never sends beyond it (see "Flow control", below). As a result, a proxy never stops reading from the transport, even if its local application has stopped reading.

The proxy data plane is completely symmetric between both ends; during steady state, absent any disconnections, the client and server behave completely symmetrically. Resumption is also parallel once a new transport channel has been established, although the process of negotiating and commencing a resumption (next section) is asymmetric.

## Resumption
The client may trigger a resumption, by initiating a new `ssh` connection, and moving the underlying transport over.

In order to do so, it initiates a new ssh connection, invokes the server-side shim, and writes a JSON "resume" message to its STDIN (see "Tunnel setup", below). The server-side shim connects to the rendezvous socket and forwards the resume message, attaching stdin and stdout as `SCM_RIGHTS` auxiliary data. It then simply sleeps until the connection is closed.

Upon receipt of a valid resume message, the proxy server writes a JSON success reply to the new channel, and then switches its transport over to that channel.

Each resume message carries an **epoch**: a counter which the client increments for every resume attempt it makes on a session. The server remembers the highest epoch it has accepted, and rejects (with a JSON error reply) any resume whose epoch is not strictly greater. The client only ever has one resume attempt outstanding at a time, so this is primarily a sanity check; but it protects against a stale attempt -- one the client has already abandoned, but whose resume message was still in flight -- arriving late and displacing a newer, working channel.

### Resumption details
The JSON handshake is used only to hand off to a new channel; it carries no stream offsets. All offset tracking and acknowledgement lives in the in-band framing protocol. This matters because a resume may be speculative (see "Choosing to resume"): the old channel can continue to carry data while the new one is being established, so any offsets exchanged during the handshake could be stale by the time they were used.

The protocol core does not see the JSON handshake, and has no notion of "resume" as such. It is handed a new channel via a "swap channel" operation: the server performs this when it accepts a resume message, and the client when it receives the server's JSON success reply. Until that point, each end continues to use the old channel as normal.

On a channel swap, each end behaves identically:
- It drops the old channel, discarding any partially-read frame.
- It rewinds its send position to its first unacknowledged offset: all sent-but-unacknowledged data moves back to the head of the pending-send buffer. The swap itself does not write anything; it only updates buffers and offsets, and the normal send path then delivers the data on the new channel like any other pending data. In particular, this means the re-send is subject to the usual backpressure mechanism. Any data written to the old channel which the peer did not receive is, by construction, unacknowledged, and so is covered by this.
- It immediately sends a frame on the new channel, even if it has no data to send (in which case the frame is zero-length, like a heartbeat). This gives the peer an up-to-date acknowledgement and receive window (see "Flow control") without waiting for the next heartbeat.
- It sets a "first frame on this channel" flag on the first frame it sends, and expects that flag on the first frame it receives. The flag is set on the first frame of **every** channel, in both directions, including the initial one, and never on any later frame. It is not strictly necessary, but serves as a consistency check for the core that the peer agrees a new channel has started.

Following a swap, each end may receive data it has already seen, either because its acknowledgements were not fully received on the old channel, or because it received the data over the old channel while the resume was in progress. The receiver is therefore briefly in a "catching up" state. This state is not tracked separately; it is determined entirely by comparing offsets:
- The first frame on a channel may start at any offset at or below the receiver's current high-water mark. While incoming frames lie below the high-water mark, the receiver discards the already-received portion, in order to prevent the application from seeing duplicate data.
- Once a frame has reached the high-water mark, the stream is resynchronized. From then on, each frame must begin exactly where the previous one ended; duplicate or out-of-order traffic results in an error and a closing of the stream.
- A frame which begins beyond the high-water mark (a gap), or an acknowledgement of data which was never sent, is an error at any time.

### Security of resumption
Resuming a connection requires access to the rendezvous socket on disk. This is a similar security model to tools like `screen` or `tmux`. No part of this system directly listens on a TCP port or outside of the machine in any way; all network access happens via `ssh` tunnels.

### Choosing to resume
The client is in charge of initiating resumption. The client has at most one resume attempt outstanding at a time; it abandons (killing the corresponding `ssh`) any failed or timed-out attempt before starting another, with a new epoch.

Resumption may be **speculative**: the client can initiate a resumption concurrently with the operation of the existing connection, and the new channel is not swapped in until it's successfully established. Establishing a new channel requires a full `ssh` handshake, which is slow relative to a transient network blip; so the old channel may well recover and carry further traffic while the resume is in progress. This is fine: the client keeps using the old channel until the new one is ready, and then switches regardless.

While a resume attempt is outstanding, the client expects the old channel to fail -- the server closes it when it switches over -- and so an EOF or error on the old channel, or the exit of the old `ssh` process, does not trigger a further resume attempt.

The client triggers a resumption:
- If the active `ssh` process exits
- If the proxy gets an EOF on the active transport socket
- If it has not seen any traffic over the transport socket in a specified timeout window

In order to support the "timeout" condition, the proxies will continually send heartbeat packets even if there's no traffic at the application layer. Heartbeat packets are just normal "data" packets with a zero-length data segment.

Heartbeat duration and timeout are configurable, but will probably default to something like "heartbeat every 10s, timeout after 20 seconds."

## Flow control
Acknowledgements and heartbeats travel in-band, on the same channel as data. If a proxy ever stopped reading from the transport -- say, because its local application had stopped reading and it had nowhere to put incoming data -- it would also stop processing acknowledgements and heartbeats. The peer would see a dead transport and resume, the new channel would stall in the same way, and data in the opposite direction would be blocked as well.

We avoid this with a receive window, which maintains the invariant that **a proxy never stops reading from the transport**: the peer is never permitted to send more data than the proxy has room to buffer.

### The receive window
Each proxy has a **receive buffer** of fixed capacity, holding data which it has received from the transport but which its local application has not yet accepted.

Every frame carries, alongside its acknowledgement, a **window**: the number of bytes currently free in the sender's receive buffer. Together these define the peer's **send limit**, `ack + window`: the peer may send data bytes only at stream offsets below it. Since the acknowledgement covers everything the proxy has received, and the window is the free space in its buffer, the limit is always equal to "offset delivered to the application, plus buffer capacity", and so never decreases.

An acknowledgement continues to mean "received by the proxy", not "delivered to the application." This allows the sender to release data from its retransmit buffer as soon as the peer holds it.

There is no dedicated window-update frame. Any frame updates the window; a proxy with no data to send uses a zero-length frame, exactly as for a heartbeat.

### Sender behavior
- The send limit starts at zero, and is updated from each frame received. No data can be sent until the first frame arrives from the peer; each end sends a frame immediately on every new channel, so this costs at most half a round trip at startup.
- Data is only ever sent below the send limit. Zero-length frames may always be sent, so acknowledgements, heartbeats, and window updates flow regardless of the window.
- Data which cannot be sent because of the window waits in the send buffer, along with sent-but-unacknowledged data. When that buffer fills, the proxy stops reading from its local fd, as described in "Proxying data."
- A frame which would decrease the send limit is a protocol error.

### Receiver behavior
- Received data is held in the receive buffer until the local application accepts it.
- A frame containing data which does not fit in the receive buffer is a protocol error. The check is made against the buffer itself, which always has at least as much room as the proxy has advertised.
- The proxy sends a window update once its limit has advanced by some threshold (a fraction of the buffer capacity, e.g. one quarter) beyond the limit it last advertised. Smaller advances are not announced on their own, to avoid trickling out tiny windows; they are carried by the next frame sent for any other reason, including the next heartbeat.

The end-to-end backpressure path is therefore: an application stops reading; its proxy's receive buffer fills and the advertised window drops to zero; the peer stops sending data, and its send buffer fills; the peer stops reading from its own application, which eventually blocks on write. Throughout, frames continue to flow in both directions, and data in the opposite direction is unaffected.

### Interaction with resumption
- The send limit is a property of the stream, not of the channel, and survives a channel swap unchanged. The data which is rewound and re-sent after a swap lies entirely below the limit already granted, so it can be sent without waiting for a new advertisement.
- Duplicate data discarded while "catching up" never enters the receive buffer, and so does not count against the window.
- Within a channel, frames are delivered reliably and in order, so a window update can only be lost if the channel itself fails. In that case the first frame on the next channel, which each end sends immediately, carries the current acknowledgement and window. No periodic window probe is required.

### Liveness
With flow control in place, the traffic timeout measures the health of the transport alone: a peer whose application has stalled still reads frames and sends heartbeats. Conversely, since the peer is always reading, a proxy which finds itself unable to write to the transport for an extended period is experiencing a real transport stall, for which a resume is the correct response.

### Sizing
- The receive buffer capacity is local configuration. The two ends need not agree on it, and it is not negotiated; each end simply advertises what it has.
- It is independent of the send buffer, which remains large in order to absorb data during an offline window.
- The receive buffer bounds throughput to roughly "capacity / round-trip time", so it should default to a few MiB, comparable to `ssh`'s own channel window.
- Frames have a modest maximum payload size (65535 bytes; see `framing.md`), so that a fresh acknowledgement and window are never stuck behind a single very large frame.

## Tunnel setup

When initiating or resuming a connection, it is necessary to exchange some out-of-band metadata, before shifting to the transport protocol.

For these interactions, the client will `ssh` to the server, and invoke the server-side binary (something like `stitch _server-shim`). The client first writes a message, and then the server side replies with exactly one message, indicating either success or failure. On success, the channel then switches over to the framed transport protocol; on failure, it shuts down. For a resume, a success reply is written by the proxy server itself (on the file descriptors passed to it by the shim), while a failure may be reported either by the proxy server (e.g. for a stale epoch) or directly by the shim (e.g. if it cannot reach the rendezvous socket).

The format for these bootstrap messages is length-delimited JSON, where the length is encoded as a 32-bit number expressed as an 8-digit hexadecimal number.

We select this framing is chosen with the goals of:
- **Length-delimited**: We need to be able to switch (within the same stream) to the framing protocol, and so it's important that the recipient not over-read the metadata message. A length-delimited protocol helps here.
- **Textual/human-readable**: A nice-to-have, but as long as we're using JSON we keep the entire metadata packet as textual for human debugging.

## Edge cases
Limits imposed by the environment `stitch` runs in (interactive authentication, server-side keepalives, `KillUserProcesses`) are described in [limitations.md](limitations.md).

### Connection shutdown
We use an approach reminiscent of TCP FIN for shutdown.

The transport layer has a "shutdown" flag, which can be set to indicate the shutdown of the sender. For the purpose of acknowledgements, the "shutdown" flag occupies a phantom byte position at the end of the byte stream -- the sender sends 1000 bytes and then shuts down, the receiver can ACK through bytes `1001` to acknowledge receipt of the shutdown.

The shutdown occupies a position in the stream, but no space in the peer's receive buffer, and so is not subject to the receive window: once all preceding data has been sent, the shutdown may be sent even if the window is zero.

A proxy acknowledges a shutdown when it receives it, but does not shut down the corresponding local fd until all preceding data in its receive buffer has been delivered to the application.

Each direction can be shutdown independently. Once either end has seen a shutdown, seen an acknowledgement of its own shutdown, and delivered the entire contents of its receive buffer to its local application, it can terminate and close down entirely.

As with TCP's `TIME_WAIT`, the end which sends the last acknowledgement cannot know that it arrived. The client exits once it has finished, closing its channel. The server, once finished, therefore lingers for a while rather than exiting at once: it keeps the channel open, and still accepts resumes, until the client closes the channel or a timeout passes. A client which missed the final acknowledgement resumes, and receives it on the new channel. Conversely, a client which has received everything it needs (its own shutdown acknowledged, and the server's shutdown received) does not treat a missing server session as an error.

### Crashed server
If the client attempts to resume, and discovers that the rendezvous socket is missing or disconnnected, it will shut down. This is implemented by the "resume" shim on the remote side writing a JSON error reply (as in "Tunnel setup") to its stdout, which will be received by the client to trigger a shutdown. This behavior means that if the server ever shuts down, the client will eventually discover this fact and exit. Similar behavior applies if the server reboots or the server process crashes.

## Open Questions/notes
- What happens if the client exists ungracefully? Does the server ever shut down automatically?
    - By design, we want to support very long-term disconnections, so timeouts are not a natural fit.
    - `mosh` prints a message on startup of future processes informing the user about possibly-orphaned sessions, but otherwise lets them be
- Forwards- and backwards- compatibility
    - The client and server may upgrade versions independently. We need to support:
    - Client initiates a connection to a newer or older server version
    - Server binary on-disk is upgraded, and server shim and a running server process are now different versions
    - Client connection lifetime is bound to the lifetime of a `stitch` process, so it's not possible for the client to change versions within the lifetime of a single connection.
    - So far there is a single protocol version, and nothing is negotiated: each JSON setup message carries a version, and a client, shim or server process which receives a version other than its own refuses it there (the server side with a JSON error reply), rather than proceeding to exchange frames it may misinterpret. In particular, a running server process refuses a resume forwarded by a newer shim without disturbing its session. Supporting the cases above across a real version change is still open.
- Local application closes its read side
    - If the local application closes the fd we deliver data to, while the peer is still sending, the receive buffer can never drain, and the peer would stall forever at a zero window.
    - The simplest answer is to discard incoming data from then on, while continuing to acknowledge it and advertise a full window. The alternative is an explicit "reset" signal which tells the peer to stop sending.

## Testing
The whole machine should be written as a pure, IO-free core, allowing us to to test as much of the machinery as possible without invoking `ssh` or any external processes. We will set up a deterministic simulation testing harness for the core protocol machine, including fuzz testing/PBT.

Flow control contributes several invariants for the harness to check: the receive buffer never exceeds its capacity; no data is ever sent beyond the send limit; and, with an application which never reads, heartbeats continue to flow in both directions and no resume is triggered.

We will also implement an e2e test harness that runs `sshd` as an unprivileged user, but we will avoid using it for anything other than minimal smoke tests, or the occasional edge case where it's necessary to test the behavior of the actual UNIX interactions.
