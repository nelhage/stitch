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
    - If `PORT` is not specified, it will be inferred based on `SSH_CONNECTION` to match the host's `ssh` server port.
    - Instead of a host/port, the user can also specify `stitch tunnel -- command...` to directly execute a command on the remote server and conect to that.
- The connection is proxied via one or more `ssh` connections to `<HOST>`. In common use, `<HOST>` and `<DEST>` will either be identical, or `<DEST>` may be `localhost`, such that you are connecting to `HOST` via itself.
    - However, setting `HOST` and `DEST` to distinct addresses can be useful in order to use `HOST` as a jump box or bastion to reach `DEST`
- In order to achieve a stable `ssh` connection, `stitch tunnel` can be used as an ssh `ProxyCommand`
    - e.g. `ssh HOST -o ProxyCommand="stitch tunnel %h 127.0.0.1:%p"`
- `stitch ssh HOST` will invoke `ssh` with `stitch` as an appropriate `ProxyCommand` and set various other `ssh` configuration options to establish a reliable tunnel.
- `stitch ssh-config` will set up `ssh` configuration such that `ssh HOST.stitch` will work for arbitrary `HOST`

# Implementation
## Connection initiation
`stitch tunnel` runs `ssh` to create an initial connection to `HOST`, and starts a pair of proxy processes, which are initially communicating with each other over the original `ssh` channel.

The server writes an initial metadata message (see section below), which contains the path to a rendezvous socket (located in `$XDG_RUNTIME_DIR` on the server, falling back to `/tmp`).

## Proxying data
The proxies communicate over the `ssh` channel. They speak a minimal "transport" protocol, which encapsulates data and acknowledges received data. Each proxy retains a buffer of sent-but-unacknowledged data for use during a resumption. The buffer is sized larger than a typical TCP send buffer to absorb flow during an extended offline window, but is fixed-size; if the buffer fills up, the proxy stops reading from the corresponding fd and relies on the underlying transport to eventually backpressure the sender.

The proxy data plane is completely symmetric between both ends; during stitch state, absent any disconnections, the client and server behave completely symmetrically. Resumption is also parallel once a new transport channel has been established, although the process of negotiating and commencing a resumption (next section) is asymmetric.

## Resumption
The client may trigger a resumption, by initiating a new `ssh` connection, and moving the underlying transport over.

In order to do so, it initiates a new ssh connection, and invokes the server-side shim, and writing a "resume" packet to its STDIN. The server-side binary connects to the rendezvous socket and forwards the resume packet, attaching stdin and stdout as `SCM_RIGHTS` auxiliary data. It then simply sleeps until the connection is closed.

Upon receipt of a resume message, the proxy server switches over to the new channel, initiating a "resume."

### Resumption details
When the server initiates a resume, it will close the old transport channel and switch to the new one. It will re-send any sent-but-unacknowledged data to the new channel, including a "resumption successful" flag in the first frame.

When the client first receives traffic over the new channel, it will swap it in in a essentially-symmetric fashion, dropping its old transport stream, resending sent-but-unacknowledged data, and then continuing.

Following resume, each end may receive duplicated data on the new "receive" fd, if its acknowledgements were not fully received on the old channel; it will discard any data up until the proper high-water-mark, in order to prevent the static side from seeing duplicate data.

Once the stream has been resynchronized, we never expect duplicated data, and duplicate or out-of-order traffic results in an error and a closing of the stream.

### Security of resumption
Resuming a connection requires access to the rendezvous socket on disk. This is a similar security model to tools like `screen` or `tmux`. No part of this system directly listens on a TCP port or outside of the machine in any way; all network access happens via `ssh` tunnels.

### Choosing to resume
The client is in charge of initiating resumption. Note that the client can initiate a resumption concurrently with the operation of the existing connection; the new channel is not swapped in until it's successfully initiated.

The client triggers a resumption:
- If the active `ssh` process exits
- If the proxy gets an EOF on the active transport socket
- If it has not seen any traffic over the transport socket in a specified timeout window

In order to support the "timeout" condition, the proxies will continually send heartbeat packets even if there's no traffic at the application layer. Heartbeat packets are just normal "data" packets with a zero-length data segment.

Heartbeat duration and timeout are configurable, but will probably default to something like "heartbeat every 30s, timeout after 60 seconds."

## Tunnel setup

When initiating or resuming a connection, it is necessary to exchange some out-of-band metadata, before shifting to the transport protocol.

For these interactions, the client will `ssh` to the server, and invoke the server-side binary (something like `stitch _server-shim`). The client first writes a message, and then the server replies. After one message in each direction, the channel switches over to the framed transport protocol (or shuts down, in the case of error).

The format for these bootstrap messages is length-delimited JSON, where the length is encoded as a 32-bit number expressed as an 8-digit hexadecimal number.

We select this framing is chosen with the goals of:
- **Length-delimited**: We need to be able to switch (within the same stream) to the framing protocol, and so it's important that the recipient not over-read the metadata message. A length-delimited protocol helps here.
- **Textual/human-readable**: A nice-to-have, but as long as we're using JSON we keep the entire metadata packet as textual for human debugging.

## Edge cases
### Connection shutdown
We use an approach reminiscent of TCP FIN for shutdown.

The transport layer has a "shutdown" flag, which can be set to indicate the shutdown of the sender. For the purpose of acknowledgements, the "shutdown" flag occupies a phantom byte position at the end of the byte stream -- the sender sends 1000 bytes and then shuts down, the receiver can ACK through bytes `1001` to acknowledge receipt of the shutdown.

Each direction can be shutdown independently. Once either end has seen both a shutdown, and an acknowledgement of its own shutdown, it can terminate and close down entirely.

### Crashed server
If the client attempts to resume, and discovers that the rendezvous socket is missing or disconnnected, it will shut down. This is implemented by the "resume" shim on the remote side writing an error frame to its stdout, which will be received by the proxy process to trigger a shutdown. This behavior means that if the server ever shuts down, the client will eventually discover this fact and exit. Similar behavior applies if the server reboots or the server process crashes.

## Open Questions/notes
- What happens if the client exists ungracefully? Does the server ever shut down automatically?
    - By design, we want to support very long-term disconnections, so timeouts are not a natural fit.
    - `mosh` prints a message on startup of future processes informing the user about possibly-orphaned sessions, but otherwise lets them be
- Forwards- and backwards- compatibility
    - The client and server may upgrade versions independently. We need to support:
    - Client initiates a connection to a newer or older server version
    - Server binary on-disk is upgraded, and server shim and a running server process are now different versions
    - Client connection lifetime is bound to the lifetime of a `stitch` process, so it's not possible for the client to change versions within the lifetime of a single connection.
- Behavior when backpressuring
    - As envisioned, the protocol sends keepalives and other metadata in-band. If one end of the connection is **alive** but has **stopped reading data**, we will not process packets and be unable to process keepalives. Is that okay? What exactly happens in that situation?

## Testing
The whole machine should be written as a pure, IO-free core, allowing us to to test as much of the machinery as possible without invoking `ssh` or any external processes. We will set up a deterministic simulation testing harness for the core protocol machine, including fuzz testing/PBT.

We will also implement an e2e test harness that runs `sshd` as an unprivileged user, but we will avoid using it for anything other than minimal smoke tests, or the occasional edge case where it's necessary to test the behavior of the actual UNIX interactions.
