This document describes how `stitch` is implemented: the module layout, the boundaries between modules, and the concrete choices the design document (`stitch.md`) leaves open. `stitch.md` and `framing.md` remain the source of truth for behaviour; where this document disagrees with them, they win.

# Crate layout

A single crate with a library (`src/lib.rs`) and a thin binary (`src/main.rs`). Integration tests live in `tests/`.

| Module | Purpose | IO? |
|---|---|---|
| `frame` | Transport frame header: encode, decode, an incremental decoder. | none |
| `core` | The protocol state machine for one end of a stream: send/receive buffers, acknowledgement, flow control, channel swaps, heartbeats/timeouts, shutdown. | none — time is passed in |
| `handshake` | Length-delimited JSON setup messages (`open`, `resume`, `ok`, `error`), with blocking and async readers/writers. | generic over `Read`/`Write` |
| `target` | Parsing `[[<DEST>:]<PORT>]` and `SSH_CONNECTION`. | none |
| `paths` | Rendezvous directory and per-session socket/log paths. | filesystem |
| `fdpass` | Sending/receiving a message plus file descriptors over a Unix socket (`SCM_RIGHTS`). | yes |
| `sshconfig` | `stitch ssh` argument construction and `stitch ssh-config` file generation/installation. | filesystem |
| `runtime` | The async (tokio, current-thread) driver which connects a `core::Session` to real file descriptors. Shared by client and server. | yes |
| `client` | `stitch tunnel`: spawns `ssh`, opens the session, decides when to resume. | yes |
| `server` | `stitch _server-shim` and the per-session daemon `stitch _server`. | yes |
| `cli` | `clap` definitions and dispatch. | — |

# The protocol core (`core`)

`core::Session` is a pure state machine. It never performs IO and never reads the clock: every method which depends on time takes `now: Instant`. The surrounding driver is responsible for moving bytes between the session and file descriptors and for calling the session at the times it asks for. This is what makes deterministic simulation possible.

The session is symmetric; it has no notion of client or server, or of "resume". It is told about a new channel via `swap_channel`, which is also how the *initial* channel is installed.

The interface (the implementation, `src/core/mod.rs`, is authoritative and documents every method):

- Application side
  - `send_space()` / `write_app(&[u8]) -> usize`: accept bytes from the local application into the send buffer (bounded by `Config::send_buffer`).
  - `close_app_send()`: the local application has reached EOF; queue our `SHUTDOWN`.
  - `app_data() -> &[u8]` / `consume_app(n)`: data received from the peer awaiting delivery to the local application (`app_data` may return only a prefix; it is non-empty whenever data is pending).
  - `app_eof() -> bool`: the peer's shutdown has been received and all preceding data delivered; the driver should close the local write side.
  - `close_app_read()`: the local application can no longer accept data (e.g. `EPIPE`). Pending data is discarded; from then on, incoming data is discarded while still being acknowledged, and a full window is advertised (see "Local application closes its read side" in `stitch.md`).
- Transport side
  - `swap_channel(now)`: install a new channel (also the initial one): drop the old channel (discarding any partial frame and any unwritten output), rewind to the first unacknowledged offset, and queue an immediate `FIRST` frame. Timeouts restart from `now`.
  - `receive(&[u8], now) -> Result<(), ProtocolError>`: feed bytes read from the channel. Handles partial frames. After a protocol error the session is dead: `receive` keeps returning the error, and nothing more is transmitted.
  - `transmit_data(now) -> &[u8]` / `consume_transmit(n, now)`: encoded output for the channel. If nothing is pending and the frame-sending policy (below) calls for a frame, one frame is encoded first; output is thus produced lazily, one frame at a time, so that output not yet written can be cheaply discarded on a swap. The driver should check `transmit_data` after every event.
- Time
  - `poll_timeout() -> Option<Instant>`: the earliest instant at which `handle_timeout` would do something (heartbeat or delayed ack due, or a timeout). Deadlines already acted on are not returned again, so a driver never spins; immediate work is not reflected here, but in `transmit_data`.
  - `handle_timeout(now)`: queue a heartbeat/delayed ack if one is due and nothing is pending; detect a dead or stalled transport.
  - `transport_timed_out() -> bool`: latched by `handle_timeout` when nothing has been received on the current channel for `Config::timeout`, or pending output has made no progress for `Config::timeout`; cleared by a swap, or once traffic means the condition no longer holds. Only the client acts on this.
- `is_finished()`: our shutdown is acknowledged, the peer's is received and acknowledged in a frame on the current channel, and the receive buffer is drained; the driver flushes any pending output and exits.
- Read-only introspection for tests and diagnostics: `send_limit()`, `send_position()`, `max_sent()`, `peer_ack()`, `send_buffered()`, `recv_high_water_mark()`, `recv_buffered()`, `recv_capacity()`, `window()`, `ack_sent()`, `advertised_limit()`, `peer_shutdown()`, `channel_resynced()`, `tx_pending()`, `frames_sent()`, `error()`, and others.

After a channel swap, if the peer's first acknowledgement on the new channel covers data which has been rewound but not yet re-sent (because the peer received it on the old channel), the send position skips forward to that acknowledgement rather than re-sending data the peer already holds. Rule 3 of `framing.md` permits this, since the frame then starts exactly at the receiver's high-water mark.

In addition to the rules in `framing.md`, a frame whose `SHUTDOWN` contradicts what has already been received (a shutdown before data already received, or at a different position from a shutdown already received) is a protocol error. For rule 6, "sent" means encoded into the transmit output, whether or not it was then written.

## Frame-sending policy

A frame is sent when any of the following holds:

- there is data (or a pending shutdown) at the send position, below the send limit (shutdown is exempt from the limit);
- a new channel was installed and its `FIRST` frame has not been sent;
- a `SHUTDOWN` was received and has not yet been acknowledged (acknowledge immediately);
- the send limit we would advertise (`delivered + capacity`) exceeds the one last advertised by at least `Config::window_update_threshold` (default ¼ of the receive buffer);
- received-but-unacknowledged data exceeds `Config::ack_threshold`, or has been waiting for `Config::ack_delay`;
- `Config::heartbeat_interval` has elapsed since the last frame was sent.

## Defaults

| Setting | Default |
|---|---|
| send buffer | 16 MiB |
| receive buffer | 4 MiB |
| max frame payload | 65535 |
| heartbeat interval | 10 s |
| timeout | 20 s |
| window update threshold | receive buffer / 4 |
| ack threshold | 256 KiB |
| ack delay | 200 ms |

# Setup handshake (`handshake`)

Messages are framed as an 8-digit hexadecimal length (ASCII, case-insensitive on read, lowercase on write) followed by that many bytes of JSON. Messages larger than 1 MiB are rejected. Unknown JSON fields are ignored, for forwards compatibility. Every message carries `"version": 1`; the field is required, but its value is left to the receiver to judge.

The readers read exactly the 8-byte prefix and then exactly the body, and so never consume any of the frame data which follows; they must not be given a buffering reader. They return the raw JSON alongside the parsed message, so that the shim can forward it verbatim. EOF before the first byte is reported distinctly from EOF part-way through a message.

Client → server:

```json
{"type":"open","version":1,"heartbeat_ms":10000,
 "target":{"kind":"tcp","host":"localhost","port":22}}
{"type":"open","version":1,"heartbeat_ms":10000,
 "target":{"kind":"tcp","host":"localhost","port":null}}
{"type":"open","version":1,"heartbeat_ms":10000,
 "target":{"kind":"command","command":"echo hi"}}
{"type":"resume","version":1,"session":"<id>","socket":"/run/user/1000/stitch/<id>.sock","epoch":3}
```

- `port: null` means "infer from `SSH_CONNECTION`" (the server's `sshd` port), evaluated on the server.
- A `command` target is run as `$SHELL -c <command>` (falling back to `/bin/sh`), mirroring how `ssh` runs a remote command; the client joins its `-- command...` words with spaces, as `ssh` does. Its stderr goes to the session log.
- `heartbeat_ms` tells the server how often to heartbeat, since the client's timeout depends on it.
- The initial open is epoch 0; the client's first resume uses epoch 1.

Server → client:

```json
{"type":"ok","version":1,"session":"<id>","socket":"/run/user/1000/stitch/<id>.sock"}
{"type":"error","version":1,"code":"no_session","message":"..."}
```

Error codes: `no_session` (rendezvous socket missing or refusing connections), `stale_epoch`, `bad_request`, `target_failed`, `internal`. An unrecognised code (from a newer peer) is preserved rather than failing to parse. The client treats an `error` reply to a resume as fatal (see "Crashed server").

# Destinations (`target`)

`[<DEST>:]<PORT>`: a bare port means `localhost`. IPv6 literals must be bracketed (`[::1]:22`), since otherwise the port would be ambiguous; the brackets are removed before the host is sent in the `open`. Ports must be 1–65535.

# Server side (`server`)

Rendezvous directory: `$XDG_RUNTIME_DIR/stitch` (if that is set to an absolute path), falling back to `/tmp/stitch-<uid>`; created with mode `0700`. If it already exists it must be a directory (not a symlink) owned by our effective uid, or we refuse to use it; a group- or world-accessible mode is reset to `0700`. Per session, with a random 128-bit id written as 32 lowercase hex digits: `<dir>/<id>.sock` and `<dir>/<id>.log`. Session ids received from elsewhere are validated as lowercase hex, so they are always safe as file names.

`stitch _server-shim` (invoked by `ssh` as the remote command):

1. Reads one handshake message from stdin.
2. `open`: generates a session id, binds the listening socket, and spawns the daemon (`<current_exe> _server`) with the listening socket inherited as an fd, in a new session (`setsid`), with stdin from `/dev/null` and stdout/stderr appended to the log. It then proceeds exactly as for a resume, against the new socket.
3. `resume`: connects to the socket named in the message. If that fails, writes an `error` (`no_session`) reply to stdout and exits.
4. Forwards the raw message bytes (unparsed beyond what it needed, so that a newer daemon can see fields the shim does not know about), with its stdin and stdout attached as `SCM_RIGHTS`, in a single `sendmsg`. (`fdpass` sends the length-prefixed message; the fds ride on that `sendmsg`, and any remainder it did not accept follows in plain writes.)
5. Closes its own copies of stdin and stdout, then blocks reading the Unix connection until EOF, then exits. The daemon holds the connection open for as long as it is using that channel.

`stitch _server` (the per-session daemon): accepts connections on the rendezvous socket; each delivers one message and two fds. The daemon receives them with `fdpass`'s async receiver, which works on a `tokio::net::UnixStream` through its readiness API, reads exactly one message, requires exactly the expected fds, and receives them close-on-exec. It applies no timeout of its own, so the daemon should wrap it in one.

- The first must be an `open`. The daemon connects to the target (TCP, or spawns the command), replies `ok` on the passed stdout fd (or `error`, then exits), and installs the channel.
- Thereafter, only `resume`s with an epoch strictly greater than the highest accepted are allowed; others get an `error` written to their fds, which are then closed. On acceptance it writes `ok`, swaps the channel, and closes the previous channel's fds and Unix connection (which lets the previous shim exit).
- It exits when the session is finished (or on a protocol error), removing its socket.
- Logging goes to stderr (i.e. the session log), controlled by `STITCH_LOG`.

# Client side (`client`)

`stitch tunnel` spawns `<ssh> <ssh-args...> -T <HOST> <remote-stitch> _server-shim`, with the ssh child's stdin/stdout as the channel and its stderr inherited (so that authentication prompts and errors reach the user). `<ssh>` defaults to `ssh` (`--ssh`, `STITCH_SSH`); `<remote-stitch>` defaults to `stitch` (`--remote-stitch`, `STITCH_REMOTE`). Extra options are passed through with `-o`.

- The initial open has no timeout of our own: the user may be authenticating. If it fails, `stitch tunnel` exits non-zero.
- Resume triggers, per `stitch.md`: active `ssh` exits, EOF/error on the channel, or `Session` reports a timeout. None of these trigger a further attempt while one is outstanding.
- A resume attempt is abandoned (its `ssh` killed) if it has not received a reply within `--connect-timeout` (default 30 s) or its `ssh` exits first. Retries back off exponentially from 1 s to 30 s, indefinitely.
- An `error` reply to a resume is fatal: the client exits non-zero.
- On success, the old `ssh` is killed, the new channel installed, and `swap_channel` called.
- When the session finishes, the client flushes, closes the channel, waits briefly for `ssh` to exit (killing it otherwise), and exits 0.

`stitch ssh HOST [args...]` execs `ssh -o ProxyCommand='<stitch> tunnel %h' -o ServerAliveInterval=0 -o TCPKeepAlive=no HOST args...` (the inner ssh never sees network failures except as a stall, which must not kill it). `<stitch>` is the absolute path of the running binary, shell-quoted (`ssh` runs the `ProxyCommand` with the user's shell) and with any `%` doubled (`ssh` expands `%` tokens). `args` are passed through verbatim, including a `--` directly after `HOST`.

`stitch ssh-config` writes `~/.ssh/stitch.conf` containing a `Host *.stitch` block whose `ProxyCommand` runs `<stitch> tunnel --strip-suffix .stitch %h` (with `<stitch>` as for `stitch ssh`), and which also sets `ServerAliveInterval 0` and `TCPKeepAlive no`, as `stitch ssh` does. It ensures `Include stitch.conf` is at the top of `~/.ssh/config`:

- `~/.ssh` (mode `0700`) and the files (mode `0600`) are created if necessary; existing content and modes are preserved, files are replaced atomically, and a symlinked `~/.ssh/config` is updated through the link.
- If `~/.ssh/config` already has an `Include` of `stitch.conf` (or `~/.ssh/stitch.conf`, or its absolute path) before any `Host` or `Match` line, nothing changes. If it has one only inside a `Host` or `Match` block, nothing changes either (to avoid a duplicate), but a warning is printed.

`--print` prints the snippet instead.

# Testing

- `core`: unit tests, plus a deterministic simulation harness (`tests/sim.rs`; see below).
- Process-level integration tests use a fake `ssh` (`--ssh` pointing at a script that runs the remote command locally), so the full client/shim/daemon path runs without `sshd`. Killing the fake `ssh` simulates a dropped connection.
- A small e2e smoke test runs a real unprivileged `sshd` when one is available, and skips otherwise.

## Simulation harness

`tests/sim.rs` (with modules in `tests/sim/`) drives a client and a server `core::Session` through a simulated world, using only the public API, with a virtual clock (microseconds; the sessions see `Instant`s derived from it) and a seeded RNG, so that each run is fully determined by a *case*, `family:seed:size:chaos`.

- **Channels** (`sim/net.rs`): pairs of in-order pipes with latency, bandwidth, packetization (so frames arrive split anywhere) and bounded in-flight capacity (so writes are partial or blocked). A channel can be cut (each pipe delivers only what arrives before a random point, possibly mid-packet; each side notices at its own time, or never), stalled in either or both directions, or closed gracefully. The JSON handshake travels in the pipes as markers.
- **Resumption** (`sim/world.rs`): the client model follows "Choosing to resume": it resumes on channel failure or `transport_timed_out()`, with at most one attempt outstanding, connect timeouts and exponential backoff; it also makes speculative resumes on healthy channels. Attempts may be doomed (too slow, `ssh` dies, reply lost, or the request held up so long it arrives after the attempt was abandoned). The server accepts a resume if its epoch is the highest yet (otherwise it replies `stale_epoch`), swaps, and closes the old channel; the client swaps only after reading the reply (and a random delay), without reading the new channel before then. After the server exits, resumes get `no_session`.
- **Applications**: writers write a random stream in random chunks at random times; readers read at random rates, pause for minutes, never read, or close their read side; each side may shut down its sending direction, at very different times.
- **Families**: `clean` (no faults, possibly speculative resumes: everything must finish, nothing may time out), `chaos` (faults until the network heals, then everything must complete; a fifth of cases use tiny, mismatched heartbeats and timeouts so that transports time out spuriously), and `liveness` (healthy channel, one application never reads: no timeouts, no resumes, heartbeats flow, the other direction is unaffected).
- **Checks**, after every step: each application receives exactly a prefix of the peer's stream; nothing is offered after `close_app_read`; buffers stay within capacity; send limit, acknowledgements, high-water marks etc. never decrease; acknowledgements never exceed what was sent; every frame written is decoded and checked (`FIRST` exactly on the first frame of each channel in each direction, payload size, data within both the sender's send limit and the receiver's advertised limit, payload equal to the application's bytes, `SHUTDOWN` placement, monotone `ack` and `ack + window`); no protocol error; `poll_timeout()` is in the future after `handle_timeout()`; a transport never times out on a healthy channel which the peer is using. At the end: all data delivered, and if both sides shut down, both sessions finished, except for the TIME_WAIT-like case where one side finished and exited and the other can never get its final acknowledgement (the last channel was lossy, or the other side was not, or no longer, on it).
- **Adversarial tests** (`sim/adversarial.rs`): specific peer misbehaviour, forged from honest state, is detected as the right `ProtocolError`; and fuzzing (corrupted honest traffic, nearly-valid hostile frames, arbitrary bytes) never panics, leaves a dead session dead, and never rejects an honest frame.

Running and reproducing:

- `cargo test --test sim` runs a few hundred cases per family (a few seconds). `SIM_CASES=N` overrides the count per family, `SIM_FUZZ_CASES=N` that of the fuzz tests.
- Soak tests: `SIM_SOAK_CASES=20000 cargo test --release --test sim soak -- --ignored`.
- A failure prints the case and a trace of recent events; rerun just that case with `SIM_CASE=chaos:123:50:80 cargo test --test sim replay -- --nocapture`, adding `SIM_TRACE=1` for a full trace including every frame. Proptest shrinks `size` and `chaos` with the seed fixed, so the reported case is usually a small one.
