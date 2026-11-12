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

Messages are framed as an 8-digit hexadecimal length (ASCII, case-insensitive on read, lowercase on write) followed by that many bytes of JSON. Messages larger than 1 MiB are rejected. Unknown JSON fields are ignored, for forwards compatibility. Every message carries `"version": 1`; the field is required.

There is exactly one protocol version so far, and no negotiation: every receiver of a message (the shim, the daemon, and the client) refuses one whose `version` is not the one it implements, before any frame is exchanged on that channel. The version covers everything which follows the handshake (see "Design notes" in `framing.md`), so carrying on with a peer of another version would only turn the first thing it does differently into a protocol error, which kills the session. The check is part of decoding (`handshake::parse`), and so cannot be forgotten by a caller:

- The version is judged before anything else in the message: a message of another version is refused as such even if it has an unknown `type`, or lacks fields we require, since another version may have changed them.
- The server (shim or daemon) refuses a request with an `error` reply whose code is `unsupported_version`, and whose message names the version received and the one supported. The client refuses a reply by failing (see "Client side").
- An `error` message is exempt: it is accepted whatever its `version`, so long as it decodes. Nothing follows an `error`, and it is how a peer of another version says that it will not talk to us; the client reports its code and message as usual. A future version must therefore keep `error` (its `type`, `version`, `code` and `message`) decodable by this one.

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

Error codes: `no_session` (rendezvous socket missing or refusing connections), `stale_epoch`, `bad_request`, `target_failed`, `internal`, `unsupported_version` (the request's `version` is not one the server implements). An unrecognised code (from a newer peer) is preserved rather than failing to parse, and is otherwise treated like any other; so a client which predates a code still reports it, and its message, to the user. The client treats an `error` reply to a resume as fatal (see "Crashed server"), with the exception described under "Client side". A resume's `ok` must name the session being resumed; the client treats one naming another session like an `error` reply.

# Destinations (`target`)

`[<DEST>:]<PORT>`: a bare port means `localhost`. IPv6 literals must be bracketed (`[::1]:22`), since otherwise the port would be ambiguous; the brackets are removed before the host is sent in the `open`. Ports must be 1–65535.

# Server side (`server`)

Rendezvous directory: `$XDG_RUNTIME_DIR/stitch` (if that is set to an absolute path), falling back to `/tmp/stitch-<uid>`; created with mode `0700`. If it already exists it must be a directory (not a symlink) owned by our effective uid, or we refuse to use it; a group- or world-accessible mode is reset to `0700`. Per session, with a random 128-bit id written as 32 lowercase hex digits: `<dir>/<id>.sock` and `<dir>/<id>.log`. Session ids received from elsewhere are validated as lowercase hex, so they are always safe as file names.

`stitch _server-shim` (invoked by `ssh` as the remote command):

1. Reads one handshake message from stdin. If its version is not supported, writes an `error` (`unsupported_version`) reply to stdout and exits, without starting or contacting any session.
2. `open`: generates a session id, binds the listening socket, and spawns the daemon (`<current_exe> _server`) with the listening socket inherited as an fd, in a new session (`setsid`), with stdin from `/dev/null` and stdout/stderr appended to the log. It then proceeds exactly as for a resume, against the new socket.
3. `resume`: connects to the socket named in the message. If that fails, writes an `error` (`no_session`) reply to stdout and exits.
4. Forwards the raw message bytes (unparsed beyond what it needed, so that a newer daemon can see fields the shim does not know about), with its stdin and stdout attached as `SCM_RIGHTS`, in a single `sendmsg`. (`fdpass` sends the length-prefixed message; the fds ride on that `sendmsg`, and any remainder it did not accept follows in plain writes.)
5. Closes its own copies of stdin and stdout, then blocks reading the Unix connection until EOF, then exits. The daemon holds the connection open for as long as it is using that channel.

`stitch _server` (the per-session daemon): accepts connections on the rendezvous socket; each delivers one message and two fds. The daemon receives them with `fdpass`'s async receiver, which works on a `tokio::net::UnixStream` through its readiness API, reads exactly one message, requires exactly the expected fds, and receives them close-on-exec. It applies no timeout of its own, so the daemon should wrap it in one.

- The first must be an `open`. The daemon connects to the target (TCP, or spawns the command), replies `ok` on the passed stdout fd (or `error`, then exits), and installs the channel.
- Thereafter, only `resume`s with an epoch strictly greater than the highest accepted are allowed; others get an `error` written to their fds, which are then closed. A message of an unsupported version gets the same treatment (`unsupported_version`), whatever else it says: this is what happens when the binary on disk is upgraded and a newer shim forwards a newer client's resume to a daemon which is still running the old version. A refused message never disturbs the session, its current channel, or its epoch. On acceptance it writes `ok`, swaps the channel, and closes the previous channel's fds and Unix connection (which lets the previous shim exit).
- When the session finishes, it *lingers* (below), and then exits, removing its socket. It exits at once on a protocol error.
- Logging goes to stderr (i.e. the session log), controlled by `STITCH_LOG`. Since that is a file rather than a terminal, the daemon's default level is `info` (it logs opens, resumes, channel losses and timeouts), where everything else defaults to `warn`.

Concrete choices:

- The shim spawns the daemon as `<current_exe> _server --listen-fd <N>`, where `N` is the listener's own fd number; `FD_CLOEXEC` is cleared on exactly that fd in a `pre_exec` hook (which also calls `setsid`). The daemon finds its socket path (and so its session id, the file stem) from the listener's address. The environment, including `STITCH_LOG` and `SSH_CONNECTION`, is inherited.
- If the `open` cannot be served by the shim (no rendezvous directory, cannot bind or spawn), the shim replies `internal`; a message which is not an `open` or `resume` gets `bad_request`. The shim checks the version itself, although the daemon would too, so that an `open` of an unsupported version does not start a daemon only for it to refuse.
- Timeouts in the daemon: 30 s for the shim which spawned it to connect, 10 s for any connection to deliver its message and fds (connections are received concurrently with the running session), 5 s to write an `error` reply, 10 s to connect to a TCP target.
- A `resume` naming a different session id gets `no_session`; a second `open` gets `bad_request`.
- The `ok` reply is written by the runtime as a *preamble* on the new channel, ahead of the first frame, so that writing it never blocks the session.
- The server's `Config::heartbeat_interval` is the `open`'s `heartbeat_ms` (the default if 0), and its `Config::timeout` is three heartbeat intervals; it only logs a timeout.
- A TCP target is connected with `TCP_NODELAY`; its write side is shut down (`shutdown(SHUT_WR)`) when the client's stream ends. A command target's stdin is closed instead. The daemon reaps a command target when it exits, but does not kill it when the session finishes.
- When the current channel is lost (EOF or error), the daemon also closes the shim's Unix connection, so that the shim exits, as `ssh` would expect of a dead session.
- The linger, the stream equivalent of TCP's `TIME_WAIT`: the session finishes once our acknowledgement of the client's shutdown has been *encoded*, but the client cannot finish until it has *received* it, and it may be lost with the channel. So once finished, the daemon keeps serving the channel (heartbeats, and reading) and keeps accepting resumes, until the channel is closed or 60 s pass (restarted by each accepted resume). A resume during the linger is handled like any other: the session sends a `FIRST` frame carrying the final acknowledgement on the new channel. The client closes the channel as soon as it has finished, which normally ends the linger at once; it also keeps the channel up while the client is still delivering the end of the stream to its stdout. If the channel is lost during the linger, the daemon exits: it cannot tell a client which has finished from a broken channel. If the client had not yet received the final acknowledgement, its resume then finds no session, and it exits non-zero (see "Client side").

# Client side (`client`)

`stitch tunnel` spawns `<ssh> -o ControlPath=none <ssh-args...> -T <HOST> <remote-stitch> _server-shim`, with the ssh child's stdin/stdout as the channel and its stderr inherited (so that authentication prompts and errors reach the user). `<ssh>` defaults to `ssh` (`--ssh`, `STITCH_SSH`); `<remote-stitch>` defaults to `stitch` (`--remote-stitch`, `STITCH_REMOTE`). Extra options are passed through with `-o`. `<HOST>` (after `--strip-suffix`) is checked before anything is run: it is refused if it is empty, starts with `-`, or contains whitespace, control characters or shell metacharacters (including the glob characters `*?[]`), so that it cannot be taken as an option by `ssh`, and a host name which only a mistake or an attack would produce fails early and clearly; `user@host` and unbracketed IPv6 literals (as `ssh` takes them) are accepted. This does not make a `ProxyCommand` with an unquoted `%h` safe (see `stitch ssh`, below): there the shell acts on the host name before `stitch` sees it. `ControlPath=none` comes first, so that it overrides both the user's configuration and any `-o`: every connection and reconnection must be a genuinely fresh `ssh` connection, never multiplexed over an existing `ControlMaster` which may not yet have noticed that its network path is dead.

- The initial open has no timeout of our own: the user may be authenticating. If it fails, `stitch tunnel` exits non-zero.
- Resume triggers, per `stitch.md`: active `ssh` exits, EOF/error on the channel, or `Session` reports a timeout. None of these trigger a further attempt while one is outstanding. An `ssh` exits only after writing out everything it received, so its exit counts only if the channel has not reached EOF 1.5 s later (some other process may hold it open); until then the client keeps reading the channel, which may yet carry the end of the session.
- A resume attempt is abandoned (its `ssh` killed) if it has not received a reply within `--connect-timeout` (default 30 s) or its `ssh` exits first. Retries back off exponentially from 1 s to 30 s, indefinitely.
- An `error` reply to a resume (or an `ok` for another session) is fatal: the client exits non-zero ("remote session is gone"). There is one exception, the other side of the `TIME_WAIT` problem: if our shutdown has been acknowledged and the peer's shutdown received, we hold the whole stream and the server holds ours; the only thing the server can be missing is our final acknowledgement, so it has almost certainly finished (e.g. its linger ended while we were still writing to a slow stdout). Then an `error` reply stops further attempts, and the client goes on delivering what it has received; once everything is delivered the session counts as finished (it needs no new channel for that), and the client exits 0. A failed attempt in that state is retried as usual meanwhile.
- A reply of an unsupported protocol version (an `ok`, or anything else which is not a decodable `error`) is refused before any frame is exchanged, and the client exits non-zero, naming the version received and the one it supports. That holds for a resume as much as for the open, and with the same exception as for an `error` reply: unlike a garbled reply or a failed `ssh`, which are retried, a version mismatch is permanent, since neither the client nor the session's daemon changes version while it runs. (By the time the client sees such an `ok`, the server has already moved the session to the new channel; the client abandons it, and the session, by exiting.) An `error` reply is reported as that error whatever its version.
- On success, the old `ssh` is killed, the new channel installed, and `swap_channel` called.
- When the session finishes, the client flushes, closes the channel, waits briefly for `ssh` to exit (killing it otherwise), and exits 0.
- `SIGHUP` does not kill the client. `ssh` sends it to its `ProxyCommand` as it exits, just after closing the proxy's stdin and stdout; dying then would lose whatever the client had not yet forwarded (such as the inner `ssh`'s disconnect message), and leave the server's session waiting for a resume forever. Instead the session carries on, and normally finishes at once (stdin is at EOF, stdout cannot be written); if it has not finished within 10 s, the client gives up and exits non-zero. A `SIGHUP` while opening the session aborts the open.
- A hangup of the whole terminal (its window closed, say) is the same thing on a larger scale: the kernel sends `SIGHUP` to every process in the terminal's foreground process group, which under `stitch ssh` is the user's `ssh`, `stitch tunnel`, and the tunnel's own `ssh`. The client needs its channel to finish the session, so it runs its `ssh` (for the open and for every resume) with `SIGHUP` ignored, a disposition which survives `exec` and which OpenSSH leaves alone. Otherwise that `ssh` would die with everything else, and the client would have to log in afresh within the 10 s, with no terminal to authenticate on; failing that, the daemon and its target would be left waiting for a resume which never comes. The `ssh` is deliberately *not* moved to a process group or session of its own: it must stay in the foreground of the controlling terminal to prompt for passwords. Since `SIGHUP` no longer ends it, the client always does: every `ssh` child is killed when dropped (an abandoned attempt, a replaced channel, an aborted open), the active one is waited for and then killed on every way out of the main loop, and if the client itself is killed, the `ssh` sees EOF on its stdin.

Concrete choices:

- The timing rules (when the triggers count, the grace period after `ssh` exits, the backoff, giving up after a hangup) live in `client::policy::ResumePolicy`, a state machine which, like `core::Session`, never reads the clock: the main loop tells it what happened, with the time, and asks it whether to start an attempt and when to wake up next.
- The resume triggers are evaluated as a level, not an edge: whenever no attempt is outstanding and no backoff delay is pending, an attempt starts if the channel has been lost, the active `ssh` exited more than the grace period ago, or the session is timed out (and the server has not been found gone after everything was received). An attempt failing therefore leads to another after the backoff only if the channel still needs replacing; events on the old channel while an attempt is outstanding start nothing.
- The backoff resets to 1 s after a successful attempt.
- Each attempt is a future owning its `ssh` child (spawned with `kill_on_drop`, and with `SIGHUP` ignored, like the first), so abandoning it is dropping it.
- "Briefly" is 3 s; the final flush is bounded by 2 s.
- The client's own stdin and stdout are taken over: they are `dup`ed and fds 0 and 1 replaced with `/dev/null`, so that closing our copy of stdout delivers EOF. Pipes and terminals are reopened through `/proc/self/fd` so that `O_NONBLOCK` stays private to us (other processes, such as the user's shell, may share the original open file description); anything else is `dup`ed, and its flags restored on exit. A closed stdin counts as empty; a closed stdout as one that cannot be written.

# Runtime (`runtime`)

`runtime::Pump` is the tokio (current-thread) driver shared by the client and the daemon. It owns a `core::Session`, the application (`runtime::App`, a pair of fds) and the current channel (a read fd and a write fd). Every fd is non-blocking and registered with tokio's reactor (`runtime::fdio::FdIo`, built on `AsyncFd`); an fd `epoll` cannot watch (a regular file, `/dev/null`) is used synchronously, since reading or writing it never blocks for long.

- `Pump::next_event()` does IO until there is something for the owner: `ChannelLost` (EOF or an error on the channel; the pump drops the channel and carries on with the application side), `TimedOut` (reported once each time the session's timeout latches; `Pump::timed_out()` gives the level), `Finished` (after closing the application and flushing output, bounded by 2 s), or `ProtocolError`. `ProtocolError` is terminal. `Finished` is reported once; the pump then carries on serving the channel side (reading, heartbeats, `ChannelLost`, and a `FIRST` frame on any new channel) for as long as the owner keeps calling it, which is what the server's linger needs. It is cancel-safe, so the owner `select!`s between it and its own work.
- `Pump::install_channel()` swaps in a new channel at any time (closing the old one) and calls `swap_channel`. A channel may carry a preamble to be written before any frame.
- Each step waits for whichever of these is possible and does one non-blocking `read`/`write`: read the channel (always); write pending output to the channel; write `app_data()` to the application; read the application (only while `send_space() > 0`); sleep until `poll_timeout()` and call `handle_timeout`. It checks `transmit_data` before every wait.
- When the session reports `app_eof()`, the application's write side is closed (`shutdown(SHUT_WR)` for a socket, since another fd refers to it for reading). EOF or an error reading the application means `close_app_send()`. An error writing to it (typically `EPIPE` or `ECONNRESET`) means `close_app_read()`.

`stitch ssh HOST [args...]` execs `ssh -o ProxyCommand="<stitch> tunnel '%h'" -o ControlPath=none -o ServerAliveInterval=0 -o TCPKeepAlive=no HOST args...`. `ControlPath=none` stops the inner ssh from piggybacking on an existing connection-sharing master (which would bypass the tunnel); the other two are because the inner ssh never sees network failures except as a stall, which must not kill it. `<stitch>` is the absolute path of the running binary, shell-quoted (`ssh` runs the `ProxyCommand` with the user's shell) and with any `%` doubled (`ssh` expands `%` tokens). `%h` is single-quoted, in both this `ProxyCommand` and the one `stitch ssh-config` writes: `ssh` substitutes the host name textually, and unquoted the shell would word-split and glob-expand it, so that `ssh '*.stitch'` in a directory containing a file named `-oProxyCommand=...;.stitch` would hand `stitch tunnel`, and through it the outer `ssh`, an option of an attacker's choosing. (Versions which wrote `%h` unquoted left such a `stitch.conf` behind; re-running `stitch ssh-config` replaces it.) `args` are passed through verbatim, including a `--` directly after `HOST`. `stitch ssh --remote-stitch=CMD HOST ...` adds `--remote-stitch=CMD` (shell-quoted, `%` doubled) to the `tunnel` command in the `ProxyCommand`; it must come before `HOST`, since everything after `HOST` belongs to `ssh`.

`stitch ssh-config` writes `~/.ssh/stitch.conf` containing a `Host *.stitch` block whose `ProxyCommand` runs `stitch tunnel --strip-suffix .stitch '%h'`, with `stitch` found on `$PATH` when `ssh` runs it (since the file is long-lived, it does not record the path of the running binary, which under e.g. Nix may later be garbage-collected; a warning is printed if `stitch` is not on `$PATH`), and which also sets `ControlPath none`, `ServerAliveInterval 0` and `TCPKeepAlive no`, as `stitch ssh` does. It ensures `Include stitch.conf` is at the top of `~/.ssh/config`:

- `~/.ssh` (mode `0700`) and the files (mode `0600`) are created if necessary; existing content and modes are preserved, files are replaced atomically, and a symlinked `~/.ssh/config` is updated through the link.
- If `~/.ssh/config` already has an `Include` of `stitch.conf` (or `~/.ssh/stitch.conf`, or its absolute path) before any `Host` or `Match` line, nothing changes. If it has one only inside a `Host` or `Match` block, nothing changes either (to avoid a duplicate), but a warning is printed.

`--print` prints the snippet instead.

# Testing

- `core`: unit tests, plus a deterministic simulation harness (`tests/sim.rs`; see below).
- `runtime`: unit tests run two pumps against each other over pipes and socket pairs, in one process, including channel swaps mid-transfer.
- `client::policy`: unit tests on a simulated clock, which pin the resume timing exactly (the grace period after `ssh` exits, the backoff, the hangup deadline), and a property test that the policy never asks to be woken in the past.
- No test's outcome may depend on how fast anything runs. Where a test must disrupt something part-way through, it holds the writer back (or the fake `ssh`, below) until the disruption has happened, rather than racing it; where it must wait for something to happen, it waits for evidence that it has (a log line, a byte count), not for a while. Real time appears only as generous bounds on waits for things which must happen (so that a bug fails a test rather than hanging it), and as the real timeouts which some process-level tests wait for to fire.
- `handshake`: unit tests, including that every message but an `error` is refused in another version. `tests/tunnel.rs` checks the refusals end to end: requests in another version sent to the shim and straight to a live session's daemon (which carries on, and can still be resumed), and replies in another version from a stand-in for the remote `stitch`.
- Process-level integration tests (`tests/tunnel.rs`) use a fake `ssh` (`tests/fake-ssh`, passed with `--ssh`), a script that ignores options and the host and runs the remote command locally, so the full client/shim/daemon path runs without `sshd`. Like a real `ssh`, it relays through intermediate processes (two `cat`s), so that killing them simulates a dropped connection and stopping them (`SIGSTOP`) a stalled one. It logs its pids, and can be told to fail, or to hang (every invocation but the first) until a file is removed, for testing retries and abandoned attempts, or to exit at once leaving the relays running (an `ssh` which exits while another process holds the channel). It sets no traps, so like OpenSSH it keeps an inherited ignored `SIGHUP`, as do its relays; the terminal hangup test relies on that, signalling the client's whole process group.
- A small e2e smoke test (`tests/e2e_sshd.rs`) runs a real `sshd` as the current user when one is available, and skips otherwise: on a free port on 127.0.0.1, with generated host and client keys, `StrictModes no` and `UsePAM no`, all in a temporary directory (as root, it also needs `/run/sshd`, which it creates). A real `ssh` reaches it through a wrapper script which passes `-F` with a matching config. The remote `stitch` is the built binary, run as `env XDG_RUNTIME_DIR=... <stitch>` (the remote command is a shell string, and `ssh` passes no environment by default). It checks a tunnel to sshd's own (inferred) port, a command round trip, a resume after killing the real `ssh`, and a nested `ssh` with `stitch tunnel` as its `ProxyCommand`.
- CI (`.github/workflows/ci.yml`) runs everything in the flake's `ci` dev shell, which adds a pinned OpenSSH to the default one, and sets `STITCH_E2E_REQUIRED`, which makes the e2e tests fail instead of skipping when `sshd` cannot be run. It runs `rustfmt` and `clippy -D warnings`, and the tests, in release mode (with debug assertions), on x86_64 and aarch64 Linux; on macOS, where the integration tests' use of `/proc` does not work, it only builds and runs `clippy`.

## Simulation harness

`tests/sim.rs` (with modules in `tests/sim/`) drives a client and a server `core::Session` through a simulated world, using only the public API, with a virtual clock (microseconds; the sessions see `Instant`s derived from it) and a seeded RNG, so that each run is fully determined by a *case*, `family:seed:size:chaos`.

- **Channels** (`sim/net.rs`): pairs of in-order pipes with latency, bandwidth, packetization (so frames arrive split anywhere) and bounded in-flight capacity (so writes are partial or blocked). A channel can be cut (each pipe delivers only what arrives before a random point, possibly mid-packet; each side notices at its own time, or never), stalled in either or both directions, or closed gracefully. The JSON handshake travels in the pipes as markers.
- **Resumption** (`sim/world.rs`): the client model follows "Choosing to resume": it resumes on channel failure or `transport_timed_out()`, with at most one attempt outstanding, connect timeouts and exponential backoff; it also makes speculative resumes on healthy channels. Attempts may be doomed (too slow, `ssh` dies, reply lost, or the request held up so long it arrives after the attempt was abandoned). The server accepts a resume if its epoch is the highest yet (otherwise it replies `stale_epoch`), swaps, and closes the old channel; the client swaps only after reading the reply (and a random delay), without reading the new channel before then. After the server exits, resumes get `no_session`.
- **Applications**: writers write a random stream in random chunks at random times; readers read at random rates, pause for minutes, never read, or close their read side; each side may shut down its sending direction, at very different times.
- **Families**: `clean` (no faults, possibly speculative resumes: everything must finish, nothing may time out), `chaos` (faults until the network heals, then everything must complete; a fifth of cases use tiny, mismatched heartbeats and timeouts so that transports time out spuriously), and `liveness` (healthy channel, one application never reads: no timeouts, no resumes, heartbeats flow, the other direction is unaffected).
- **Checks**, after every step: each application receives exactly a prefix of the peer's stream; nothing is offered after `close_app_read`; buffers stay within capacity; send limit, acknowledgements, high-water marks etc. never decrease; acknowledgements never exceed what was sent; every frame written is decoded and checked (`FIRST` exactly on the first frame of each channel in each direction, payload size, data within both the sender's send limit and the receiver's advertised limit, payload equal to the application's bytes, `SHUTDOWN` placement, monotone `ack` and `ack + window`); no protocol error; the documented contracts of the API: whenever `transmit_data` declines to produce a frame, none of the frame-sending policy's reasons holds and `poll_timeout()` is no later than the next heartbeat or delayed acknowledgement; `poll_timeout()` is in the future after `handle_timeout()`; after a swap a `FIRST` frame is ready and the timeout is cleared; `transport_timed_out()` is set exactly when nothing has been received, or output has been stuck, for the timeout; `is_finished()` implies its documented conditions; and a transport never times out on a healthy channel which the peer is using. At the end: all data delivered, and if both sides shut down, both sessions finished, except for the TIME_WAIT-like case where one side finished and exited and the other can never get its final acknowledgement (the last channel was lossy, or the other side was not, or no longer, on it).
- **Adversarial tests** (`sim/adversarial.rs`): specific peer misbehaviour, forged from honest state, is detected as the right `ProtocolError`; and fuzzing (corrupted honest traffic, nearly-valid hostile frames, arbitrary bytes) never panics, leaves a dead session dead, and never rejects an honest frame.

Running and reproducing:

- `cargo test --test sim` runs a few hundred cases per family (a few seconds). `SIM_CASES=N` overrides the count per family, `SIM_FUZZ_CASES=N` that of the fuzz tests.
- Soak tests: `SIM_SOAK_CASES=20000 cargo test --release --test sim soak -- --ignored`.
- A failure prints the case and a trace of recent events; rerun just that case with `SIM_CASE=chaos:123:50:80 cargo test --test sim replay -- --nocapture`, adding `SIM_TRACE=1` for a full trace including every frame. Proptest shrinks `size` and `chaos` with the seed fixed, so the reported case is usually a small one.
