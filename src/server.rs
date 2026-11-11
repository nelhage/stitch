//! The server side: the `stitch _server-shim` invoked by `ssh`, and the
//! per-session daemon `stitch _server`. See "Server side" in
//! `doc/implementation.md`.

use std::os::fd::RawFd;

/// Run `stitch _server-shim` (the remote command the client's `ssh` runs),
/// and return the process exit code.
///
/// Contract (from `doc/implementation.md`, "Server side"):
/// 1. Reads one handshake message from stdin (never reading past it).
/// 2. `open`: generates a session id, binds `<rendezvous>/<id>.sock`, and
///    spawns `<current_exe> _server --listen-fd <N>` with the listening socket
///    inherited, in a new session (`setsid`), stdin from `/dev/null`, and
///    stdout/stderr appended to `<rendezvous>/<id>.log`. Then proceeds as for
///    a resume, against the new socket.
/// 3. `resume`: connects to the socket named in the message; on failure,
///    writes an `error` (`no_session`) reply to stdout and exits.
/// 4. Forwards the raw message bytes, with its stdin and stdout attached via
///    `SCM_RIGHTS`, in a single `sendmsg` (`fdpass::send_with_fds`).
/// 5. Closes its own stdin/stdout, blocks reading the Unix connection until
///    EOF (the daemon holds it open while it uses this channel), and exits.
pub fn run_shim() -> anyhow::Result<i32> {
    anyhow::bail!("not yet implemented")
}

/// Run the per-session daemon `stitch _server`, which accepts connections on
/// the inherited listening socket `listen_fd`, and return the process exit
/// code.
///
/// Contract (from `doc/implementation.md`, "Server side"): each connection
/// delivers one message and two fds (`fdpass::recv_message_with_fds_async`).
/// - The first must be an `open`: connect to the target (TCP, or
///   `$SHELL -c <command>`), reply `ok` on the passed stdout (or `error`, then
///   exit), and install the channel.
/// - Thereafter, only `resume`s with an epoch strictly greater than the
///   highest accepted are allowed; others get an `error` reply and their fds
///   closed. On acceptance: reply `ok`, swap the channel, and close the
///   previous channel's fds and Unix connection (letting the old shim exit).
/// - Exits when the session finishes (or on a protocol error), removing its
///   socket. Logs to stderr (the session log), controlled by `STITCH_LOG`.
pub fn run_daemon(listen_fd: RawFd) -> anyhow::Result<i32> {
    let _ = listen_fd;
    anyhow::bail!("not yet implemented")
}
