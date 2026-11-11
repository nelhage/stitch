//! The client side: `stitch tunnel`. See "Client side" in
//! `doc/implementation.md`.

use std::ffi::OsString;
use std::time::Duration;

use crate::handshake::Target;

/// Fully-resolved configuration for `stitch tunnel`, produced by the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunnelArgs {
    /// The host to `ssh` to, with any `--strip-suffix` already removed.
    pub host: String,
    /// What the server should connect the session to (sent in the `open`).
    pub target: Target,
    /// The `ssh` program to run (`--ssh` / `STITCH_SSH`).
    pub ssh: OsString,
    /// Extra `ssh` options, each to be passed as `-o <OPT>`.
    pub ssh_options: Vec<String>,
    /// The remote `stitch` command (`--remote-stitch` / `STITCH_REMOTE`). It is
    /// placed on `ssh`'s command line, so the remote shell interprets it.
    pub remote_stitch: String,
    /// How often each end sends a heartbeat; sent to the server as
    /// `heartbeat_ms`.
    pub heartbeat_interval: Duration,
    /// How long without traffic before the client resumes.
    pub timeout: Duration,
    /// How long a resume attempt may take before it is abandoned.
    pub connect_timeout: Duration,
}

/// Run `stitch tunnel`, connecting stdin/stdout to the remote target, and
/// return the process exit code.
///
/// Contract (from `doc/implementation.md`, "Client side"):
/// - Spawns `<ssh> [-o OPT]... -T <host> <remote_stitch> _server-shim`, using
///   the child's stdin/stdout as the channel and inheriting its stderr. Sends
///   an `open` (epoch 0) and waits, with no timeout of its own, for the reply;
///   if the open fails, returns an error (non-zero exit).
/// - Resumes (new `ssh`, `resume` message with the next epoch) when the
///   active `ssh` exits, the channel reaches EOF or errors, or the session
///   reports a transport timeout; never while an attempt is outstanding.
/// - Abandons (kills) an attempt with no reply within `connect_timeout` or
///   whose `ssh` exits first; retries back off exponentially from 1 s to 30 s,
///   indefinitely. An `error` reply to a resume is fatal.
/// - On success, kills the old `ssh` and swaps the channel.
/// - When the session finishes, flushes, closes the channel, waits briefly
///   for `ssh` to exit (killing it otherwise), and returns `Ok(0)`.
pub fn run_tunnel(args: TunnelArgs) -> anyhow::Result<i32> {
    let _ = args;
    anyhow::bail!("not yet implemented")
}
