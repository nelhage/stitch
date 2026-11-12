//! The client side: `stitch tunnel`. See "Client side" in
//! `doc/implementation.md`.
//!
//! The client opens the session over a first `ssh`, then runs a
//! [`Pump`] between its own stdin/stdout and that `ssh`'s stdout/stdin. Its
//! main loop `select!`s between the pump, the exit of the active `ssh`, an
//! outstanding resume attempt (a future which runs a new `ssh` and performs
//! the `resume` handshake), and the backoff timer between attempts.

use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use nix::sys::signal;
use tokio::process::{Child, Command};
use tokio::signal::unix::{Signal, SignalKind, signal as signal_stream};
use tokio::time::Instant;

use crate::core::Config;
use crate::handshake::{self, ErrorCode, ErrorReply, HandshakeError, Message, Target};
use crate::runtime::{App, Channel, Event, Pump};

/// Delay before retrying after the first failed resume attempt; it doubles
/// with each further failure, up to [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// The longest delay between resume attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How long to wait for `ssh` to exit by itself once the session has
/// finished, before killing it.
const EXIT_WAIT: Duration = Duration::from_secs(3);

/// How long we allow the session to finish after `SIGHUP`, before giving
/// up on it.
const HANGUP_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the channel may outlive the active `ssh` before we treat it as
/// lost. An `ssh` normally exits only after writing everything it received
/// to the channel, so we keep reading until EOF; this bounds the wait in
/// case some other process (a child of `ssh`, say) holds the channel open.
const SSH_EXIT_GRACE: Duration = Duration::from_millis(1500);

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
///   channel reaches EOF or errors, the session reports a transport timeout,
///   or the active `ssh` has exited and the channel has not reached EOF
///   within a short grace period; never while an attempt is outstanding.
/// - Abandons (kills) an attempt with no reply within `connect_timeout` or
///   whose `ssh` exits first; retries back off exponentially from 1 s to 30 s,
///   indefinitely. An `error` reply to a resume is fatal, unless we have
///   already received everything we need from the server (both shutdowns
///   are through); then we stop resuming, and finish delivering to stdout.
/// - On success, kills the old `ssh` and swaps the channel.
/// - When the session finishes, flushes, closes the channel, waits briefly
///   for `ssh` to exit (killing it otherwise), and returns `Ok(0)`.
/// - `SIGHUP` does not kill the process: the session carries on until it
///   finishes (normally at once, since stdin and stdout have been closed
///   too), for up to 10 s. It comes from `ssh`, which sends it to its
///   `ProxyCommand` as it exits, or from the terminal going away, which
///   sends it to the whole foreground process group: our `ssh` runs with
///   `SIGHUP` ignored, so that the channel survives that to carry the end
///   of the session.
pub fn run_tunnel(args: TunnelArgs) -> anyhow::Result<i32> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(Client::run(args))
}

/// The arguments (excluding `argv[0]`) with which the client runs `ssh`:
/// `-o ControlPath=none [-o OPT]... -T <host> <remote-stitch> _server-shim`.
///
/// `ControlPath=none` comes first, so that it takes precedence over both the
/// user's configuration and `--ssh-option`s: each connection must be a fresh
/// one, never multiplexed over an existing master which may itself be stuck
/// on a network path that no longer works.
fn ssh_args(args: &TunnelArgs) -> Vec<OsString> {
    let mut out: Vec<OsString> = vec!["-o".into(), "ControlPath=none".into()];
    for opt in &args.ssh_options {
        out.push("-o".into());
        out.push(opt.into());
    }
    out.push("-T".into());
    out.push(args.host.clone().into());
    out.push(args.remote_stitch.clone().into());
    out.push("_server-shim".into());
    out
}

/// Runs `ssh` to the host, sends `msg` to the shim, and reads its reply.
/// Returns the `ssh` process, the channel (its stdout and stdin), and the
/// reply.
///
/// The `ssh` runs with `SIGHUP` ignored (which OpenSSH respects: it leaves
/// an ignored `SIGHUP` alone). It shares our process group, so when the
/// terminal goes away it is sent `SIGHUP` along with us; if that killed
/// it, we could only finish the session (see [`HANGUP_TIMEOUT`]) by logging
/// in again, within seconds and with no terminal to authenticate on, and
/// failing that would leave the server's session waiting forever. It stays
/// in our process group and session all the same, since it must be able to
/// prompt on the terminal. Its lifetime is ours to manage instead: it is
/// killed when dropped, and should we die first, it sees EOF on its stdin.
async fn handshake(args: &TunnelArgs, msg: &Message) -> anyhow::Result<(Child, Channel, Message)> {
    let mut cmd = Command::new(&args.ssh);
    cmd.args(ssh_args(args))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    // SAFETY: the closure runs between `fork` and `exec`, and makes only an
    // async-signal-safe call (`signal`).
    unsafe {
        cmd.pre_exec(|| {
            signal::signal(signal::Signal::SIGHUP, signal::SigHandler::SigIgn)?;
            Ok(())
        });
    }
    let mut child = cmd
        .spawn()
        .with_context(|| format!("cannot run {}", args.ssh.to_string_lossy()))?;
    let mut stdin = child.stdin.take().expect("piped");
    let mut stdout = child.stdout.take().expect("piped");
    // If ssh has already failed, writing may fail; the read below then
    // reports it.
    if let Err(e) = handshake::write_message_async(&mut stdin, msg).await {
        log::debug!("writing to ssh: {e}");
    }
    let reply = match handshake::read_message_async(&mut stdout).await {
        Ok((_, reply)) => reply,
        Err(HandshakeError::Eof | HandshakeError::Truncated) => {
            let status = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
            return Err(match status {
                Ok(Ok(status)) => anyhow!("ssh exited ({status}) without a reply from the server"),
                _ => anyhow!("connection closed without a reply from the server"),
            });
        }
        Err(e) => return Err(e).context("bad reply from the server"),
    };
    let channel = Channel::new(stdout.into_owned_fd()?, stdin.into_owned_fd()?);
    Ok((child, channel, reply))
}

/// Why a resume attempt failed.
enum AttemptError {
    /// The server replied with an error, or answered for a different
    /// session: our session is gone.
    Rejected(ErrorReply),
    /// Anything else, such as `ssh` failing or the attempt timing out.
    Failed(anyhow::Error),
}

type AttemptResult = Result<(Child, Channel), AttemptError>;

/// One resume attempt, with the `connect_timeout` applied. Dropping it
/// abandons the attempt and kills its `ssh`.
async fn resume_attempt(
    args: TunnelArgs,
    session: String,
    socket: PathBuf,
    epoch: u64,
) -> AttemptResult {
    let msg = Message::resume(session.clone(), socket, epoch);
    let result = tokio::time::timeout(args.connect_timeout, handshake(&args, &msg)).await;
    match result {
        Err(_) => Err(AttemptError::Failed(anyhow!(
            "no reply within {:?}",
            args.connect_timeout
        ))),
        Ok(Err(e)) => Err(AttemptError::Failed(e)),
        Ok(Ok((child, channel, reply))) => match reply {
            Message::Ok(ok) if ok.session == session => Ok((child, channel)),
            // Whatever answered at our socket is not our session, and
            // trying again would only reach it again.
            Message::Ok(ok) => Err(AttemptError::Rejected(ErrorReply {
                version: ok.version,
                code: ErrorCode::NoSession,
                message: format!("the server answered for session {}", ok.session),
            })),
            Message::Error(e) => Err(AttemptError::Rejected(e)),
            other => Err(AttemptError::Failed(anyhow!(
                "unexpected reply from the server: {other:?}"
            ))),
        },
    }
}

/// Polls `fut` if there is one, and otherwise waits forever.
async fn poll_opt<F: Future + Unpin>(fut: Option<&mut F>) -> F::Output {
    match fut {
        Some(f) => f.await,
        None => std::future::pending().await,
    }
}

/// Sleeps until `at`, or forever if it is `None`.
async fn sleep_opt(at: Option<Instant>) {
    match at {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// The `ssh` process carrying the current channel.
struct ActiveSsh {
    child: Child,
    /// When the process was seen to exit.
    exited_at: Option<Instant>,
}

impl ActiveSsh {
    fn new(child: Child) -> ActiveSsh {
        ActiveSsh {
            child,
            exited_at: None,
        }
    }

    fn exited(&self) -> bool {
        self.exited_at.is_some()
    }

    /// Waits for the process to exit, or forever if it already has.
    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if self.exited() {
            std::future::pending().await
        } else {
            self.child.wait().await
        }
    }
}

/// The client's state, once the session is open.
struct Client {
    args: TunnelArgs,
    session: String,
    socket: PathBuf,
    pump: Pump,
    active: ActiveSsh,
    /// The outstanding resume attempt, if any.
    attempt: Option<Pin<Box<dyn Future<Output = AttemptResult>>>>,
    /// The epoch of the most recent resume attempt (the open is epoch 0).
    epoch: u64,
    /// The delay before the next attempt, should the current one fail.
    backoff: Duration,
    /// When the next attempt may start, after a failure.
    retry_at: Option<Instant>,
    /// The server rejected a resume after we had received everything we
    /// need from it (see [`Client::all_received`]): there is no point in
    /// trying again, so we just finish delivering to the application.
    server_gone: bool,
    /// `SIGHUP`: `ssh` sends it to its `ProxyCommand` as it exits, and a
    /// terminal which goes away sends it to its foreground process group.
    hangup: Signal,
    /// After `SIGHUP`, when we give up on finishing the session.
    hangup_deadline: Option<Instant>,
}

impl Client {
    async fn run(args: TunnelArgs) -> anyhow::Result<i32> {
        // The open has no timeout of our own: the user may be
        // authenticating.
        let heartbeat_ms = u64::try_from(args.heartbeat_interval.as_millis()).unwrap_or(u64::MAX);
        let open = Message::open(heartbeat_ms, args.target.clone());
        // `ssh` sends its ProxyCommand SIGHUP as it exits, typically just
        // after closing our stdin and stdout; and when the terminal goes
        // away, we are sent it along with the rest of the foreground
        // process group. We then still need to finish the session (send
        // what we have read, and our shutdown), so that the server does not
        // wait for us forever. Our own `ssh` ignores SIGHUP (see
        // `handshake`), so the channel is normally still there to do it on.
        let mut hangup = signal_stream(SignalKind::hangup())?;
        let (child, channel, reply) = tokio::select! {
            r = handshake(&args, &open) => r.context("cannot open the session")?,
            _ = hangup.recv() => bail!("hung up while opening the session"),
        };
        let (session, socket) = match reply {
            Message::Ok(ok) => (ok.session, ok.socket),
            Message::Error(e) => bail!("cannot open the session: {}: {}", e.code, e.message),
            other => bail!("cannot open the session: unexpected reply {other:?}"),
        };
        log::info!("opened session {session}");

        let config = Config {
            heartbeat_interval: args.heartbeat_interval,
            timeout: args.timeout,
            ..Config::default()
        };
        let mut pump = Pump::new(config, App::stdio()?);
        pump.install_channel(channel)?;
        let mut client = Client {
            args,
            session,
            socket,
            pump,
            active: ActiveSsh::new(child),
            attempt: None,
            epoch: 0,
            backoff: INITIAL_BACKOFF,
            retry_at: None,
            server_gone: false,
            hangup,
            hangup_deadline: None,
        };
        let result = client.main_loop().await;
        client.shutdown().await;
        result
    }

    /// Whether the current channel needs replacing: a resume trigger. The
    /// exit of the active `ssh` counts only once the channel has outlived it
    /// by [`SSH_EXIT_GRACE`]; until then, we keep reading what it wrote.
    fn channel_broken(&self) -> bool {
        !self.pump.has_channel()
            || self.pump.timed_out()
            || self
                .ssh_exit_deadline()
                .is_some_and(|t| t <= Instant::now())
    }

    /// When the exit of the active `ssh` becomes a resume trigger, if it has
    /// exited.
    fn ssh_exit_deadline(&self) -> Option<Instant> {
        self.active.exited_at.map(|t| t + SSH_EXIT_GRACE)
    }

    /// Whether we need nothing more from the server: it has acknowledged
    /// our shutdown (so it holds our whole stream), and we have received its
    /// shutdown (so we hold its whole stream, though perhaps not yet
    /// delivered to the application). The only thing the server may still
    /// be missing is our acknowledgement of its shutdown. If the server is
    /// gone now, it has almost certainly finished (the stream equivalent of
    /// TCP's `TIME_WAIT` problem), and losing it costs us nothing.
    fn all_received(&self) -> bool {
        let s = self.pump.session();
        s.shutdown_acked() && s.peer_shutdown().is_some()
    }

    fn start_attempt(&mut self) {
        self.epoch += 1;
        log::info!("reconnecting (epoch {})", self.epoch);
        self.attempt = Some(Box::pin(resume_attempt(
            self.args.clone(),
            self.session.clone(),
            self.socket.clone(),
            self.epoch,
        )));
    }

    async fn main_loop(&mut self) -> anyhow::Result<i32> {
        loop {
            // Resume triggers are level-triggered, but only act when no
            // attempt is outstanding and no backoff delay is pending.
            if self.attempt.is_none()
                && self.retry_at.is_none()
                && !self.server_gone
                && self.channel_broken()
            {
                self.start_attempt();
            }
            // Once the active ssh's grace period runs out, wake up to
            // re-evaluate the triggers (only if they are being evaluated, and
            // only once).
            let grace_end = self
                .ssh_exit_deadline()
                .filter(|&t| t > Instant::now())
                .filter(|_| self.attempt.is_none() && self.retry_at.is_none());
            tokio::select! {
                ev = self.pump.next_event() => match ev {
                    Event::Finished { flushed } => {
                        if !flushed {
                            log::info!(
                                "session finished, but the final acknowledgement may not have been sent"
                            );
                        }
                        return Ok(0);
                    }
                    Event::ProtocolError(e) => bail!("protocol error: {e}"),
                    Event::ChannelLost(e) => match e {
                        None => log::info!("connection closed"),
                        Some(e) => log::info!("connection failed: {e}"),
                    },
                    Event::TimedOut => log::info!("no traffic for {:?}", self.args.timeout),
                },
                status = self.active.wait() => {
                    self.active.exited_at = Some(Instant::now());
                    match status {
                        Ok(s) => log::info!("ssh exited: {s}"),
                        Err(e) => log::info!("waiting for ssh: {e}"),
                    }
                }
                r = poll_opt(self.attempt.as_mut()) => {
                    self.attempt = None;
                    self.attempt_done(r)?;
                }
                () = sleep_opt(self.retry_at), if self.attempt.is_none() => {
                    self.retry_at = None;
                }
                () = sleep_opt(grace_end) => {}
                _ = self.hangup.recv(), if self.hangup_deadline.is_none() => {
                    log::info!("hung up; finishing the session");
                    self.hangup_deadline = Some(Instant::now() + HANGUP_TIMEOUT);
                }
                () = sleep_opt(self.hangup_deadline) => {
                    bail!("hung up, and the session did not finish within {HANGUP_TIMEOUT:?}");
                }
            }
        }
    }

    /// Handles the outcome of a resume attempt. Returns an error if the
    /// client must give up.
    fn attempt_done(&mut self, r: AttemptResult) -> anyhow::Result<()> {
        match r {
            Ok((child, channel)) => {
                if let Err(e) = self.pump.install_channel(channel) {
                    // Dropping `child` kills the new ssh.
                    return self.attempt_done(Err(AttemptError::Failed(
                        anyhow::Error::new(e).context("cannot use the new channel"),
                    )));
                }
                log::info!("reconnected (epoch {})", self.epoch);
                let mut old = std::mem::replace(&mut self.active, ActiveSsh::new(child));
                if !old.exited() {
                    let _ = old.child.start_kill();
                    tokio::spawn(async move {
                        let _ = old.child.wait().await;
                    });
                }
                self.backoff = INITIAL_BACKOFF;
                self.retry_at = None;
                Ok(())
            }
            Err(AttemptError::Rejected(e)) => {
                if self.all_received() {
                    // The pump reports `Finished` once the application has
                    // taken everything: the session's last channel (as far
                    // as it knows) carried all the acknowledgements it
                    // needs.
                    log::info!(
                        "the server session has gone ({}), but everything was received",
                        e.code
                    );
                    self.server_gone = true;
                    return Ok(());
                }
                bail!("remote session is gone: {}: {}", e.code, e.message)
            }
            Err(AttemptError::Failed(e)) => {
                log::info!(
                    "reconnect attempt failed: {e:#}; retrying in {:?}",
                    self.backoff
                );
                self.retry_at = Some(Instant::now() + self.backoff);
                self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
                Ok(())
            }
        }
    }

    /// Closes everything down: the application and channel, any attempt in
    /// progress, and the active `ssh` (given a moment to exit by itself).
    async fn shutdown(self) {
        let Client {
            pump,
            attempt,
            mut active,
            ..
        } = self;
        drop(pump);
        drop(attempt);
        if !active.exited()
            && tokio::time::timeout(EXIT_WAIT, active.child.wait())
                .await
                .is_err()
        {
            log::debug!("killing ssh");
            let _ = active.child.kill().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_never_shares_a_connection() {
        let args = TunnelArgs {
            host: "example.com".into(),
            target: Target::Tcp {
                host: "localhost".into(),
                port: None,
            },
            ssh: "ssh".into(),
            ssh_options: vec!["ControlPath=/tmp/master".into(), "Port=2222".into()],
            remote_stitch: "stitch".into(),
            heartbeat_interval: Duration::from_secs(10),
            timeout: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(30),
        };
        // ssh uses the first value it sees for an option, so ours must come
        // before the user's.
        assert_eq!(
            ssh_args(&args),
            [
                "-o",
                "ControlPath=none",
                "-o",
                "ControlPath=/tmp/master",
                "-o",
                "Port=2222",
                "-T",
                "example.com",
                "stitch",
                "_server-shim",
            ]
        );
    }
}
