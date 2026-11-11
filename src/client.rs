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
use tokio::process::{Child, Command};
use tokio::time::Instant;

use crate::core::Config;
use crate::handshake::{self, ErrorReply, HandshakeError, Message, Target};
use crate::runtime::{App, Channel, Event, Pump};

/// Delay before retrying after the first failed resume attempt; it doubles
/// with each further failure, up to [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// The longest delay between resume attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How long to wait for `ssh` to exit by itself once the session has
/// finished, before killing it.
const EXIT_WAIT: Duration = Duration::from_secs(3);

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
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(Client::run(args))
}

/// Runs `ssh` to the host, sends `msg` to the shim, and reads its reply.
/// Returns the `ssh` process, the channel (its stdout and stdin), and the
/// reply.
async fn handshake(args: &TunnelArgs, msg: &Message) -> anyhow::Result<(Child, Channel, Message)> {
    let mut cmd = Command::new(&args.ssh);
    for opt in &args.ssh_options {
        cmd.arg("-o").arg(opt);
    }
    cmd.arg("-T")
        .arg(&args.host)
        .arg(&args.remote_stitch)
        .arg("_server-shim")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
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
    /// The server replied with an error: the session is gone.
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
    let msg = Message::resume(session, socket, epoch);
    let result = tokio::time::timeout(args.connect_timeout, handshake(&args, &msg)).await;
    match result {
        Err(_) => Err(AttemptError::Failed(anyhow!(
            "no reply within {:?}",
            args.connect_timeout
        ))),
        Ok(Err(e)) => Err(AttemptError::Failed(e)),
        Ok(Ok((child, channel, reply))) => match reply {
            Message::Ok(_) => Ok((child, channel)),
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
    exited: bool,
}

impl ActiveSsh {
    /// Waits for the process to exit, or forever if it already has.
    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if self.exited {
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
}

impl Client {
    async fn run(args: TunnelArgs) -> anyhow::Result<i32> {
        // The open has no timeout of our own: the user may be
        // authenticating.
        let heartbeat_ms = u64::try_from(args.heartbeat_interval.as_millis()).unwrap_or(u64::MAX);
        let open = Message::open(heartbeat_ms, args.target.clone());
        let (child, channel, reply) = handshake(&args, &open)
            .await
            .context("cannot open the session")?;
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
            active: ActiveSsh {
                child,
                exited: false,
            },
            attempt: None,
            epoch: 0,
            backoff: INITIAL_BACKOFF,
            retry_at: None,
        };
        let result = client.main_loop().await;
        client.shutdown().await;
        result
    }

    /// Whether the current channel needs replacing: a resume trigger.
    fn channel_broken(&self) -> bool {
        !self.pump.has_channel() || self.active.exited || self.pump.timed_out()
    }

    /// Whether everything has been delivered in both directions, so that the
    /// only thing possibly missing is the server seeing our final
    /// acknowledgement. If the server is then gone, it has almost certainly
    /// finished (the stream equivalent of TCP's `TIME_WAIT` problem).
    fn all_delivered(&self) -> bool {
        let s = self.pump.session();
        s.shutdown_acked() && s.app_eof()
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
            if self.attempt.is_none() && self.retry_at.is_none() && self.channel_broken() {
                self.start_attempt();
            }
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
                    self.active.exited = true;
                    match status {
                        Ok(s) => log::info!("ssh exited: {s}"),
                        Err(e) => log::info!("waiting for ssh: {e}"),
                    }
                }
                r = poll_opt(self.attempt.as_mut()) => {
                    self.attempt = None;
                    if let Some(code) = self.attempt_done(r)? {
                        return Ok(code);
                    }
                }
                () = sleep_opt(self.retry_at), if self.attempt.is_none() => {
                    self.retry_at = None;
                }
            }
        }
    }

    /// Handles the outcome of a resume attempt. Returns an exit code if the
    /// client should now exit successfully.
    fn attempt_done(&mut self, r: AttemptResult) -> anyhow::Result<Option<i32>> {
        match r {
            Ok((child, channel)) => {
                if let Err(e) = self.pump.install_channel(channel) {
                    // Dropping `child` kills the new ssh.
                    return self.attempt_done(Err(AttemptError::Failed(
                        anyhow::Error::new(e).context("cannot use the new channel"),
                    )));
                }
                log::info!("reconnected (epoch {})", self.epoch);
                let mut old = std::mem::replace(
                    &mut self.active,
                    ActiveSsh {
                        child,
                        exited: false,
                    },
                );
                if !old.exited {
                    let _ = old.child.start_kill();
                    tokio::spawn(async move {
                        let _ = old.child.wait().await;
                    });
                }
                self.backoff = INITIAL_BACKOFF;
                self.retry_at = None;
                Ok(None)
            }
            Err(AttemptError::Rejected(e)) => {
                if self.all_delivered() {
                    log::info!(
                        "the server session has gone ({}), but everything was delivered",
                        e.code
                    );
                    return Ok(Some(0));
                }
                bail!("remote session is gone: {}: {}", e.code, e.message)
            }
            Err(AttemptError::Failed(e)) => {
                if self.all_delivered() {
                    log::info!("cannot reconnect ({e:#}), but everything was delivered");
                    return Ok(Some(0));
                }
                log::info!(
                    "reconnect attempt failed: {e:#}; retrying in {:?}",
                    self.backoff
                );
                self.retry_at = Some(Instant::now() + self.backoff);
                self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
                Ok(None)
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
        if !active.exited
            && tokio::time::timeout(EXIT_WAIT, active.child.wait())
                .await
                .is_err()
        {
            log::debug!("killing ssh");
            let _ = active.child.kill().await;
        }
    }
}
