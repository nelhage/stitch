//! The server side: the `stitch _server-shim` invoked by `ssh`, and the
//! per-session daemon `stitch _server`. See "Server side" in
//! `doc/implementation.md`.

use std::fs::File;
use std::io::{self, Read};
use std::mem::ManuallyDrop;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::Interest;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinSet;

use crate::core::Config;
use crate::fdpass::{self, Received};
use crate::handshake::{self, ErrorCode, HandshakeError, Message, Open, Target};
use crate::paths::{Rendezvous, SessionId};
use crate::runtime::{App, Channel, Event, FdIo, Pump};
use crate::target;

/// How long the daemon waits for the shim which spawned it to connect.
const FIRST_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the daemon allows a connection on the rendezvous socket to
/// deliver its message and fds.
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the daemon allows for writing an `error` reply.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the daemon allows for connecting to a TCP target.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the daemon lingers once its session has finished, unless the
/// client closes the channel first. The client may not have received our
/// final acknowledgement (it may be lost with the channel), and then needs to
/// resume to get it: the stream equivalent of TCP's `TIME_WAIT`.
const LINGER: Duration = Duration::from_secs(60);

/// The server's transport timeout, as a multiple of the heartbeat interval.
/// The server never acts on a timeout (only the client resumes), beyond
/// logging it.
const TIMEOUT_HEARTBEATS: u32 = 3;

// ---------------------------------------------------------------------
// The shim
// ---------------------------------------------------------------------

/// A blocking, unbuffered view of one of our standard fds, which is not
/// closed when dropped.
fn std_fd(fd: RawFd) -> ManuallyDrop<File> {
    // SAFETY: fds 0 and 1 stay open for the life of the process (we replace
    // them with /dev/null rather than closing them), and ManuallyDrop means
    // this `File` never closes them.
    ManuallyDrop::new(unsafe { File::from_raw_fd(fd) })
}

/// The `error` code with which to refuse a request we could not decode:
/// `unsupported_version` if it is of a version we do not support (which
/// [`handshake::parse`] checks, for the shim and the daemon alike), and
/// `bad_request` otherwise.
fn request_error_code(e: &HandshakeError) -> ErrorCode {
    match e {
        HandshakeError::UnsupportedVersion(_) => ErrorCode::UnsupportedVersion,
        _ => ErrorCode::BadRequest,
    }
}

/// Writes an `error` reply to stdout, and returns the shim's exit code.
fn shim_error(code: ErrorCode, message: impl Into<String>) -> i32 {
    let message = message.into();
    log::warn!("{code}: {message}");
    let reply = Message::error(code, message);
    if let Err(e) = handshake::write_message(&mut *std_fd(1), &reply) {
        log::warn!("cannot write error reply: {e}");
    }
    1
}

/// Run `stitch _server-shim` (the remote command the client's `ssh` runs),
/// and return the process exit code.
///
/// Contract (from `doc/implementation.md`, "Server side"):
/// 1. Reads one handshake message from stdin (never reading past it). One
///    of a version we do not support gets an `error` (`unsupported_version`)
///    reply, before anything else is done with it.
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
    let raw = match handshake::read_raw(&mut *std_fd(0)) {
        Ok(raw) => raw,
        Err(e) => return Ok(shim_error(ErrorCode::BadRequest, e.to_string())),
    };
    let socket = match handshake::parse(&raw) {
        Ok(Message::Open(_)) => match spawn_daemon() {
            Ok(socket) => socket,
            Err(e) => {
                return Ok(shim_error(
                    ErrorCode::Internal,
                    format!("cannot start session: {e:#}"),
                ));
            }
        },
        Ok(Message::Resume(r)) => r.socket,
        Ok(other) => {
            return Ok(shim_error(
                ErrorCode::BadRequest,
                format!("unexpected {} message", message_type(&other)),
            ));
        }
        Err(e) => return Ok(shim_error(request_error_code(&e), e.to_string())),
    };

    let conn = match StdUnixStream::connect(&socket) {
        Ok(conn) => conn,
        Err(e) => {
            return Ok(shim_error(
                ErrorCode::NoSession,
                format!("cannot connect to {}: {e}", socket.display()),
            ));
        }
    };
    let wire = handshake::frame(&raw)?;
    {
        let stdin = std_fd(0);
        let stdout = std_fd(1);
        if let Err(e) = fdpass::send_with_fds(&conn, &wire, &[stdin.as_fd(), stdout.as_fd()]) {
            return Ok(shim_error(
                ErrorCode::NoSession,
                format!("cannot pass connection to {}: {e}", socket.display()),
            ));
        }
    }

    // Close our copies of the channel, so that only the daemon holds it.
    let null = File::options().read(true).write(true).open("/dev/null")?;
    nix::unistd::dup2_stdin(&null)?;
    nix::unistd::dup2_stdout(&null)?;
    drop(null);

    // The daemon holds the connection open for as long as it uses this
    // channel.
    let mut conn = conn;
    let mut buf = [0u8; 256];
    loop {
        match conn.read(&mut buf) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                log::debug!("rendezvous connection: {e}");
                break;
            }
        }
    }
    Ok(0)
}

fn message_type(m: &Message) -> &'static str {
    match m {
        Message::Open(_) => "open",
        Message::Resume(_) => "resume",
        Message::Ok(_) => "ok",
        Message::Error(_) => "error",
    }
}

/// Creates a new session: binds its rendezvous socket and spawns the daemon
/// with it. Returns the socket's path.
fn spawn_daemon() -> anyhow::Result<PathBuf> {
    let rendezvous = Rendezvous::open().context("cannot set up the rendezvous directory")?;
    let id = SessionId::generate()?;
    let socket = rendezvous.socket_path(&id);
    let log_path = rendezvous.log_path(&id);
    let listener = StdUnixListener::bind(&socket)
        .with_context(|| format!("cannot bind {}", socket.display()))?;
    let log = File::options()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(&log_path)
        .with_context(|| format!("cannot open {}", log_path.display()))?;
    let exe = std::env::current_exe().context("cannot find path to stitch")?;
    let listen_fd = listener.as_raw_fd();

    let mut cmd = Command::new(exe);
    cmd.arg("_server")
        .arg("--listen-fd")
        .arg(listen_fd.to_string())
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    // SAFETY: the closure only makes async-signal-safe system calls.
    unsafe {
        cmd.pre_exec(move || {
            nix::unistd::setsid()?;
            // Let exactly the listening socket survive exec.
            let fd = std::os::fd::BorrowedFd::borrow_raw(listen_fd);
            nix::fcntl::fcntl(
                fd,
                nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::empty()),
            )?;
            Ok(())
        });
    }
    let child = cmd.spawn().context("cannot spawn the session daemon");
    if child.is_err() {
        let _ = std::fs::remove_file(&socket);
    }
    let child = child?;
    log::info!(
        "started session {} (daemon pid {}, log {})",
        id.as_str(),
        child.id(),
        log_path.display()
    );
    // The daemon now owns the listening socket; our copy closes here, but
    // connections are queued on it regardless.
    drop(listener);
    Ok(socket)
}

// ---------------------------------------------------------------------
// The daemon
// ---------------------------------------------------------------------

/// Removes the rendezvous socket when dropped.
struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.0) {
            log::warn!("cannot remove {}: {e}", self.0.display());
        }
    }
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
///   closed, leaving the session and its channel as they were. So does any
///   message of a version we do not support (`unsupported_version`), which
///   a newer shim may forward once the binary on disk has been upgraded. On
///   acceptance: reply `ok`, swap the channel, and close the
///   previous channel's fds and Unix connection (letting the old shim exit).
/// - Once the session finishes, lingers: it keeps serving the channel, and
///   accepting resumes, until the client closes the channel or [`LINGER`]
///   passes since the session finished (or since the last resume). Then it
///   exits, removing its socket. Exits at once on a protocol error.
/// - Logs to stderr (the session log), controlled by `STITCH_LOG`.
pub fn run_daemon(listen_fd: RawFd) -> anyhow::Result<i32> {
    if listen_fd < 0
        || nix::fcntl::fcntl(
            // SAFETY: only used for this call; fcntl fails with EBADF if the fd
            // is not open.
            unsafe { std::os::fd::BorrowedFd::borrow_raw(listen_fd) },
            nix::fcntl::FcntlArg::F_GETFD,
        )
        .is_err()
    {
        bail!("--listen-fd {listen_fd} is not an open file descriptor");
    }
    // SAFETY: the fd is open (checked above), and was handed to us on the
    // command line to own; nothing else in this process refers to it.
    let fd = unsafe { OwnedFd::from_raw_fd(listen_fd) };
    nix::fcntl::fcntl(
        &fd,
        nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::FD_CLOEXEC),
    )?;
    let listener = StdUnixListener::from(fd);
    let path = listener
        .local_addr()
        .context("listening socket")?
        .as_pathname()
        .map(Path::to_owned)
        .context("listening socket has no path")?;
    let guard = SocketGuard(path.clone());
    let id = path
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| SessionId::parse(s).ok())
        .with_context(|| format!("cannot find a session id in {}", path.display()))?;
    listener.set_nonblocking(true)?;
    log::info!(
        "session {} starting (pid {})",
        id.as_str(),
        std::process::id()
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let code = rt.block_on(async {
        let listener = UnixListener::from_std(listener)?;
        Daemon::start(id, path, listener).await?.run().await
    });
    drop(guard);
    code
}

/// Receives one message and its two fds from a rendezvous connection.
async fn receive(conn: UnixStream) -> anyhow::Result<(UnixStream, Received)> {
    let received = tokio::time::timeout(
        RECEIVE_TIMEOUT,
        fdpass::recv_message_with_fds_async(&conn, 2),
    )
    .await
    .context("timed out receiving a message")??;
    Ok((conn, received))
}

/// Writes an `error` reply to `fd`, best effort.
async fn reply_error(fd: OwnedFd, code: ErrorCode, message: String) {
    log::warn!("rejecting connection: {code}: {message}");
    let msg = Message::error(code, message).encode();
    let result = async {
        let io = FdIo::new(fd, Interest::WRITABLE)?;
        tokio::time::timeout(REPLY_TIMEOUT, io.write_all(&msg))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
    }
    .await;
    if let Err(e) = result {
        log::warn!("cannot write error reply: {e}");
    }
}

/// The application side of the session: what the target turned out to be.
struct Connected {
    app: App,
    child: Option<tokio::process::Child>,
}

/// Connects to the session's target.
async fn connect_target(target: &Target) -> anyhow::Result<Connected> {
    match target {
        Target::Tcp { host, port } => {
            let port = match port {
                Some(p) => *p,
                None => target::ssh_connection_port()
                    .context("cannot infer the port from SSH_CONNECTION")?,
            };
            log::info!("connecting to {host} port {port}");
            let stream = tokio::time::timeout(
                CONNECT_TIMEOUT,
                tokio::net::TcpStream::connect((host.as_str(), port)),
            )
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))
            .and_then(|r| r)
            .with_context(|| format!("cannot connect to {host} port {port}"))?;
            let _ = stream.set_nodelay(true);
            let fd = OwnedFd::from(stream.into_std()?);
            let fd2 = fd.try_clone()?;
            Ok(Connected {
                app: App::new(Some(fd), Some(fd2))?,
                child: None,
            })
        }
        Target::Command { command } => {
            let shell = std::env::var_os("SHELL")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "/bin/sh".into());
            log::info!("running {command:?} with {}", shell.to_string_lossy());
            let mut child = tokio::process::Command::new(&shell)
                .arg("-c")
                .arg(command)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .with_context(|| format!("cannot run {}", shell.to_string_lossy()))?;
            let stdin = child.stdin.take().expect("piped").into_owned_fd()?;
            let stdout = child.stdout.take().expect("piped").into_owned_fd()?;
            Ok(Connected {
                app: App::new(Some(stdout), Some(stdin))?,
                child: Some(child),
            })
        }
    }
}

/// Waits for `child` to exit, or forever if there is none.
async fn wait_child(
    child: &mut Option<tokio::process::Child>,
) -> io::Result<std::process::ExitStatus> {
    match child {
        Some(c) => c.wait().await,
        None => std::future::pending().await,
    }
}

/// Sleeps until `at`, or forever if it is `None`.
async fn sleep_opt(at: Option<tokio::time::Instant>) {
    match at {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// The session's configuration, given the client's `open`.
fn session_config(open: &Open) -> Config {
    let heartbeat = match open.heartbeat_ms {
        0 => Config::DEFAULT_HEARTBEAT_INTERVAL,
        ms => Duration::from_millis(ms),
    };
    Config {
        heartbeat_interval: heartbeat,
        timeout: heartbeat.saturating_mul(TIMEOUT_HEARTBEATS),
        ..Config::default()
    }
}

/// A running session daemon.
struct Daemon {
    id: SessionId,
    socket: PathBuf,
    listener: UnixListener,
    pump: Pump,
    /// The rendezvous connection from the shim whose channel is in use; the
    /// shim exits once it is closed.
    conn: Option<UnixStream>,
    /// The highest epoch accepted so far (the open is epoch 0).
    epoch: u64,
    /// The command target's process, until it exits.
    child: Option<tokio::process::Child>,
    /// Once the session has finished, when the daemon stops lingering.
    linger_until: Option<tokio::time::Instant>,
}

impl Daemon {
    /// Accepts the first connection, which must carry an `open`, connects to
    /// the target, and installs the first channel.
    async fn start(
        id: SessionId,
        socket: PathBuf,
        listener: UnixListener,
    ) -> anyhow::Result<Daemon> {
        let first = async {
            let (conn, _) = listener.accept().await?;
            receive(conn).await
        };
        let (conn, received) = tokio::time::timeout(FIRST_CONNECTION_TIMEOUT, first)
            .await
            .context("timed out waiting for the first connection")??;
        let [read, write] = <[OwnedFd; 2]>::try_from(received.fds).expect("two fds");
        let open = match handshake::parse(&received.json) {
            Ok(Message::Open(open)) => open,
            Ok(other) => {
                let msg = format!("expected open, got {}", message_type(&other));
                reply_error(write, ErrorCode::BadRequest, msg.clone()).await;
                bail!("{msg}");
            }
            Err(e) => {
                reply_error(write, request_error_code(&e), e.to_string()).await;
                bail!("bad open message: {e}");
            }
        };
        log::info!(
            "open: {:?}, heartbeat {} ms",
            open.target,
            open.heartbeat_ms
        );
        let target = match connect_target(&open.target).await {
            Ok(t) => t,
            Err(e) => {
                reply_error(write, ErrorCode::TargetFailed, format!("{e:#}")).await;
                return Err(e);
            }
        };
        let mut pump = Pump::new(session_config(&open), target.app);
        let ok = Message::ok(id.as_str(), &socket).encode();
        pump.install_channel(Channel::new(read, write).with_preamble(ok))?;
        Ok(Daemon {
            id,
            socket,
            listener,
            pump,
            conn: Some(conn),
            epoch: 0,
            child: target.child,
            linger_until: None,
        })
    }

    async fn run(mut self) -> anyhow::Result<i32> {
        let mut pending: JoinSet<anyhow::Result<(UnixStream, Received)>> = JoinSet::new();
        loop {
            tokio::select! {
                ev = self.pump.next_event() => match ev {
                    Event::Finished { flushed } => {
                        log::info!(
                            "session finished{}; lingering for up to {LINGER:?} until the client disconnects",
                            if flushed { "" } else { " (final output not flushed)" },
                        );
                        self.linger_until = Some(tokio::time::Instant::now() + LINGER);
                    }
                    Event::ProtocolError(e) => {
                        log::error!("protocol error: {e}");
                        return Ok(1);
                    }
                    Event::ChannelLost(e) => {
                        match e {
                            None => log::info!("channel closed"),
                            Some(e) => log::info!("channel failed: {e}"),
                        }
                        self.conn = None;
                        if self.linger_until.is_some() {
                            // The client normally closes the channel once it
                            // has finished.
                            log::info!("exiting");
                            return Ok(0);
                        }
                    }
                    Event::TimedOut => log::info!("no traffic on the channel for {:?}", self.pump.session().config().timeout),
                },
                r = self.listener.accept() => match r {
                    Ok((conn, _)) => {
                        pending.spawn(receive(conn));
                    }
                    Err(e) => {
                        log::warn!("accept: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                Some(r) = pending.join_next(), if !pending.is_empty() => match r {
                    Ok(Ok((conn, received))) => self.handle_resume(conn, received),
                    Ok(Err(e)) => log::warn!("bad connection on the rendezvous socket: {e:#}"),
                    Err(e) => log::warn!("receive task failed: {e}"),
                },
                () = sleep_opt(self.linger_until) => {
                    log::info!("the client did not disconnect within {LINGER:?}; exiting");
                    return Ok(0);
                }
                status = wait_child(&mut self.child) => {
                    match status {
                        Ok(s) => log::info!("command exited: {s}"),
                        Err(e) => log::warn!("waiting for command: {e}"),
                    }
                    self.child = None;
                }
            }
        }
    }

    /// Checks a message received on the rendezvous socket after the open.
    fn check_resume(&self, json: &[u8]) -> Result<u64, (ErrorCode, String)> {
        let resume = match handshake::parse(json) {
            Ok(Message::Resume(r)) => r,
            Ok(other) => {
                return Err((
                    ErrorCode::BadRequest,
                    format!("expected resume, got {}", message_type(&other)),
                ));
            }
            // Including a version we do not support, which must be refused
            // here, before the session's channel is touched.
            Err(e) => return Err((request_error_code(&e), e.to_string())),
        };
        if resume.session != self.id.as_str() {
            return Err((
                ErrorCode::NoSession,
                format!(
                    "this is session {}, not {}",
                    self.id.as_str(),
                    resume.session
                ),
            ));
        }
        if resume.epoch <= self.epoch {
            return Err((
                ErrorCode::StaleEpoch,
                format!("epoch {} is not greater than {}", resume.epoch, self.epoch),
            ));
        }
        Ok(resume.epoch)
    }

    fn handle_resume(&mut self, conn: UnixStream, received: Received) {
        let [read, write] = <[OwnedFd; 2]>::try_from(received.fds).expect("two fds");
        let epoch = match self.check_resume(&received.json) {
            Ok(epoch) => epoch,
            Err((code, msg)) => {
                tokio::spawn(reply_error(write, code, msg));
                return;
            }
        };
        log::info!("resuming on epoch {epoch}");
        self.epoch = epoch;
        let ok = Message::ok(self.id.as_str(), &self.socket).encode();
        if let Err(e) = self
            .pump
            .install_channel(Channel::new(read, write).with_preamble(ok))
        {
            // The client sees its attempt fail, and tries again.
            log::warn!("cannot use the new channel: {e}");
            return;
        }
        // Replacing the previous connection closes it, which lets the
        // previous shim exit.
        self.conn = Some(conn);
        if self.linger_until.is_some() {
            // The client missed the end of the session, and is now getting
            // it on this channel.
            self.linger_until = Some(tokio::time::Instant::now() + LINGER);
        }
    }
}
