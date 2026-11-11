//! The async driver which connects a [`core::Session`](crate::core::Session)
//! to real file descriptors. Shared by the client and the server.
//!
//! A [`Pump`] owns a session, the local *application* (the client's own
//! stdin/stdout; on the server, a TCP connection or a child process), and the
//! current *channel* (a read fd and a write fd: on the client, an `ssh`
//! child's stdout and stdin; on the server, the fds passed by the shim). It
//! moves bytes between them, following the session's flow control:
//!
//! - the application is read only while the session has send space;
//! - the channel is always read (the session's receive window guarantees
//!   room for whatever the peer may send);
//! - data for the application is written whenever the session has some, and
//!   the application's write side is shut down once the peer's stream ends;
//! - heartbeats, acknowledgements and timeouts are scheduled from
//!   [`Session::poll_timeout`](crate::core::Session::poll_timeout).
//!
//! The pump is driven by its owner calling [`Pump::next_event`], which does
//! IO until something happens that the owner must decide about (the channel
//! failed, the transport timed out, the session finished or hit a protocol
//! error). `next_event` is cancel-safe, so the owner can `select!` between it
//! and its own work (a resume attempt, accepting connections, ...), and swap
//! in a new channel with [`Pump::install_channel`] at any time.
//!
//! Once the session has finished, the pump closes the application, but goes
//! on serving the channel side (including any new channel) for as long as
//! its owner keeps calling `next_event`: the server uses this to linger in
//! case the client missed the final acknowledgement.
//!
//! Everything runs on a single-threaded tokio runtime; no fd is ever read or
//! written in blocking mode (see [`fdio`]).

pub mod fdio;
#[cfg(test)]
mod tests;

use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::time::{Duration, Instant};

use tokio::io::Interest;

use crate::core::{Config, ProtocolError, Session};
pub use fdio::FdIo;

/// Size of the buffer used for each read.
const READ_BUF: usize = 64 << 10;

/// How long [`Pump::next_event`] spends flushing the last output once the
/// session has finished.
pub const FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// The local application, as a pair of fds.
#[derive(Debug)]
pub struct App {
    input: Option<FdIo>,
    output: Option<FdIo>,
}

impl App {
    /// An application this process owns exclusively: `input` is read for
    /// data to send, `output` written with data received. They may refer to
    /// the same socket (e.g. two `dup`s of a TCP connection); when the peer's
    /// stream ends, a socket's write side is shut down rather than merely
    /// closed. Either may be `None` if the application has no such side.
    ///
    /// Must be called within a tokio runtime.
    pub fn new(input: Option<OwnedFd>, output: Option<OwnedFd>) -> io::Result<App> {
        Ok(App {
            input: input
                .map(|fd| FdIo::new(fd, Interest::READABLE))
                .transpose()?,
            output: output
                .map(|fd| FdIo::new(fd, Interest::WRITABLE))
                .transpose()?,
        })
    }

    /// This process's own stdin and stdout. Takes them over: fds 0 and 1 are
    /// replaced with `/dev/null`, so that closing our copy of stdout really
    /// does signal EOF to whoever reads it. Where possible, the fds are
    /// reopened so that putting them in non-blocking mode does not affect
    /// other processes sharing them (see [`FdIo::new_shared`]).
    ///
    /// A closed stdin is treated as empty, and a closed stdout as one which
    /// cannot be written. Must be called within a tokio runtime.
    pub fn stdio() -> io::Result<App> {
        let take = |fd: i32| -> io::Result<Option<OwnedFd>> {
            match nix::fcntl::fcntl(
                // SAFETY: only used for the duration of this call; if fd is
                // not open, fcntl fails with EBADF.
                unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) },
                nix::fcntl::FcntlArg::F_DUPFD_CLOEXEC(3),
            ) {
                Ok(new) => {
                    // SAFETY: F_DUPFD_CLOEXEC returned a new fd we own.
                    Ok(Some(unsafe {
                        <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(new)
                    }))
                }
                Err(nix::errno::Errno::EBADF) => Ok(None),
                Err(e) => Err(e.into()),
            }
        };
        let input = take(0)?;
        let output = take(1)?;
        let null = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")?;
        nix::unistd::dup2_stdin(&null)?;
        nix::unistd::dup2_stdout(&null)?;
        Ok(App {
            input: input
                .map(|fd| FdIo::new_shared(fd, Interest::READABLE))
                .transpose()?,
            output: output
                .map(|fd| FdIo::new_shared(fd, Interest::WRITABLE))
                .transpose()?,
        })
    }
}

/// A channel to install with [`Pump::install_channel`].
#[derive(Debug)]
pub struct Channel {
    read: OwnedFd,
    write: OwnedFd,
    preamble: Vec<u8>,
}

impl Channel {
    /// A channel which reads frames from `read` and writes them to `write`
    /// (which may be the same socket, `dup`ed).
    pub fn new(read: OwnedFd, write: OwnedFd) -> Channel {
        Channel {
            read,
            write,
            preamble: Vec::new(),
        }
    }

    /// Bytes to write to the channel before any frame: the server uses this
    /// for its `ok` handshake reply, which must directly precede its first
    /// frame.
    pub fn with_preamble(mut self, preamble: Vec<u8>) -> Channel {
        self.preamble = preamble;
        self
    }
}

/// The installed channel, with its fds registered.
struct ActiveChannel {
    read: FdIo,
    write: FdIo,
    /// The part of the preamble not yet written.
    preamble: Vec<u8>,
}

/// Something the owner of a [`Pump`] must know about.
#[derive(Debug)]
pub enum Event {
    /// The current channel reached EOF (`None`) or failed with an error. The
    /// pump has dropped it (closing its fds) and carries on with the
    /// application side until a new channel is installed; the session itself
    /// is unaffected.
    ChannelLost(Option<io::Error>),
    /// The session reports that the transport has timed out (see
    /// [`Session::transport_timed_out`]): nothing received, or no output
    /// progress, for [`Config::timeout`]. Reported once each time the
    /// condition arises; [`Pump::timed_out`] gives the current state.
    TimedOut,
    /// The session has finished: both directions are shut down and all data
    /// delivered. The application's fds are closed, and pending output has
    /// been written to the channel; `flushed` is false if that failed or did
    /// not complete within [`FLUSH_TIMEOUT`].
    ///
    /// Reported once. The pump then goes on serving the channel side: it
    /// reads the channel, sends heartbeats, reports [`Event::ChannelLost`],
    /// and on a newly installed channel sends a `FIRST` frame carrying the
    /// final acknowledgement, for as long as the owner wants.
    Finished { flushed: bool },
    /// The peer violated the protocol; the session is dead. Terminal: it is
    /// returned again by every later call.
    ProtocolError(ProtocolError),
}

/// Drives a [`Session`] between an application and a channel. See the
/// [module documentation](self).
pub struct Pump {
    session: Session,
    app_in: Option<FdIo>,
    app_out: Option<FdIo>,
    channel: Option<ActiveChannel>,
    buf: Box<[u8]>,
    /// Whether the current timeout condition has been reported.
    timeout_reported: bool,
    /// Whether [`Event::Finished`] has been reported.
    finished_reported: bool,
}

/// Waits for `io` to become ready if `enabled`, and otherwise forever.
async fn ready(io: Option<&FdIo>, enabled: bool) -> io::Result<fdio::Ready<'_>> {
    match io {
        Some(io) if enabled => io.ready().await,
        _ => std::future::pending().await,
    }
}

/// Sleeps until `deadline`, or forever if there is none.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d.into()).await,
        None => std::future::pending().await,
    }
}

/// Whether a write error means the reader has gone away.
fn is_disconnect(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
    )
}

impl Pump {
    /// A pump for a new session with `config`, connected to `app`, with no
    /// channel yet. An application without input is treated as having
    /// reached EOF, and one without output as having closed its read side.
    pub fn new(config: Config, app: App) -> Pump {
        let mut session = Session::new(config);
        if app.input.is_none() {
            session.close_app_send();
        }
        if app.output.is_none() {
            session.close_app_read();
        }
        Pump {
            session,
            app_in: app.input,
            app_out: app.output,
            channel: None,
            buf: vec![0; READ_BUF].into_boxed_slice(),
            timeout_reported: false,
            finished_reported: false,
        }
    }

    /// The session, for inspection.
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Whether a channel is installed (and has not been lost since).
    pub fn has_channel(&self) -> bool {
        self.channel.is_some()
    }

    /// Whether the session currently considers the transport timed out.
    pub fn timed_out(&self) -> bool {
        self.session.transport_timed_out()
    }

    /// Installs `channel`, replacing (and closing) the current one if any,
    /// and tells the session to swap to it. Must be called within a tokio
    /// runtime.
    pub fn install_channel(&mut self, channel: Channel) -> io::Result<()> {
        let active = ActiveChannel {
            read: FdIo::new(channel.read, Interest::READABLE)?,
            write: FdIo::new(channel.write, Interest::WRITABLE)?,
            preamble: channel.preamble,
        };
        log::debug!(
            "installing channel (read fd {}, write fd {})",
            active.read.as_fd().as_raw_fd(),
            active.write.as_fd().as_raw_fd()
        );
        self.channel = Some(active);
        self.session.swap_channel(Instant::now());
        self.timeout_reported = false;
        Ok(())
    }

    /// Drops the current channel, if any, closing its fds. The session is
    /// not told; it carries on as if the channel were merely silent.
    pub fn drop_channel(&mut self) {
        self.channel = None;
    }

    fn lose_channel(&mut self, err: Option<io::Error>) -> Event {
        self.channel = None;
        Event::ChannelLost(err)
    }

    fn close_app_output(&mut self) {
        if let Some(out) = self.app_out.take() {
            log::debug!("closing the application's write side");
            out.shutdown_write();
        }
    }

    /// Does IO until there is an [`Event`] for the owner, and returns it.
    ///
    /// Cancel-safe: if the future is dropped before completing, no data is
    /// lost, and calling it again carries on. After [`Event::ProtocolError`],
    /// it returns that event again immediately. After [`Event::Finished`],
    /// it carries on serving the channel (see there).
    pub async fn next_event(&mut self) -> Event {
        loop {
            if let Some(e) = self.session.error() {
                return Event::ProtocolError(e.clone());
            }
            if self.session.app_eof() {
                self.close_app_output();
            }
            if !self.finished_reported && self.session.is_finished() {
                self.close_app_output();
                self.app_in = None;
                let flushed = tokio::time::timeout(FLUSH_TIMEOUT, self.flush())
                    .await
                    .unwrap_or(false);
                self.finished_reported = true;
                return Event::Finished { flushed };
            }
            if self.session.transport_timed_out() {
                if !self.timeout_reported {
                    self.timeout_reported = true;
                    return Event::TimedOut;
                }
            } else {
                self.timeout_reported = false;
            }

            let now = Instant::now();
            let send_space = self.session.send_space();
            let want_app_read = send_space > 0;
            let want_app_write = !self.session.app_data().is_empty();
            let want_tx = match &self.channel {
                Some(ch) => !ch.preamble.is_empty() || !self.session.transmit_data(now).is_empty(),
                None => false,
            };
            let deadline = self.session.poll_timeout();

            let Pump {
                session,
                app_in,
                app_out,
                channel,
                buf,
                ..
            } = self;
            let (chan_read, chan_write, preamble) = match channel {
                Some(ActiveChannel {
                    read,
                    write,
                    preamble,
                }) => (Some(&*read), Some(&*write), Some(preamble)),
                None => (None, None, None),
            };

            tokio::select! {
                r = ready(chan_read, true) => {
                    let r = r.and_then(|mut g| g.read(buf).transpose()).transpose();
                    match r {
                        None => {}
                        Some(Ok(0)) => return self.lose_channel(None),
                        Some(Ok(n)) => {
                            // A protocol error is picked up at the top of the
                            // loop.
                            let _ = session.receive(&buf[..n], Instant::now());
                        }
                        Some(Err(e)) => return self.lose_channel(Some(e)),
                    }
                }
                r = ready(chan_write, want_tx) => {
                    let preamble = preamble.expect("channel is installed");
                    let r = r.and_then(|mut g| write_output(&mut g, preamble, session).transpose());
                    match r.transpose() {
                        None | Some(Ok(())) => {}
                        Some(Err(e)) => return self.lose_channel(Some(e)),
                    }
                }
                r = ready(app_out.as_ref(), want_app_write) => {
                    let r = r.and_then(|mut g| g.write(session.app_data()).transpose());
                    match r.transpose() {
                        None => {}
                        Some(Ok(n)) => session.consume_app(n),
                        Some(Err(e)) => {
                            if is_disconnect(&e) {
                                log::debug!("the application stopped reading: {e}");
                            } else {
                                log::warn!("error writing to the application: {e}");
                            }
                            session.close_app_read();
                            *app_out = None;
                        }
                    }
                }
                r = ready(app_in.as_ref(), want_app_read) => {
                    let limit = send_space.min(buf.len());
                    let r = r.and_then(|mut g| g.read(&mut buf[..limit]).transpose());
                    match r.transpose() {
                        None => {}
                        Some(Ok(0)) => {
                            log::debug!("the application's input reached EOF");
                            session.close_app_send();
                            *app_in = None;
                        }
                        Some(Ok(n)) => {
                            let accepted = session.write_app(&buf[..n]);
                            debug_assert_eq!(accepted, n);
                        }
                        Some(Err(e)) => {
                            log::warn!("error reading from the application: {e}; treating as EOF");
                            session.close_app_send();
                            *app_in = None;
                        }
                    }
                }
                () = sleep_until(deadline) => {
                    session.handle_timeout(Instant::now());
                }
            }
        }
    }

    /// Writes pending output to the channel until there is none. Returns
    /// whether that succeeded.
    async fn flush(&mut self) -> bool {
        let Pump {
            session, channel, ..
        } = self;
        let Some(ch) = channel.as_mut() else {
            return false;
        };
        loop {
            if ch.preamble.is_empty() && session.transmit_data(Instant::now()).is_empty() {
                return true;
            }
            let r = match ch.write.ready().await {
                Ok(mut g) => write_output(&mut g, &mut ch.preamble, session),
                Err(e) => Some(Err(e)),
            };
            if let Some(Err(e)) = r {
                log::debug!("error flushing final output: {e}");
                return false;
            }
        }
    }
}

/// Performs one write of pending channel output: what remains of the
/// preamble, and then the session's transmit data. `None` if the fd was not
/// ready after all.
fn write_output(
    g: &mut fdio::Ready<'_>,
    preamble: &mut Vec<u8>,
    session: &mut Session,
) -> Option<io::Result<()>> {
    if !preamble.is_empty() {
        let n = match g.write(preamble)? {
            Ok(n) => n,
            Err(e) => return Some(Err(e)),
        };
        preamble.drain(..n);
    } else {
        let now = Instant::now();
        let n = match g.write(session.transmit_data(now))? {
            Ok(n) => n,
            Err(e) => return Some(Err(e)),
        };
        session.consume_transmit(n, now);
    }
    Some(Ok(()))
}
