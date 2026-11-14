//! Capturing the stderr of the `ssh`s which the client runs.
//!
//! What an `ssh` writes to stderr is held back until we know whether its
//! handshake succeeded. If it did not, the output is most likely why (`ssh`
//! could not connect or authenticate, or the remote `stitch` failed), and
//! is shown: on our own stderr for the open, in the log for a resume. If it
//! did, the output is noise as far as the user is concerned (such as a
//! known-hosts warning), and anything more it writes would otherwise land
//! in the middle of the user's terminal session; from then on, it is only
//! logged, at `debug`.
//!
//! Authentication prompts are unaffected: `ssh` reads passwords and
//! confirmations on `/dev/tty`, not through stderr.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::ChildStderr;
use tokio::task::JoinHandle;

/// How much output is kept: the end of it, which is where an error is.
const MAX_CAPTURED: usize = 64 << 10;

/// How long to wait, once the handshake has failed, for the rest of the
/// output: until `ssh`, and anything else holding its stderr, has gone.
const EOF_WAIT: Duration = Duration::from_secs(1);

#[derive(Default)]
struct State {
    /// The output not yet shown or logged. Once discarding, at most a
    /// partial line.
    buf: Vec<u8>,
    discarding: bool,
}

impl State {
    /// Logs the complete lines in the buffer, leaving any partial line, or,
    /// with `all`, everything.
    fn log_lines(&mut self, all: bool) {
        let end = if all {
            self.buf.len()
        } else {
            match self.buf.iter().rposition(|&b| b == b'\n') {
                Some(i) => i + 1,
                None => return,
            }
        };
        log_output(log::Level::Debug, &self.buf[..end]);
        self.buf.drain(..end);
    }
}

/// Logs `output` line by line, as `ssh`'s.
fn log_output(level: log::Level, output: &[u8]) {
    for line in String::from_utf8_lossy(output).lines() {
        if !line.trim().is_empty() {
            log::log!(level, "ssh: {line}");
        }
    }
}

/// The stderr of one `ssh`: captured until [`discard`](SshStderr::discard)
/// or [`report`](SshStderr::report).
#[derive(Default)]
pub(super) struct SshStderr {
    state: Arc<Mutex<State>>,
    /// The task reading the pipe, until we have waited for it to finish.
    /// Dropping it detaches the task, which keeps draining the pipe (so
    /// that `ssh` never blocks writing to it) until EOF.
    reader: Option<JoinHandle<()>>,
}

impl SshStderr {
    pub(super) fn new() -> SshStderr {
        SshStderr::default()
    }

    /// Starts reading `pipe`.
    pub(super) fn capture(&mut self, mut pipe: ChildStderr) {
        let state = self.state.clone();
        self.reader = Some(tokio::spawn(async move {
            let mut chunk = vec![0u8; 4096];
            loop {
                let n = match pipe.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => {
                        log::debug!("reading ssh's stderr: {e}");
                        break;
                    }
                };
                let mut s = state.lock().unwrap();
                s.buf.extend_from_slice(&chunk[..n]);
                if s.discarding {
                    s.log_lines(false);
                    if s.buf.len() > MAX_CAPTURED {
                        s.log_lines(true);
                    }
                } else if s.buf.len() > MAX_CAPTURED {
                    let excess = s.buf.len() - MAX_CAPTURED;
                    s.buf.drain(..excess);
                }
            }
            let mut s = state.lock().unwrap();
            if s.discarding {
                s.log_lines(true);
            }
        }));
    }

    /// The handshake succeeded: logs what has been captured, and everything
    /// to come, at `debug`.
    pub(super) fn discard(mut self) {
        let mut s = self.state.lock().unwrap();
        s.discarding = true;
        s.log_lines(false);
        drop(s);
        self.reader.take();
    }

    /// The handshake failed: waits briefly for the rest of the output, and
    /// returns everything captured. The caller should have dropped (and so
    /// killed) the `ssh` first.
    async fn finish(mut self) -> Vec<u8> {
        if let Some(reader) = self.reader.take()
            && tokio::time::timeout(EOF_WAIT, reader).await.is_err()
        {
            log::debug!("ssh's stderr is still open");
        }
        std::mem::take(&mut self.state.lock().unwrap().buf)
    }

    /// The open failed: copies the output to our stderr, for the user.
    pub(super) async fn report(self) {
        let out = self.finish().await;
        let _ = std::io::stderr().write_all(&out);
    }

    /// A resume attempt failed: logs the output, at `info`, as the attempt
    /// itself is.
    pub(super) async fn log(self) {
        let out = self.finish().await;
        log_output(log::Level::Info, &out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_lines_keeps_partial_line() {
        let mut s = State {
            buf: b"one\ntwo\nthr".to_vec(),
            discarding: true,
        };
        s.log_lines(false);
        assert_eq!(s.buf, b"thr");
        s.log_lines(false);
        assert_eq!(s.buf, b"thr");
        s.log_lines(true);
        assert!(s.buf.is_empty());
    }
}
