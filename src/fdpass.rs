//! Passing a handshake message together with file descriptors over a Unix
//! stream socket (`SCM_RIGHTS`).
//!
//! This is how the server shim hands its stdin/stdout to the session daemon:
//! it forwards the client's handshake message, with the fds attached, in a
//! single `sendmsg` ([`send_with_fds`]); the daemon receives exactly one
//! complete length-delimited message plus its fds ([`recv_message_with_fds`]
//! or, from tokio, [`recv_message_with_fds_async`]).
//!
//! Both the blocking and the async receivers read exactly one message and
//! never read past it. Received fds are always close-on-exec. The receiver
//! requires exactly the expected number of fds; any fds received are closed
//! if the message turns out to be invalid.
//!
//! The async versions operate on a `tokio::net::UnixStream` using its
//! readiness API (`async_io`), so the daemon needs no `spawn_blocking`. They
//! impose no timeout; a caller facing an untrusted peer should wrap them in
//! `tokio::time::timeout`.

use std::io::{self, IoSlice, IoSliceMut};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

use nix::sys::socket::{ControlMessage, ControlMessageOwned, MsgFlags, recvmsg, send, sendmsg};
use tokio::io::Interest;

use crate::handshake::{self, HandshakeError, PREFIX_LEN};

/// Linux's limit on fds in one `SCM_RIGHTS` message (`SCM_MAX_FD`). Our
/// control buffer is this large so that ancillary data is never truncated,
/// which would leave fds we could not see (and so could not close).
const MAX_FDS_PER_MSG: usize = 253;

#[derive(Debug, thiserror::Error)]
pub enum FdPassError {
    #[error("I/O error passing file descriptors: {0}")]
    Io(#[from] io::Error),
    /// The message itself was malformed, truncated, or absent (EOF).
    #[error(transparent)]
    Handshake(#[from] HandshakeError),
    #[error("expected {expected} file descriptors with the message, received {received}")]
    FdCount { expected: usize, received: usize },
}

/// A message received with [`recv_message_with_fds`].
#[derive(Debug)]
pub struct Received {
    /// The message's JSON body, without the length prefix. Decode it with
    /// [`handshake::parse`].
    pub json: Vec<u8>,
    /// The attached fds, in the order they were sent.
    pub fds: Vec<OwnedFd>,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_FLAGS: MsgFlags = MsgFlags::MSG_NOSIGNAL;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const SEND_FLAGS: MsgFlags = MsgFlags::empty();

#[cfg(any(target_os = "linux", target_os = "android"))]
const RECV_FLAGS: MsgFlags = MsgFlags::MSG_CMSG_CLOEXEC;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const RECV_FLAGS: MsgFlags = MsgFlags::empty();

/// Ensure `fd` is close-on-exec. On Linux `MSG_CMSG_CLOEXEC` already did so
/// atomically; elsewhere there is an unavoidable window.
fn set_cloexec(fd: &OwnedFd) -> io::Result<()> {
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        use nix::fcntl::{FcntlArg, FdFlag, fcntl};
        fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
    }
    let _ = fd;
    Ok(())
}

/// One `sendmsg` carrying (a prefix of) `data` and all of `fds`. Returns the
/// number of bytes sent.
fn sendmsg_once(sock: RawFd, data: &[u8], fds: &[RawFd]) -> io::Result<usize> {
    let iov = [IoSlice::new(data)];
    let cmsgs: &[ControlMessage] = if fds.is_empty() {
        &[]
    } else {
        &[ControlMessage::ScmRights(fds)]
    };
    Ok(sendmsg::<()>(sock, &iov, cmsgs, SEND_FLAGS, None)?)
}

fn check_send_args(data: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<Vec<RawFd>> {
    if data.is_empty() && !fds.is_empty() {
        // A stream socket cannot carry ancillary data without at least one
        // byte of data.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cannot pass file descriptors without data",
        ));
    }
    if fds.len() > MAX_FDS_PER_MSG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many file descriptors",
        ));
    }
    Ok(fds.iter().map(|fd| fd.as_raw_fd()).collect())
}

/// Send `data` over a blocking Unix stream socket, attaching `fds` to it with
/// `SCM_RIGHTS` in a single `sendmsg` (which carries at least the first byte
/// of `data`). Any remainder not accepted by that call is then sent normally.
///
/// To forward a handshake message, `data` should be its complete wire
/// encoding (e.g. [`handshake::frame`] of the raw JSON).
pub fn send_with_fds(sock: impl AsFd, data: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<()> {
    let sock = sock.as_fd().as_raw_fd();
    let raw = check_send_args(data, fds)?;
    let mut sent = loop {
        match sendmsg_once(sock, data, &raw) {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            r => break r?,
        }
    };
    while sent < data.len() {
        match send(sock, &data[sent..], SEND_FLAGS) {
            Ok(n) => sent += n,
            Err(nix::errno::Errno::EINTR) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Async [`send_with_fds`], on a tokio Unix stream.
pub async fn send_with_fds_async(
    sock: &tokio::net::UnixStream,
    data: &[u8],
    fds: &[BorrowedFd<'_>],
) -> io::Result<()> {
    let raw_sock = sock.as_raw_fd();
    let raw = check_send_args(data, fds)?;
    let mut sent = sock
        .async_io(Interest::WRITABLE, || sendmsg_once(raw_sock, data, &raw))
        .await?;
    while sent < data.len() {
        sent += sock
            .async_io(Interest::WRITABLE, || {
                send(raw_sock, &data[sent..], SEND_FLAGS).map_err(io::Error::from)
            })
            .await?;
    }
    Ok(())
}

/// Incremental state for receiving one length-delimited message plus fds.
struct MessageReceiver {
    expected_fds: usize,
    prefix: [u8; PREFIX_LEN],
    prefix_got: usize,
    /// Allocated once the prefix is complete.
    body: Option<Vec<u8>>,
    body_got: usize,
    fds: Vec<OwnedFd>,
}

impl MessageReceiver {
    fn new(expected_fds: usize) -> Self {
        MessageReceiver {
            expected_fds,
            prefix: [0; PREFIX_LEN],
            prefix_got: 0,
            body: None,
            body_got: 0,
            fds: Vec::new(),
        }
    }

    fn is_complete(&self) -> bool {
        self.body.as_ref().is_some_and(|b| self.body_got == b.len())
    }

    /// The buffer the next read should fill: exactly the remaining part of
    /// the prefix or body, so that we never read beyond the message.
    fn want(&mut self) -> &mut [u8] {
        match &mut self.body {
            None => &mut self.prefix[self.prefix_got..],
            Some(b) => &mut b[self.body_got..],
        }
    }

    /// Perform one `recvmsg` into [`Self::want`], collecting any fds. Returns
    /// the byte count (0 at EOF). Errors are raw `io::Error`s, so that
    /// `WouldBlock` works with tokio's `async_io`.
    fn recv_once(&mut self, sock: RawFd) -> io::Result<usize> {
        let mut cmsg_buf = nix::cmsg_space!([RawFd; MAX_FDS_PER_MSG]);
        let (n, received) = {
            let buf = self.want();
            let mut iov = [IoSliceMut::new(buf)];
            let msg = recvmsg::<()>(sock, &mut iov, Some(&mut cmsg_buf), RECV_FLAGS)?;
            let mut received = Vec::new();
            // Our control buffer can hold the kernel's maximum, so it is never
            // truncated in practice; if it were, `cmsgs()` refuses to iterate.
            for cmsg in msg.cmsgs().map_err(io::Error::from)? {
                if let ControlMessageOwned::ScmRights(fds) = cmsg {
                    // SAFETY: the kernel has just installed these fds in our
                    // table, and nothing else refers to them.
                    received.extend(
                        fds.into_iter()
                            .map(|fd| unsafe { OwnedFd::from_raw_fd(fd) }),
                    );
                }
            }
            (msg.bytes, received)
        };
        for fd in &received {
            set_cloexec(fd)?;
        }
        self.fds.extend(received);
        Ok(n)
    }

    /// Account for `n` bytes read by [`Self::recv_once`].
    fn advance(&mut self, n: usize) -> Result<(), FdPassError> {
        if self.fds.len() > self.expected_fds {
            return Err(self.fd_count_error());
        }
        if n == 0 {
            return Err(if self.prefix_got == 0 && self.fds.is_empty() {
                HandshakeError::Eof
            } else {
                HandshakeError::Truncated
            }
            .into());
        }
        match &self.body {
            None => {
                self.prefix_got += n;
                if self.prefix_got == PREFIX_LEN {
                    let len = handshake::parse_prefix(&self.prefix)?;
                    self.body = Some(vec![0; len]);
                }
            }
            Some(_) => self.body_got += n,
        }
        Ok(())
    }

    fn fd_count_error(&self) -> FdPassError {
        FdPassError::FdCount {
            expected: self.expected_fds,
            received: self.fds.len(),
        }
    }

    fn finish(self) -> Result<Received, FdPassError> {
        if self.fds.len() != self.expected_fds {
            return Err(self.fd_count_error());
        }
        Ok(Received {
            json: self.body.expect("complete message has a body"),
            fds: self.fds,
        })
    }
}

/// Receive one complete length-delimited handshake message, and exactly
/// `expected_fds` attached fds, from a blocking Unix stream socket. Reads
/// nothing beyond the end of the message.
pub fn recv_message_with_fds(
    sock: impl AsFd,
    expected_fds: usize,
) -> Result<Received, FdPassError> {
    let sock = sock.as_fd().as_raw_fd();
    let mut rx = MessageReceiver::new(expected_fds);
    while !rx.is_complete() {
        let n = loop {
            match rx.recv_once(sock) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                r => break r?,
            }
        };
        rx.advance(n)?;
    }
    rx.finish()
}

/// Async [`recv_message_with_fds`], on a tokio Unix stream.
pub async fn recv_message_with_fds_async(
    sock: &tokio::net::UnixStream,
    expected_fds: usize,
) -> Result<Received, FdPassError> {
    let raw = sock.as_raw_fd();
    let mut rx = MessageReceiver::new(expected_fds);
    while !rx.is_complete() {
        let n = sock
            .async_io(Interest::READABLE, || rx.recv_once(raw))
            .await?;
        rx.advance(n)?;
    }
    rx.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handshake::Message;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    fn pipe() -> (File, File) {
        let (r, w) = nix::unistd::pipe().unwrap();
        (File::from(r), File::from(w))
    }

    fn is_cloexec(fd: &OwnedFd) -> bool {
        use nix::fcntl::{FcntlArg, FdFlag, fcntl};
        let flags = fcntl(fd, FcntlArg::F_GETFD).unwrap();
        FdFlag::from_bits_truncate(flags).contains(FdFlag::FD_CLOEXEC)
    }

    /// Check that `fds` are usable: `fds[0]` is the read end of `w0`'s pipe,
    /// `fds[1]` the write end of `r1`'s.
    fn check_pipes(fds: Vec<OwnedFd>, mut w0: File, mut r1: File) {
        assert!(fds.iter().all(is_cloexec));
        let mut fds = fds.into_iter();
        let mut a = File::from(fds.next().unwrap());
        let mut b = File::from(fds.next().unwrap());
        w0.write_all(b"ping").unwrap();
        let mut buf = [0u8; 4];
        a.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");
        b.write_all(b"pong").unwrap();
        r1.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"pong");
    }

    #[test]
    fn passes_fds_blocking() {
        let (tx, rx) = UnixStream::pair().unwrap();
        let (r0, w0) = pipe();
        let (r1, w1) = pipe();
        let msg = Message::resume("ab", "/x/ab.sock", 1);
        send_with_fds(&tx, &msg.encode(), &[r0.as_fd(), w1.as_fd()]).unwrap();
        // Our copies can be closed; the receiver's are independent.
        drop((r0, w1));
        let got = recv_message_with_fds(&rx, 2).unwrap();
        assert_eq!(handshake::parse(&got.json).unwrap(), msg);
        check_pipes(got.fds, w0, r1);
    }

    #[test]
    fn large_message_without_over_read() {
        let (tx, mut rx) = UnixStream::pair().unwrap();
        let (r0, w0) = pipe();
        let (r1, w1) = pipe();
        // Larger than the socket buffer, so it arrives in several chunks.
        let json = format!(
            r#"{{"type":"ok","version":1,"session":"ab","socket":"/x","pad":"{}"}}"#,
            "p".repeat(900_000)
        );
        let mut data = handshake::frame(json.as_bytes()).unwrap();
        data.extend_from_slice(b"AFTER");
        let sender = std::thread::spawn(move || {
            send_with_fds(&tx, &data, &[r0.as_fd(), w1.as_fd()]).unwrap();
            tx
        });
        let got = recv_message_with_fds(&rx, 2).unwrap();
        assert_eq!(got.json, json.as_bytes());
        let _tx = sender.join().unwrap();
        let mut after = [0u8; 5];
        rx.read_exact(&mut after).unwrap();
        assert_eq!(&after, b"AFTER");
        check_pipes(got.fds, w0, r1);
    }

    #[test]
    fn wrong_fd_count() {
        let (r0, _w0) = pipe();
        for (sent, expected) in [(1, 2), (2, 1), (0, 2)] {
            let (tx, rx) = UnixStream::pair().unwrap();
            let fds = vec![r0.as_fd(); sent];
            send_with_fds(&tx, &Message::ok("a", "/b").encode(), &fds).unwrap();
            match recv_message_with_fds(&rx, expected) {
                Err(FdPassError::FdCount { expected: e, .. }) => assert_eq!(e, expected),
                other => panic!("{sent}/{expected}: {other:?}"),
            }
        }
    }

    #[test]
    fn no_fds_expected() {
        let (tx, rx) = UnixStream::pair().unwrap();
        send_with_fds(&tx, &Message::ok("a", "/b").encode(), &[]).unwrap();
        let got = recv_message_with_fds(&rx, 0).unwrap();
        assert!(got.fds.is_empty());
    }

    #[test]
    fn eof_and_truncation() {
        let (tx, rx) = UnixStream::pair().unwrap();
        drop(tx);
        assert!(matches!(
            recv_message_with_fds(&rx, 2),
            Err(FdPassError::Handshake(HandshakeError::Eof))
        ));

        let (tx, rx) = UnixStream::pair().unwrap();
        let (r0, _w0) = pipe();
        let data = Message::ok("a", "/b").encode();
        send_with_fds(&tx, &data[..12], &[r0.as_fd(), r0.as_fd()]).unwrap();
        drop(tx);
        assert!(matches!(
            recv_message_with_fds(&rx, 2),
            Err(FdPassError::Handshake(HandshakeError::Truncated))
        ));
    }

    #[test]
    fn bad_prefix() {
        let (tx, rx) = UnixStream::pair().unwrap();
        let (r0, _w0) = pipe();
        send_with_fds(&tx, b"nothex!!{}", &[r0.as_fd()]).unwrap();
        assert!(matches!(
            recv_message_with_fds(&rx, 1),
            Err(FdPassError::Handshake(HandshakeError::BadPrefix(_)))
        ));
    }

    #[test]
    fn fds_need_data() {
        let (tx, _rx) = UnixStream::pair().unwrap();
        let (r0, _w0) = pipe();
        let err = send_with_fds(&tx, b"", &[r0.as_fd()]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn passes_fds_async() {
        let (tx, mut rx) = tokio::net::UnixStream::pair().unwrap();
        let (r0, w0) = pipe();
        let (r1, w1) = pipe();
        let json = format!(
            r#"{{"type":"ok","version":1,"session":"ab","socket":"/x","pad":"{}"}}"#,
            "p".repeat(500_000)
        );
        let mut data = handshake::frame(json.as_bytes()).unwrap();
        data.extend_from_slice(b"AFTER");
        let send = async {
            send_with_fds_async(&tx, &data, &[r0.as_fd(), w1.as_fd()])
                .await
                .unwrap();
        };
        let recv = async { recv_message_with_fds_async(&rx, 2).await.unwrap() };
        let ((), got) = tokio::join!(send, recv);
        assert_eq!(got.json, json.as_bytes());
        let mut after = [0u8; 5];
        tokio::io::AsyncReadExt::read_exact(&mut rx, &mut after)
            .await
            .unwrap();
        assert_eq!(&after, b"AFTER");
        check_pipes(got.fds, w0, r1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_recv_from_blocking_sender() {
        let (tx, rx) = UnixStream::pair().unwrap();
        rx.set_nonblocking(true).unwrap();
        let rx = tokio::net::UnixStream::from_std(rx).unwrap();
        let (r0, w0) = pipe();
        let (r1, w1) = pipe();
        let sender = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            let data = Message::resume("ab", "/x", 2).encode();
            // Split, so the receiver sees the fds with only part of the message.
            send_with_fds(&tx, &data[..5], &[r0.as_fd(), w1.as_fd()]).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20));
            (&tx).write_all(&data[5..]).unwrap();
        });
        let got = recv_message_with_fds_async(&rx, 2).await.unwrap();
        sender.join().unwrap();
        assert_eq!(
            handshake::parse(&got.json).unwrap(),
            Message::resume("ab", "/x", 2)
        );
        check_pipes(got.fds, w0, r1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_eof() {
        let (tx, rx) = tokio::net::UnixStream::pair().unwrap();
        drop(tx);
        assert!(matches!(
            recv_message_with_fds_async(&rx, 2).await,
            Err(FdPassError::Handshake(HandshakeError::Eof))
        ));
    }
}
