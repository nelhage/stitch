//! Non-blocking file descriptors driven by tokio's reactor.
//!
//! [`FdIo`] wraps one direction (read or write) of a file descriptor. It puts
//! the fd in non-blocking mode and registers it with tokio via [`AsyncFd`],
//! so that the driver can wait for readiness and then perform a single
//! non-blocking `read`/`write` without ever blocking the event loop.
//!
//! Some fds cannot be watched by `epoll`/`kqueue` at all: regular files and
//! some character devices such as `/dev/null` (`epoll_ctl` fails with
//! `EPERM`, `kevent` with `EINVAL`).
//! Reads and writes on those never block for long, so they are simply treated
//! as always ready and used synchronously.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};

use nix::errno::Errno;
use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::sys::stat::{Mode, SFlag, fstat};
use tokio::io::Interest;
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};

/// One direction of a non-blocking file descriptor. See the [module
/// documentation](self).
#[derive(Debug)]
pub struct FdIo {
    inner: Inner,
    interest: Interest,
    is_socket: bool,
    /// File status flags to restore when dropped, if we changed them on an
    /// open file description which other processes may share.
    restore_flags: Option<OFlag>,
}

#[derive(Debug)]
enum Inner {
    Async(AsyncFd<OwnedFd>),
    /// An fd `epoll` cannot watch (a regular file, `/dev/null`, ...), used
    /// synchronously.
    Sync(OwnedFd),
}

/// A readiness token from [`FdIo::ready`]: perform the IO through it, so that
/// a `WouldBlock` result clears tokio's readiness state.
pub struct Ready<'a> {
    inner: ReadyInner<'a>,
}

enum ReadyInner<'a> {
    Async(AsyncFdReadyGuard<'a, OwnedFd>),
    Sync(&'a OwnedFd),
}

fn set_nonblocking(fd: &OwnedFd) -> io::Result<OFlag> {
    let flags = OFlag::from_bits_retain(fcntl(fd, FcntlArg::F_GETFL)?);
    if !flags.contains(OFlag::O_NONBLOCK) {
        fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    }
    Ok(flags)
}

fn read_fd(fd: impl AsFd, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        match nix::unistd::read(fd.as_fd(), buf) {
            Err(Errno::EINTR) => continue,
            r => return Ok(r?),
        }
    }
}

fn write_fd(fd: impl AsFd, buf: &[u8]) -> io::Result<usize> {
    loop {
        match nix::unistd::write(fd.as_fd(), buf) {
            Err(Errno::EINTR) => continue,
            r => return Ok(r?),
        }
    }
}

impl FdIo {
    /// Takes ownership of `fd`, which this process uses exclusively, for IO
    /// in the direction given by `interest` (`READABLE` or `WRITABLE`). The
    /// fd is put in non-blocking mode and registered with the current tokio
    /// runtime (so this must be called from within one).
    pub fn new(fd: OwnedFd, interest: Interest) -> io::Result<FdIo> {
        Self::build(fd, interest, false)
    }

    /// Like [`new`](FdIo::new), but for an fd whose open file description
    /// may be shared with other processes, such as our own stdin or stdout:
    /// `O_NONBLOCK` is a property of the description, so setting it would
    /// affect them too (e.g. the user's shell, if stdin is a terminal).
    ///
    /// Pipes and terminals are therefore reopened through `/proc/self/fd`,
    /// which creates a private description. Anything else (e.g. a socket) is
    /// used as is, and its original flags are restored when this is dropped.
    pub fn new_shared(fd: OwnedFd, interest: Interest) -> io::Result<FdIo> {
        let st = fstat(&fd)?;
        let kind = SFlag::from_bits_truncate(st.st_mode) & SFlag::S_IFMT;
        if kind == SFlag::S_IFIFO || kind == SFlag::S_IFCHR {
            let access = if interest.is_writable() {
                OFlag::O_WRONLY
            } else {
                OFlag::O_RDONLY
            };
            let path = format!("/proc/self/fd/{}", fd.as_raw_fd());
            let flags = access | OFlag::O_NOCTTY | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK;
            match nix::fcntl::open(path.as_str(), flags, Mode::empty()) {
                Ok(reopened) => {
                    // A reopened terminal starts with fresh (blocking) flags
                    // but is ours alone; `new` sets O_NONBLOCK again
                    // harmlessly.
                    drop(fd);
                    return Self::build(reopened, interest, false);
                }
                Err(e) => log::debug!("cannot reopen {path}: {e}; using it as is"),
            }
        }
        Self::build(fd, interest, true)
    }

    fn build(fd: OwnedFd, interest: Interest, shared: bool) -> io::Result<FdIo> {
        let kind = fstat(&fd)
            .map(|st| SFlag::from_bits_truncate(st.st_mode) & SFlag::S_IFMT)
            .ok();
        let is_socket = kind == Some(SFlag::S_IFSOCK);
        // kqueue (unlike epoll) accepts regular files, but its read filter
        // fires only while the offset is short of EOF, so waiting on it would
        // hang rather than read the EOF. Always use them synchronously.
        if kind == Some(SFlag::S_IFREG) {
            return Ok(FdIo {
                inner: Inner::Sync(fd),
                interest,
                is_socket,
                restore_flags: None,
            });
        }
        let orig = set_nonblocking(&fd)?;
        let changed = !orig.contains(OFlag::O_NONBLOCK);
        let restore_flags = (shared && changed).then_some(orig);
        // SAFETY: an `OwnedFd` is a valid open fd, which stays open (and
        // its `as_raw_fd` unchanged) for as long as the `AsyncFd` owns it.
        let inner = match unsafe { AsyncFd::register_with_interest(fd, interest) } {
            Ok(afd) => Inner::Async(afd),
            Err(e) => {
                let (fd, err) = e.into_parts();
                // epoll reports an fd it cannot watch with EPERM; kqueue
                // (e.g. for /dev/null) with EINVAL.
                let errno = err.raw_os_error();
                if errno != Some(Errno::EPERM as i32) && errno != Some(Errno::EINVAL as i32) {
                    return Err(err);
                }
                // `epoll` cannot watch this fd; IO on it never blocks for
                // long, so use it synchronously, with its original flags.
                if changed {
                    let _ = fcntl(&fd, FcntlArg::F_SETFL(orig));
                }
                return Ok(FdIo {
                    inner: Inner::Sync(fd),
                    interest,
                    is_socket,
                    restore_flags: None,
                });
            }
        };
        Ok(FdIo {
            inner,
            interest,
            is_socket,
            restore_flags,
        })
    }

    /// Whether the underlying fd is a socket.
    pub fn is_socket(&self) -> bool {
        self.is_socket
    }

    /// Whether the fd is used synchronously because `epoll` cannot watch it.
    pub fn is_sync(&self) -> bool {
        matches!(self.inner, Inner::Sync(_))
    }

    /// Waits until the fd is (probably) ready for IO in its direction. The
    /// IO itself is performed through the returned [`Ready`]. Cancel-safe.
    pub async fn ready(&self) -> io::Result<Ready<'_>> {
        let inner = match &self.inner {
            Inner::Async(afd) => ReadyInner::Async(afd.ready(self.interest).await?),
            Inner::Sync(fd) => ReadyInner::Sync(fd),
        };
        Ok(Ready { inner })
    }

    /// Reads into `buf`, waiting until data (or EOF) is available.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(r) = self.ready().await?.read(buf) {
                return r;
            }
        }
    }

    /// Writes some of `buf`, waiting until the fd is writable.
    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            if let Some(r) = self.ready().await?.write(buf) {
                return r;
            }
        }
    }

    /// Writes all of `buf`.
    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write(buf).await? {
                0 => return Err(io::ErrorKind::WriteZero.into()),
                n => buf = &buf[n..],
            }
        }
        Ok(())
    }

    /// If this is a socket, shuts down its write side (sending a FIN), which
    /// closing this fd alone would not do while another fd refers to the
    /// same socket (e.g. the copy used for reading). Errors are ignored.
    pub fn shutdown_write(&self) {
        if self.is_socket {
            let _ = nix::sys::socket::shutdown(
                self.as_fd().as_raw_fd(),
                nix::sys::socket::Shutdown::Write,
            );
        }
    }
}

impl AsFd for FdIo {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match &self.inner {
            Inner::Async(afd) => afd.get_ref().as_fd(),
            Inner::Sync(fd) => fd.as_fd(),
        }
    }
}

impl Drop for FdIo {
    fn drop(&mut self) {
        if let Some(flags) = self.restore_flags {
            let _ = fcntl(self.as_fd(), FcntlArg::F_SETFL(flags));
        }
    }
}

impl Ready<'_> {
    /// Performs one non-blocking read. `None` means the fd turned out not to
    /// be ready after all (readiness has been cleared; wait again).
    pub fn read(&mut self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        match &mut self.inner {
            ReadyInner::Async(g) => g.try_io(|afd| read_fd(afd, buf)).ok(),
            ReadyInner::Sync(fd) => Some(read_fd(fd, buf)),
        }
    }

    /// Performs one non-blocking write. `None` means the fd turned out not
    /// to be ready after all (readiness has been cleared; wait again).
    pub fn write(&mut self, buf: &[u8]) -> Option<io::Result<usize>> {
        match &mut self.inner {
            ReadyInner::Async(g) => g.try_io(|afd| write_fd(afd, buf)).ok(),
            ReadyInner::Sync(fd) => Some(write_fd(fd, buf)),
        }
    }
}
