//! The server's rendezvous directory, session ids, and per-session paths.
//!
//! The rendezvous directory is `$XDG_RUNTIME_DIR/stitch`, falling back to
//! `/tmp/stitch-<uid>`. It holds, per session, `<id>.sock` (the daemon's
//! listening socket) and `<id>.log` (the daemon's log). Access to the
//! directory is what authorizes a resume, so it must be private to us.

use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Number of random bytes in a generated session id.
const SESSION_ID_BYTES: usize = 16;

/// Maximum accepted length of a session id, in characters.
const MAX_SESSION_ID_LEN: usize = 64;

/// A session id: lowercase hexadecimal, so that it is always safe to use as a
/// file name component.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid session id {0:?}")]
pub struct InvalidSessionId(pub String);

impl SessionId {
    /// A new random 128-bit session id, as 32 lowercase hex digits.
    pub fn generate() -> io::Result<Self> {
        let mut bytes = [0u8; SESSION_ID_BYTES];
        getrandom::fill(&mut bytes).map_err(io::Error::other)?;
        Ok(SessionId(
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        ))
    }

    /// Validate a session id received from elsewhere: 1–64 lowercase hex
    /// digits. Rejecting anything else rules out path traversal.
    pub fn parse(s: &str) -> Result<Self, InvalidSessionId> {
        let ok = !s.is_empty()
            && s.len() <= MAX_SESSION_ID_LEN
            && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if ok {
            Ok(SessionId(s.to_owned()))
        } else {
            Err(InvalidSessionId(s.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SessionId {
    type Err = InvalidSessionId;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SessionId::parse(s)
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The rendezvous directory path for the given `$XDG_RUNTIME_DIR` value and
/// uid. An unset, empty or relative `XDG_RUNTIME_DIR` is ignored, per the XDG
/// base directory specification.
pub fn rendezvous_dir_for(xdg_runtime_dir: Option<&OsStr>, uid: u32) -> PathBuf {
    match xdg_runtime_dir.map(Path::new) {
        Some(p) if p.is_absolute() => p.join("stitch"),
        _ => PathBuf::from(format!("/tmp/stitch-{uid}")),
    }
}

/// The rendezvous directory path for this process's environment and
/// effective uid. Does not create it; see [`Rendezvous::open`].
pub fn default_rendezvous_dir() -> PathBuf {
    rendezvous_dir_for(
        std::env::var_os("XDG_RUNTIME_DIR").as_deref(),
        nix::unistd::geteuid().as_raw(),
    )
}

/// Ensure that `path` is a directory private to us, creating it (mode `0700`)
/// if necessary; its parent must exist.
///
/// If it already exists, it must be a real directory (not a symlink) owned by
/// our effective uid; if it is group- or world-accessible, its mode is reset
/// to `0700`. Checks are made on an `O_NOFOLLOW` handle, so they cannot be
/// raced by swapping the path.
pub fn ensure_private_dir(path: &Path) -> io::Result<()> {
    match DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let dir: File = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| {
            // With O_NOFOLLOW|O_DIRECTORY, Linux reports a symlink as either
            // ELOOP or ENOTDIR; look again to give a clear message.
            let why = match fs::symlink_metadata(path) {
                Ok(md) if md.file_type().is_symlink() => "is a symlink".to_owned(),
                Ok(md) if !md.is_dir() => "is not a directory".to_owned(),
                _ => e.to_string(),
            };
            io::Error::new(
                e.kind(),
                format!("rendezvous directory {}: {why}", path.display()),
            )
        })?;
    let md = dir.metadata()?;
    let euid = nix::unistd::geteuid().as_raw();
    if md.uid() != euid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "rendezvous directory {} is owned by uid {}, not us (uid {euid})",
                path.display(),
                md.uid()
            ),
        ));
    }
    let mode = md.mode() & 0o7777;
    if mode != 0o700 {
        if mode & 0o077 != 0 {
            log::warn!(
                "rendezvous directory {} had mode {mode:o}; resetting to 700",
                path.display()
            );
        }
        dir.set_permissions(fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// A verified rendezvous directory.
#[derive(Debug, Clone)]
pub struct Rendezvous {
    dir: PathBuf,
}

impl Rendezvous {
    /// The default rendezvous directory ([`default_rendezvous_dir`]), created
    /// or verified with [`ensure_private_dir`].
    pub fn open() -> io::Result<Self> {
        Self::open_at(default_rendezvous_dir())
    }

    /// A rendezvous directory at an explicit path, created or verified with
    /// [`ensure_private_dir`].
    pub fn open_at(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        ensure_private_dir(&dir)?;
        Ok(Rendezvous { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `<dir>/<id>.sock`: the session daemon's listening socket.
    pub fn socket_path(&self, id: &SessionId) -> PathBuf {
        self.dir.join(format!("{id}.sock"))
    }

    /// `<dir>/<id>.log`: the session daemon's log.
    pub fn log_path(&self, id: &SessionId) -> PathBuf {
        self.dir.join(format!("{id}.log"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn mode(p: &Path) -> u32 {
        fs::symlink_metadata(p).unwrap().mode() & 0o7777
    }

    #[test]
    fn dir_selection() {
        assert_eq!(
            rendezvous_dir_for(Some(OsStr::new("/run/user/1000")), 1000),
            PathBuf::from("/run/user/1000/stitch")
        );
        assert_eq!(
            rendezvous_dir_for(None, 1000),
            PathBuf::from("/tmp/stitch-1000")
        );
        assert_eq!(
            rendezvous_dir_for(Some(OsStr::new("")), 7),
            PathBuf::from("/tmp/stitch-7")
        );
        assert_eq!(
            rendezvous_dir_for(Some(OsStr::new("relative/dir")), 7),
            PathBuf::from("/tmp/stitch-7")
        );
    }

    #[test]
    fn session_ids() {
        let a = SessionId::generate().unwrap();
        let b = SessionId::generate().unwrap();
        assert_ne!(a, b);
        assert_eq!(a.as_str().len(), 32);
        assert_eq!(SessionId::parse(a.as_str()).unwrap(), a);
        assert_eq!(a.to_string().parse::<SessionId>().unwrap(), a);
        for bad in [
            "",
            "../etc",
            "ABCDEF",
            "abc/def",
            "abc.sock",
            " abc",
            "abc\0",
            &"a".repeat(65),
        ] {
            assert!(SessionId::parse(bad).is_err(), "{bad:?}");
        }
        assert!(SessionId::parse(&"a".repeat(64)).is_ok());
    }

    #[test]
    fn creates_private_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("stitch");
        let r = Rendezvous::open_at(&dir).unwrap();
        assert!(dir.is_dir());
        assert_eq!(mode(&dir), 0o700);
        let id = SessionId::parse("00ff").unwrap();
        assert_eq!(r.socket_path(&id), dir.join("00ff.sock"));
        assert_eq!(r.log_path(&id), dir.join("00ff.log"));
        // Idempotent.
        Rendezvous::open_at(&dir).unwrap();
        assert_eq!(mode(&dir), 0o700);
    }

    #[test]
    fn fixes_loose_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("stitch");
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o777)).unwrap();
        ensure_private_dir(&dir).unwrap();
        assert_eq!(mode(&dir), 0o700);
    }

    #[test]
    fn rejects_non_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("file");
        fs::write(&file, b"").unwrap();
        assert!(ensure_private_dir(&file).is_err());

        let real = tmp.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o700)).unwrap();
        let link = tmp.path().join("link");
        symlink(&real, &link).unwrap();
        let err = ensure_private_dir(&link).unwrap_err();
        assert!(err.to_string().contains("symlink"), "{err}");
    }

    #[test]
    fn missing_parent_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(ensure_private_dir(&tmp.path().join("a/b")).is_err());
    }

    #[test]
    fn rejects_foreign_owner() {
        // Only root can create a directory owned by someone else.
        if !nix::unistd::geteuid().is_root() {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("stitch");
        fs::create_dir(&dir).unwrap();
        nix::unistd::chown(&dir, Some(nix::unistd::Uid::from_raw(12345)), None).unwrap();
        let err = ensure_private_dir(&dir).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }
}
