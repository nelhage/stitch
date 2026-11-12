//! End-to-end smoke tests against a real, unprivileged `sshd` and a real
//! `ssh`. See "Testing" in `doc/stitch.md`.
//!
//! Each test starts its own `sshd` on a free port on 127.0.0.1, with a
//! generated host key and a generated client key in `authorized_keys`, all
//! in a temporary directory; it runs as the current user, who is the only
//! user it can log in. `ssh` is run through a wrapper script which passes
//! `-F` with a matching config, so that the user's own ssh configuration is
//! not involved. The remote `stitch` is the one just built, with its
//! rendezvous directory in the temporary directory.
//!
//! The tests are skipped (they pass, printing a message) if `sshd`, `ssh`
//! or `ssh-keygen` cannot be found, or `sshd` cannot be started, unless
//! `STITCH_E2E_REQUIRED` is set (as in CI), in which case they fail.
//!
//! Linux-only: the tests find and inspect processes through `/proc`.

#![cfg(target_os = "linux")]

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

const STITCH: &str = env!("CARGO_BIN_EXE_stitch");

/// Generous bound on anything we wait for.
const WAIT: Duration = Duration::from_secs(30);

fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Finds `name` on `PATH`, or in the usual `sbin` directories (which an
/// unprivileged user's `PATH` often lacks).
fn find_program(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain(["/usr/sbin", "/usr/local/sbin", "/sbin"].map(PathBuf::from))
        .map(|d| d.join(name))
        .find(|p| {
            p.is_absolute()
                && fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
}

fn test_data(len: usize, seed: u8) -> Vec<u8> {
    let mut x = seed as u32 | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

fn run(cmd: &mut Command) {
    let out = cmd.output().unwrap();
    assert!(out.status.success(), "{cmd:?}: {out:?}");
}

/// A running `sshd`, and everything needed to reach it.
struct Sshd {
    dir: tempfile::TempDir,
    child: Child,
    /// A wrapper script which runs the real `ssh` with our config.
    ssh: PathBuf,
}

/// Skips the calling test (by returning `None`, after printing why), or
/// fails it if `STITCH_E2E_REQUIRED` is set.
fn skip(why: &str) -> Option<Sshd> {
    assert!(
        std::env::var_os("STITCH_E2E_REQUIRED").is_none(),
        "cannot run e2e test, and STITCH_E2E_REQUIRED is set: {why}"
    );
    eprintln!("skipping: {why}");
    None
}

impl Sshd {
    /// Starts an `sshd`, or returns `None` (after printing why) if that is
    /// not possible here.
    fn start() -> Option<Sshd> {
        let (Some(sshd), Some(ssh), Some(keygen)) = (
            find_program("sshd"),
            find_program("ssh"),
            find_program("ssh-keygen"),
        ) else {
            return skip("sshd, ssh or ssh-keygen not found");
        };
        if nix::unistd::geteuid().is_root() {
            // A root sshd needs its privilege separation directory.
            if let Err(e) = fs::create_dir_all("/run/sshd") {
                return skip(&format!("cannot create /run/sshd: {e}"));
            }
        }
        // Unix socket paths are limited to ~108 bytes, so keep this short.
        let dir = tempfile::Builder::new()
            .prefix("se")
            .tempdir_in("/tmp")
            .unwrap();
        let d = dir.path().to_owned();
        for key in ["host_key", "client_key"] {
            run(Command::new(&keygen)
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(d.join(key)));
        }
        fs::copy(d.join("client_key.pub"), d.join("authorized_keys")).unwrap();
        let user = nix::unistd::User::from_uid(nix::unistd::geteuid())
            .unwrap()
            .expect("current user")
            .name;

        for _ in 0..5 {
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            fs::write(
                d.join("sshd_config"),
                format!(
                    "Port {port}\n\
                     ListenAddress 127.0.0.1\n\
                     HostKey {d}/host_key\n\
                     AuthorizedKeysFile {d}/authorized_keys\n\
                     PidFile none\n\
                     StrictModes no\n\
                     UsePAM no\n\
                     PubkeyAuthentication yes\n\
                     PasswordAuthentication no\n\
                     KbdInteractiveAuthentication no\n\
                     PermitRootLogin prohibit-password\n\
                     LogLevel VERBOSE\n",
                    d = d.display()
                ),
            )
            .unwrap();
            fs::write(
                d.join("ssh_config"),
                format!(
                    "Host *\n\
                     \x20 HostName 127.0.0.1\n\
                     \x20 Port {port}\n\
                     \x20 User {user}\n\
                     \x20 IdentityFile {d}/client_key\n\
                     \x20 IdentitiesOnly yes\n\
                     \x20 UserKnownHostsFile /dev/null\n\
                     \x20 GlobalKnownHostsFile /dev/null\n\
                     \x20 StrictHostKeyChecking no\n\
                     \x20 BatchMode yes\n\
                     \x20 LogLevel ERROR\n",
                    d = d.display()
                ),
            )
            .unwrap();
            let log = d.join("sshd.log");
            let mut child = Command::new(&sshd)
                .arg("-D")
                .arg("-e")
                .arg("-f")
                .arg(d.join("sshd_config"))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(fs::File::create(&log).unwrap())
                .spawn()
                .unwrap();
            // Wait for it to listen, or to fail (e.g. the port was taken in
            // the meantime).
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let text = fs::read_to_string(&log).unwrap_or_default();
                if text.contains("Server listening") {
                    let wrapper = d.join("ssh");
                    fs::write(
                        &wrapper,
                        format!(
                            "#!/bin/sh\nexec {} -F {}/ssh_config \"$@\"\n",
                            ssh.display(),
                            d.display()
                        ),
                    )
                    .unwrap();
                    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
                    return Some(Sshd {
                        dir,
                        child,
                        ssh: wrapper,
                    });
                }
                if child.try_wait().unwrap().is_some() || Instant::now() > deadline {
                    eprintln!("sshd did not start:\n{text}");
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        skip("cannot start sshd")
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// The remote `stitch` command: the built binary, with its rendezvous
    /// directory here. `ssh` passes no environment by default, so it is set
    /// in the command, which the remote shell interprets.
    fn remote_stitch(&self) -> String {
        format!(
            "env XDG_RUNTIME_DIR={} STITCH_LOG=debug {STITCH}",
            self.dir.path().display()
        )
    }

    /// A `stitch tunnel` command through this sshd, with `args` after the
    /// host.
    fn tunnel(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(STITCH);
        cmd.env("STITCH_LOG", "debug")
            .env_remove("SSH_CONNECTION")
            .arg("tunnel")
            .arg("--ssh")
            .arg(&self.ssh)
            .arg("--remote-stitch")
            .arg(self.remote_stitch())
            .arg("testhost")
            .args(args)
            .stderr(fs::File::create(self.path("client.err")).unwrap());
        cmd
    }

    fn logs(&self) -> String {
        let mut out = String::new();
        for name in ["client.err", "ssh.err", "sshd.log"] {
            if let Ok(s) = fs::read_to_string(self.path(name)) {
                out += &format!("--- {name} ---\n{s}");
            }
        }
        if let Ok(d) = fs::read_dir(self.path("stitch")) {
            for e in d.flatten() {
                if e.path().extension().is_some_and(|e| e == "log") {
                    let s = fs::read_to_string(e.path()).unwrap_or_default();
                    out += &format!("--- {} ---\n{s}", e.path().display());
                }
            }
        }
        out
    }

    fn client_log(&self) -> String {
        fs::read_to_string(self.path("client.err")).unwrap_or_default()
    }

    /// Session daemons whose rendezvous directory is ours.
    fn daemons(&self) -> Vec<i32> {
        let marker = format!("XDG_RUNTIME_DIR={}", self.dir.path().display());
        processes()
            .into_iter()
            .filter(|&pid| {
                let cmdline = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
                let args: Vec<&[u8]> = cmdline.split(|&b| b == 0).collect();
                args.len() >= 2
                    && args[1] == b"_server"
                    && fs::read(format!("/proc/{pid}/environ"))
                        .unwrap_or_default()
                        .split(|&b| b == 0)
                        .any(|v| v == marker.as_bytes())
            })
            .collect()
    }

    fn assert_cleaned_up(&self) {
        wait_until("the session daemon to exit", || self.daemons().is_empty());
    }
}

impl Drop for Sshd {
    fn drop(&mut self) {
        for d in self.daemons() {
            let _ = kill(Pid::from_raw(d), Signal::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() || std::env::var_os("STITCH_TEST_LOGS").is_some() {
            eprintln!("{}", self.logs());
        }
    }
}

fn processes() -> Vec<i32> {
    fs::read_dir("/proc")
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.parse().ok())
        .collect()
}

/// The children of `pid` whose command name is `comm`.
fn children_named(pid: u32, comm: &str) -> Vec<i32> {
    processes()
        .into_iter()
        .filter(|&p| {
            let Ok(stat) = fs::read_to_string(format!("/proc/{p}/stat")) else {
                return false;
            };
            // "pid (comm) state ppid ..."; comm may contain spaces.
            let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
                return false;
            };
            let name = &stat[open + 1..close];
            let ppid = stat[close + 1..].split_whitespace().nth(1);
            name == comm && ppid == Some(&pid.to_string())
        })
        .collect()
}

fn wait_exit(child: &mut Child) -> ExitStatus {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("process did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn tunnel_to_sshd_port_inferred() {
    // No port: the server infers it from SSH_CONNECTION, i.e. the sshd's own
    // port, whose banner we should then see.
    let Some(sshd) = Sshd::start() else { return };
    let mut t = sshd
        .tunnel(&[])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(t.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert!(line.starts_with("SSH-2.0-"), "{line:?}\n{}", sshd.logs());
    // Hang up: sshd sees EOF, and closes the connection.
    drop(t.stdin.take());
    let mut rest = Vec::new();
    stdout.read_to_end(&mut rest).unwrap();
    let status = wait_exit(&mut t);
    assert!(status.success(), "{status}\n{}", sshd.logs());
    sshd.assert_cleaned_up();
}

/// Runs `stitch tunnel testhost -- cat` with `len` bytes, calling `during`
/// (with the client's pid) once `trigger_at` bytes have come back.
fn cat_round_trip(sshd: &Sshd, len: usize, trigger_at: usize, during: impl FnOnce(u32)) {
    let mut t = sshd
        .tunnel(&["--", "cat"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let data = test_data(len, 5);
    let mut stdin = t.stdin.take().unwrap();
    let writer = std::thread::spawn({
        let data = data.clone();
        move || {
            for chunk in data.chunks(64 << 10) {
                stdin.write_all(chunk).unwrap();
            }
        }
    });
    let count = Arc::new(AtomicUsize::new(0));
    let mut stdout = t.stdout.take().unwrap();
    let reader = std::thread::spawn({
        let count = count.clone();
        move || {
            let mut out = Vec::new();
            let mut buf = vec![0u8; 64 << 10];
            loop {
                match stdout.read(&mut buf).unwrap() {
                    0 => return out,
                    n => {
                        out.extend_from_slice(&buf[..n]);
                        count.store(out.len(), Ordering::SeqCst);
                    }
                }
            }
        }
    });
    wait_until("data to flow", || {
        count.load(Ordering::SeqCst) >= trigger_at
    });
    during(t.id());
    let status = wait_exit(&mut t);
    writer.join().unwrap();
    let got = reader.join().unwrap();
    assert!(status.success(), "{status}\n{}", sshd.logs());
    assert_eq!(got.len(), data.len());
    assert!(got == data, "data corrupted");
    sshd.assert_cleaned_up();
}

#[test]
fn command_round_trip() {
    let Some(sshd) = Sshd::start() else { return };
    cat_round_trip(&sshd, 4 << 20, 0, |_| {});
    assert!(!sshd.client_log().contains("reconnecting"));
}

#[test]
fn resume_after_ssh_killed() {
    let Some(sshd) = Sshd::start() else { return };
    cat_round_trip(&sshd, 16 << 20, 2 << 20, |client| {
        let ssh = children_named(client, "ssh");
        assert_eq!(ssh.len(), 1, "{ssh:?}");
        kill(Pid::from_raw(ssh[0]), Signal::SIGKILL).unwrap();
    });
    let log = sshd.client_log();
    assert!(log.contains("reconnected (epoch 1)"), "{log}");
}

#[test]
fn ssh_through_stitch_proxy_command() {
    // A real ssh session through `stitch tunnel` as its ProxyCommand: the
    // primary use case.
    let Some(sshd) = Sshd::start() else { return };
    let proxy = format!(
        "{STITCH} tunnel --ssh {} --remote-stitch '{}' '%h'",
        sshd.ssh.display(),
        sshd.remote_stitch()
    );
    let out = Command::new(&sshd.ssh)
        .env("STITCH_LOG", "debug")
        .arg("-o")
        .arg(format!("ProxyCommand={proxy}"))
        .arg("testhost")
        .arg("echo hello")
        .stdin(Stdio::null())
        .stderr(fs::File::create(sshd.path("ssh.err")).unwrap())
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}\n{}", sshd.logs());
    assert_eq!(out.stdout, b"hello\n");
    sshd.assert_cleaned_up();
}
