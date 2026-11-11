//! Process-level tests of `stitch tunnel` and the server, using a fake `ssh`
//! (`tests/fake-ssh`) which runs the remote command locally. See "Testing"
//! in `doc/implementation.md`.

use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use stitch::handshake::{self, ErrorCode, Message};

const STITCH: &str = env!("CARGO_BIN_EXE_stitch");

/// Generous bound on anything we wait for.
const WAIT: Duration = Duration::from_secs(20);

fn fake_ssh() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fake-ssh")
}

/// Polls `cond` until it holds, panicking after [`WAIT`].
fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
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

/// One run of the fake ssh, from its log.
#[derive(Debug, Clone, Default)]
struct Invocation {
    ssh: i32,
    relays: Vec<i32>,
}

fn signal(pid: i32, sig: Signal) {
    let _ = kill(Pid::from_raw(pid), sig);
}

/// A test environment: a private rendezvous directory and fake-ssh log.
struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Env {
        // Unix socket paths are limited to ~108 bytes, so keep this short.
        let dir = tempfile::Builder::new()
            .prefix("st")
            .tempdir_in("/tmp")
            .unwrap();
        Env { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn rendezvous(&self) -> PathBuf {
        self.path("stitch")
    }

    /// A `stitch` command with this environment.
    fn command(&self) -> Command {
        let mut cmd = Command::new(STITCH);
        cmd.env("XDG_RUNTIME_DIR", self.dir.path())
            .env("FAKE_SSH_LOG", self.path("ssh.log"))
            .env("FAKE_SSH_FAIL", self.path("fail"))
            .env("FAKE_SSH_DELAY", self.path("delay"))
            .env("STITCH_LOG", "debug")
            .env_remove("SSH_CONNECTION");
        cmd
    }

    /// Starts `stitch tunnel` with `args` (after the host).
    fn tunnel(&self, args: &[&str]) -> Tunnel {
        self.tunnel_with(args, |_| {})
    }

    /// Starts `stitch tunnel` with `args`, after `f` has adjusted the
    /// command (e.g. to set `STITCH_REMOTE`, the remote `stitch` command).
    fn tunnel_with(&self, args: &[&str], f: impl FnOnce(&mut Command)) -> Tunnel {
        let mut cmd = self.command();
        cmd.arg("tunnel")
            .arg("--ssh")
            .arg(fake_ssh())
            .env("STITCH_REMOTE", STITCH)
            .arg("host")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(fs::File::create(self.path("client.err")).unwrap());
        f(&mut cmd);
        let mut child = cmd.spawn().unwrap();
        Tunnel {
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            child,
        }
    }

    fn invocations(&self) -> Vec<Invocation> {
        let log = fs::read_to_string(self.path("ssh.log")).unwrap_or_default();
        let mut out: Vec<Invocation> = Vec::new();
        for line in log.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            let [id, role, pid] = f[..] else { continue };
            let (id, pid): (i32, i32) = (id.parse().unwrap(), pid.parse().unwrap());
            let inv = match out.iter_mut().position(|i| i.ssh == id) {
                Some(n) => &mut out[n],
                None => {
                    out.push(Invocation {
                        ssh: id,
                        relays: vec![],
                    });
                    out.last_mut().unwrap()
                }
            };
            if role == "relay" {
                inv.relays.push(pid);
            }
        }
        out
    }

    /// Waits until the `n`th (from 0) fake ssh is running both relays.
    fn invocation(&self, n: usize) -> Invocation {
        wait_until(&format!("ssh invocation {n}"), || {
            self.invocations()
                .get(n)
                .is_some_and(|i| i.relays.len() == 2)
        });
        self.invocations()[n].clone()
    }

    /// Simulates a network failure: kills a fake ssh and its relays.
    fn break_connection(&self, inv: &Invocation) {
        for &p in &inv.relays {
            signal(p, Signal::SIGKILL);
        }
        signal(inv.ssh, Signal::SIGKILL);
    }

    /// Simulates a stalled network: stops a fake ssh's relays.
    fn stall_connection(&self, inv: &Invocation) {
        for &p in &inv.relays {
            signal(p, Signal::SIGSTOP);
        }
    }

    /// Session daemons running in this environment.
    fn daemons(&self) -> Vec<i32> {
        let marker = format!("XDG_RUNTIME_DIR={}", self.dir.path().display());
        let mut out = Vec::new();
        for entry in fs::read_dir("/proc").unwrap().flatten() {
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<i32>() else {
                continue;
            };
            let Ok(cmdline) = fs::read(entry.path().join("cmdline")) else {
                continue;
            };
            let args: Vec<&[u8]> = cmdline.split(|&b| b == 0).collect();
            if args.len() < 2 || args[1] != b"_server" {
                continue;
            }
            let Ok(environ) = fs::read(entry.path().join("environ")) else {
                continue;
            };
            if environ.split(|&b| b == 0).any(|v| v == marker.as_bytes()) {
                // Zombies have an empty cmdline, so this is a live process.
                out.push(pid);
            }
        }
        out
    }

    /// Rendezvous sockets present.
    fn sockets(&self) -> Vec<PathBuf> {
        match fs::read_dir(self.rendezvous()) {
            Ok(d) => d
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "sock"))
                .collect(),
            Err(_) => vec![],
        }
    }

    fn logs(&self) -> String {
        let mut out = String::new();
        if let Ok(s) = fs::read_to_string(self.path("client.err")) {
            out += &format!("--- client stderr ---\n{s}");
        }
        if let Ok(d) = fs::read_dir(self.rendezvous()) {
            for e in d.flatten() {
                if e.path().extension().is_some_and(|e| e == "log") {
                    let s = fs::read_to_string(e.path()).unwrap_or_default();
                    out += &format!("--- {} ---\n{s}", e.path().display());
                }
            }
        }
        out
    }

    /// Checks that the session has been cleaned up: no daemon, no socket.
    fn assert_cleaned_up(&self) {
        wait_until("daemon exit", || self.daemons().is_empty());
        assert!(self.sockets().is_empty(), "{:?}", self.sockets());
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for inv in self.invocations() {
            for p in inv.relays {
                signal(p, Signal::SIGKILL);
            }
            signal(inv.ssh, Signal::SIGKILL);
        }
        for d in self.daemons() {
            signal(d, Signal::SIGKILL);
        }
        if std::thread::panicking() || std::env::var_os("STITCH_TEST_LOGS").is_some() {
            eprintln!("{}", self.logs());
        }
    }
}

/// A running `stitch tunnel`.
struct Tunnel {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
}

impl Tunnel {
    /// Writes `data` to the tunnel's stdin from another thread, then closes
    /// it. `gate` is called between chunks, and may block.
    fn feed(&mut self, data: Vec<u8>, gate: impl Fn(usize) + Send + 'static) -> JoinHandle<()> {
        let mut stdin = self.stdin.take().unwrap();
        std::thread::spawn(move || {
            for (i, chunk) in data.chunks(64 << 10).enumerate() {
                gate(i * (64 << 10));
                stdin.write_all(chunk).unwrap();
            }
        })
    }

    /// Reads the tunnel's stdout to EOF in another thread, counting progress
    /// in `count`.
    fn drain(&mut self, count: Arc<AtomicUsize>) -> JoinHandle<Vec<u8>> {
        let mut stdout = self.stdout.take().unwrap();
        std::thread::spawn(move || {
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
        })
    }

    /// Waits for the tunnel to exit.
    fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() > deadline {
                let _ = self.child.kill();
                panic!("stitch tunnel did not exit");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A TCP echo server for one connection: echoes everything, then closes
/// its write side at EOF.
fn echo_server() -> (u16, JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = vec![0u8; 64 << 10];
        let mut total = 0;
        loop {
            match s.read(&mut buf).unwrap() {
                0 => break,
                n => {
                    s.write_all(&buf[..n]).unwrap();
                    total += n;
                }
            }
        }
        s.shutdown(Shutdown::Write).unwrap();
        total
    });
    (port, handle)
}

/// Sends `data` through a tunnel to an echo server, calling `during` once
/// `trigger_at` bytes have come back. Returns the tunnel's exit status.
fn echo_through(
    env: &Env,
    args: &[&str],
    len: usize,
    trigger_at: usize,
    during: impl FnOnce(),
) -> ExitStatus {
    let (port, server) = echo_server();
    let dest = format!("127.0.0.1:{port}");
    let mut all_args = args.to_vec();
    all_args.push(&dest);
    let mut t = env.tunnel(&all_args);
    let data = test_data(len, 7);
    let count = Arc::new(AtomicUsize::new(0));
    let writer = t.feed(data.clone(), |_| {});
    let reader = t.drain(count.clone());
    wait_until("data to flow", || {
        count.load(Ordering::SeqCst) >= trigger_at
    });
    during();
    let status = t.wait();
    writer.join().unwrap();
    let got = reader.join().unwrap();
    assert_eq!(got.len(), data.len());
    assert!(got == data, "data corrupted");
    assert_eq!(server.join().unwrap(), len);
    status
}

#[test]
fn tcp_echo_large() {
    let env = Env::new();
    let status = echo_through(&env, &[], 6 << 20, 0, || {});
    assert!(status.success(), "{status}");
    env.assert_cleaned_up();
    assert_eq!(env.invocations().len(), 1);
}

#[test]
fn tcp_port_from_ssh_connection() {
    let env = Env::new();
    let (port, server) = echo_server();
    let mut t = env.tunnel_with(&[], |cmd| {
        cmd.env(
            "SSH_CONNECTION",
            format!("127.0.0.1 50000 127.0.0.1 {port}"),
        );
    });
    let mut stdin = t.stdin.take().unwrap();
    stdin.write_all(b"hello").unwrap();
    drop(stdin);
    let mut out = Vec::new();
    t.stdout.take().unwrap().read_to_end(&mut out).unwrap();
    assert_eq!(out, b"hello");
    assert!(t.wait().success());
    assert_eq!(server.join().unwrap(), 5);
}

#[test]
fn command_target() {
    let env = Env::new();
    let mut t = env.tunnel(&["--", "wc", "-c"]);
    let writer = t.feed(test_data(3 << 20, 1), |_| {});
    let out = t.drain(Arc::default());
    assert!(t.wait().success());
    writer.join().unwrap();
    let out = String::from_utf8(out.join().unwrap()).unwrap();
    assert_eq!(out.trim(), (3 << 20).to_string());
    env.assert_cleaned_up();
}

#[test]
fn command_target_bidirectional() {
    let env = Env::new();
    let mut t = env.tunnel(&["--", "cat"]);
    let data = test_data(5 << 20, 2);
    let writer = t.feed(data.clone(), |_| {});
    let out = t.drain(Arc::default());
    assert!(t.wait().success());
    writer.join().unwrap();
    assert!(out.join().unwrap() == data);
}

#[test]
fn session_cleanup() {
    let env = Env::new();
    let mut t = env.tunnel(&["--", "echo", "hello"]);
    drop(t.stdin.take());
    let out = t.drain(Arc::default());
    assert!(t.wait().success());
    assert_eq!(out.join().unwrap(), b"hello\n");
    env.assert_cleaned_up();
    // The log is kept.
    let logs: Vec<_> = fs::read_dir(env.rendezvous()).unwrap().flatten().collect();
    assert_eq!(logs.len(), 1, "{logs:?}");
    let log = fs::read_to_string(logs[0].path()).unwrap();
    assert!(log.contains("session finished"), "{log}");
}

#[test]
fn resume_after_connection_killed() {
    let env = Env::new();
    let status = echo_through(&env, &[], 8 << 20, 1 << 20, || {
        let first = env.invocation(0);
        env.break_connection(&first);
    });
    assert!(status.success(), "{status}");
    assert!(env.invocations().len() >= 2);
    env.assert_cleaned_up();
}

#[test]
fn resume_after_repeated_kills() {
    let env = Env::new();
    let (port, server) = echo_server();
    let mut t = env.tunnel(&[&format!("127.0.0.1:{port}")]);
    let data = test_data(4 << 20, 9);
    let count = Arc::new(AtomicUsize::new(0));
    let writer = t.feed(data.clone(), |_| {});
    let reader = t.drain(count.clone());
    for n in 0..3 {
        wait_until("data to flow", || {
            count.load(Ordering::SeqCst) >= (n + 1) * (1 << 20)
        });
        let inv = env.invocation(n);
        env.break_connection(&inv);
    }
    assert!(t.wait().success());
    writer.join().unwrap();
    assert!(reader.join().unwrap() == data);
    assert_eq!(server.join().unwrap(), data.len());
}

#[test]
fn resume_after_stall() {
    let env = Env::new();
    let status = echo_through(
        &env,
        &["--heartbeat-interval", "0.1", "--timeout", "0.5"],
        4 << 20,
        1 << 20,
        || {
            let first = env.invocation(0);
            env.stall_connection(&first);
        },
    );
    assert!(status.success(), "{status}");
    assert!(env.invocations().len() >= 2);
    let client = fs::read_to_string(env.path("client.err")).unwrap();
    assert!(client.contains("no traffic"), "{client}");
    env.assert_cleaned_up();
}

#[test]
fn resume_retries_until_ssh_works() {
    let env = Env::new();
    let fail = env.path("fail");
    let status = echo_through(&env, &[], 4 << 20, 1 << 20, || {
        fs::write(&fail, "").unwrap();
        let first = env.invocation(0);
        env.break_connection(&first);
        // Let at least one attempt fail.
        wait_until("a failed attempt", || {
            fs::read_to_string(env.path("client.err"))
                .unwrap()
                .contains("reconnect attempt failed")
        });
        fs::remove_file(&fail).unwrap();
    });
    assert!(status.success(), "{status}");
    env.assert_cleaned_up();
}

/// Waits until the client's log contains `text`.
fn wait_for_client_log(env: &Env, text: &str) {
    wait_until(text, || client_log(env).contains(text));
}

fn client_log(env: &Env) -> String {
    fs::read_to_string(env.path("client.err")).unwrap_or_default()
}

#[test]
fn speculative_resume() {
    // The old channel stalls, so the client starts a (slow) resume; the old
    // channel then recovers and carries data while the attempt is in
    // progress, until the client switches to the new channel regardless.
    let env = Env::new();
    let (port, server) = echo_server();
    let mut t = env.tunnel(&[
        "--heartbeat-interval",
        "0.1",
        "--timeout",
        "0.5",
        &format!("127.0.0.1:{port}"),
    ]);
    let data = test_data(4 << 20, 11);
    let count = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicUsize::new(0));
    let writer = t.feed(data.clone(), {
        let gate = gate.clone();
        move |offset| {
            if offset >= 2 << 20 {
                while gate.load(Ordering::SeqCst) == 0 {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
    });
    let reader = t.drain(count.clone());
    wait_until("data to flow", || count.load(Ordering::SeqCst) >= 1 << 20);
    fs::write(env.path("delay"), "2").unwrap();
    let first = env.invocation(0);
    env.stall_connection(&first);
    wait_for_client_log(&env, "reconnecting (epoch 1)");
    for &p in &first.relays {
        signal(p, Signal::SIGCONT);
    }
    // Everything written so far comes back over the old channel.
    wait_until("old channel to recover", || {
        count.load(Ordering::SeqCst) >= 2 << 20
    });
    assert!(!client_log(&env).contains("reconnected"));
    wait_for_client_log(&env, "reconnected (epoch 1)");
    gate.store(1, Ordering::SeqCst);

    assert!(t.wait().success());
    writer.join().unwrap();
    assert!(reader.join().unwrap() == data);
    assert_eq!(server.join().unwrap(), data.len());
    assert_eq!(env.invocations().len(), 2);
    env.assert_cleaned_up();
}

#[test]
fn slow_resume_is_abandoned() {
    let env = Env::new();
    let status = echo_through(
        &env,
        &["--connect-timeout", "0.3"],
        4 << 20,
        1 << 20,
        || {
            fs::write(env.path("delay"), "3").unwrap();
            let first = env.invocation(0);
            env.break_connection(&first);
            wait_for_client_log(&env, "no reply within");
            fs::remove_file(env.path("delay")).unwrap();
        },
    );
    assert!(status.success(), "{status}");
    let invocations = env.invocations();
    assert!(invocations.len() >= 3, "{invocations:?}");
    // The abandoned attempt's ssh was killed.
    assert!(
        kill(Pid::from_raw(invocations[1].ssh), None).is_err(),
        "abandoned ssh still running"
    );
    env.assert_cleaned_up();
}

#[test]
fn crashed_server() {
    let env = Env::new();
    let mut t = env.tunnel(&["--", "cat"]);
    let mut stdin = t.stdin.take().unwrap();
    let mut stdout = t.stdout.take().unwrap();
    stdin.write_all(b"ping").unwrap();
    let mut buf = [0u8; 4];
    stdout.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ping");

    let daemons = env.daemons();
    assert_eq!(daemons.len(), 1);
    signal(daemons[0], Signal::SIGKILL);
    let status = t.wait();
    assert!(!status.success(), "{status}");
    let client = fs::read_to_string(env.path("client.err")).unwrap();
    assert!(client.contains("remote session is gone"), "{client}");
}

#[test]
fn open_failures() {
    let env = Env::new();
    // Nothing listens on this port.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut t = env.tunnel(&[&format!("127.0.0.1:{port}")]);
    assert!(!t.wait().success());
    let client = fs::read_to_string(env.path("client.err")).unwrap();
    assert!(client.contains("target_failed"), "{client}");
    env.assert_cleaned_up();

    // ssh itself fails.
    fs::write(env.path("fail"), "").unwrap();
    let mut t = env.tunnel(&["--", "cat"]);
    assert!(!t.wait().success());
    let client = fs::read_to_string(env.path("client.err")).unwrap();
    assert!(client.contains("cannot open the session"), "{client}");
}

/// Runs the shim directly, sending it `msg` (raw bytes), and returns its
/// reply and exit status.
fn shim_exchange(env: &Env, msg: &[u8]) -> (Message, ExitStatus) {
    let mut shim = env
        .command()
        .arg("_server-shim")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = shim.stdin.take().unwrap();
    stdin.write_all(msg).unwrap();
    let mut stdout = shim.stdout.take().unwrap();
    let (_, reply) = handshake::read_message(&mut stdout).unwrap();
    drop(stdin);
    let status = shim.wait().unwrap();
    (reply, status)
}

fn expect_error(reply: Message, code: ErrorCode) {
    match reply {
        Message::Error(e) => assert_eq!(e.code, code, "{e:?}"),
        other => panic!("expected {code} error, got {other:?}"),
    }
}

#[test]
fn resume_answered_for_another_session() {
    // Whatever answers a resume must be our session; otherwise the session
    // is as good as gone.
    let env = Env::new();
    let remote = env.path("remote");
    let script = format!(
        r#"#!/bin/sh
# The first time, the real stitch; then a server for another session.
if [ -e "{dir}/opened" ]; then
    msg='{{"type":"ok","version":1,"session":"0123456789abcdef0123456789abcdef","socket":"/nonexistent"}}'
    printf '%08x%s' ${{#msg}} "$msg"
    exec sleep 30
fi
touch "{dir}/opened"
exec {stitch} "$@"
"#,
        dir = env.dir.path().display(),
        stitch = STITCH,
    );
    fs::write(&remote, script).unwrap();
    fs::set_permissions(&remote, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let mut t = env.tunnel_with(&["--", "cat"], |cmd| {
        cmd.env("STITCH_REMOTE", &remote);
    });
    let mut stdin = t.stdin.take().unwrap();
    let mut stdout = t.stdout.take().unwrap();
    stdin.write_all(b"ping").unwrap();
    assert_eq!(read_n(&mut stdout, 4), b"ping");
    env.break_connection(&env.invocation(0));
    let status = t.wait();
    assert!(!status.success(), "{status}");
    let log = client_log(&env);
    assert!(log.contains("remote session is gone"), "{log}");
    assert!(log.contains("answered for session"), "{log}");
}

#[test]
fn bad_resumes_are_rejected() {
    let env = Env::new();
    let mut t = env.tunnel(&["--", "cat"]);
    let mut stdin = t.stdin.take().unwrap();
    let mut stdout = t.stdout.take().unwrap();
    stdin.write_all(b"one").unwrap();
    let mut buf = [0u8; 3];
    stdout.read_exact(&mut buf).unwrap();

    let socket = env.sockets().pop().unwrap();
    let id = socket.file_stem().unwrap().to_str().unwrap().to_owned();

    // The open was epoch 0.
    let (reply, _) = shim_exchange(&env, &Message::resume(&id, &socket, 0).encode());
    expect_error(reply, ErrorCode::StaleEpoch);
    // Another session's id.
    let other = "0123456789abcdef0123456789abcdef";
    let (reply, _) = shim_exchange(&env, &Message::resume(other, &socket, 7).encode());
    expect_error(reply, ErrorCode::NoSession);
    // No such socket (reported by the shim itself).
    let missing = env.rendezvous().join(format!("{other}.sock"));
    let (reply, status) = shim_exchange(&env, &Message::resume(other, &missing, 1).encode());
    expect_error(reply, ErrorCode::NoSession);
    assert!(!status.success());
    // Not a handshake message at all.
    let (reply, status) = shim_exchange(&env, b"hello, world");
    expect_error(reply, ErrorCode::BadRequest);
    assert!(!status.success());
    // An open, sent straight to the daemon.
    {
        let conn = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        let (r, w_far) = nix::unistd::pipe().unwrap();
        let (r_far, w) = nix::unistd::pipe().unwrap();
        let open = Message::open(
            1000,
            handshake::Target::Command {
                command: "true".into(),
            },
        );
        use std::os::fd::AsFd;
        stitch::fdpass::send_with_fds(&conn, &open.encode(), &[r.as_fd(), w.as_fd()]).unwrap();
        drop((r, w, w_far));
        let (_, reply) = handshake::read_message(&mut fs::File::from(r_far)).unwrap();
        expect_error(reply, ErrorCode::BadRequest);
    }

    // The session is unaffected.
    stdin.write_all(b"two").unwrap();
    drop(stdin);
    let mut rest = Vec::new();
    stdout.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"two");
    assert!(t.wait().success());
    assert_eq!(env.invocations().len(), 1);
    env.assert_cleaned_up();
}

#[test]
fn stdin_from_dev_null() {
    let env = Env::new();
    let mut t = env.tunnel_with(&["--", "echo", "hi"], |cmd| {
        cmd.stdin(Stdio::null());
    });
    let out = t.drain(Arc::default());
    assert!(t.wait().success());
    assert_eq!(out.join().unwrap(), b"hi\n");
}

#[test]
fn stdout_to_regular_file() {
    let env = Env::new();
    let path = env.path("out");
    let data = test_data(2 << 20, 3);
    fs::write(env.path("in"), &data).unwrap();
    let mut t = env.tunnel_with(&["--", "cat"], |cmd| {
        cmd.stdin(fs::File::open(env.path("in")).unwrap())
            .stdout(fs::File::create(&path).unwrap());
    });
    assert!(t.wait().success());
    assert!(fs::read(&path).unwrap() == data);
}

// ---------------------------------------------------------------------
// The end of a session
// ---------------------------------------------------------------------

fn server_log(env: &Env) -> String {
    let mut out = String::new();
    if let Ok(d) = fs::read_dir(env.rendezvous()) {
        for e in d.flatten() {
            if e.path().extension().is_some_and(|e| e == "log") {
                out += &fs::read_to_string(e.path()).unwrap_or_default();
            }
        }
    }
    out
}

fn wait_for_server_log(env: &Env, text: &str) {
    wait_until(text, || server_log(env).contains(text));
}

/// Reads exactly `n` bytes.
fn read_n(r: &mut impl Read, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).unwrap();
    buf
}

/// The relay of a fake ssh which carries data from the server to the
/// client: the one writing to the same pipe as the ssh's own stdout.
fn server_to_client_relay(inv: &Invocation) -> i32 {
    let stdout = |pid: i32| fs::read_link(format!("/proc/{pid}/fd/1")).unwrap();
    let ssh = stdout(inv.ssh);
    *inv.relays
        .iter()
        .find(|&&p| stdout(p) == ssh)
        .expect("a relay writing to ssh's stdout")
}

#[test]
fn slow_reader_at_session_end() {
    // The server finishes while the client still holds data for a reader
    // which is slow to take it. The client must still deliver all of it
    // and exit successfully.
    let env = Env::new();
    let len = 3_000_000;
    let mut t = env.tunnel_with(
        &["--", "head", "-c", &len.to_string(), "/dev/zero"],
        |cmd| {
            cmd.stdin(Stdio::null());
        },
    );
    let mut stdout = t.stdout.take().unwrap();
    // Enough to make the client send a window update after the server has
    // finished.
    let mut got = read_n(&mut stdout, 1_300_000);
    wait_for_server_log(&env, "session finished");
    std::thread::sleep(Duration::from_millis(500));
    stdout.read_to_end(&mut got).unwrap();
    assert_eq!(got.len(), len);
    assert!(got.iter().all(|&b| b == 0));
    let status = t.wait();
    assert!(status.success(), "{status}");
    env.assert_cleaned_up();
}

#[test]
fn channel_lost_at_session_end_while_delivering() {
    // As above, but the channel is lost once the server has finished, so
    // the server goes away and the client's resume finds no session. The
    // client has received everything, and so still exits successfully once
    // it has delivered it.
    let env = Env::new();
    let len = 3_000_000;
    let mut t = env.tunnel_with(
        &["--", "head", "-c", &len.to_string(), "/dev/zero"],
        |cmd| {
            cmd.stdin(Stdio::null());
        },
    );
    let mut stdout = t.stdout.take().unwrap();
    let mut got = read_n(&mut stdout, 100_000);
    wait_for_server_log(&env, "session finished");
    env.break_connection(&env.invocation(0));
    wait_for_client_log(&env, "reconnecting (epoch 1)");
    std::thread::sleep(Duration::from_millis(500));
    stdout.read_to_end(&mut got).unwrap();
    assert_eq!(got.len(), len);
    let status = t.wait();
    assert!(status.success(), "{status}");
    env.assert_cleaned_up();
}

#[test]
fn final_ack_lost_resume_during_linger() {
    // The server's acknowledgement of the client's shutdown, the last thing
    // the client needs, is lost: the server finishes, but the client cannot
    // until it resumes and receives it on the new channel.
    let env = Env::new();
    let mut t = env.tunnel(&[
        "--heartbeat-interval",
        "0.1",
        "--timeout",
        "0.5",
        "--",
        "echo",
        "hello",
    ]);
    let mut out = Vec::new();
    t.stdout.take().unwrap().read_to_end(&mut out).unwrap();
    assert_eq!(out, b"hello\n");
    // The server's shutdown has been received, and its acknowledgement
    // sent; from now on nothing reaches the client on this channel.
    let first = env.invocation(0);
    signal(server_to_client_relay(&first), Signal::SIGSTOP);
    drop(t.stdin.take());
    wait_for_server_log(&env, "session finished");
    let status = t.wait();
    assert!(status.success(), "{status}");
    assert!(client_log(&env).contains("reconnected (epoch 1)"));
    env.assert_cleaned_up();
}

#[test]
fn ssh_exits_before_the_channel_closes() {
    // An ssh which exits while its channel is still open (held by another
    // process) is not, by itself, a reason to resume at once: the channel
    // may still carry the end of the session.
    let env = Env::new();
    fs::write(env.path("detach"), "").unwrap();
    let mut t = env.tunnel_with(&["--", "echo", "hello"], |cmd| {
        cmd.env("FAKE_SSH_DETACH", env.path("detach"));
    });
    drop(t.stdin.take());
    let out = t.drain(Arc::default());
    let status = t.wait();
    assert!(status.success(), "{status}");
    assert_eq!(out.join().unwrap(), b"hello\n");
    let log = client_log(&env);
    assert!(log.contains("ssh exited"), "{log}");
    assert!(!log.contains("reconnecting"), "{log}");
    env.assert_cleaned_up();
}

#[test]
fn session_end_stress() {
    // Many short sessions at once, with readers which pause, to shake out
    // races at the end of a session.
    let runs: Vec<_> = (0..6u8)
        .map(|i| {
            std::thread::spawn(move || {
                for j in 0..2u8 {
                    let env = Env::new();
                    let mut t = env.tunnel(&["--", "cat"]);
                    let data = test_data((3 << 20) + 4099 * usize::from(i + j), i * 3 + j);
                    let writer = t.feed(data.clone(), |_| {});
                    let mut stdout = t.stdout.take().unwrap();
                    let mut got = Vec::new();
                    let mut buf = vec![0u8; 32 << 10];
                    let mut reads = 0usize;
                    loop {
                        let n = stdout.read(&mut buf).unwrap();
                        if n == 0 {
                            break;
                        }
                        got.extend_from_slice(&buf[..n]);
                        reads += 1;
                        if reads % 8 == usize::from(i % 8) {
                            std::thread::sleep(Duration::from_millis(40));
                        }
                    }
                    writer.join().unwrap();
                    let status = t.wait();
                    assert!(status.success(), "{status}\n{}", env.logs());
                    assert!(got == data);
                }
            })
        })
        .collect();
    for r in runs {
        r.join().unwrap();
    }
}
