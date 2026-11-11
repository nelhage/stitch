//! Tests of the [`Pump`] over real pipes and sockets, in one process.

use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::thread::JoinHandle;
use std::time::Duration;

use super::*;

fn pipe() -> (OwnedFd, OwnedFd) {
    nix::unistd::pipe().unwrap()
}

/// Two channels joined by a pair of pipes: what one end writes, the other
/// reads.
fn channel_pair() -> (Channel, Channel) {
    let (a_read, b_write) = pipe();
    let (b_read, a_write) = pipe();
    (Channel::new(a_read, a_write), Channel::new(b_read, b_write))
}

/// An application backed by a Unix socket pair; returns the far end.
fn socket_app() -> (App, UnixStream) {
    let (ours, theirs) = UnixStream::pair().unwrap();
    let fd = OwnedFd::from(ours);
    let fd2 = fd.try_clone().unwrap();
    (App::new(Some(fd), Some(fd2)).unwrap(), theirs)
}

fn test_data(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 13) as u8)
        .collect()
}

/// Writes `data` to `s` and then shuts down its write side; reads `s` to
/// EOF in another thread. Returns the reader's result.
fn exchange(s: UnixStream, data: Vec<u8>) -> JoinHandle<Vec<u8>> {
    let mut w = s.try_clone().unwrap();
    std::thread::spawn(move || {
        w.write_all(&data).unwrap();
        w.shutdown(Shutdown::Write).unwrap();
    });
    let mut r = s;
    std::thread::spawn(move || {
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        out
    })
}

fn config() -> Config {
    Config {
        heartbeat_interval: Duration::from_millis(200),
        timeout: Duration::from_secs(5),
        ..Config::with_recv_buffer(256 << 10)
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Runs both pumps until both finish, calling `tick` every 5 ms with both.
async fn run_pair(a: &mut Pump, b: &mut Pump, mut tick: impl FnMut(&mut Pump, &mut Pump)) {
    let (mut a_done, mut b_done) = (false, false);
    let mut interval = tokio::time::interval(Duration::from_millis(5));
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    while !(a_done && b_done) {
        tokio::select! {
            ev = a.next_event(), if !a_done => match ev {
                Event::Finished { flushed } => { assert!(flushed); a_done = true }
                Event::ChannelLost(_) | Event::TimedOut => {}
                e => panic!("a: {e:?}"),
            },
            ev = b.next_event(), if !b_done => match ev {
                Event::Finished { flushed } => { assert!(flushed); b_done = true }
                Event::ChannelLost(_) | Event::TimedOut => {}
                e => panic!("b: {e:?}"),
            },
            _ = interval.tick() => tick(a, b),
            () = &mut deadline => panic!("timed out"),
        }
    }
}

#[test]
fn bulk_transfer_both_ways() {
    rt().block_on(async {
        let (app_a, sock_a) = socket_app();
        let (app_b, sock_b) = socket_app();
        let mut a = Pump::new(config(), app_a);
        let mut b = Pump::new(config(), app_b);
        let (ca, cb) = channel_pair();
        a.install_channel(ca).unwrap();
        b.install_channel(cb).unwrap();

        let data_a = test_data(3 << 20, 1);
        let data_b = test_data(2 << 20, 2);
        let got_b = exchange(sock_a, data_a.clone());
        let got_a = exchange(sock_b, data_b.clone());
        run_pair(&mut a, &mut b, |_, _| {}).await;
        assert!(got_a.join().unwrap() == data_a);
        assert!(got_b.join().unwrap() == data_b);
    });
}

#[test]
fn preamble_precedes_frames() {
    rt().block_on(async {
        let (app, _sock) = socket_app();
        let mut p = Pump::new(config(), app);
        let (ours, theirs) = UnixStream::pair().unwrap();
        let ours = OwnedFd::from(ours);
        let ours2 = ours.try_clone().unwrap();
        p.install_channel(Channel::new(ours, ours2).with_preamble(b"hello".to_vec()))
            .unwrap();
        let reader = std::thread::spawn(move || {
            let mut buf = [0u8; 5 + crate::frame::HEADER_LEN];
            (&theirs).read_exact(&mut buf).unwrap();
            buf
        });
        // The first frame is written straight away.
        let _ = tokio::time::timeout(Duration::from_millis(200), p.next_event()).await;
        let buf = reader.join().unwrap();
        assert_eq!(&buf[..5], b"hello");
        let h = crate::frame::FrameHeader::decode(buf[5..].try_into().unwrap()).unwrap();
        assert!(h.first);
    });
}

#[test]
fn swap_mid_transfer() {
    rt().block_on(async {
        let (app_a, sock_a) = socket_app();
        let (app_b, sock_b) = socket_app();
        let mut a = Pump::new(config(), app_a);
        let mut b = Pump::new(config(), app_b);
        let (ca, cb) = channel_pair();
        a.install_channel(ca).unwrap();
        b.install_channel(cb).unwrap();

        let data_a = test_data(4 << 20, 3);
        let data_b = test_data(4 << 20, 4);
        let got_b = exchange(sock_a, data_a.clone());
        let got_a = exchange(sock_b, data_b.clone());
        let mut swaps = 0;
        run_pair(&mut a, &mut b, |a, b| {
            // Swap every megabyte or so, sometimes dropping the old channel
            // at one end before the other end swaps.
            let pos = a.session().send_position() + b.session().send_position();
            if pos > (swaps + 1) * (1 << 20) && swaps < 5 {
                swaps += 1;
                let (ca, cb) = channel_pair();
                if swaps % 2 == 0 {
                    b.drop_channel();
                }
                a.install_channel(ca).unwrap();
                b.install_channel(cb).unwrap();
            }
        })
        .await;
        assert!(swaps >= 3, "only {swaps} swaps");
        assert!(got_a.join().unwrap() == data_a);
        assert!(got_b.join().unwrap() == data_b);
    });
}

#[test]
fn unpollable_application_fds() {
    // stdin from /dev/null and stdout to a regular file: epoll cannot watch
    // either.
    rt().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let null = std::fs::File::open("/dev/null").unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let app_a = App::new(Some(null.into()), Some(file.into())).unwrap();
        let (app_b, sock_b) = socket_app();
        let mut a = Pump::new(config(), app_a);
        let mut b = Pump::new(config(), app_b);
        let (ca, cb) = channel_pair();
        a.install_channel(ca).unwrap();
        b.install_channel(cb).unwrap();
        let data_b = test_data(1 << 20, 5);
        let got_a = exchange(sock_b, data_b.clone());
        run_pair(&mut a, &mut b, |_, _| {}).await;
        assert!(got_a.join().unwrap().is_empty());
        assert!(std::fs::read(&path).unwrap() == data_b);
    });
}

#[test]
fn application_stops_reading() {
    // A's application closes its read side; data sent to it is discarded,
    // and the session still completes.
    rt().block_on(async {
        let (app_a, sock_a) = socket_app();
        let (app_b, sock_b) = socket_app();
        let mut a = Pump::new(config(), app_a);
        let mut b = Pump::new(config(), app_b);
        let (ca, cb) = channel_pair();
        a.install_channel(ca).unwrap();
        b.install_channel(cb).unwrap();
        sock_a.shutdown(Shutdown::Both).unwrap();
        drop(sock_a);
        let got = exchange(sock_b, test_data(1 << 20, 6));
        run_pair(&mut a, &mut b, |_, _| {}).await;
        assert!(got.join().unwrap().is_empty());
    });
}

#[test]
fn reports_channel_loss_and_timeout() {
    rt().block_on(async {
        let (app, _sock) = socket_app();
        let mut p = Pump::new(
            Config {
                timeout: Duration::from_millis(100),
                ..config()
            },
            app,
        );
        // A silent channel: its other ends are held open but never used.
        let (r, _w_far) = pipe();
        let (_r_far, w) = pipe();
        p.install_channel(Channel::new(r, w)).unwrap();
        let ev = tokio::time::timeout(Duration::from_secs(5), p.next_event())
            .await
            .unwrap();
        assert!(matches!(ev, Event::TimedOut), "{ev:?}");
        assert!(p.timed_out());

        // A channel whose peer has gone.
        let (r, w_far) = pipe();
        let (_r_far, w) = pipe();
        p.install_channel(Channel::new(r, w)).unwrap();
        assert!(!p.timed_out());
        drop(w_far);
        let ev = tokio::time::timeout(Duration::from_secs(5), p.next_event())
            .await
            .unwrap();
        assert!(matches!(ev, Event::ChannelLost(None)), "{ev:?}");
        assert!(!p.has_channel());
    });
}

#[test]
fn reports_protocol_error() {
    rt().block_on(async {
        let (app, _sock) = socket_app();
        let mut p = Pump::new(config(), app);
        let (r, w_far) = pipe();
        let (_r_far, w) = pipe();
        p.install_channel(Channel::new(r, w)).unwrap();
        nix::unistd::write(&w_far, &[0xffu8; 64]).unwrap();
        let ev = tokio::time::timeout(Duration::from_secs(5), p.next_event())
            .await
            .unwrap();
        assert!(matches!(ev, Event::ProtocolError(_)), "{ev:?}");
        // Terminal events repeat.
        assert!(matches!(p.next_event().await, Event::ProtocolError(_)));
    });
}
