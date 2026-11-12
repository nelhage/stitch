//! Adversarial and fuzz-style tests through the public API: corrupted or
//! hostile input must be reported as a [`ProtocolError`], never a panic,
//! after which the session is dead; and specific misbehaviour by a peer
//! (window violation, acknowledging unsent data, gaps, a missing `FIRST`, a
//! decreasing limit, ...) must be detected as such, starting from state
//! reached by an honest exchange.

use std::time::{Duration, Instant};

use proptest::prelude::*;
use stitch::core::{Config, ProtocolError, Session};
use stitch::frame::{FrameDecoder, FrameHeader, HEADER_LEN, encode_frame};

use crate::rng::Rng;

/// Takes all pending output from `s`.
fn drain(s: &mut Session, now: Instant, out: &mut Vec<u8>) {
    loop {
        let d = s.transmit_data(now);
        if d.is_empty() {
            return;
        }
        let n = d.len();
        out.extend_from_slice(d);
        s.consume_transmit(n, now);
    }
}

fn read_all(s: &mut Session) -> usize {
    let mut total = 0;
    loop {
        let n = s.app_data().len();
        if n == 0 {
            return total;
        }
        s.consume_app(n);
        total += n;
    }
}

/// Two sessions joined back to back, with a manual clock.
struct Loop {
    a: Session,
    b: Session,
    now: Instant,
}

impl Loop {
    fn new(ca: Config, cb: Config) -> Loop {
        let now = Instant::now();
        let mut l = Loop {
            a: Session::new(ca),
            b: Session::new(cb),
            now,
        };
        l.a.swap_channel(now);
        l.b.swap_channel(now);
        l
    }

    /// Exchanges frames until both sides are quiet.
    fn settle(&mut self) {
        for _ in 0..100_000 {
            let (mut ab, mut ba) = (Vec::new(), Vec::new());
            drain(&mut self.a, self.now, &mut ab);
            drain(&mut self.b, self.now, &mut ba);
            if ab.is_empty() && ba.is_empty() {
                return;
            }
            self.b.receive(&ab, self.now).unwrap();
            self.a.receive(&ba, self.now).unwrap();
        }
        panic!("loop did not settle");
    }

    /// An honest exchange in both directions, leaving `a` with non-trivial
    /// state: data sent and acknowledged, data received, unacknowledged data
    /// in flight, and a send limit above its acknowledgement.
    fn established() -> Loop {
        let cfg = |recv| Config {
            max_payload: 200,
            ..Config::with_recv_buffer(recv)
        };
        let mut l = Loop::new(cfg(1000), cfg(1000));
        l.settle();
        assert_eq!(l.a.write_app(&[1; 1500]), 1500);
        assert_eq!(l.b.write_app(&[2; 700]), 700);
        for _ in 0..5 {
            l.settle();
            read_all(&mut l.a);
            read_all(&mut l.b);
            l.now += Duration::from_secs(1);
            l.a.handle_timeout(l.now);
            l.b.handle_timeout(l.now);
        }
        l.settle();
        // Leave some of a's data unacknowledged.
        assert_eq!(l.a.write_app(&[3; 50]), 50);
        let mut lost = Vec::new();
        drain(&mut l.a, l.now, &mut lost);
        assert!(l.a.max_sent() > l.a.peer_ack() && l.a.peer_ack() > 0);
        assert!(l.a.recv_high_water_mark() > 0);
        assert!(l.a.send_limit() > l.a.peer_ack());
        l
    }
}

/// A frame from `a`'s peer which is valid for `a` in every respect except
/// those the caller changes.
fn honest_header(a: &Session) -> FrameHeader {
    FrameHeader {
        first: false,
        shutdown: false,
        length: 0,
        window: (a.send_limit() - a.peer_ack()) as u32,
        seq: a.recv_high_water_mark(),
        ack: a.peer_ack(),
    }
}

fn frame(h: FrameHeader, payload: &[u8]) -> Vec<u8> {
    let h = FrameHeader {
        length: payload.len() as u16,
        ..h
    };
    let mut out = Vec::new();
    encode_frame(&h, payload, &mut out);
    out
}

/// After a protocol error the session is dead: the error sticks and
/// nothing more is produced.
fn assert_dead(s: &mut Session, now: Instant, e: &ProtocolError) {
    assert_eq!(s.error(), Some(e));
    assert_eq!(s.receive(&[0; HEADER_LEN], now).as_ref(), Err(e));
    assert!(s.transmit_data(now).is_empty());
    assert_eq!(s.poll_timeout(), None);
    assert_eq!(s.send_space(), 0);
    assert!(!s.is_finished());
    s.handle_timeout(now + Duration::from_secs(3600));
    assert!(s.transmit_data(now).is_empty());
}

fn expect(l: &mut Loop, bytes: &[u8], check: impl Fn(&ProtocolError) -> bool) {
    let before = (l.a.recv_high_water_mark(), l.a.peer_ack(), l.a.send_limit());
    let e =
        l.a.receive(bytes, l.now)
            .expect_err("misbehaviour was not detected");
    assert!(check(&e), "wrong error: {e:?}");
    // Nothing from the bad frame was applied.
    assert_eq!(
        before,
        (l.a.recv_high_water_mark(), l.a.peer_ack(), l.a.send_limit())
    );
    assert_dead(&mut l.a, l.now, &e);
}

#[test]
fn honest_forgery_is_accepted() {
    // The baseline for the tests below: only the violation differs.
    let mut l = Loop::established();
    let h = honest_header(&l.a);
    let free = l.a.window() as usize;
    l.a.receive(&frame(h, &vec![7; free]), l.now).unwrap();
    assert_eq!(l.a.recv_buffered(), free);
}

#[test]
fn peer_window_violation_detected() {
    let mut l = Loop::established();
    let h = honest_header(&l.a);
    let n = l.a.window() as usize + 1;
    expect(&mut l, &frame(h, &vec![7; n]), |e| {
        matches!(e, ProtocolError::ReceiveOverflow { .. })
    });
}

#[test]
fn peer_ack_beyond_sent_detected() {
    let mut l = Loop::established();
    let mut h = honest_header(&l.a);
    h.ack = l.a.max_sent() + 1;
    h.window = 1 << 20;
    expect(&mut l, &frame(h, b""), |e| {
        matches!(e, ProtocolError::AckBeyondSent { .. })
    });
}

#[test]
fn peer_ack_backwards_detected() {
    let mut l = Loop::established();
    let mut h = honest_header(&l.a);
    h.ack -= 1;
    h.window += 1;
    expect(&mut l, &frame(h, b""), |e| {
        matches!(e, ProtocolError::AckWentBackwards { .. })
    });
}

#[test]
fn peer_limit_decrease_detected() {
    let mut l = Loop::established();
    let mut h = honest_header(&l.a);
    h.window -= 1;
    expect(&mut l, &frame(h, b""), |e| {
        matches!(e, ProtocolError::LimitDecreased { .. })
    });
}

#[test]
fn peer_gap_detected() {
    let mut l = Loop::established();
    let mut h = honest_header(&l.a);
    h.seq += 1;
    expect(&mut l, &frame(h, b"x"), |e| {
        matches!(e, ProtocolError::Gap { .. })
    });
}

#[test]
fn peer_duplicate_after_resync_detected() {
    let mut l = Loop::established();
    let mut h = honest_header(&l.a);
    h.seq -= 1;
    expect(&mut l, &frame(h, b"xy"), |e| {
        matches!(e, ProtocolError::BehindHighWaterMark { .. })
    });
}

#[test]
fn peer_duplicate_after_exact_catch_up_detected() {
    // After a swap, a frame which ends exactly at the high-water mark
    // resynchronizes the channel; a duplicate after it is an error.
    let mut l = Loop::established();
    l.a.swap_channel(l.now);
    let hwm = l.a.recv_high_water_mark();
    let mut h = honest_header(&l.a);
    h.first = true;
    h.seq = hwm - 5;
    l.a.receive(&frame(h, &[2; 5]), l.now).unwrap();
    assert_eq!(l.a.recv_high_water_mark(), hwm);
    assert!(l.a.channel_resynced());
    let mut h = honest_header(&l.a);
    h.seq = hwm - 1;
    expect(&mut l, &frame(h, b"xy"), |e| {
        matches!(e, ProtocolError::BehindHighWaterMark { .. })
    });
}

#[test]
fn peer_missing_first_detected() {
    let mut l = Loop::established();
    l.a.swap_channel(l.now);
    let h = honest_header(&l.a);
    expect(&mut l, &frame(h, b""), |e| {
        *e == ProtocolError::MissingFirst
    });
}

#[test]
fn peer_unexpected_first_detected() {
    let mut l = Loop::established();
    let mut h = honest_header(&l.a);
    h.first = true;
    expect(&mut l, &frame(h, b""), |e| {
        matches!(e, ProtocolError::UnexpectedFirst { .. })
    });
}

#[test]
fn peer_data_beyond_shutdown_detected() {
    let mut l = Loop::established();
    l.b.close_app_send();
    // (Some of a's output was dropped; a new channel recovers it.)
    l.a.swap_channel(l.now);
    l.b.swap_channel(l.now);
    l.settle();
    assert!(l.a.peer_shutdown().is_some());
    let h = honest_header(&l.a);
    expect(&mut l, &frame(h, b"late"), |e| {
        matches!(e, ProtocolError::BeyondShutdown { .. })
    });
}

#[test]
fn peer_shutdown_before_received_data_detected() {
    let mut l = Loop::established();
    let mut h = honest_header(&l.a);
    h.shutdown = true;
    l.a.swap_channel(l.now);
    h.first = true;
    h.seq -= 10;
    expect(&mut l, &frame(h, b"abc"), |e| {
        matches!(e, ProtocolError::ConflictingShutdown { .. })
    });
}

#[test]
fn peer_bad_flags_detected() {
    let mut l = Loop::established();
    let mut f = frame(honest_header(&l.a), b"");
    f[0] |= 0x80;
    expect(&mut l, &f, |e| matches!(e, ProtocolError::Header(_)));
}

// --------------------------------------------------------------------
// Fuzzing
// --------------------------------------------------------------------

/// Checks the frames a session emits: well-formed, `FIRST` exactly on the
/// first frame of a channel, and no data beyond its send limit.
#[derive(Default)]
struct OutputCheck {
    decoder: FrameDecoder,
    frames: u64,
}

impl OutputCheck {
    fn new_channel(&mut self) {
        *self = OutputCheck::default();
    }

    fn check(&mut self, s: &Session, bytes: &[u8]) {
        self.decoder.push(bytes);
        while let Some((h, _)) = self
            .decoder
            .next_frame()
            .expect("session emitted a bad header")
        {
            assert_eq!(
                h.first,
                self.frames == 0,
                "FIRST flag on frame {}",
                self.frames
            );
            self.frames += 1;
            if h.length > 0 {
                assert!(
                    h.seq + u64::from(h.length) <= s.send_limit(),
                    "data beyond send limit"
                );
            }
        }
    }
}

fn check_state(s: &Session) {
    assert!(s.recv_buffered() <= s.recv_capacity());
    assert!(s.send_buffered() <= s.send_capacity());
    assert!(s.peer_ack() <= s.max_sent());
}

/// Corrupts `wire` in a random way.
fn mutate(r: &mut Rng, wire: &mut Vec<u8>) {
    if wire.is_empty() {
        let n = r.usize_range(1, 40);
        wire.extend(r.bytes(n));
        return;
    }
    let len = wire.len();
    let i = r.usize_range(0, len - 1);
    match r.below(7) {
        0 => wire[i] ^= 1 << r.below(8),
        1 => {
            let j = r.usize_range(i, len);
            wire.drain(i..j);
        }
        2 => {
            let n = r.usize_range(1, 40);
            let extra = r.bytes(n);
            wire.splice(i..i, extra);
        }
        3 => {
            let j = r.usize_range(i, len);
            let dup = wire[i..j].to_vec();
            wire.splice(j..j, dup);
        }
        4 => wire.truncate(i),
        5 => {
            // Reorder: move a range to the end.
            let j = r.usize_range(i, len);
            let moved: Vec<u8> = wire.drain(i..j).collect();
            wire.extend(moved);
        }
        _ => {
            // Overwrite a header-sized range with garbage.
            let j = (i + HEADER_LEN).min(len);
            let junk = r.bytes(j - i);
            wire[i..j].copy_from_slice(&junk);
        }
    }
}

/// Honest traffic between two sessions, with random corruption injected
/// into the wire: nothing panics; a session either survives or dies with a
/// sticky error; live sessions keep their invariants.
fn corrupted_traffic(seed: u64) {
    let mut r = Rng::new(seed);
    let mut cfg = || Config {
        max_payload: r.usize_log(1, 3000),
        send_buffer: r.usize_log(1, 20_000),
        window_update_threshold: r.usize_range(0, 500),
        ack_threshold: r.usize_log(1, 5000),
        ..Config::with_recv_buffer(r.usize_log(1, 5000))
    };
    let cfgs = [cfg(), cfg()];
    let mut now = Instant::now();
    let mut ends = cfgs.map(Session::new);
    for e in &mut ends {
        e.swap_channel(now);
    }
    let mut wires = [Vec::new(), Vec::new()]; // written by ends[0], ends[1]
    let mut out = [OutputCheck::default(), OutputCheck::default()];
    let data = r.bytes(30_000);
    let mut written = [0usize; 2];
    let corrupt_at = r.range(0, 150);
    let mut errors: [Option<ProtocolError>; 2] = [None, None];
    for step in 0..200 {
        if step == corrupt_at {
            let w = r.below(2) as usize;
            mutate(&mut r, &mut wires[w]);
        }
        match r.below(8) {
            0 => {
                for x in 0..2 {
                    let off = written[x];
                    let end = (off + r.usize_range(0, 3000)).min(data.len());
                    if !ends[x].send_closed() {
                        written[x] += ends[x].write_app(&data[off..end]);
                    }
                }
            }
            1 | 2 => {
                for x in 0..2 {
                    let mut buf = Vec::new();
                    drain(&mut ends[x], now, &mut buf);
                    out[x].check(&ends[x], &buf);
                    wires[x].extend(buf);
                }
            }
            3 | 4 => {
                for x in 0..2 {
                    let y = 1 - x;
                    let k = r.usize_range(0, wires[x].len());
                    let part: Vec<u8> = wires[x].drain(..k).collect();
                    match ends[y].receive(&part, now) {
                        Ok(()) => assert!(errors[y].is_none()),
                        Err(e) => {
                            if let Some(prev) = &errors[y] {
                                assert_eq!(prev, &e, "error changed");
                            }
                            errors[y] = Some(e);
                        }
                    }
                }
            }
            5 => {
                for e in &mut ends {
                    read_all(e);
                    if r.chance(1, 20) {
                        e.close_app_read();
                    }
                }
            }
            6 => {
                now += Duration::from_millis(r.range(1, 5000));
                for e in &mut ends {
                    e.handle_timeout(now);
                }
            }
            _ => {
                if r.chance(1, 3) {
                    wires = [Vec::new(), Vec::new()];
                    for x in 0..2 {
                        ends[x].swap_channel(now);
                        out[x].new_channel();
                    }
                } else if r.chance(1, 4) {
                    ends[r.below(2) as usize].close_app_send();
                }
            }
        }
        for x in 0..2 {
            check_state(&ends[x]);
            if let Some(e) = &errors[x] {
                assert_dead(&mut ends[x], now, e);
            }
        }
    }
}

/// Random frames with fields close to the victim's real state (so that many
/// are almost valid), split at random points and interleaved with
/// application activity, timeouts and swaps.
/// Returns how many frames the victim survived.
fn hostile_frames(seed: u64) -> u64 {
    let mut r = Rng::new(seed);
    let mut l = Loop::established();
    let mut out = OutputCheck {
        frames: 1,
        ..Default::default()
    };
    let mut pending = Vec::new();
    let mut prev = (l.a.send_limit(), l.a.peer_ack(), l.a.recv_high_water_mark());
    let mut first_due = false;
    let mut survived = 0;
    let mut v = (0, 0, 0, false);
    // Whether any frame so far was deliberately wrong.
    let mut tainted = false;
    for _ in 0..120 {
        // An honest frame for the state the victim will be in once it has
        // processed everything pending (`v`: hwm, ack, limit, shut down)...
        let a = &l.a;
        if pending.is_empty() {
            v = (
                a.recv_high_water_mark(),
                a.peer_ack(),
                a.send_limit(),
                a.peer_shutdown().is_some(),
            );
        }
        let (hwm, v_ack, v_limit, v_shut) = v;
        let ack = r.range(v_ack, a.max_sent().max(v_ack));
        let window = (v_limit.max(ack) - ack + r.range(0, 200)).min(u64::from(u32::MAX));
        // `v` is only a guess: once a `garbage` frame has corrupted a length
        // field, the victim's framing can drift from ours (it may still hold
        // part of a frame when `pending` is empty, then swallow later frames
        // as its payload), so its high-water mark can pass `hwm`.
        let unfed = hwm.saturating_sub(a.recv_high_water_mark()) as usize;
        let free = (a.window() as usize).saturating_sub(unfed);
        let mut seq = hwm;
        let mut len = r.usize_range(0, free.min(300));
        if first_due && r.chance(1, 2) && !v_shut {
            // Catching up after a swap: start below the high-water mark.
            let back = r.range(0, hwm.min(100));
            seq = hwm - back;
            len = (back as usize + r.usize_range(0, free.min(100))).min(65535);
        }
        let mut h = FrameHeader {
            first: first_due,
            shutdown: !v_shut && r.chance(1, 40),
            length: 0,
            window: window as u32,
            seq,
            ack,
        };
        if v_shut {
            len = 0;
            h.seq = hwm;
        }
        // ... with, sometimes, one thing wrong with it.
        let mut garbage = false;
        if r.chance(1, 12) {
            tainted = true;
            match r.below(9) {
                0 => h.seq = h.seq.saturating_sub(r.range(1, 5)),
                1 => h.seq += r.range(1, 5),
                2 => h.ack = h.ack.saturating_sub(r.range(1, 5)),
                3 => h.ack += r.range(1, 5),
                4 => h.window = h.window.saturating_sub(r.range(1, 300) as u32),
                5 => len = free + r.usize_range(1, 3),
                6 => h.first = !h.first,
                7 => h.shutdown = !h.shutdown,
                _ => garbage = true,
            }
        }
        first_due = false;
        let len = len.min(65535);
        let end = h.seq + len as u64;
        v = (
            v.0.max(end + u64::from(h.shutdown && !v_shut)),
            v.1.max(h.ack),
            v.2.max(h.ack + u64::from(h.window)),
            v.3 || h.shutdown,
        );
        let mut f = frame(h, &r.bytes(len));
        if garbage {
            let i = r.usize_range(0, f.len() - 1);
            f[i] = r.below(256) as u8;
        }
        pending.extend(f);
        // Feed what is pending (usually all of it), in random pieces.
        let k = if r.chance(1, 4) {
            r.usize_range(0, pending.len())
        } else {
            pending.len()
        };
        let feed: Vec<u8> = pending.drain(..k).collect();
        let mut dead = false;
        for piece in feed.chunks(r.usize_range(1, 64)) {
            if let Err(e) = l.a.receive(piece, l.now) {
                assert!(tainted, "an honest frame was rejected: {e}");
                assert_dead(&mut l.a, l.now, &e);
                dead = true;
                break;
            }
        }
        if dead {
            return survived;
        }
        survived += 1;
        check_state(&l.a);
        let now = (l.a.send_limit(), l.a.peer_ack(), l.a.recv_high_water_mark());
        assert!(
            now.0 >= prev.0 && now.1 >= prev.1 && now.2 >= prev.2,
            "went backwards"
        );
        prev = now;
        match r.below(6) {
            0 => {
                let n = r.usize_range(0, 500);
                l.a.write_app(&r.bytes(n));
            }
            1 => {
                read_all(&mut l.a);
            }
            2 => {
                l.now += Duration::from_millis(r.range(1, 30_000));
                l.a.handle_timeout(l.now);
            }
            3 if r.chance(1, 4) => {
                l.a.swap_channel(l.now);
                out.new_channel();
                pending.clear();
                first_due = true;
            }
            _ => {}
        }
        let mut buf = Vec::new();
        drain(&mut l.a, l.now, &mut buf);
        out.check(&l.a, &buf);
    }
    survived
}

/// Most hostile frames are nearly valid, so the fuzzer gets deep into a
/// session's life rather than killing it with the first frame.
#[test]
fn hostile_frames_go_deep() {
    let total: u64 = (0..200).map(hostile_frames).sum();
    println!("average frames survived: {}", total / 200);
    assert!(
        total / 200 >= 8,
        "hostile frames kill the session too early"
    );
}

/// A corrupted length field desynchronized the victim's framing from the
/// generator's model, which used to underflow computing `unfed`.
#[test]
fn hostile_frames_regression_length_corruption() {
    hostile_frames(12581757328608198469);
}

fn fuzz_cases() -> u32 {
    std::env::var("SIM_FUZZ_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}

proptest! {
    // A failure prints the seed: call `corrupted_traffic(seed)` or
    // `hostile_frames(seed)` from a test to reproduce it.
    #![proptest_config(ProptestConfig {
        cases: fuzz_cases(),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn fuzz_corrupted_traffic_never_panics(seed in any::<u64>()) {
        corrupted_traffic(seed);
    }

    #[test]
    fn fuzz_hostile_frames_never_panic(seed in any::<u64>()) {
        hostile_frames(seed);
    }

    #[test]
    fn fuzz_arbitrary_bytes_never_panic(
        established in any::<bool>(),
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..100), 0..20),
    ) {
        let (mut s, now) = if established {
            let l = Loop::established();
            (l.a, l.now)
        } else {
            let now = Instant::now();
            let mut s = Session::new(Config::with_recv_buffer(64));
            s.swap_channel(now);
            (s, now)
        };
        for c in &chunks {
            match s.receive(c, now) {
                Ok(()) => check_state(&s),
                Err(e) => {
                    assert_dead(&mut s, now, &e);
                    break;
                }
            }
            s.transmit_data(now);
        }
    }
}
