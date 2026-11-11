//! Unit tests for the protocol core.
//!
//! [`Pair`] is a minimal loopback: two sessions joined by in-memory "wires",
//! with a manual clock. A full simulation harness lives elsewhere.

use std::time::{Duration, Instant};

use proptest::prelude::*;

use super::*;
use crate::frame::{FrameDecoder, FrameError, FrameHeader, encode_frame};

const MS: Duration = Duration::from_millis(1);
const SEC: Duration = Duration::from_secs(1);

fn decode_all(bytes: &[u8]) -> Vec<(FrameHeader, Vec<u8>)> {
    let mut d = FrameDecoder::new();
    d.push(bytes);
    let mut out = Vec::new();
    while let Some((h, p)) = d.next_frame().unwrap() {
        out.push((h, p.to_vec()));
    }
    assert_eq!(d.buffered(), 0, "trailing partial frame");
    out
}

/// Takes all pending output from `s` (one frame at a time) as raw bytes.
fn take_tx(s: &mut Session, now: Instant) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let d = s.transmit_data(now).to_vec();
        if d.is_empty() {
            return out;
        }
        s.consume_transmit(d.len(), now);
        out.extend(d);
    }
}

fn take_frames(s: &mut Session, now: Instant) -> Vec<(FrameHeader, Vec<u8>)> {
    decode_all(&take_tx(s, now))
}

/// Builds a frame as the peer would send it.
fn fr(first: bool, shutdown: bool, seq: u64, ack: u64, window: u32, payload: &[u8]) -> Vec<u8> {
    let h = FrameHeader {
        first,
        shutdown,
        length: payload.len() as u16,
        window,
        seq,
        ack,
    };
    let mut out = Vec::new();
    encode_frame(&h, payload, &mut out);
    out
}

fn hdr(first: bool, shutdown: bool, len: u16, window: u32, seq: u64, ack: u64) -> FrameHeader {
    FrameHeader {
        first,
        shutdown,
        length: len,
        window,
        seq,
        ack,
    }
}

/// Two sessions joined back to back.
struct Pair {
    a: Session,
    b: Session,
    now: Instant,
    /// Bytes emitted by `a` and not yet delivered to `b`.
    ab: Vec<u8>,
    /// Bytes emitted by `b` and not yet delivered to `a`.
    ba: Vec<u8>,
    /// Every frame header emitted by each side.
    log_a: Vec<FrameHeader>,
    log_b: Vec<FrameHeader>,
}

impl Pair {
    fn new(ca: Config, cb: Config) -> Pair {
        Pair {
            a: Session::new(ca),
            b: Session::new(cb),
            now: Instant::now(),
            ab: Vec::new(),
            ba: Vec::new(),
            log_a: Vec::new(),
            log_b: Vec::new(),
        }
    }

    fn connected(ca: Config, cb: Config) -> Pair {
        let mut p = Pair::new(ca, cb);
        p.swap();
        p.flush();
        p
    }

    /// Installs a new channel on both ends; anything in flight is lost.
    fn swap(&mut self) {
        self.ab.clear();
        self.ba.clear();
        self.a.swap_channel(self.now);
        self.b.swap_channel(self.now);
    }

    fn emit(s: &mut Session, wire: &mut Vec<u8>, log: &mut Vec<FrameHeader>, now: Instant) -> bool {
        let mut any = false;
        loop {
            let d = s.transmit_data(now).to_vec();
            if d.is_empty() {
                return any;
            }
            any = true;
            s.consume_transmit(d.len(), now);
            for (h, _) in decode_all(&d) {
                // Invariant: no data beyond the send limit.
                if h.length > 0 {
                    assert!(h.seq + u64::from(h.length) <= s.send_limit());
                }
                log.push(h);
            }
            wire.extend(d);
        }
    }

    fn emit_a(&mut self) -> bool {
        Pair::emit(&mut self.a, &mut self.ab, &mut self.log_a, self.now)
    }

    fn emit_b(&mut self) -> bool {
        Pair::emit(&mut self.b, &mut self.ba, &mut self.log_b, self.now)
    }

    fn deliver_ab(&mut self) {
        let d = std::mem::take(&mut self.ab);
        self.b.receive(&d, self.now).unwrap();
    }

    fn deliver_ba(&mut self) {
        let d = std::mem::take(&mut self.ba);
        self.a.receive(&d, self.now).unwrap();
    }

    /// Exchanges frames until both sides are quiet.
    fn flush(&mut self) {
        for _ in 0..1_000_000 {
            let any = self.emit_a() | self.emit_b();
            self.deliver_ab();
            self.deliver_ba();
            self.check_invariants();
            if !any {
                return;
            }
        }
        panic!("flush did not converge");
    }

    /// Runs for `d`, processing timeouts when they fall due.
    fn advance(&mut self, d: Duration) {
        let target = self.now + d;
        for _ in 0..1_000_000 {
            self.flush();
            let next = [self.a.poll_timeout(), self.b.poll_timeout()]
                .into_iter()
                .flatten()
                .min();
            match next {
                Some(t) if t <= target => {
                    self.now = self.now.max(t);
                    self.a.handle_timeout(self.now);
                    self.b.handle_timeout(self.now);
                }
                _ => {
                    self.now = target;
                    self.a.handle_timeout(self.now);
                    self.b.handle_timeout(self.now);
                    self.flush();
                    return;
                }
            }
        }
        panic!("advance did not converge");
    }

    fn check_invariants(&self) {
        for s in [&self.a, &self.b] {
            assert!(s.recv_buffered() <= s.recv_capacity());
            assert!(s.send_buffered() <= s.send_capacity());
            assert!(s.peer_ack() <= s.max_sent());
        }
    }
}

/// Drains everything the application side of `s` can read.
fn read_all(s: &mut Session) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let d = s.app_data();
        if d.is_empty() {
            return out;
        }
        let n = d.len();
        out.extend_from_slice(d);
        s.consume_app(n);
    }
}

/// Writes as much of `data[*off..]` as `s` accepts.
fn write_some(s: &mut Session, data: &[u8], off: &mut usize) {
    *off += s.write_app(&data[*off..]);
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

// --------------------------------------------------------------------
// Basic behaviour
// --------------------------------------------------------------------

#[test]
fn framing_md_example() {
    let t0 = Instant::now();
    let mut c = Session::new(Config::default());
    let mut s = Session::new(Config::default());
    c.swap_channel(t0);
    s.swap_channel(t0);
    c.write_app(b"hello");

    // Neither end may send data before learning its send limit.
    let c1 = take_tx(&mut c, t0);
    assert_eq!(
        decode_all(&c1),
        vec![(hdr(true, false, 0, 4194304, 0, 0), vec![])]
    );
    let s1 = take_tx(&mut s, t0);
    assert_eq!(
        decode_all(&s1),
        vec![(hdr(true, false, 0, 4194304, 0, 0), vec![])]
    );
    s.receive(&c1, t0).unwrap();
    c.receive(&s1, t0).unwrap();
    assert_eq!(c.send_limit(), 4194304);

    let c2 = take_tx(&mut c, t0);
    assert_eq!(
        decode_all(&c2),
        vec![(hdr(false, false, 5, 4194304, 0, 0), b"hello".to_vec())]
    );
    s.receive(&c2, t0).unwrap();

    // The acknowledgement is delayed.
    assert!(take_tx(&mut s, t0).is_empty());
    let due = t0 + Config::DEFAULT_ACK_DELAY;
    assert_eq!(s.poll_timeout(), Some(due));
    s.handle_timeout(due);
    let s2 = take_tx(&mut s, due);
    assert_eq!(
        decode_all(&s2),
        vec![(hdr(false, false, 0, 4194299, 0, 5), vec![])]
    );
    c.receive(&s2, due).unwrap();
    assert_eq!(c.peer_ack(), 5);
    assert_eq!(c.send_buffered(), 0);

    // Reading the data frees the window, but too little to announce.
    assert_eq!(read_all(&mut s), b"hello");
    assert_eq!(s.window(), 4194304);
    assert!(take_tx(&mut s, due).is_empty());
}

#[test]
fn bulk_transfer_both_directions() {
    let mut p = Pair::connected(Config::with_recv_buffer(100_000), Config::default());
    let da = pattern(1_000_000, 1);
    let db = pattern(300_000, 7);
    let (mut oa, mut ob) = (0, 0);
    let (mut ra, mut rb) = (Vec::new(), Vec::new());
    while ra.len() < db.len() || rb.len() < da.len() {
        write_some(&mut p.a, &da, &mut oa);
        write_some(&mut p.b, &db, &mut ob);
        p.flush();
        ra.extend(read_all(&mut p.a));
        rb.extend(read_all(&mut p.b));
        p.advance(MS);
    }
    assert_eq!(ra, db);
    assert_eq!(rb, da);
}

#[test]
fn nothing_happens_without_a_channel() {
    let t0 = Instant::now();
    let mut s = Session::new(Config::default());
    s.write_app(b"data");
    assert!(!s.has_channel());
    assert_eq!(s.poll_timeout(), None);
    s.handle_timeout(t0 + 1000 * SEC);
    assert!(!s.transport_timed_out());
    assert!(s.transmit_data(t0 + 1000 * SEC).is_empty());
}

// --------------------------------------------------------------------
// Channel swaps
// --------------------------------------------------------------------

/// The peer received data on the old channel but its acknowledgements were
/// lost: after the swap, the retransmitted data is discarded as duplicate.
#[test]
fn swap_retransmits_and_discards_duplicates() {
    let cfg = Config {
        max_payload: 100,
        ..Config::default()
    };
    let mut p = Pair::connected(cfg.clone(), cfg);
    let data = pattern(1000, 3);
    p.a.write_app(&data);
    // All data reaches b, but b's acks never reach a.
    p.emit_a();
    p.deliver_ab();
    p.now += SEC;
    p.b.handle_timeout(p.now);
    p.emit_b();
    p.ba.clear();
    assert_eq!(p.b.recv_high_water_mark(), 1000);
    assert_eq!(p.a.peer_ack(), 0);
    assert_eq!(p.a.max_sent(), 1000);

    p.swap();
    assert_eq!(p.a.send_position(), 0);
    // a's FIRST frame retransmits from offset 0, before b's ack arrives.
    p.emit_a();
    let retransmitted = decode_all(&p.ab);
    assert_eq!(retransmitted.len(), 10);
    assert!(retransmitted[0].0.first);
    assert_eq!(retransmitted[0].0.seq, 0);
    // Deliver the frames one at a time; b stays unsynchronized until the
    // last one reaches its high-water mark.
    let wire = std::mem::take(&mut p.ab);
    for (i, chunk) in wire.chunks(124).enumerate() {
        p.b.receive(chunk, p.now).unwrap();
        assert_eq!(p.b.channel_resynced(), i == 9);
    }
    assert_eq!(p.b.recv_high_water_mark(), 1000);
    assert_eq!(read_all(&mut p.b), data);

    // Further data flows normally.
    p.a.write_app(b"more");
    p.flush();
    assert_eq!(read_all(&mut p.b), b"more");
    p.advance(SEC);
    assert_eq!(p.a.peer_ack(), 1004);
    assert_eq!(p.a.send_buffered(), 0);
}

/// If the peer's acknowledgement on the new channel arrives first, data it
/// already holds is skipped rather than retransmitted.
#[test]
fn swap_skips_data_acked_on_new_channel() {
    let cfg = Config {
        max_payload: 100,
        ..Config::default()
    };
    let mut p = Pair::connected(cfg.clone(), cfg);
    let data = pattern(1000, 3);
    p.a.write_app(&data);
    p.emit_a();
    p.deliver_ab();
    p.ba.clear();

    p.swap();
    // b's FIRST frame reaches a before a sends anything.
    p.emit_b();
    p.deliver_ba();
    assert_eq!(p.a.peer_ack(), 1000);
    assert_eq!(p.a.send_position(), 1000);
    let frames = take_frames(&mut p.a, p.now);
    assert_eq!(
        frames,
        vec![(hdr(true, false, 0, cfg_window(), 1000, 0), vec![])]
    );
    assert_eq!(read_all(&mut p.b), data);
}

fn cfg_window() -> u32 {
    Config::DEFAULT_RECV_BUFFER as u32
}

/// Data lost in flight on the old channel is retransmitted on the new one,
/// subject to the send limit already granted.
#[test]
fn swap_mid_transfer_with_loss() {
    let mut p = Pair::connected(Config::default(), Config::with_recv_buffer(200_000));
    let data = pattern(2_000_000, 9);
    let mut off = 0;
    let mut got = Vec::new();
    let mut round = 0;
    while got.len() < data.len() {
        round += 1;
        write_some(&mut p.a, &data, &mut off);
        p.emit_a();
        p.emit_b();
        if round % 7 == 3 {
            // Deliver only part of what is in flight, cutting a frame in
            // half, then swap.
            let n = p.ab.len() / 2;
            let part: Vec<u8> = p.ab[..n].to_vec();
            p.b.receive(&part, p.now).unwrap();
            p.swap();
        } else {
            p.deliver_ab();
            p.deliver_ba();
        }
        got.extend(read_all(&mut p.b));
        p.advance(10 * MS);
    }
    assert_eq!(got, data);
}

#[test]
fn send_limit_survives_swap() {
    let mut p = Pair::connected(Config::default(), Config::with_recv_buffer(1000));
    assert_eq!(p.a.send_limit(), 1000);
    p.swap();
    assert_eq!(p.a.send_limit(), 1000);
    // Data may be sent in the FIRST frame, without waiting for b.
    p.a.write_app(&[1; 10]);
    let f = take_frames(&mut p.a, p.now);
    assert_eq!(f.len(), 1);
    assert!(f[0].0.first);
    assert_eq!(f[0].0.length, 10);
}

// --------------------------------------------------------------------
// Validation rules
// --------------------------------------------------------------------

/// A session with a channel installed and its FIRST frame taken.
fn fresh(cfg: Config) -> (Session, Instant) {
    let t0 = Instant::now();
    let mut s = Session::new(cfg);
    s.swap_channel(t0);
    take_tx(&mut s, t0);
    (s, t0)
}

fn expect_err(s: &mut Session, bytes: &[u8], t: Instant, e: ProtocolError) {
    assert_eq!(s.receive(bytes, t), Err(e.clone()));
    // The session is now dead.
    assert_eq!(s.error(), Some(&e));
    assert_eq!(s.receive(&fr(false, false, 0, 0, 0, b""), t), Err(e));
    assert!(s.transmit_data(t).is_empty());
    assert_eq!(s.poll_timeout(), None);
    assert_eq!(s.send_space(), 0);
    assert!(!s.is_finished());
}

#[test]
fn rule1_bad_flags_and_reserved() {
    let (mut s, t) = fresh(Config::default());
    let mut f = fr(true, false, 0, 0, 0, b"");
    f[0] |= 0x40;
    expect_err(&mut s, &f, t, FrameError::UnknownFlags(0x41).into());

    let (mut s, t) = fresh(Config::default());
    let mut f = fr(true, false, 0, 0, 0, b"");
    f[1] = 1;
    expect_err(&mut s, &f, t, FrameError::ReservedNonZero(1).into());
}

#[test]
fn rule2_first_flag() {
    let (mut s, t) = fresh(Config::default());
    expect_err(
        &mut s,
        &fr(false, false, 0, 0, 0, b""),
        t,
        ProtocolError::MissingFirst,
    );

    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 0, b""), t).unwrap();
    expect_err(
        &mut s,
        &fr(true, false, 0, 0, 0, b""),
        t,
        ProtocolError::UnexpectedFirst { index: 1 },
    );

    // After a swap, FIRST is expected again.
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 0, b""), t).unwrap();
    s.swap_channel(t);
    expect_err(
        &mut s,
        &fr(false, false, 0, 0, 0, b""),
        t,
        ProtocolError::MissingFirst,
    );
}

#[test]
fn rule3_gap() {
    let (mut s, t) = fresh(Config::default());
    expect_err(
        &mut s,
        &fr(true, false, 1, 0, 0, b"x"),
        t,
        ProtocolError::Gap { seq: 1, hwm: 0 },
    );

    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 0, b"abc"), t).unwrap();
    expect_err(
        &mut s,
        &fr(false, false, 4, 0, 0, b"x"),
        t,
        ProtocolError::Gap { seq: 4, hwm: 3 },
    );
}

#[test]
fn rule3_behind_after_resync() {
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 0, b"abcde"), t).unwrap();
    assert!(s.channel_resynced());
    expect_err(
        &mut s,
        &fr(false, false, 3, 0, 0, b"de"),
        t,
        ProtocolError::BehindHighWaterMark { seq: 3, hwm: 5 },
    );

    // A zero-length frame behind the high-water mark, too.
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 0, b"abcde"), t).unwrap();
    expect_err(
        &mut s,
        &fr(false, false, 4, 0, 0, b""),
        t,
        ProtocolError::BehindHighWaterMark { seq: 4, hwm: 5 },
    );
}

#[test]
fn rule3_catching_up_after_swap() {
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 0, b"0123456789"), t)
        .unwrap();
    s.swap_channel(t);
    // Duplicates below the high-water mark are accepted and discarded, in
    // any number of frames, until one reaches it.
    s.receive(&fr(true, false, 0, 0, 0, b"0123"), t).unwrap();
    assert!(!s.channel_resynced());
    s.receive(&fr(false, false, 2, 0, 0, b""), t).unwrap();
    s.receive(&fr(false, false, 4, 0, 0, b"4567"), t).unwrap();
    assert!(!s.channel_resynced());
    s.receive(&fr(false, false, 8, 0, 0, b"89ab"), t).unwrap();
    assert!(s.channel_resynced());
    assert_eq!(read_all(&mut s), b"0123456789ab");
    // Now frames must start exactly at the high-water mark.
    expect_err(
        &mut s,
        &fr(false, false, 11, 0, 0, b"b"),
        t,
        ProtocolError::BehindHighWaterMark { seq: 11, hwm: 12 },
    );
}

#[test]
fn rule4_beyond_shutdown() {
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, true, 0, 0, 0, b"abc"), t).unwrap();
    assert_eq!(s.peer_shutdown(), Some(3));
    assert_eq!(s.recv_high_water_mark(), 4);
    // Zero-length frames after the shutdown are fine.
    s.receive(&fr(false, false, 4, 0, 0, b""), t).unwrap();
    s.swap_channel(t);
    // Data at the shutdown position, even as a "duplicate".
    expect_err(
        &mut s,
        &fr(true, false, 0, 0, 0, b"abcd"),
        t,
        ProtocolError::BeyondShutdown {
            seq: 0,
            end: 4,
            shutdown: 3,
        },
    );

    // A shutdown at a different position from the one received.
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, true, 0, 0, 0, b"abc"), t).unwrap();
    s.swap_channel(t);
    expect_err(
        &mut s,
        &fr(true, true, 0, 0, 0, b"ab"),
        t,
        ProtocolError::ConflictingShutdown { position: 2 },
    );

    // A shutdown before data already received.
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 0, b"abc"), t).unwrap();
    s.swap_channel(t);
    expect_err(
        &mut s,
        &fr(true, true, 0, 0, 0, b"ab"),
        t,
        ProtocolError::ConflictingShutdown { position: 2 },
    );
}

#[test]
fn rule5_receive_buffer_overflow() {
    let (mut s, t) = fresh(Config::with_recv_buffer(10));
    expect_err(
        &mut s,
        &fr(true, false, 0, 0, 0, &[0; 11]),
        t,
        ProtocolError::ReceiveOverflow { len: 11, free: 10 },
    );

    // Duplicate data does not count against the buffer.
    let (mut s, t) = fresh(Config::with_recv_buffer(10));
    s.receive(&fr(true, false, 0, 0, 0, &[1; 8]), t).unwrap();
    s.swap_channel(t);
    s.receive(&fr(true, false, 0, 0, 0, &[1; 10]), t).unwrap();
    assert_eq!(s.recv_buffered(), 10);
    expect_err(
        &mut s,
        &fr(false, false, 10, 0, 0, &[1; 1]),
        t,
        ProtocolError::ReceiveOverflow { len: 1, free: 0 },
    );

    // A shutdown takes no space in the buffer.
    let (mut s, t) = fresh(Config::with_recv_buffer(10));
    s.receive(&fr(true, true, 0, 0, 0, &[1; 10]), t).unwrap();
    assert_eq!(s.peer_shutdown(), Some(10));
}

#[test]
fn rule6_ack_beyond_sent() {
    let (mut s, t) = fresh(Config::default());
    expect_err(
        &mut s,
        &fr(true, false, 0, 1, 100, b""),
        t,
        ProtocolError::AckBeyondSent { ack: 1, sent: 0 },
    );

    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 100, b""), t).unwrap();
    s.write_app(b"0123456789");
    take_tx(&mut s, t);
    assert_eq!(s.max_sent(), 10);
    expect_err(
        &mut s,
        &fr(false, false, 0, 11, 100, b""),
        t,
        ProtocolError::AckBeyondSent { ack: 11, sent: 10 },
    );
}

#[test]
fn rule6_ack_went_backwards() {
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 100, b""), t).unwrap();
    s.write_app(b"0123456789");
    take_tx(&mut s, t);
    s.receive(&fr(false, false, 0, 5, 100, b""), t).unwrap();
    assert_eq!(s.peer_ack(), 5);
    assert_eq!(s.send_buffered(), 5);
    expect_err(
        &mut s,
        &fr(false, false, 0, 3, 200, b""),
        t,
        ProtocolError::AckWentBackwards {
            ack: 3,
            previous: 5,
        },
    );
}

#[test]
fn rule7_limit_decreased() {
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 100, b""), t).unwrap();
    assert_eq!(s.send_limit(), 100);
    expect_err(
        &mut s,
        &fr(false, false, 0, 0, 99, b""),
        t,
        ProtocolError::LimitDecreased {
            limit: 99,
            previous: 100,
        },
    );

    // Also across a swap.
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, false, 0, 0, 100, b""), t).unwrap();
    s.swap_channel(t);
    take_tx(&mut s, t);
    expect_err(
        &mut s,
        &fr(true, false, 0, 0, 0, b""),
        t,
        ProtocolError::LimitDecreased {
            limit: 0,
            previous: 100,
        },
    );
}

#[test]
fn invalid_frame_changes_nothing_before_it() {
    // Frames before the bad one in the same input are processed.
    let (mut s, t) = fresh(Config::default());
    let mut input = fr(true, false, 0, 0, 0, b"good");
    input.extend(fr(false, false, 9, 0, 0, b"bad"));
    assert_eq!(
        s.receive(&input, t),
        Err(ProtocolError::Gap { seq: 9, hwm: 4 })
    );
    assert_eq!(s.app_data(), b"good");
}

// --------------------------------------------------------------------
// Flow control
// --------------------------------------------------------------------

#[test]
fn non_reading_application_backpressure() {
    let cap = 64 * 1024;
    let mut p = Pair::connected(Config::default(), Config::with_recv_buffer(cap));
    let data = pattern(1 << 20, 5);
    let mut off = 0;
    write_some(&mut p.a, &data, &mut off);
    p.advance(SEC);
    assert_eq!(p.b.recv_buffered(), cap);
    assert_eq!(p.b.window(), 0);
    assert_eq!(p.a.send_limit(), cap as u64);
    assert_eq!(p.a.max_sent(), cap as u64);

    // Hours pass; heartbeats flow both ways and nothing times out.
    let (fa, fb) = (p.a.frames_sent(), p.b.frames_sent());
    for _ in 0..360 {
        p.advance(10 * SEC);
        assert!(!p.a.transport_timed_out());
        assert!(!p.b.transport_timed_out());
    }
    assert!(p.a.frames_sent() - fa >= 359);
    assert!(p.b.frames_sent() - fb >= 359);
    assert_eq!(p.a.max_sent(), cap as u64);
    assert_eq!(p.b.recv_buffered(), cap);

    // Reading a little does not trigger a window update by itself...
    let before = p.log_b.len();
    p.b.consume_app(100);
    p.flush();
    assert_eq!(p.log_b.len(), before);
    // ...but reading a quarter of the buffer does.
    p.b.consume_app(cap / 4);
    p.flush();
    assert!(p.log_b.len() > before);
    assert_eq!(p.a.send_limit(), (cap + 100 + cap / 4) as u64);

    // Eventually everything arrives.
    let mut got = data[..100 + cap / 4].to_vec();
    while got.len() < data.len() {
        write_some(&mut p.a, &data, &mut off);
        p.flush();
        got.extend(read_all(&mut p.b));
        p.advance(MS);
    }
    assert_eq!(got, data);
}

#[test]
fn send_buffer_fills() {
    let cfg = Config {
        send_buffer: 1000,
        ..Config::default()
    };
    let mut p = Pair::connected(cfg, Config::with_recv_buffer(300));
    assert_eq!(p.a.write_app(&[0; 2000]), 1000);
    assert_eq!(p.a.send_space(), 0);
    p.advance(SEC);
    // 300 bytes are acknowledged (received by b, though not read).
    assert_eq!(p.a.peer_ack(), 300);
    assert_eq!(p.a.send_space(), 300);
}

#[test]
fn ack_threshold_triggers_immediate_ack() {
    let mut p = Pair::connected(Config::default(), Config::default());
    p.a.write_app(&vec![0; Config::DEFAULT_ACK_THRESHOLD]);
    p.flush();
    // No time has passed, but b has acknowledged.
    assert_eq!(p.a.peer_ack(), Config::DEFAULT_ACK_THRESHOLD as u64);
}

#[test]
fn app_read_closed_discards_and_acks() {
    let mut p = Pair::connected(Config::default(), Config::with_recv_buffer(100));
    p.a.write_app(&[1; 50]);
    p.flush();
    assert_eq!(p.b.recv_buffered(), 50);
    p.b.close_app_read();
    assert_eq!(p.b.recv_buffered(), 0);
    assert_eq!(p.b.window(), 100);
    let data = pattern(10_000, 0);
    let mut off = 0;
    while off < data.len() {
        write_some(&mut p.a, &data, &mut off);
        p.advance(SEC);
        assert!(p.b.app_data().is_empty());
        assert_eq!(p.b.window(), 100);
    }
    p.advance(SEC);
    assert_eq!(p.a.peer_ack(), 10_050);
    assert_eq!(p.a.send_buffered(), 0);
    assert_eq!(p.b.delivered(), 10_050);

    // A shutdown then gives EOF at once.
    p.a.close_app_send();
    p.flush();
    assert!(p.b.app_eof());
    assert!(p.a.shutdown_acked());
}

// --------------------------------------------------------------------
// Shutdown
// --------------------------------------------------------------------

#[test]
fn shutdown_one_direction_then_other() {
    let mut p = Pair::connected(Config::default(), Config::default());
    p.a.write_app(b"abc");
    p.a.close_app_send();
    p.flush();
    // The data and the shutdown travel in one frame, acknowledged at once
    // (including the phantom position).
    let last = p.log_a.last().unwrap();
    assert!(last.shutdown && last.length == 3);
    assert_eq!(p.b.peer_shutdown(), Some(3));
    assert_eq!(p.a.peer_ack(), 4);
    assert!(p.a.shutdown_acked());
    // EOF only after the data has been delivered.
    assert!(!p.b.app_eof());
    assert_eq!(read_all(&mut p.b), b"abc");
    assert!(p.b.app_eof());
    assert!(!p.a.is_finished());
    assert!(!p.b.is_finished());

    // Later frames from a carry seq = 4.
    p.advance(11 * SEC);
    let last = p.log_a.last().unwrap();
    assert_eq!((last.seq, last.length, last.shutdown), (4, 0, false));

    // b can still send to a.
    p.b.write_app(b"reply");
    p.flush();
    assert_eq!(read_all(&mut p.a), b"reply");
    p.b.close_app_send();
    p.flush();
    assert!(p.a.app_eof());
    assert!(p.a.is_finished());
    assert!(p.b.is_finished());
}

#[test]
fn finished_requires_receive_buffer_drained() {
    let mut p = Pair::connected(Config::default(), Config::default());
    p.a.write_app(b"abc");
    p.a.close_app_send();
    p.b.close_app_send();
    p.flush();
    assert!(p.a.is_finished());
    assert!(!p.b.is_finished());
    read_all(&mut p.b);
    assert!(p.b.is_finished());
}

#[test]
fn shutdown_sent_with_zero_window() {
    let mut p = Pair::connected(Config::default(), Config::with_recv_buffer(10));
    p.a.write_app(&[7; 10]);
    p.flush();
    assert_eq!(p.b.window(), 0);
    p.a.close_app_send();
    p.flush();
    let last = p.log_a.last().unwrap();
    assert_eq!((last.seq, last.length, last.shutdown), (10, 0, true));
    assert_eq!(p.b.peer_shutdown(), Some(10));
    assert!(p.a.shutdown_acked());
}

#[test]
fn shutdown_waits_for_preceding_data() {
    let mut p = Pair::connected(Config::default(), Config::with_recv_buffer(10));
    p.a.write_app(&[7; 15]);
    p.a.close_app_send();
    p.flush();
    // Only 10 bytes fit; the shutdown cannot overtake the rest.
    assert_eq!(p.a.max_sent(), 10);
    assert!(!p.a.shutdown_sent());
    read_all(&mut p.b);
    p.flush();
    assert_eq!(read_all(&mut p.b), [7; 5]);
    assert!(p.b.app_eof());
    assert!(p.a.shutdown_acked());
}

#[test]
fn lost_shutdown_is_rewound_and_resent() {
    let mut p = Pair::connected(Config::default(), Config::default());
    p.a.write_app(b"xyz");
    p.flush();
    p.advance(SEC);
    assert_eq!(p.a.peer_ack(), 3);
    p.a.close_app_send();
    p.emit_a();
    p.ab.clear(); // lost
    assert!(p.a.shutdown_sent());
    assert_eq!(p.a.send_position(), 4);
    p.swap();
    assert_eq!(p.a.send_position(), 3);
    let f = take_frames(&mut p.a, p.now);
    assert_eq!(f.len(), 1);
    assert_eq!(
        (f[0].0.first, f[0].0.shutdown, f[0].0.seq, f[0].0.length),
        (true, true, 3, 0)
    );
    p.b.receive(&encode(&f), p.now).unwrap();
    assert_eq!(p.b.peer_shutdown(), Some(3));
    p.flush();
    assert!(p.a.shutdown_acked());
}

fn encode(frames: &[(FrameHeader, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (h, p) in frames {
        encode_frame(h, p, &mut out);
    }
    out
}

#[test]
fn unacked_shutdown_duplicate_is_discarded() {
    let mut p = Pair::connected(Config::default(), Config::default());
    p.a.write_app(b"xyz");
    p.a.close_app_send();
    p.emit_a();
    p.deliver_ab();
    assert_eq!(p.b.peer_shutdown(), Some(3));
    p.emit_b();
    p.ba.clear(); // b's ack of the shutdown is lost
    assert!(!p.a.shutdown_acked());

    p.swap();
    // a resends everything, data and shutdown; b discards it all.
    p.emit_a();
    let f = decode_all(&p.ab);
    assert_eq!(f.len(), 1);
    assert_eq!(
        (f[0].0.first, f[0].0.shutdown, f[0].0.seq, f[0].0.length),
        (true, true, 0, 3)
    );
    p.deliver_ab();
    assert!(p.b.channel_resynced());
    assert_eq!(read_all(&mut p.b), b"xyz");
    p.flush();
    assert!(p.a.shutdown_acked());
    assert!(p.b.app_eof());
}

#[test]
fn finished_needs_ack_on_current_channel() {
    let mut p = Pair::connected(Config::default(), Config::default());
    p.a.close_app_send();
    p.b.close_app_send();
    p.flush();
    assert!(p.a.is_finished() && p.b.is_finished());
    p.swap();
    // Until the FIRST frame (carrying the ack) is produced, not finished.
    assert!(!p.a.is_finished());
    p.flush();
    assert!(p.a.is_finished() && p.b.is_finished());
}

// --------------------------------------------------------------------
// Timing
// --------------------------------------------------------------------

#[test]
fn heartbeat_and_receive_timeout() {
    let t0 = Instant::now();
    let mut s = Session::new(Config::default());
    s.swap_channel(t0);
    // The FIRST frame is due immediately; poll_timeout is about timers.
    assert_eq!(take_frames(&mut s, t0).len(), 1);
    assert_eq!(s.poll_timeout(), Some(t0 + 10 * SEC));
    s.handle_timeout(t0 + 5 * SEC);
    assert!(take_tx(&mut s, t0 + 5 * SEC).is_empty());

    s.handle_timeout(t0 + 10 * SEC);
    assert!(!s.transport_timed_out());
    let f = take_frames(&mut s, t0 + 10 * SEC);
    assert_eq!(f, vec![(hdr(false, false, 0, cfg_window(), 0, 0), vec![])]);

    // Heartbeat and receive timeout coincide at 20 s.
    assert_eq!(s.poll_timeout(), Some(t0 + 20 * SEC));
    s.handle_timeout(t0 + 20 * SEC);
    assert!(s.transport_timed_out());
    assert_eq!(take_frames(&mut s, t0 + 20 * SEC).len(), 1);
    // The timeout is not reported again; the next deadline is the heartbeat.
    assert_eq!(s.poll_timeout(), Some(t0 + 30 * SEC));

    // Traffic clears it.
    s.receive(&fr(true, false, 0, 0, 0, b""), t0 + 25 * SEC)
        .unwrap();
    assert!(!s.transport_timed_out());
    assert_eq!(s.poll_timeout(), Some(t0 + 30 * SEC));
    s.handle_timeout(t0 + 30 * SEC);
    take_tx(&mut s, t0 + 30 * SEC);
    assert_eq!(s.poll_timeout(), Some(t0 + 40 * SEC));
    s.handle_timeout(t0 + 40 * SEC);
    take_tx(&mut s, t0 + 40 * SEC);
    assert_eq!(s.poll_timeout(), Some(t0 + 45 * SEC));
    s.handle_timeout(t0 + 45 * SEC);
    assert!(s.transport_timed_out());

    // A swap resets it.
    s.swap_channel(t0 + 46 * SEC);
    assert!(!s.transport_timed_out());
}

#[test]
fn transmit_stall_timeout() {
    let t0 = Instant::now();
    let mut s = Session::new(Config::default());
    s.swap_channel(t0);
    // The FIRST frame is produced but only partly written.
    let n = s.transmit_data(t0).len();
    s.consume_transmit(5, t0 + SEC);
    assert_eq!(s.tx_pending(), n - 5);
    // Incoming heartbeats keep arriving, but output is stuck.
    let mut t = t0;
    s.receive(&fr(true, false, 0, 0, 0, b""), t).unwrap();
    for _ in 0..4 {
        t += 5 * SEC;
        s.receive(&fr(false, false, 0, 0, 0, b""), t).unwrap();
        assert_eq!(s.poll_timeout(), Some(t0 + 21 * SEC));
        s.handle_timeout(t);
        assert!(!s.transport_timed_out());
    }
    s.handle_timeout(t0 + 21 * SEC);
    assert!(s.transport_timed_out());
    assert_eq!(s.poll_timeout(), None);
    // Progress clears it.
    s.consume_transmit(1, t0 + 22 * SEC);
    assert!(!s.transport_timed_out());
    // Next: the receive timeout (last traffic at 20 s).
    assert_eq!(s.poll_timeout(), Some(t0 + 40 * SEC));
}

#[test]
fn delayed_ack_timing() {
    let (mut s, t0) = fresh(Config::default());
    let hb = t0 + 10 * SEC;
    assert_eq!(s.poll_timeout(), Some(hb));
    s.receive(&fr(true, false, 0, 0, 0, b"abc"), t0 + SEC)
        .unwrap();
    let due = t0 + SEC + Config::DEFAULT_ACK_DELAY;
    assert_eq!(s.poll_timeout(), Some(due));
    // More data does not push the deadline back.
    s.receive(&fr(false, false, 3, 0, 0, b"def"), t0 + SEC + 100 * MS)
        .unwrap();
    assert_eq!(s.poll_timeout(), Some(due));
    assert!(take_tx(&mut s, due - MS).is_empty());
    let f = take_frames(&mut s, due);
    assert_eq!(f[0].0.ack, 6);
    // The ack resets the heartbeat timer.
    assert_eq!(s.poll_timeout(), Some(due + 10 * SEC));
}

#[test]
fn received_shutdown_acked_immediately() {
    let (mut s, t) = fresh(Config::default());
    s.receive(&fr(true, true, 0, 0, 0, b"ab"), t).unwrap();
    let f = take_frames(&mut s, t);
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].0.ack, 3);
    assert_eq!(f[0].0.window, cfg_window() - 2);
}

// --------------------------------------------------------------------
// Property tests
// --------------------------------------------------------------------

proptest! {
    #[test]
    fn arbitrary_input_never_panics(
        prefix in any::<bool>(),
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16)
    ) {
        let (mut s, t) = fresh(Config::with_recv_buffer(100));
        if prefix {
            s.receive(&fr(true, false, 0, 0, 10, b"hi"), t).unwrap();
        }
        for c in &chunks {
            if s.receive(c, t).is_err() {
                break;
            }
            s.transmit_data(t);
        }
    }

    #[test]
    fn arbitrary_headers_never_panic(
        frames in prop::collection::vec(
            (any::<bool>(), any::<bool>(), 0u64..40, 0u64..40, 0u32..200, 0usize..30), 1..12)
    ) {
        let (mut s, t) = fresh(Config::with_recv_buffer(50));
        s.write_app(&[0; 30]);
        for (first, shutdown, seq, ack, window, len) in frames {
            if s.receive(&fr(first, shutdown, seq, ack, window, &vec![9; len]), t).is_err() {
                break;
            }
            take_tx(&mut s, t);
            prop_assert!(s.recv_buffered() <= 50);
        }
    }

    /// Random interleavings of writes, reads, partial deliveries, swaps and
    /// time: both streams arrive intact.
    #[test]
    fn loopback_with_random_swaps(
        ops in prop::collection::vec((0u8..6, 1usize..5000), 1..60),
        recv_a in 100usize..20_000,
        recv_b in 100usize..20_000,
        payload in 1usize..2000,
    ) {
        let cfg = |r| Config { max_payload: payload, ..Config::with_recv_buffer(r) };
        let mut p = Pair::connected(cfg(recv_a), cfg(recv_b));
        let da = pattern(30_000, 1);
        let db = pattern(20_000, 2);
        let (mut oa, mut ob) = (0, 0);
        let (mut ra, mut rb) = (Vec::new(), Vec::new());
        for (op, n) in ops {
            match op {
                0 => {
                    let end = (oa + n).min(da.len());
                    oa += p.a.write_app(&da[oa..end]);
                    let end = (ob + n).min(db.len());
                    ob += p.b.write_app(&db[ob..end]);
                }
                1 => {
                    p.emit_a();
                    p.emit_b();
                    let k = n.min(p.ab.len());
                    let part: Vec<u8> = p.ab.drain(..k).collect();
                    p.b.receive(&part, p.now).unwrap();
                    let k = n.min(p.ba.len());
                    let part: Vec<u8> = p.ba.drain(..k).collect();
                    p.a.receive(&part, p.now).unwrap();
                }
                2 => p.swap(),
                3 => p.advance(Duration::from_millis(n as u64)),
                4 => {
                    let k = n.min(p.a.app_data().len());
                    ra.extend_from_slice(&p.a.app_data()[..k]);
                    p.a.consume_app(k);
                    let k = n.min(p.b.app_data().len());
                    rb.extend_from_slice(&p.b.app_data()[..k]);
                    p.b.consume_app(k);
                }
                _ => p.flush(),
            }
            p.check_invariants();
        }
        // Finish: a final swap, then run to completion.
        p.swap();
        for _ in 0..10_000 {
            write_some(&mut p.a, &da, &mut oa);
            write_some(&mut p.b, &db, &mut ob);
            if oa == da.len() { p.a.close_app_send(); }
            if ob == db.len() { p.b.close_app_send(); }
            p.flush();
            ra.extend(read_all(&mut p.a));
            rb.extend(read_all(&mut p.b));
            if p.a.is_finished() && p.b.is_finished() {
                break;
            }
            p.advance(100 * MS);
        }
        prop_assert!(p.a.is_finished() && p.b.is_finished());
        prop_assert_eq!(&ra, &db);
        prop_assert_eq!(&rb, &da);
    }
}
