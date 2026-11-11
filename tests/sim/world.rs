//! The simulated world: two [`Session`]s (client and server), their
//! applications, the channels between them, a model of the client's resume
//! logic ("Choosing to resume" in `doc/stitch.md`) and of the server's
//! acceptance of resumes, all driven by a virtual clock.
//!
//! Every step is followed by invariant checks; a violation panics with the
//! case needed to reproduce it and a tail of recent events.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use stitch::core::Session;
use stitch::frame::{FrameHeader, HEADER_LEN};

use crate::net::{C2S, Channel, Item, Link, Phase, Reply, S2C};
use crate::rng::Rng;
use crate::scenario::{Case, Family, Fault, Params, ReaderMode};

/// Virtual time, in microseconds since the start of the simulation.
pub type Tick = u64;
pub const MS: Tick = 1_000;
pub const SEC: Tick = 1_000_000;
pub const MIN: Tick = 60 * SEC;

pub const CLIENT: usize = 0;
pub const SERVER: usize = 1;
const NAME: [&str; 2] = ["client", "server"];

/// After the network heals, everything must complete within this long.
const PROGRESS_BOUND: Tick = 120 * MIN;
/// A finishing side gives up flushing its output after this long.
const FLUSH_TIMEOUT: Tick = 5 * SEC;
/// Spin detection: passes allowed at a single instant.
const MAX_PASSES: u32 = 100_000;

pub fn fmt_t(t: Tick) -> String {
    format!("{}.{:06}s", t / SEC, t % SEC)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Running,
    /// The session is finished; writing out the last of its output.
    Flushing {
        deadline: Tick,
    },
    /// (Client only.) A resume failed fatally (`no_session`); the client
    /// no longer uses any channel but still delivers what it holds to its
    /// application.
    Draining,
    Exited {
        at: Tick,
    },
}

impl Status {
    fn driven(self) -> bool {
        matches!(self, Status::Running | Status::Flushing { .. })
    }
}

#[derive(Debug)]
struct App {
    written: usize,
    next_write: Tick,
    close_send_at: Option<Tick>,
    send_closed: bool,
    reader: ReaderMode,
    next_read: Tick,
    received: usize,
    read_closed: bool,
    eof: bool,
}

/// Previous values of quantities which must never decrease.
#[derive(Debug, Default, Clone, Copy)]
struct Mono {
    send_limit: u64,
    peer_ack: u64,
    max_sent: u64,
    hwm: u64,
    delivered: u64,
    ack_sent: u64,
    advertised_limit: u64,
    frames_sent: u64,
    /// `ack` and `ack + window` of the last frame written to any channel.
    wire_ack: u64,
    wire_limit: u64,
}

#[derive(Debug)]
struct End {
    s: Session,
    app: App,
    /// The channel installed in the session.
    active: Option<usize>,
    /// The failure of `active` has been observed; stop using it.
    fail_seen: bool,
    status: Status,
    mon: Mono,
    finished: bool,
    timeouts: u64,
    /// When exiting: the channel in use, whether the peer was using it
    /// too, and whether output was left unwritten.
    exit_channel: Option<usize>,
    peer_on_channel_at_exit: bool,
    flush_incomplete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptPhase {
    /// The new `ssh` is being established.
    Connecting { ready_at: Tick },
    /// The `resume` message has been sent; awaiting the reply.
    Sent,
    /// `ok` received; the client swaps at `swap_at`.
    GotReply { swap_at: Tick },
}

#[derive(Debug, Clone)]
struct Attempt {
    epoch: u64,
    ch: usize,
    phase: AttemptPhase,
    deadline: Tick,
    request_delay: Tick,
    /// Started while the active channel looked fine.
    speculative: bool,
    /// Bytes received on the old channel while this attempt was outstanding.
    old_rx: u64,
}

#[derive(Debug)]
struct ClientCtl {
    next_epoch: u64,
    attempt: Option<Attempt>,
    backoff: Tick,
    not_before: Tick,
    speculative: bool,
    /// A resume got `no_session`: the server has exited.
    server_gone: bool,
}

#[derive(Debug)]
enum Ev {
    Fault(Fault),
    Heal,
    Unstall {
        ch: usize,
        dirs: [bool; 2],
    },
    /// A doomed attempt's `ssh` dies.
    Die {
        ch: usize,
    },
    /// Liveness: record frame counts.
    Snapshot,
}

/// Counts of interesting things that happened, to check that the scenarios
/// really exercise what they are meant to.
#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub attempts: u64,
    pub abandoned: u64,
    pub client_swaps: u64,
    pub server_swaps: u64,
    pub speculative_swaps: u64,
    /// Swaps after which the client had received data on the old channel
    /// while the attempt was outstanding.
    pub data_during_resume: u64,
    /// Swaps where the peer had received data we had not seen acknowledged
    /// (so the rewind re-sends duplicates).
    pub rewind_duplicates: u64,
    pub stale_rejected: u64,
    /// Resumes accepted on a channel the client had already abandoned.
    pub zombie_accepts: u64,
    pub no_session: u64,
    pub fatal: u64,
    pub timeouts: u64,
    /// Timeouts while the peer was still running.
    pub timeouts_peer_running: u64,
    pub cuts: u64,
    pub stalls: u64,
    pub partial_writes: u64,
    pub frames: u64,
    pub orphans: u64,
    pub both_finished: u64,
    pub end_time: Tick,
}

impl Stats {
    pub fn add(&mut self, o: &Stats) {
        self.attempts += o.attempts;
        self.abandoned += o.abandoned;
        self.client_swaps += o.client_swaps;
        self.server_swaps += o.server_swaps;
        self.speculative_swaps += o.speculative_swaps;
        self.data_during_resume += o.data_during_resume;
        self.rewind_duplicates += o.rewind_duplicates;
        self.stale_rejected += o.stale_rejected;
        self.zombie_accepts += o.zombie_accepts;
        self.no_session += o.no_session;
        self.fatal += o.fatal;
        self.timeouts += o.timeouts;
        self.timeouts_peer_running += o.timeouts_peer_running;
        self.cuts += o.cuts;
        self.stalls += o.stalls;
        self.partial_writes += o.partial_writes;
        self.frames += o.frames;
        self.orphans += o.orphans;
        self.both_finished += o.both_finished;
        self.end_time = self.end_time.max(o.end_time);
    }
}

pub struct World {
    case: Case,
    p: Params,
    rng: Rng,
    base: Instant,
    now: Tick,
    passes: u32,
    ends: [End; 2],
    streams: [Vec<u8>; 2],
    chans: Vec<Channel>,
    cli: ClientCtl,
    srv_highest: Option<u64>,
    agenda: Vec<(Tick, Ev)>,
    healed: bool,
    complete_at: Option<Tick>,
    snapshot: Option<[u64; 2]>,
    trace: VecDeque<String>,
    verbose: bool,
    stats: Stats,
}

/// Runs one case to completion, checking invariants throughout.
pub fn run(case: &Case, verbose: bool) -> Stats {
    let mut w = World::new(case, verbose);
    w.run();
    w.stats
}

impl World {
    pub fn new(case: &Case, verbose: bool) -> World {
        let p = Params::generate(case);
        let mut rng = Rng::new(case.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xA5A5);
        let streams = [
            rng.bytes(p.side[0].stream_len),
            rng.bytes(p.side[1].stream_len),
        ];
        let ends = [0, 1].map(|x| {
            let sp = &p.side[x];
            End {
                s: Session::new(sp.cfg.clone()),
                app: App {
                    written: 0,
                    next_write: sp.write_start,
                    close_send_at: None,
                    send_closed: false,
                    reader: sp.reader,
                    next_read: sp.read_start,
                    received: 0,
                    read_closed: false,
                    eof: false,
                },
                active: None,
                fail_seen: false,
                status: Status::Running,
                mon: Mono::default(),
                finished: false,
                timeouts: 0,
                exit_channel: None,
                peer_on_channel_at_exit: false,
                flush_incomplete: false,
            }
        });
        let mut agenda: Vec<(Tick, Ev)> = p
            .faults
            .iter()
            .map(|(t, f)| (*t, Ev::Fault(f.clone())))
            .collect();
        if let Some(end) = p.end_at {
            agenda.push((end.saturating_sub(5 * MIN), Ev::Snapshot));
        } else {
            agenda.push((p.heal_at, Ev::Heal));
        }
        World {
            case: *case,
            rng,
            base: Instant::now(),
            now: 0,
            passes: 0,
            ends,
            streams,
            chans: Vec::new(),
            cli: ClientCtl {
                next_epoch: 0,
                attempt: None,
                backoff: p.ctl.backoff_init,
                not_before: 0,
                speculative: false,
                server_gone: false,
            },
            srv_highest: None,
            agenda,
            healed: p.family == Family::Liveness,
            complete_at: None,
            snapshot: None,
            trace: VecDeque::new(),
            verbose,
            stats: Stats::default(),
            p,
        }
    }

    // ------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------

    fn inst(&self, t: Tick) -> Instant {
        self.base + Duration::from_micros(t)
    }

    fn tick(&self, i: Instant) -> Tick {
        i.saturating_duration_since(self.base)
            .as_nanos()
            .div_ceil(1000) as Tick
    }

    fn log(&mut self, msg: String) {
        let line = format!("{:>14} {msg}", fmt_t(self.now));
        if self.verbose {
            println!("{line}");
        }
        if self.trace.len() == 60 {
            self.trace.pop_front();
        }
        self.trace.push_back(line);
    }

    fn summary(&self) -> String {
        let mut out = String::new();
        for x in 0..2 {
            let e = &self.ends[x];
            let s = &e.s;
            let _ = writeln!(
                out,
                "  {}: {:?} active={:?} fail_seen={} timed_out={} finished={} | app wrote {}/{} closed={} received {} read_closed={} eof={} reader={:?} | \
                 session: send pos={} max_sent={} peer_ack={} limit={} data_end={} buffered={} closed={} shutdown_acked={} | \
                 recv hwm={} delivered={} buffered={}/{} peer_shutdown={:?} ack_sent={} adv_limit={} tx_pending={} poll={:?}",
                NAME[x],
                e.status,
                e.active,
                e.fail_seen,
                s.transport_timed_out(),
                e.finished,
                e.app.written,
                self.streams[x].len(),
                e.app.send_closed,
                e.app.received,
                e.app.read_closed,
                e.app.eof,
                e.app.reader,
                s.send_position(),
                s.max_sent(),
                s.peer_ack(),
                s.send_limit(),
                s.send_data_end(),
                s.send_buffered(),
                s.send_closed(),
                s.shutdown_acked(),
                s.recv_high_water_mark(),
                s.delivered(),
                s.recv_buffered(),
                s.recv_capacity(),
                s.peer_shutdown(),
                s.ack_sent(),
                s.advertised_limit(),
                s.tx_pending(),
                s.poll_timeout().map(|t| fmt_t(self.tick(t))),
            );
        }
        let _ = writeln!(
            out,
            "  attempt={:?} healed={} channels={} server_highest_epoch={:?}",
            self.cli.attempt,
            self.healed,
            self.chans.len(),
            self.srv_highest
        );
        out
    }

    #[track_caller]
    fn fail(&self, msg: impl std::fmt::Display) -> ! {
        let trace: Vec<&str> = self.trace.iter().map(String::as_str).collect();
        panic!(
            "simulation check failed: {msg}\n  at t={}\n{}  reproduce: SIM_CASE={} cargo test --test sim replay -- --nocapture\n  (add SIM_TRACE=1 for a full event trace)\n  recent events:\n    {}\n",
            fmt_t(self.now),
            self.summary(),
            self.case,
            trace.join("\n    "),
        )
    }

    // ------------------------------------------------------------------
    // Main loop
    // ------------------------------------------------------------------

    pub fn run(&mut self) {
        self.log(format!("start {}: {:?}", self.case, self.p));
        self.start_attempt(true);
        loop {
            let deadline = self.deadline();
            let t = self.next_event();
            if t >= deadline {
                self.now = self.now.max(deadline);
                break;
            }
            if t > self.now {
                self.now = t;
                self.passes = 0;
            } else {
                self.passes += 1;
                if self.passes > MAX_PASSES {
                    self.fail("livelock: no progress in virtual time");
                }
            }
            self.step();
            if self
                .ends
                .iter()
                .all(|e| matches!(e.status, Status::Exited { .. }))
            {
                break;
            }
        }
        self.stats.end_time = self.now;
        for ch in &self.chans {
            for p in &ch.pipes {
                self.stats.partial_writes += p.partial_writes;
            }
        }
        self.final_checks();
    }

    /// When to stop, if nothing else stops the simulation first.
    fn deadline(&self) -> Tick {
        if let Some(end) = self.p.end_at {
            return end;
        }
        let linger = 2 * self.p.timeout + 30 * SEC;
        let mut d = self.p.heal_at + PROGRESS_BOUND;
        // Once the client has exited, the server can only make progress by
        // delivering what it holds; after that, give in-flight traffic time
        // to land, then stop (the server may legitimately wait forever).
        if let Status::Exited { at } = self.ends[CLIENT].status
            && self.ends[SERVER].s.recv_buffered() == 0
        {
            d = d.min(at.max(self.p.heal_at) + linger).max(self.now);
        }
        let both_close = self.p.side.iter().all(|s| s.close_send_delay.is_some());
        if !both_close && let Some(c) = self.complete_at {
            d = d.min(c + linger);
        }
        d
    }

    fn reader_can_read(&self, x: usize) -> bool {
        let a = &self.ends[x].app;
        matches!(a.reader, ReaderMode::Eager | ReaderMode::Bursty) && !a.read_closed
    }

    fn next_event(&self) -> Tick {
        let now = self.now;
        let mut t = Tick::MAX;
        let mut at = |x: Tick| t = t.min(x.max(now));
        for (et, _) in &self.agenda {
            at(*et);
        }
        for x in 0..2 {
            let e = &self.ends[x];
            if matches!(e.status, Status::Exited { .. }) {
                continue;
            }
            let driven = e.status.driven();
            if driven
                && e.active.is_some()
                && let Some(i) = e.s.poll_timeout()
            {
                at(self.fire_time(self.tick(i)));
            }
            if let Status::Flushing { deadline } = e.status {
                at(deadline);
            }
            if e.status == Status::Draining && e.s.recv_buffered() == 0 {
                at(now);
            }
            let a = &e.app;
            if e.status == Status::Running && !a.send_closed {
                if a.written < self.streams[x].len() {
                    if a.next_write > now {
                        at(a.next_write);
                    } else if e.s.send_space() > 0 {
                        at(now);
                    }
                } else if let Some(c) = a.close_send_at {
                    at(c);
                } else if self.p.side[x].close_send_delay.is_some() {
                    at(now);
                }
            }
            if let Some(c) = self.p.side[x].close_read_at
                && !a.read_closed
            {
                at(c);
            }
            if self.reader_can_read(x) && !e.s.app_data().is_empty() {
                at(a.next_read);
            }
            if !a.eof && e.s.app_eof() {
                at(now);
            }
            if driven
                && !e.fail_seen
                && let Some(ci) = e.active
                && let Some(f) = self.chans[ci].fail_at[x]
            {
                at(f);
            }
        }
        if self.ends[CLIENT].status == Status::Running {
            if let Some(a) = &self.cli.attempt {
                match a.phase {
                    AttemptPhase::Connecting { ready_at } => at(ready_at),
                    AttemptPhase::GotReply { swap_at } => at(swap_at),
                    AttemptPhase::Sent => {}
                }
                if !matches!(a.phase, AttemptPhase::GotReply { .. }) {
                    at(a.deadline);
                    if let Some(f) = self.chans[a.ch].fail_at[CLIENT] {
                        at(f);
                    }
                }
            } else if self.client_wants_resume() {
                at(self.cli.not_before);
            }
        }
        for ch in &self.chans {
            for x in 0..2 {
                let pipe = &ch.pipes[1 - x];
                match ch.phase[x] {
                    Phase::Handshake => {
                        if let Some(p) = pipe.head()
                            && !matches!(p.item, Item::Bytes(_))
                            && let Some(a) = pipe.next_arrival()
                        {
                            at(a);
                        }
                    }
                    Phase::Active => {
                        let e = &self.ends[x];
                        if e.status.driven()
                            && !e.fail_seen
                            && let Some(a) = pipe.next_arrival()
                        {
                            at(a);
                        }
                    }
                    Phase::Done => {}
                }
            }
        }
        t
    }

    fn step(&mut self) {
        self.process_agenda();
        self.read_channels();
        self.observe_failures();
        self.client_control();
        for x in 0..2 {
            self.app_step(x);
        }
        for x in 0..2 {
            self.timers(x);
        }
        for x in 0..2 {
            self.pump(x);
        }
        for x in 0..2 {
            self.lifecycle(x);
        }
        self.check_invariants();
        if self.complete_at.is_none() && self.healed && self.data_complete() {
            self.complete_at = Some(self.now);
        }
    }

    // ------------------------------------------------------------------
    // Scenario events
    // ------------------------------------------------------------------

    fn process_agenda(&mut self) {
        loop {
            let now = self.now;
            let Some(i) = self
                .agenda
                .iter()
                .enumerate()
                .filter(|(_, (t, _))| *t <= now)
                .min_by_key(|(i, (t, _))| (*t, *i))
                .map(|(i, _)| i)
            else {
                return;
            };
            let (_, ev) = self.agenda.remove(i);
            self.apply(ev);
        }
    }

    fn apply(&mut self, ev: Ev) {
        let now = self.now;
        match ev {
            Ev::Heal => {
                self.log("network heals".into());
                self.healed = true;
                for ch in &mut self.chans {
                    for p in &mut ch.pipes {
                        p.stalled = false;
                    }
                }
                for e in &mut self.ends {
                    if matches!(
                        e.app.reader,
                        ReaderMode::Bursty | ReaderMode::NeverUntilHeal
                    ) {
                        e.app.reader = ReaderMode::Eager;
                        e.app.next_read = e.app.next_read.min(now);
                    }
                }
            }
            Ev::Unstall { ch, dirs } => {
                for d in 0..2 {
                    if dirs[d] {
                        self.chans[ch].pipes[d].stalled = false;
                    }
                }
                self.log(format!("channel {ch}: stall ends {dirs:?}"));
            }
            Ev::Die { ch } => {
                if self.chans[ch].phase.iter().any(|p| *p != Phase::Done) {
                    self.log(format!("channel {ch}: doomed ssh dies"));
                    self.cut_channel(ch);
                }
            }
            Ev::Snapshot => {
                self.snapshot = Some([0, 1].map(|x| self.ends[x].s.frames_received()));
            }
            Ev::Fault(_) if self.healed => {}
            Ev::Fault(f) => {
                let active = self.ends[CLIENT]
                    .active
                    .filter(|&c| self.chans[c].phase[CLIENT] == Phase::Active);
                match f {
                    Fault::Cut => {
                        if let Some(ci) = active {
                            self.log(format!("fault: cut channel {ci}"));
                            self.cut_channel(ci);
                        }
                    }
                    Fault::CutAttempt => {
                        if let Some(a) = &self.cli.attempt
                            && a.epoch > 0
                        {
                            let ci = a.ch;
                            self.log(format!("fault: cut attempt channel {ci}"));
                            self.cut_channel(ci);
                        }
                    }
                    Fault::Stall { dirs, dur } => {
                        if let Some(ci) = active {
                            self.log(format!("fault: stall channel {ci} {dirs:?} for {dur:?}"));
                            self.stats.stalls += 1;
                            let ch = &mut self.chans[ci];
                            ch.disturbed = true;
                            for d in 0..2 {
                                if dirs[d] {
                                    ch.pipes[d].stalled = true;
                                }
                            }
                            if let Some(d) = dur {
                                self.agenda.push((now + d, Ev::Unstall { ch: ci, dirs }));
                            }
                        }
                    }
                    Fault::Speculative => {
                        if self.cli.attempt.is_none() && self.ends[CLIENT].status == Status::Running
                        {
                            self.log("fault: speculative resume requested".into());
                            self.cli.speculative = true;
                        }
                    }
                }
            }
        }
    }

    /// A lossy failure of a whole channel: each pipe delivers only some of
    /// what is in flight, and each side notices at its own time (or never).
    fn cut_channel(&mut self, ci: usize) {
        let now = self.now;
        self.stats.cuts += 1;
        let ch = &mut self.chans[ci];
        ch.lossy = true;
        ch.disturbed = true;
        for d in 0..2 {
            let lat = ch.pipes[d].link.latency;
            let until = now + self.rng.range(0, 2 * lat);
            ch.pipes[d].cut(until, &mut self.rng);
        }
        for side in 0..2 {
            if ch.fail_at[side].is_none() && !self.rng.chance(1, 6) {
                ch.fail_at[side] = Some(now + self.rng.range(0, 3 * SEC));
            }
        }
    }

    /// One side kills a channel (e.g. the client killing an `ssh`). What is
    /// in flight towards the other side may still arrive until
    /// `c2s_until`/`s2c_until`; the other side notices shortly.
    fn kill(&mut self, ci: usize, by: usize, until: Tick) {
        let now = self.now;
        let ch = &mut self.chans[ci];
        ch.lossy = true;
        ch.disturbed = true;
        ch.phase[by] = Phase::Done;
        let towards_other = by; // pipe `by` is written by `by`
        ch.pipes[towards_other].cut(until, &mut self.rng);
        ch.pipes[1 - towards_other].cut(now, &mut self.rng);
        let other = 1 - by;
        if ch.fail_at[other].is_none() {
            ch.fail_at[other] = Some(until.max(now) + self.rng.range(0, 2 * SEC));
        }
    }

    fn new_channel(&mut self) -> usize {
        let n = &self.p.net;
        let mut link = || {
            let bandwidth = self.rng.range(n.bandwidth.0, n.bandwidth.1);
            Link {
                latency: self.rng.range(n.latency.0, n.latency.1),
                bandwidth,
                mss: self.rng.usize_log(n.mss.0, n.mss.1),
                // At most 1.5 s of queueing, so that a healthy channel never
                // delays a heartbeat by anything like the timeout.
                capacity: self
                    .rng
                    .usize_log(64, n.capacity_max.max(64))
                    .min(bandwidth as usize * 1500),
            }
        };
        let links = [link(), link()];
        let id = self.chans.len();
        self.chans.push(Channel::new(links));
        id
    }

    // ------------------------------------------------------------------
    // Channels and the resume handshake
    // ------------------------------------------------------------------

    fn read_channels(&mut self) {
        for ci in 0..self.chans.len() {
            for x in [SERVER, CLIENT] {
                match self.chans[ci].phase[x] {
                    Phase::Handshake => self.handshake_read(x, ci),
                    Phase::Active => self.data_read(x, ci),
                    Phase::Done => {}
                }
            }
        }
    }

    fn handshake_read(&mut self, x: usize, ci: usize) {
        let now = self.now;
        let pipe = &mut self.chans[ci].pipes[1 - x];
        match pipe.ready(now).map(|p| &p.item) {
            None => return,
            // Frames queued behind the reply: the client does not read the
            // new channel until it has swapped.
            Some(Item::Bytes(_)) if x == CLIENT => return,
            Some(_) => {}
        }
        let pk = pipe.pop_ready(now).unwrap();
        match (pk.item, x) {
            (Item::Request { epoch }, SERVER) => self.server_request(ci, epoch),
            (Item::Reply(r), CLIENT) => self.client_reply(ci, r),
            (Item::Eof, _) => {
                self.chans[ci].pipes[1 - x].eof_read = true;
                if x == SERVER {
                    self.chans[ci].phase[SERVER] = Phase::Done;
                }
            }
            (item, _) => self.fail(format!(
                "{} read unexpected {item:?} during the handshake on channel {ci}",
                NAME[x]
            )),
        }
    }

    fn data_read(&mut self, x: usize, ci: usize) {
        let now = self.now;
        let e = &self.ends[x];
        if e.active != Some(ci) {
            self.fail(format!(
                "{} channel {ci} is active but not installed",
                NAME[x]
            ));
        }
        if e.fail_seen || !e.status.driven() {
            return;
        }
        let pipe = &mut self.chans[ci].pipes[1 - x];
        let mut buf = Vec::new();
        let mut bad = None;
        while let Some(pk) = pipe.pop_ready(now) {
            match pk.item {
                Item::Bytes(b) => buf.extend_from_slice(&b),
                Item::Eof => {
                    pipe.eof_read = true;
                    break;
                }
                item => {
                    bad = Some(item);
                    break;
                }
            }
        }
        if let Some(item) = bad {
            self.fail(format!(
                "{} read unexpected {item:?} on active channel {ci}",
                NAME[x]
            ));
        }
        if buf.is_empty() {
            return;
        }
        if x == CLIENT
            && let Some(a) = &mut self.cli.attempt
        {
            a.old_rx += buf.len() as u64;
        }
        // Feed it to the session in arbitrary pieces.
        let piece = if self.rng.chance(1, 8) {
            self.rng.usize_range(1, 32)
        } else {
            self.rng.usize_log(64, 1 << 17)
        };
        let t = self.inst(now);
        for chunk in buf.chunks(piece) {
            if let Err(err) = self.ends[x].s.receive(chunk, t) {
                self.fail(format!(
                    "protocol error at {} receiving on channel {ci}: {err}",
                    NAME[x]
                ));
            }
        }
    }

    fn server_request(&mut self, ci: usize, epoch: u64) {
        let now = self.now;
        if matches!(self.ends[SERVER].status, Status::Exited { .. }) {
            // The rendezvous socket is gone: the shim replies `no_session`.
            self.log(format!(
                "server gone: no_session for epoch {epoch} on channel {ci}"
            ));
            self.stats.no_session += 1;
            let ch = &mut self.chans[ci];
            ch.pipes[S2C].send_marker(now, Item::Reply(Reply::NoSession), 0);
            ch.pipes[S2C].close(now);
            ch.phase[SERVER] = Phase::Done;
            return;
        }
        if let Some(h) = self.srv_highest
            && epoch <= h
        {
            self.log(format!(
                "server rejects stale epoch {epoch} (highest {h}) on channel {ci}"
            ));
            self.stats.stale_rejected += 1;
            let ch = &mut self.chans[ci];
            ch.pipes[S2C].send_marker(now, Item::Reply(Reply::Stale), 0);
            ch.pipes[S2C].close(now);
            ch.phase[SERVER] = Phase::Done;
            return;
        }
        self.srv_highest = Some(epoch);
        self.chans[ci].pipes[S2C].send_marker(now, Item::Reply(Reply::Ok), 0);
        self.note_rewind(SERVER);
        let old = self.ends[SERVER].active.replace(ci);
        let t = self.inst(now);
        self.ends[SERVER].s.swap_channel(t);
        self.ends[SERVER].fail_seen = false;
        self.chans[ci].phase[SERVER] = Phase::Active;
        if let Some(o) = old {
            // The daemon closes the previous channel's fds.
            let ch = &mut self.chans[o];
            ch.pipes[S2C].close(now);
            ch.phase[SERVER] = Phase::Done;
            ch.disturbed = true;
        }
        self.stats.server_swaps += 1;
        if self.chans[ci].phase[CLIENT] == Phase::Done {
            self.stats.zombie_accepts += 1;
        }
        self.log(format!(
            "server accepts epoch {epoch}: swaps to channel {ci} (from {old:?})"
        ));
    }

    fn client_reply(&mut self, ci: usize, r: Reply) {
        let now = self.now;
        let ok =
            matches!(&self.cli.attempt, Some(a) if a.ch == ci && a.phase == AttemptPhase::Sent);
        if !ok {
            self.fail(format!(
                "client got reply {r:?} on channel {ci} which is not the outstanding attempt"
            ));
        }
        match r {
            Reply::Ok => {
                let swap_at = now + self.rng.range(0, self.p.ctl.swap_delay_max);
                self.cli.attempt.as_mut().unwrap().phase = AttemptPhase::GotReply { swap_at };
                self.log(format!(
                    "client receives ok on channel {ci}; swaps at {}",
                    fmt_t(swap_at)
                ));
            }
            Reply::NoSession => {
                // The server has exited, presumably because the stream is
                // complete, and its final frames may still be in flight on
                // the current channel; so this is only fatal once that
                // channel fails too (see `client_control`).
                self.log(format!("client receives no_session on channel {ci}"));
                let a = self.cli.attempt.take().unwrap();
                self.kill(a.ch, CLIENT, now);
                self.cli.server_gone = true;
            }
            Reply::Stale => self.fail("the outstanding attempt was rejected as stale"),
        }
    }

    /// An `error` reply to a resume is fatal to the client.
    fn client_fatal(&mut self) {
        let now = self.now;
        self.log("client: resume failed with no_session; giving up".into());
        self.stats.fatal += 1;
        if let Some(a) = self.cli.attempt.take() {
            self.kill(a.ch, CLIENT, now);
        }
        if let Some(ci) = self.ends[CLIENT].active
            && self.chans[ci].phase[CLIENT] != Phase::Done
        {
            self.kill(ci, CLIENT, now);
        }
        self.ends[CLIENT].status = Status::Draining;
    }

    fn start_attempt(&mut self, initial: bool) {
        let now = self.now;
        let epoch = self.cli.next_epoch;
        self.cli.next_epoch += 1;
        let ci = self.new_channel();
        let ctl = self.p.ctl.clone();
        let mut establish = self.rng.range(ctl.establish.0, ctl.establish.1);
        let mut request_delay = 0;
        let deadline = if initial {
            Tick::MAX
        } else {
            now + ctl.connect_timeout
        };
        let mut doom = "";
        if !initial && !self.healed && self.rng.below(1000) < ctl.doom_permille {
            match self.rng.below(4) {
                0 => {
                    doom = "slow to establish";
                    establish = ctl.connect_timeout + self.rng.range(1, 10 * SEC);
                }
                1 => {
                    doom = "ssh dies";
                    let t = now + self.rng.range(0, ctl.connect_timeout);
                    self.agenda.push((t, Ev::Die { ch: ci }));
                }
                2 => {
                    doom = "reply lost in a stall";
                    self.chans[ci].pipes[S2C].stalled = true;
                    self.chans[ci].disturbed = true;
                }
                _ => {
                    doom = "very slow request";
                    request_delay = self
                        .rng
                        .range(ctl.connect_timeout / 2, 2 * ctl.connect_timeout);
                }
            }
        }
        let e = &self.ends[CLIENT];
        let speculative = e.active.is_some() && !e.fail_seen && !e.s.transport_timed_out();
        self.cli.attempt = Some(Attempt {
            epoch,
            ch: ci,
            phase: AttemptPhase::Connecting {
                ready_at: now + establish,
            },
            deadline,
            request_delay,
            speculative,
            old_rx: 0,
        });
        self.stats.attempts += 1;
        self.log(format!(
            "client starts attempt epoch {epoch} on channel {ci} (speculative={speculative}) {doom}"
        ));
    }

    fn abandon(&mut self, why: &str) {
        let now = self.now;
        let a = self.cli.attempt.take().unwrap();
        // The request may still be in flight. If it was held up past `ssh`
        // (in the shim, say), it may arrive much later: a stale epoch.
        let until = if a.request_delay > 0 || self.rng.chance(1, 4) {
            now + self.rng.range(0, 60 * SEC)
        } else {
            now + self.rng.range(0, SEC)
        };
        self.kill(a.ch, CLIENT, until);
        self.cli.not_before = now + self.cli.backoff;
        self.cli.backoff = (2 * self.cli.backoff).min(self.p.ctl.backoff_max);
        self.stats.abandoned += 1;
        self.log(format!(
            "client abandons attempt epoch {} on channel {} ({why})",
            a.epoch, a.ch
        ));
    }

    fn client_swap(&mut self) {
        let now = self.now;
        let a = self.cli.attempt.take().unwrap();
        self.note_rewind(CLIENT);
        let old = self.ends[CLIENT].active.replace(a.ch);
        let t = self.inst(now);
        self.ends[CLIENT].s.swap_channel(t);
        self.ends[CLIENT].fail_seen = false;
        self.chans[a.ch].phase[CLIENT] = Phase::Active;
        if let Some(o) = old
            && self.chans[o].phase[CLIENT] != Phase::Done
        {
            self.kill(o, CLIENT, now);
        }
        self.cli.backoff = self.p.ctl.backoff_init;
        self.stats.client_swaps += 1;
        if a.speculative {
            self.stats.speculative_swaps += 1;
        }
        if a.old_rx > 0 {
            self.stats.data_during_resume += 1;
        }
        self.log(format!(
            "client swaps to channel {} (epoch {}, from {old:?}; {} bytes arrived on the old channel meanwhile)",
            a.ch, a.epoch, a.old_rx
        ));
    }

    fn note_rewind(&mut self, x: usize) {
        if self.ends[1 - x].s.recv_high_water_mark() > self.ends[x].s.peer_ack() {
            self.stats.rewind_duplicates += 1;
        }
    }

    fn observe_failures(&mut self) {
        let now = self.now;
        for x in 0..2 {
            let e = &self.ends[x];
            if !e.status.driven() || e.fail_seen {
                continue;
            }
            if let Some(ci) = e.active
                && self.chans[ci].phase[x] == Phase::Active
                && self.chans[ci].failed_for(x, now)
            {
                self.ends[x].fail_seen = true;
                self.log(format!("{} sees channel {ci} fail", NAME[x]));
            }
        }
    }

    /// The client's resume logic.
    fn client_control(&mut self) {
        let now = self.now;
        if self.ends[CLIENT].status != Status::Running {
            return;
        }
        if let Some(a) = self.cli.attempt.clone() {
            match a.phase {
                AttemptPhase::Connecting { ready_at } if now >= ready_at => {
                    if self.chans[a.ch].failed_for(CLIENT, now) {
                        self.abandon("ssh failed while connecting");
                        return;
                    }
                    self.chans[a.ch].pipes[C2S].send_marker(
                        now,
                        Item::Request { epoch: a.epoch },
                        a.request_delay,
                    );
                    self.cli.attempt.as_mut().unwrap().phase = AttemptPhase::Sent;
                    self.log(format!(
                        "client sends resume epoch {} on channel {}",
                        a.epoch, a.ch
                    ));
                }
                AttemptPhase::GotReply { swap_at } if now >= swap_at => {
                    self.client_swap();
                    return;
                }
                _ => {}
            }
            if !matches!(a.phase, AttemptPhase::GotReply { .. }) {
                if now >= a.deadline {
                    self.abandon("connect timeout");
                } else if self.chans[a.ch].failed_for(CLIENT, now) {
                    self.abandon("ssh failed");
                }
            }
            return;
        }
        if now < self.cli.not_before {
            return;
        }
        if self.client_wants_resume() {
            if self.cli.server_gone {
                self.client_fatal();
            } else {
                self.start_attempt(false);
            }
        }
        self.cli.speculative = false;
    }

    /// Whether the client (with no attempt outstanding) wants to resume:
    /// its channel failed or timed out, or a speculative resume was
    /// requested.
    fn client_wants_resume(&self) -> bool {
        let e = &self.ends[CLIENT];
        if e.active.is_none() || self.cli.attempt.is_some() {
            return false;
        }
        // Once our shutdown is acknowledged and the peer's received and
        // acknowledged, the transport has nothing left to do: only local
        // delivery remains, and a resume would be pointless (and, if the
        // server has already exited, fatal).
        let transport_done = e.s.shutdown_acked()
            && e.s.peer_shutdown().is_some()
            && e.s.ack_sent() == e.s.recv_high_water_mark();
        let speculative = self.cli.speculative && !self.cli.server_gone;
        !transport_done && (e.fail_seen || e.s.transport_timed_out() || speculative)
    }

    // ------------------------------------------------------------------
    // Applications
    // ------------------------------------------------------------------

    fn app_step(&mut self, x: usize) {
        let now = self.now;
        let y = 1 - x;
        let status = self.ends[x].status;
        if matches!(status, Status::Exited { .. }) {
            return;
        }
        let sp = self.p.side[x].clone();
        let e = &mut self.ends[x];
        if status == Status::Running && !e.app.send_closed {
            let len = self.streams[x].len();
            let w = e.app.written;
            if w < len && now >= e.app.next_write {
                let space = e.s.send_space();
                if space > 0 {
                    let k = self
                        .rng
                        .usize_range(1, sp.write_chunk_max)
                        .min(len - w)
                        .min(space);
                    let n = e.s.write_app(&self.streams[x][w..w + k]);
                    assert_eq!(n, k, "write_app accepted less than send_space");
                    e.app.written += k;
                    e.app.next_write = now + self.rng.range(0, sp.write_gap_max);
                }
            }
            if e.app.written == len
                && let Some(d) = sp.close_send_delay
            {
                let at = *e.app.close_send_at.get_or_insert(now + d);
                if now >= at {
                    e.s.close_app_send();
                    e.app.send_closed = true;
                    let msg = format!("{} app closes its sending side", NAME[x]);
                    self.log(msg);
                }
            }
        }
        let e = &mut self.ends[x];
        if let Some(t) = sp.close_read_at
            && !e.app.read_closed
            && now >= t
        {
            e.s.close_app_read();
            e.app.read_closed = true;
            let msg = format!("{} app closes its reading side", NAME[x]);
            self.log(msg);
        }
        let mut err = None;
        if self.reader_can_read(x) && now >= self.ends[x].app.next_read {
            let e = &mut self.ends[x];
            let data = e.s.app_data();
            if !data.is_empty() {
                let k = self.rng.usize_range(1, sp.read_chunk_max).min(data.len());
                let off = e.app.received;
                let expect = &self.streams[y];
                if off + k > expect.len() {
                    err = Some(format!(
                        "{} app received bytes [{off}, {}) beyond the {} bytes the peer has",
                        NAME[x],
                        off + k,
                        expect.len()
                    ));
                } else if data[..k] != expect[off..off + k] {
                    let i = (0..k).find(|&i| data[i] != expect[off + i]).unwrap();
                    err = Some(format!(
                        "{} app received corrupt data at offset {}",
                        NAME[x],
                        off + i
                    ));
                } else {
                    e.s.consume_app(k);
                    e.app.received += k;
                    e.app.next_read = now + self.rng.range(0, sp.read_gap_max);
                    if e.app.reader == ReaderMode::Bursty && self.rng.chance(1, 40) {
                        e.app.next_read += self.rng.range(5 * SEC, 120 * SEC);
                    }
                }
            }
        }
        if let Some(m) = err {
            self.fail(m);
        }
        let e = &mut self.ends[x];
        if !e.app.eof && e.s.app_eof() {
            e.app.eof = true;
            if !e.app.read_closed && e.app.received != self.streams[y].len() {
                let m = format!(
                    "{} app sees EOF after {} of {} bytes",
                    NAME[x],
                    e.app.received,
                    self.streams[y].len()
                );
                self.fail(m);
            }
            let msg = format!("{} app sees EOF", NAME[x]);
            self.log(msg);
        }
    }

    // ------------------------------------------------------------------
    // Driving the sessions
    // ------------------------------------------------------------------

    /// When the driver actually wakes for a deadline: possibly a little late.
    fn fire_time(&self, deadline: Tick) -> Tick {
        if self.p.timer_jitter == 0 {
            return deadline;
        }
        let h = Rng::new(deadline ^ self.case.seed).next_u64();
        deadline + h % (self.p.timer_jitter + 1)
    }

    fn timers(&mut self, x: usize) {
        let now = self.now;
        let e = &self.ends[x];
        if !e.status.driven() || e.active.is_none() {
            return;
        }
        let due =
            e.s.poll_timeout()
                .is_some_and(|t| self.fire_time(self.tick(t)) <= now);
        // Calling `handle_timeout` early is documented as harmless.
        if !due && !self.rng.chance(1, 64) {
            return;
        }
        let before = e.s.frames_sent();
        let was = e.s.transport_timed_out();
        let i = self.inst(now);
        self.ends[x].s.handle_timeout(i);
        if due
            && let Some(t2) = self.ends[x].s.poll_timeout()
            && self.tick(t2) <= now
        {
            self.fail(format!(
                "{}: poll_timeout() is still {} after handle_timeout: a driver would spin",
                NAME[x],
                fmt_t(self.tick(t2))
            ));
        }
        self.check_produced(x, before);
        if !was && self.ends[x].s.transport_timed_out() {
            self.ends[x].timeouts += 1;
            self.stats.timeouts += 1;
            let ci = self.ends[x].active.unwrap();
            self.log(format!("{} transport timed out on channel {ci}", NAME[x]));
            let y = 1 - x;
            let peer = &self.ends[y];
            let ch = &self.chans[ci];
            if peer.status.driven() {
                self.stats.timeouts_peer_running += 1;
            }
            let excused = ch.disturbed
                || peer.active != Some(ci)
                || ch.phase[y] != Phase::Active
                || !peer.status.driven()
                || peer.fail_seen;
            if !excused && !self.p.aggressive {
                self.fail(format!(
                    "{} transport timed out on channel {ci}, which is healthy and in use by the peer",
                    NAME[x]
                ));
            }
        }
    }

    /// If `x` has produced a frame since `frames_sent` was `before`, checks
    /// it against the send limit as it stands at production time.
    fn check_produced(&mut self, x: usize, before: u64) {
        let t = self.inst(self.now);
        let s = &mut self.ends[x].s;
        let n = s.frames_sent() - before;
        if n == 0 {
            return;
        }
        let limit = s.send_limit();
        let data = s.transmit_data(t).to_vec();
        if n > 1 || data.len() < HEADER_LEN {
            self.fail(format!(
                "{}: unexpected frame production ({n} frames)",
                NAME[x]
            ));
        }
        let h = FrameHeader::decode(data[..HEADER_LEN].try_into().unwrap())
            .unwrap_or_else(|e| self.fail(format!("{} produced a bad header: {e}", NAME[x])));
        if data.len() != HEADER_LEN + usize::from(h.length) {
            self.fail(format!("{} produced a frame of the wrong length", NAME[x]));
        }
        if h.length > 0 && h.seq + u64::from(h.length) > limit {
            self.fail(format!(
                "{} produced data [{}, {}) beyond its send limit {limit}",
                NAME[x],
                h.seq,
                h.seq + u64::from(h.length)
            ));
        }
    }

    /// Writes the session's output to its channel, as far as the channel
    /// accepts it.
    fn pump(&mut self, x: usize) {
        let e = &self.ends[x];
        if !e.status.driven() || e.fail_seen {
            return;
        }
        let Some(ci) = e.active else {
            return;
        };
        if self.chans[ci].phase[x] != Phase::Active {
            return;
        }
        let now = self.now;
        let t = self.inst(now);
        loop {
            let before = self.ends[x].s.frames_sent();
            let data = self.ends[x].s.transmit_data(t).to_vec();
            self.check_produced(x, before);
            if data.is_empty() {
                break;
            }
            let n = self.chans[ci].pipes[x].write(now, &data);
            if n > 0 {
                self.tap(x, ci, &data[..n]);
                self.ends[x].s.consume_transmit(n, t);
            }
            if n < data.len() {
                break;
            }
        }
    }

    /// Checks the frames `x` writes on channel `ci`, as they are written.
    fn tap(&mut self, x: usize, ci: usize, bytes: &[u8]) {
        let y = 1 - x;
        let tap = &mut self.chans[ci].taps[x];
        tap.decoder.push(bytes);
        let mut frames = Vec::new();
        loop {
            match tap.decoder.next_frame() {
                Ok(Some((h, p))) => {
                    frames.push((tap.frames, tap.next_seq, h, p.to_vec()));
                    tap.frames += 1;
                    tap.next_seq = Some(h.seq + h.span());
                }
                Ok(None) => break,
                Err(e) => {
                    let m = format!("{} wrote a malformed header: {e}", NAME[x]);
                    self.fail(m)
                }
            }
        }
        for (idx, prev_end, h, payload) in frames {
            self.stats.frames += 1;
            let s = &self.ends[x].s;
            let r = &self.ends[y].s;
            let what = format!("{} frame #{idx} on channel {ci} ({h:?})", NAME[x]);
            if self.verbose {
                println!("{:>14}   {what}", fmt_t(self.now));
            }
            let len = u64::from(h.length);
            let end = h.seq + len;
            let mut err = Vec::new();
            if h.first != (idx == 0) {
                err.push(
                    "FIRST flag must be set on exactly the first frame of a channel".to_string(),
                );
            }
            if usize::from(h.length) > s.config().max_payload {
                err.push("payload exceeds max_payload".into());
            }
            if len > 0 {
                if end > s.send_limit() {
                    err.push(format!(
                        "data beyond the sender's send limit {}",
                        s.send_limit()
                    ));
                }
                if end > r.advertised_limit() {
                    err.push(format!(
                        "data beyond the limit the receiver advertised ({})",
                        r.advertised_limit()
                    ));
                }
                let stream = &self.streams[x];
                if end > s.send_data_end() || end as usize > stream.len() {
                    err.push("data beyond what the application wrote".into());
                } else if payload[..] != stream[h.seq as usize..end as usize] {
                    err.push("payload does not match the application's stream".into());
                }
            }
            if h.shutdown && !(s.send_closed() && end == s.send_data_end()) {
                err.push("SHUTDOWN not at the end of a closed stream".into());
            }
            if h.ack > s.recv_high_water_mark() {
                err.push("ack beyond what the sender has received".into());
            }
            if h.ack > r.max_sent() {
                err.push("ack beyond what the receiver has sent".into());
            }
            let mon = &mut self.ends[x].mon;
            if h.ack < mon.wire_ack {
                err.push(format!("ack decreased from {}", mon.wire_ack));
            }
            if h.limit() < mon.wire_limit {
                err.push(format!(
                    "advertised limit decreased from {}",
                    mon.wire_limit
                ));
            }
            mon.wire_ack = mon.wire_ack.max(h.ack);
            mon.wire_limit = mon.wire_limit.max(h.limit());
            if h.window as usize > self.ends[x].s.recv_capacity() {
                err.push("window exceeds the receive buffer".into());
            }
            if let Some(p) = prev_end
                && h.seq < p
            {
                err.push(format!(
                    "seq went backwards within the channel (previous frame ended at {p})"
                ));
            }
            if !err.is_empty() {
                self.fail(format!("{what}: {}", err.join("; ")));
            }
        }
    }

    fn lifecycle(&mut self, x: usize) {
        let now = self.now;
        let e = &mut self.ends[x];
        if e.status == Status::Running && e.s.is_finished() {
            e.finished = true;
            e.status = Status::Flushing {
                deadline: now + FLUSH_TIMEOUT,
            };
            let msg = format!("{} session is finished", NAME[x]);
            self.log(msg);
        }
        let e = &mut self.ends[x];
        match e.status {
            Status::Flushing { deadline } => {
                if e.s.tx_pending() == 0 || now >= deadline || e.fail_seen {
                    e.flush_incomplete = e.s.tx_pending() > 0;
                    self.exit(x);
                }
            }
            Status::Draining if e.s.recv_buffered() == 0 => self.exit(x),
            _ => {}
        }
    }

    fn exit(&mut self, x: usize) {
        let now = self.now;
        let y = 1 - x;
        let active = self.ends[x].active;
        self.ends[x].status = Status::Exited { at: now };
        self.ends[x].exit_channel = active;
        self.ends[x].peer_on_channel_at_exit = active.is_some_and(|ci| {
            let p = &self.ends[y];
            p.active == Some(ci)
                && self.chans[ci].phase[y] == Phase::Active
                && self.chans[ci].phase[x] == Phase::Active
                && !p.fail_seen
                && !self.ends[x].fail_seen
                && p.status.driven()
        });
        if let Some(ci) = active
            && self.chans[ci].phase[x] == Phase::Active
        {
            self.chans[ci].pipes[x].close(now);
            self.chans[ci].phase[x] = Phase::Done;
        }
        if x == CLIENT
            && let Some(a) = self.cli.attempt.take()
        {
            self.kill(a.ch, CLIENT, now);
        }
        self.log(format!(
            "{} exits (finished={}, channel {active:?})",
            NAME[x], self.ends[x].finished
        ));
    }

    // ------------------------------------------------------------------
    // Checks
    // ------------------------------------------------------------------

    fn check_invariants(&mut self) {
        for x in 0..2 {
            let y = 1 - x;
            let e = &self.ends[x];
            let s = &e.s;
            let r = &self.ends[y].s;
            let mut err = Vec::new();
            if let Some(pe) = s.error() {
                err.push(format!("protocol error: {pe}"));
            }
            if s.recv_buffered() > s.recv_capacity() {
                err.push("receive buffer over capacity".to_string());
            }
            if s.send_buffered() > s.send_capacity() {
                err.push("send buffer over capacity".to_string());
            }
            let m = e.mon;
            let mono = [
                ("send_limit", m.send_limit, s.send_limit()),
                ("peer_ack", m.peer_ack, s.peer_ack()),
                ("max_sent", m.max_sent, s.max_sent()),
                ("recv_high_water_mark", m.hwm, s.recv_high_water_mark()),
                ("delivered", m.delivered, s.delivered()),
                ("ack_sent", m.ack_sent, s.ack_sent()),
                ("advertised_limit", m.advertised_limit, s.advertised_limit()),
                ("frames_sent", m.frames_sent, s.frames_sent()),
            ];
            for (name, old, new) in mono {
                if new < old {
                    err.push(format!("{name} decreased from {old} to {new}"));
                }
            }
            if s.peer_ack() > s.max_sent() {
                err.push("peer_ack beyond max_sent".into());
            }
            if s.send_position() < s.peer_ack() || s.send_position() > s.max_sent() {
                err.push("send position outside [peer_ack, max_sent]".into());
            }
            if s.peer_ack() > r.recv_high_water_mark() {
                err.push("acknowledged beyond what the peer has received".into());
            }
            if r.recv_high_water_mark() > s.max_sent() {
                err.push("peer received beyond what we have sent".into());
            }
            if s.send_limit() > r.advertised_limit() {
                err.push("send limit beyond what the peer has advertised".into());
            }
            let shut = u64::from(s.peer_shutdown().is_some());
            if s.delivered() + s.recv_buffered() as u64 + shut != s.recv_high_water_mark() {
                err.push("delivered + buffered + shutdown != high-water mark".into());
            }
            if s.send_data_end() != e.app.written as u64 {
                err.push("send_data_end differs from what the application wrote".into());
            }
            if s.send_closed() != e.app.send_closed {
                err.push("send_closed differs from the application".into());
            }
            if e.app.read_closed {
                if !s.app_data().is_empty() {
                    err.push("data offered to the application after close_app_read".into());
                }
                if e.app.received as u64 > s.delivered() {
                    err.push("application received more than delivered".into());
                }
            } else if e.app.received as u64 != s.delivered() {
                err.push("application received != delivered".into());
            }
            if s.app_eof() && !(r.send_closed() && s.delivered() == r.send_data_end()) {
                err.push("app_eof before the whole peer stream was delivered".into());
            }
            if !err.is_empty() {
                self.fail(format!("{}: {}", NAME[x], err.join("; ")));
            }
            self.ends[x].mon = Mono {
                send_limit: s.send_limit(),
                peer_ack: s.peer_ack(),
                max_sent: s.max_sent(),
                hwm: s.recv_high_water_mark(),
                delivered: s.delivered(),
                ack_sent: s.ack_sent(),
                advertised_limit: s.advertised_limit(),
                frames_sent: s.frames_sent(),
                ..m
            };
        }
    }

    /// Every byte written has been delivered (or discarded after the
    /// receiver closed its read side).
    fn data_complete(&self) -> bool {
        (0..2).all(|x| {
            let y = 1 - x;
            let len = self.streams[x].len();
            let r = &self.ends[y];
            self.ends[x].app.written == len
                && if r.app.read_closed {
                    r.s.delivered() == len as u64
                } else {
                    r.app.received == len
                }
        })
    }

    fn final_checks(&mut self) {
        if self.p.family == Family::Liveness {
            return self.liveness_checks();
        }
        for x in 0..2 {
            let y = 1 - x;
            let len = self.streams[x].len();
            if self.ends[x].app.written != len {
                self.fail(format!(
                    "no progress: {} application wrote only {} of {len} bytes",
                    NAME[x], self.ends[x].app.written
                ));
            }
            let r = &self.ends[y];
            let got = if r.app.read_closed {
                r.s.delivered() as usize
            } else {
                r.app.received
            };
            if got != len {
                self.fail(format!(
                    "no progress: {} received only {got} of {len} bytes",
                    NAME[y]
                ));
            }
        }
        let both_close = self.p.side.iter().all(|s| s.close_send_delay.is_some());
        if both_close {
            let fin = [self.ends[0].finished, self.ends[1].finished];
            match fin {
                [true, true] => self.stats.both_finished += 1,
                [false, false] => self.fail("no progress: neither session finished"),
                [c, _] => {
                    let (f, o) = if c {
                        (CLIENT, SERVER)
                    } else {
                        (SERVER, CLIENT)
                    };
                    let fe = &self.ends[f];
                    let lossy = fe.exit_channel.is_some_and(|ci| self.chans[ci].lossy);
                    // The orphan can miss the final frame only if it was
                    // lost, never written, or the orphan was not (or no
                    // longer) reading that channel.
                    let moved = self.ends[o].active != fe.exit_channel;
                    if fe.peer_on_channel_at_exit && !lossy && !fe.flush_incomplete && !moved {
                        self.fail(format!(
                            "{} never finished, although {} finished and exited cleanly on a healthy channel they shared",
                            NAME[o], NAME[f]
                        ));
                    }
                    if o == CLIENT && !matches!(self.ends[CLIENT].status, Status::Exited { .. }) {
                        self.fail("the client is stuck: the server finished but the client neither finished nor gave up");
                    }
                    self.stats.orphans += 1;
                }
            }
        }
        if self.p.family == Family::Clean {
            let mut err = Vec::new();
            if self.stats.timeouts_peer_running > 0 {
                err.push("a transport timed out while both sides were running");
            }
            if self.stats.abandoned > 0 {
                err.push("a resume attempt was abandoned");
            }
            if self.stats.fatal > 0 {
                err.push("the client failed fatally");
            }
            if !err.is_empty() {
                self.fail(format!("clean scenario: {}", err.join("; ")));
            }
        }
    }

    fn liveness_checks(&mut self) {
        let n = self.p.never_reader.unwrap();
        let r = 1 - n;
        if self.stats.timeouts > 0 {
            self.fail("liveness: a transport timed out although the channel is healthy");
        }
        if self.stats.attempts != 1 {
            self.fail("liveness: the client resumed although the channel is healthy");
        }
        let len_n = self.streams[n].len();
        if self.ends[n].app.written != len_n || self.ends[r].app.received != len_n {
            self.fail(format!(
                "liveness: the direction towards the reading {} is affected ({} of {len_n} bytes received)",
                NAME[r], self.ends[r].app.received
            ));
        }
        let len_r = self.streams[r].len();
        let cap_n = self.p.side[n].cfg.recv_buffer;
        if self.ends[n].app.read_closed {
            if self.ends[n].s.delivered() != len_r as u64 {
                self.fail(format!(
                    "liveness: after the non-reader closed its read side, only {} of {len_r} bytes were discarded",
                    self.ends[n].s.delivered()
                ));
            }
        } else {
            if self.ends[n].s.recv_buffered() != cap_n.min(len_r) {
                self.fail("liveness: the non-reader's receive buffer is not exactly full");
            }
            let expect = (cap_n + self.p.side[r].cfg.send_buffer).min(len_r);
            if self.ends[r].app.written != expect {
                self.fail(format!(
                    "liveness: the writer towards the non-reader wrote {} bytes, expected {expect} (buffers full)",
                    self.ends[r].app.written
                ));
            }
        }
        let snap = self.snapshot.unwrap();
        for x in 0..2 {
            let got = self.ends[x].s.frames_received() - snap[x];
            let hb =
                self.p.side[1 - x].cfg.heartbeat_interval.as_micros() as u64 + self.p.timer_jitter;
            let want = 5 * MIN / hb - 1;
            if got < want {
                self.fail(format!(
                    "liveness: {} received only {got} frames in the last 5 minutes (expected at least {want} heartbeats)",
                    NAME[x]
                ));
            }
        }
    }
}
