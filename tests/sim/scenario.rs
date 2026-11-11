//! Scenario parameters, derived deterministically from a [`Case`].

use std::fmt;
use std::time::Duration;

use stitch::core::Config;

use crate::rng::Rng;
use crate::world::{MIN, MS, SEC, Tick};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// No faults; possibly speculative resumes. Everything must complete
    /// cleanly: both sides finish, nothing ever times out.
    Clean,
    /// Cuts, stalls, failing and slow resume attempts, speculative resumes,
    /// stalled readers; then the network heals and everything must complete.
    Chaos,
    /// A healthy channel and an application which never reads: nothing may
    /// time out, heartbeats keep flowing, the other direction is unaffected.
    Liveness,
}

/// One simulation, fully determined by these four numbers. `size` (1–100)
/// scales the amount of data and `chaos` (0–100) the number of faults, so
/// that proptest can shrink a failure to a smaller one with the same seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Case {
    pub family: Family,
    pub seed: u64,
    pub size: u32,
    pub chaos: u32,
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fam = match self.family {
            Family::Clean => "clean",
            Family::Chaos => "chaos",
            Family::Liveness => "liveness",
        };
        write!(f, "{fam}:{}:{}:{}", self.seed, self.size, self.chaos)
    }
}

impl Case {
    /// Parses the `family:seed:size:chaos` form printed on failure.
    pub fn parse(s: &str) -> Result<Case, String> {
        let parts: Vec<&str> = s.trim().split(':').collect();
        let [fam, seed, size, chaos] = parts[..] else {
            return Err(format!("expected family:seed:size:chaos, got {s:?}"));
        };
        let family = match fam {
            "clean" => Family::Clean,
            "chaos" => Family::Chaos,
            "liveness" => Family::Liveness,
            _ => return Err(format!("unknown family {fam:?}")),
        };
        let num = |v: &str| v.parse::<u64>().map_err(|e| format!("{v:?}: {e}"));
        Ok(Case {
            family,
            seed: num(seed)?,
            size: num(size)? as u32,
            chaos: num(chaos)? as u32,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderMode {
    /// Reads promptly (at its own pace).
    Eager,
    /// Reads, but occasionally stops for a long time (well beyond the
    /// transport timeout).
    Bursty,
    /// Never reads.
    Never,
    /// Does not read until the network heals.
    NeverUntilHeal,
}

#[derive(Debug, Clone)]
pub struct SideParams {
    pub cfg: Config,
    /// Bytes this side's application writes.
    pub stream_len: usize,
    pub write_chunk_max: usize,
    pub write_gap_max: Tick,
    pub write_start: Tick,
    /// Close the sending direction this long after writing everything
    /// (never, if `None`).
    pub close_send_delay: Option<Tick>,
    pub reader: ReaderMode,
    pub read_chunk_max: usize,
    pub read_gap_max: Tick,
    pub read_start: Tick,
    /// Close the application's read side at this time.
    pub close_read_at: Option<Tick>,
}

#[derive(Debug, Clone)]
pub struct NetParams {
    pub latency: (Tick, Tick),
    pub bandwidth: (u64, u64),
    pub mss: (usize, usize),
    pub capacity_max: usize,
}

/// The client's resume logic (see "Choosing to resume").
#[derive(Debug, Clone)]
pub struct CtlParams {
    /// Abandon an attempt with no reply after this long.
    pub connect_timeout: Tick,
    /// Time for a new `ssh` to be established.
    pub establish: (Tick, Tick),
    /// Delay between the client receiving the `ok` reply and swapping.
    pub swap_delay_max: Tick,
    pub backoff_init: Tick,
    pub backoff_max: Tick,
    /// Probability (per mille) that a resume attempt started before the
    /// network heals is doomed (too slow, dies, reply lost, or very slow
    /// request delivery).
    pub doom_permille: u64,
}

#[derive(Debug, Clone)]
pub enum Fault {
    /// Lossy failure of the client's active channel.
    Cut,
    /// Lossy failure of the outstanding resume attempt's channel.
    CutAttempt,
    /// Stall the client's active channel (`dirs` by pipe index), for `dur`
    /// or until the network heals.
    Stall { dirs: [bool; 2], dur: Option<Tick> },
    /// The client starts a resume although nothing is wrong.
    Speculative,
}

#[derive(Debug, Clone)]
pub struct Params {
    pub family: Family,
    /// The longer of the two ends' transport timeouts.
    pub timeout: Tick,
    /// Heartbeats and timeouts so short, and mismatched, that transports
    /// time out spuriously.
    pub aggressive: bool,
    /// The driver calls `handle_timeout` up to this late.
    pub timer_jitter: Tick,
    pub side: [SideParams; 2],
    pub net: NetParams,
    pub ctl: CtlParams,
    /// No faults after this; stalls end; readers read.
    pub heal_at: Tick,
    pub faults: Vec<(Tick, Fault)>,
    /// Liveness: how long to run.
    pub end_at: Option<Tick>,
    /// Liveness: the side whose application never reads.
    pub never_reader: Option<usize>,
}

fn dur(t: Tick) -> Duration {
    Duration::from_micros(t)
}

fn gen_config(r: &mut Rng, heartbeat: Tick, timeout: Tick, live: bool, aggressive: bool) -> Config {
    let timeout = if aggressive {
        // Mismatched, possibly shorter than the peer's heartbeat: spurious
        // timeouts and resumes on healthy channels.
        heartbeat * r.range(1, 3) + r.range(0, heartbeat)
    } else {
        timeout
    };
    let recv_buffer = if live {
        r.usize_log(1, 32 << 10)
    } else if r.chance(1, 6) {
        r.usize_range(1, 64)
    } else {
        r.usize_log(64, 256 << 10)
    };
    let send_buffer = if live {
        // Not tiny: when the non-reader closes its read side, the whole
        // stream must get through in the remaining time.
        r.usize_log(512, 32 << 10)
    } else if r.chance(1, 6) {
        r.usize_range(1, 64)
    } else {
        r.usize_log(64, 512 << 10)
    };
    let max_payload = if !live && r.chance(1, 6) {
        r.usize_range(1, 64)
    } else if r.chance(1, 4) {
        stitch::frame::MAX_PAYLOAD
    } else {
        r.usize_log(64, stitch::frame::MAX_PAYLOAD)
    };
    let window_update_threshold = match r.below(3) {
        0 => recv_buffer / 4,
        1 => r.usize_range(0, recv_buffer),
        _ => 0,
    };
    Config {
        send_buffer,
        recv_buffer,
        max_payload,
        heartbeat_interval: dur(heartbeat - r.range(0, heartbeat / 4)),
        timeout: dur(timeout),
        window_update_threshold,
        ack_threshold: r.usize_log(1, 512 << 10),
        ack_delay: dur(r.range(0, 500) * MS),
    }
}

impl Params {
    pub fn generate(case: &Case) -> Params {
        use Family::*;
        let mut r = Rng::new(case.seed);
        let size = u64::from(case.size.clamp(1, 100));
        let chaos = u64::from(case.chaos.min(100));
        let family = case.family;
        let live = family == Liveness;

        let aggressive = family == Chaos && r.chance(1, 5);
        let heartbeat = if aggressive {
            r.log_range(20 * MS, 2 * SEC)
        } else {
            r.range(3, 10) * SEC
        };
        let cfg = [0, 1].map(|_| {
            let timeout = 2 * heartbeat + r.range(0, heartbeat);
            gen_config(&mut r, heartbeat, timeout, live, aggressive)
        });
        let timeout = cfg
            .iter()
            .map(|c| c.timeout.as_micros() as Tick)
            .max()
            .unwrap();
        let timer_jitter = if r.chance(1, 2) {
            r.range(0, 50 * MS)
        } else {
            0
        };
        let heal_at = match family {
            Clean => r.range(10 * SEC, 120 * SEC),
            Chaos => r.range(20 * SEC, 20 * SEC + chaos * 4 * SEC),
            Liveness => 0,
        };
        let never_reader = live.then(|| r.below(2) as usize);
        let both_close = !live && r.chance(85, 100);

        // Application rates first: stream lengths are bounded by them.
        let min_chunk = if live { 512 } else { 1 };
        let rates: Vec<(usize, Tick, usize, Tick)> = (0..2)
            .map(|_| {
                (
                    r.usize_log(min_chunk, 1 << 16),
                    r.log_range(1, if live { 200 } else { 2000 }) * MS,
                    r.usize_log(min_chunk, 1 << 16),
                    r.log_range(1, if live { 200 } else { 1000 }) * MS,
                )
            })
            .collect();
        let mut sides = Vec::new();
        for x in 0..2 {
            let y = 1 - x;
            let (write_chunk_max, write_gap_max, read_chunk_max, read_gap_max) = rates[x];
            // Bound the number of frames, round trips, writes and reads, so
            // that tiny buffers, payloads or chunks do not make a case slow.
            let unit = cfg[x]
                .max_payload
                .min(cfg[y].recv_buffer)
                .min(cfg[x].send_buffer)
                .max(1);
            let bound = (unit * if live { 300 } else { 1500 })
                .min(write_chunk_max * 400)
                .min(rates[y].2 * 400);
            let stream_len = if live && never_reader == Some(y) {
                // Enough to fill the never-reading peer's receive buffer and
                // our own send buffer, so our writer ends up blocked.
                cfg[y].recv_buffer + cfg[x].send_buffer + r.usize_range(1, 5000)
            } else if r.chance(1, 10) {
                0
            } else {
                r.usize_log(1, size as usize * 3000).min(bound).max(1)
            };
            let close_send_delay = if !live && (both_close || r.chance(1, 2)) {
                Some(if r.chance(1, 4) {
                    r.range(30 * SEC, 240 * SEC)
                } else {
                    r.range(0, 5 * SEC)
                })
            } else {
                None
            };
            let reader = match family {
                Liveness if never_reader == Some(x) => ReaderMode::Never,
                Liveness => ReaderMode::Eager,
                Clean => {
                    if r.chance(6, 10) {
                        ReaderMode::Eager
                    } else {
                        ReaderMode::Bursty
                    }
                }
                Chaos => match r.below(20) {
                    0..9 => ReaderMode::Eager,
                    9..16 => ReaderMode::Bursty,
                    _ => ReaderMode::NeverUntilHeal,
                },
            };
            let close_read_at = match family {
                // (Only with a decent receive buffer: once the read side is
                // closed, throughput is one buffer per round trip.)
                Liveness
                    if never_reader == Some(x) && cfg[x].recv_buffer >= 2048 && r.chance(1, 3) =>
                {
                    Some(r.range(MIN, 3 * MIN))
                }
                Liveness => None,
                _ if r.chance(1, 12) => Some(r.range(0, heal_at + 60 * SEC)),
                _ => None,
            };
            sides.push(SideParams {
                cfg: cfg[x].clone(),
                stream_len,
                write_chunk_max,
                write_gap_max,
                write_start: r.range(0, 5 * SEC),
                close_send_delay,
                reader,
                read_chunk_max,
                read_gap_max,
                read_start: r.range(0, 5 * SEC),
                close_read_at,
            });
        }

        let lat_lo = r.log_range(100, 50 * MS);
        let bw_lo = r.log_range(20, 2000);
        let net = NetParams {
            latency: (lat_lo, r.range(lat_lo, 300 * MS)),
            bandwidth: (bw_lo, r.range(bw_lo, 5000)),
            mss: (64, 9000),
            capacity_max: r.usize_log(256, 256 << 10),
        };

        let connect_timeout = r.range(5, 30) * SEC;
        let est_lo = r.log_range(10 * MS, 2 * SEC);
        let backoff_init = r.range(200 * MS, 2 * SEC);
        let ctl = CtlParams {
            connect_timeout,
            establish: (est_lo, r.range(est_lo, (connect_timeout / 2).max(est_lo))),
            swap_delay_max: r.range(0, 1500) * MS,
            backoff_init,
            backoff_max: r.range(backoff_init, 30 * SEC),
            doom_permille: if family == Chaos { chaos * 5 } else { 0 },
        };

        let mut faults = Vec::new();
        match family {
            Clean => {
                for _ in 0..r.range(0, 1 + chaos / 15) {
                    faults.push((r.range(SEC, heal_at), Fault::Speculative));
                }
            }
            Chaos => {
                for _ in 0..r.range(chaos / 12, chaos / 6 + 1) {
                    let at = r.range(SEC, heal_at);
                    let f = match r.below(100) {
                        0..30 => Fault::Cut,
                        30..40 => Fault::CutAttempt,
                        40..70 => Fault::Stall {
                            dirs: match r.below(3) {
                                0 => [true, false],
                                1 => [false, true],
                                _ => [true, true],
                            },
                            dur: match r.below(3) {
                                0 => Some(r.range(heartbeat.min(50 * MS), heartbeat)),
                                1 => Some(r.range(timeout, 3 * timeout)),
                                _ => None,
                            },
                        },
                        _ => Fault::Speculative,
                    };
                    faults.push((at, f));
                }
            }
            Liveness => {}
        }
        faults.sort_by_key(|(t, _)| *t);

        Params {
            family,
            timeout,
            aggressive,
            timer_jitter,
            side: sides.try_into().unwrap(),
            net,
            ctl,
            heal_at,
            faults,
            end_at: live.then(|| r.range(15 * MIN, 25 * MIN)),
            never_reader,
        }
    }
}
