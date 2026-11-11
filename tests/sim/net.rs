//! Simulated channels: pairs of in-order byte pipes with latency, bandwidth,
//! bounded in-flight capacity, stalls, lossy cuts and graceful closes.
//!
//! A pipe carries [`Item`]s. Besides frame bytes, it carries the JSON
//! handshake messages of a resume (modelled as markers, since the core never
//! sees them) and a graceful end-of-file, so that all of them are subject to
//! the same latency, ordering, stalls and losses as the data.

use std::collections::VecDeque;

use stitch::frame::FrameDecoder;

use crate::rng::Rng;
use crate::world::{MS, Tick};

/// Index of the client-to-server pipe of a channel; the client writes it.
pub const C2S: usize = 0;
/// Index of the server-to-client pipe of a channel; the server writes it.
pub const S2C: usize = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Stale,
    NoSession,
}

#[derive(Debug)]
pub enum Item {
    Bytes(Vec<u8>),
    /// The client's JSON `resume` (or initial `open`, epoch 0).
    Request {
        epoch: u64,
    },
    /// The server's (or shim's) JSON reply.
    Reply(Reply),
    /// The writer closed its end gracefully.
    Eof,
}

#[derive(Debug)]
pub struct Packet {
    /// Arrival time at the reader.
    pub at: Tick,
    pub item: Item,
}

#[derive(Debug, Clone, Copy)]
pub struct Link {
    pub latency: Tick,
    /// Bytes per millisecond.
    pub bandwidth: u64,
    /// Bytes are delivered in packets of at most this size.
    pub mss: usize,
    /// Maximum bytes written but not yet read (in flight or in the reader's
    /// socket buffer); writes beyond it are partial or blocked.
    pub capacity: usize,
}

#[derive(Debug)]
pub struct Pipe {
    pub link: Link,
    queue: VecDeque<Packet>,
    /// Bytes in `queue`.
    queued: usize,
    /// When the link finishes serializing what has been written so far.
    link_free: Tick,
    /// Delivers nothing while set; writes still fill the capacity.
    pub stalled: bool,
    /// Lossy failure: what remains in `queue` may still arrive; anything
    /// written from now on vanishes.
    pub cut: bool,
    /// The writer has closed its end (an `Eof` is queued).
    pub closed: bool,
    /// The reader has read the `Eof`.
    pub eof_read: bool,
    pub bytes_written: u64,
    pub partial_writes: u64,
}

impl Pipe {
    pub fn new(link: Link) -> Pipe {
        Pipe {
            link,
            queue: VecDeque::new(),
            queued: 0,
            link_free: 0,
            stalled: false,
            cut: false,
            closed: false,
            eof_read: false,
            bytes_written: 0,
            partial_writes: 0,
        }
    }

    /// How many bytes a write would accept now.
    pub fn space(&self) -> usize {
        if self.cut {
            usize::MAX
        } else {
            self.link.capacity.saturating_sub(self.queued)
        }
    }

    fn last_at(&self) -> Tick {
        self.queue.back().map_or(0, |p| p.at)
    }

    /// Writes as much of `data` as the capacity allows; returns how much.
    /// After a cut, everything is accepted and lost.
    pub fn write(&mut self, now: Tick, data: &[u8]) -> usize {
        assert!(!self.closed, "write to a pipe after closing it");
        if self.cut {
            self.bytes_written += data.len() as u64;
            return data.len();
        }
        let n = data.len().min(self.space());
        if n < data.len() {
            self.partial_writes += 1;
        }
        for chunk in data[..n].chunks(self.link.mss) {
            let start = now.max(self.link_free);
            let ser = (chunk.len() as u64 * MS).div_ceil(self.link.bandwidth);
            self.link_free = start + ser;
            let at = (self.link_free + self.link.latency).max(self.last_at());
            self.queue.push_back(Packet {
                at,
                item: Item::Bytes(chunk.to_vec()),
            });
            self.queued += chunk.len();
        }
        self.bytes_written += n as u64;
        n
    }

    /// Sends a handshake marker, arriving `extra` later than it otherwise
    /// would (in order: nothing written after it can overtake it).
    pub fn send_marker(&mut self, now: Tick, item: Item, extra: Tick) {
        assert!(!self.closed, "marker sent on a closed pipe");
        if self.cut {
            return;
        }
        let at = (now.max(self.link_free) + self.link.latency + extra).max(self.last_at());
        self.link_free = self.link_free.max(at - self.link.latency);
        self.queue.push_back(Packet { at, item });
    }

    /// Closes the writer's end gracefully: everything already written is
    /// delivered, followed by EOF.
    pub fn close(&mut self, now: Tick) {
        if self.closed {
            return;
        }
        if !self.cut {
            self.send_marker(now, Item::Eof, 0);
        }
        self.closed = true;
    }

    /// The packet at the head of the queue, if it has arrived and the pipe
    /// is not stalled.
    pub fn ready(&self, now: Tick) -> Option<&Packet> {
        if self.stalled {
            return None;
        }
        self.queue.front().filter(|p| p.at <= now)
    }

    pub fn pop_ready(&mut self, now: Tick) -> Option<Packet> {
        self.ready(now)?;
        let p = self.queue.pop_front().unwrap();
        if let Item::Bytes(b) = &p.item {
            self.queued -= b.len();
        }
        Some(p)
    }

    /// Arrival time of the head packet (if not stalled).
    pub fn next_arrival(&self) -> Option<Tick> {
        if self.stalled {
            return None;
        }
        self.queue.front().map(|p| p.at)
    }

    pub fn head(&self) -> Option<&Packet> {
        self.queue.front()
    }

    /// Lossy failure. Packets arriving by `deliver_until` survive (the last
    /// of them possibly truncated); the rest are lost, as is anything written
    /// later.
    pub fn cut(&mut self, deliver_until: Tick, rng: &mut Rng) {
        self.queue.retain(|p| p.at <= deliver_until);
        if rng.chance(1, 2)
            && let Some(Packet {
                item: Item::Bytes(b),
                ..
            }) = self.queue.back_mut()
        {
            let keep = rng.below(b.len() as u64) as usize;
            b.truncate(keep);
        }
        self.queue
            .retain(|p| !matches!(&p.item, Item::Bytes(b) if b.is_empty()));
        self.queued = self
            .queue
            .iter()
            .map(|p| match &p.item {
                Item::Bytes(b) => b.len(),
                _ => 0,
            })
            .sum();
        self.cut = true;
    }
}

/// Where one side of a channel is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Exchanging the JSON handshake: the server awaits the request; the
    /// client awaits the reply (and then its own swap), and reads nothing
    /// else.
    Handshake,
    /// Installed in that side's session.
    Active,
    /// That side has left the channel.
    Done,
}

/// Checks on the frames written in one direction of a channel.
#[derive(Debug, Default)]
pub struct Tap {
    pub decoder: FrameDecoder,
    pub frames: u64,
    /// Where the previous frame on this channel ended.
    pub next_seq: Option<u64>,
}

#[derive(Debug)]
pub struct Channel {
    pub pipes: [Pipe; 2],
    /// By side (client 0, server 1).
    pub phase: [Phase; 2],
    /// When each side observes a lossy failure (its `ssh` or the shim's
    /// stdio failing), if ever.
    pub fail_at: [Option<Tick>; 2],
    /// Data may have been lost on this channel (cut or killed).
    pub lossy: bool,
    /// Anything abnormal happened: lossy, stalled, or left by one side.
    pub disturbed: bool,
    pub taps: [Tap; 2],
}

impl Channel {
    pub fn new(links: [Link; 2]) -> Channel {
        Channel {
            pipes: links.map(Pipe::new),
            phase: [Phase::Handshake; 2],
            fail_at: [None; 2],
            lossy: false,
            disturbed: false,
            taps: Default::default(),
        }
    }

    /// Whether `side` has observed this channel failing by `now`: an error,
    /// or EOF read from the pipe it reads.
    pub fn failed_for(&self, side: usize, now: Tick) -> bool {
        self.fail_at[side].is_some_and(|t| t <= now) || self.pipes[1 - side].eof_read
    }
}
