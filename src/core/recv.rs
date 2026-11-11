//! The receiving half of a session: the peer's stream, flowing to us.

use std::collections::VecDeque;

use super::ProtocolError;
use crate::frame::FrameHeader;

/// What happened when a validated frame was accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Accepted {
    /// The frame reached the high-water mark as it was before the frame
    /// (i.e. the channel is now resynchronized).
    pub reached_hwm: bool,
    /// The high-water mark advanced (new data or a new shutdown).
    pub advanced: bool,
}

#[derive(Debug)]
pub(crate) struct RecvSide {
    /// Received data not yet delivered to the local application.
    buf: VecDeque<u8>,
    capacity: usize,
    /// Next position expected from the peer: everything below it has been
    /// received. Includes the phantom shutdown position once received.
    hwm: u64,
    /// Data delivered to the application (or discarded after the
    /// application closed its read side).
    delivered: u64,
    /// Position of the peer's shutdown, once received.
    shutdown: Option<u64>,
    /// The local application no longer accepts data; incoming data is
    /// discarded.
    read_closed: bool,
}

impl RecvSide {
    pub fn new(capacity: usize) -> RecvSide {
        RecvSide {
            buf: VecDeque::new(),
            capacity,
            hwm: 0,
            delivered: 0,
            shutdown: None,
            read_closed: false,
        }
    }

    pub fn hwm(&self) -> u64 {
        self.hwm
    }

    pub fn delivered(&self) -> u64 {
        self.delivered
    }

    pub fn shutdown(&self) -> Option<u64> {
        self.shutdown
    }

    pub fn read_closed(&self) -> bool {
        self.read_closed
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn free(&self) -> usize {
        self.capacity - self.buf.len()
    }

    /// The window to advertise: free space in the receive buffer.
    pub fn window(&self) -> u32 {
        // Config::validate ensures the capacity fits.
        self.free() as u32
    }

    /// The peer's send limit we would advertise now: `ack + window`.
    /// Never decreases.
    pub fn limit(&self) -> u64 {
        self.hwm + self.free() as u64
    }

    /// Data awaiting delivery to the application (a prefix of it, if the
    /// buffer wraps).
    pub fn data(&self) -> &[u8] {
        self.buf.as_slices().0
    }

    pub fn consume(&mut self, n: usize) {
        assert!(n <= self.buf.len(), "consume_app beyond available data");
        self.buf.drain(..n);
        self.delivered += n as u64;
    }

    /// The peer has shut down and everything before it has been delivered.
    pub fn eof(&self) -> bool {
        self.shutdown.is_some() && self.buf.is_empty()
    }

    pub fn close_read(&mut self) {
        self.read_closed = true;
        let n = self.buf.len();
        self.consume(n);
    }

    /// Checks rules 3, 4 and 5 for a received frame.
    pub fn validate(&self, h: &FrameHeader, resynced: bool) -> Result<(), ProtocolError> {
        let hwm = self.hwm;
        if h.seq > hwm {
            return Err(ProtocolError::Gap { seq: h.seq, hwm });
        }
        if resynced && h.seq < hwm {
            return Err(ProtocolError::BehindHighWaterMark { seq: h.seq, hwm });
        }
        // `seq <= hwm`, so this cannot overflow for any reachable `hwm`.
        let end = h.seq + u64::from(h.length);
        match self.shutdown {
            Some(shutdown) => {
                if h.length > 0 && end > shutdown {
                    return Err(ProtocolError::BeyondShutdown {
                        seq: h.seq,
                        end,
                        shutdown,
                    });
                }
                if h.shutdown && end != shutdown {
                    return Err(ProtocolError::ConflictingShutdown { position: end });
                }
            }
            None => {
                if h.shutdown && end < hwm {
                    return Err(ProtocolError::ConflictingShutdown { position: end });
                }
            }
        }
        let new = end.saturating_sub(hwm);
        let free = self.free() as u64;
        if new > free {
            return Err(ProtocolError::ReceiveOverflow { len: new, free });
        }
        Ok(())
    }

    /// Accepts a validated frame, keeping only the part of the payload at or
    /// beyond the high-water mark.
    pub fn accept(&mut self, h: &FrameHeader, payload: &[u8]) -> Accepted {
        let old = self.hwm;
        let end = h.seq + u64::from(h.length);
        let reached_hwm = h.seq + h.span() >= old;
        if end > old {
            let fresh = &payload[(old - h.seq) as usize..];
            if self.read_closed {
                self.delivered += fresh.len() as u64;
            } else {
                self.buf.extend(fresh);
            }
            self.hwm = end;
        }
        if h.shutdown && self.shutdown.is_none() {
            debug_assert_eq!(self.hwm, end);
            self.shutdown = Some(end);
            self.hwm = end + 1;
        }
        Accepted {
            reached_hwm,
            advanced: self.hwm > old,
        }
    }
}
