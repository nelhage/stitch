//! The sending half of a session: our outgoing stream.
//!
//! Stream positions are numbered from zero. Data occupies positions
//! `[0, data_end)`; once the local application has closed its side, the
//! shutdown occupies one further phantom position, `data_end`.

use std::collections::VecDeque;

use super::ProtocolError;

/// One chunk of the outgoing stream, ready to be put in a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Chunk {
    pub seq: u64,
    pub len: usize,
    pub shutdown: bool,
}

#[derive(Debug)]
pub(crate) struct SendSide {
    /// Data from `base` to `data_end`: unacknowledged, whether sent or not.
    buf: VecDeque<u8>,
    capacity: usize,
    /// Stream offset of `buf[0]`; equal to `min(peer_ack, data_end)`.
    base: u64,
    /// Next position to put in a frame.
    pos: u64,
    /// Highest position ever put in a frame, plus one.
    max_sent: u64,
    /// Highest acknowledgement received: the first unacknowledged position.
    peer_ack: u64,
    /// We may send data only at offsets below this.
    limit: u64,
    /// The local application has closed; the shutdown is at `data_end`.
    closed: bool,
}

impl SendSide {
    pub fn new(capacity: usize) -> SendSide {
        SendSide {
            buf: VecDeque::new(),
            capacity,
            base: 0,
            pos: 0,
            max_sent: 0,
            peer_ack: 0,
            limit: 0,
            closed: false,
        }
    }

    pub fn data_end(&self) -> u64 {
        self.base + self.buf.len() as u64
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn max_sent(&self) -> u64 {
        self.max_sent
    }

    pub fn peer_ack(&self) -> u64 {
        self.peer_ack
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn closed(&self) -> bool {
        self.closed
    }

    pub fn space(&self) -> usize {
        if self.closed {
            0
        } else {
            self.capacity - self.buf.len()
        }
    }

    pub fn write(&mut self, data: &[u8]) -> usize {
        assert!(!self.closed, "write_app called after close_app_send");
        let n = data.len().min(self.space());
        self.buf.extend(&data[..n]);
        n
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Our shutdown has been sent (at least once).
    pub fn shutdown_sent(&self) -> bool {
        self.closed && self.max_sent > self.data_end()
    }

    /// Our shutdown has been acknowledged by the peer.
    pub fn shutdown_acked(&self) -> bool {
        self.closed && self.peer_ack > self.data_end()
    }

    /// Moves the send position back to the first unacknowledged position.
    pub fn rewind(&mut self) {
        self.pos = self.peer_ack;
    }

    /// Checks rules 6 and 7 for a received frame.
    pub fn validate_ack(&self, ack: u64, limit: u64) -> Result<(), ProtocolError> {
        if ack > self.max_sent {
            return Err(ProtocolError::AckBeyondSent {
                ack,
                sent: self.max_sent,
            });
        }
        if ack < self.peer_ack {
            return Err(ProtocolError::AckWentBackwards {
                ack,
                previous: self.peer_ack,
            });
        }
        if limit < self.limit {
            return Err(ProtocolError::LimitDecreased {
                limit,
                previous: self.limit,
            });
        }
        Ok(())
    }

    /// Applies a validated acknowledgement and send limit.
    pub fn apply_ack(&mut self, ack: u64, limit: u64) {
        self.peer_ack = ack;
        self.limit = limit;
        let new_base = ack.min(self.data_end());
        let drop = (new_base - self.base) as usize;
        self.buf.drain(..drop);
        self.base = new_base;
        // After a rewind, the peer may acknowledge data we were about to
        // re-send (it received it on the old channel); skip over it. We no
        // longer hold it in any case.
        self.pos = self.pos.max(ack);
    }

    /// The next chunk we may send, if any: data at the send position below
    /// the send limit, or the shutdown once all data before it is sent.
    pub fn next_chunk(&self, max_payload: usize) -> Option<Chunk> {
        let data_end = self.data_end();
        if self.pos < data_end {
            let len = (data_end - self.pos)
                .min(self.limit.saturating_sub(self.pos))
                .min(max_payload as u64) as usize;
            if len == 0 {
                return None;
            }
            Some(Chunk {
                seq: self.pos,
                len,
                shutdown: self.closed && self.pos + len as u64 == data_end,
            })
        } else if self.closed && self.pos == data_end {
            Some(Chunk {
                seq: self.pos,
                len: 0,
                shutdown: true,
            })
        } else {
            None
        }
    }

    /// Appends the data of `chunk` to `out`.
    pub fn copy_chunk(&self, chunk: Chunk, out: &mut Vec<u8>) {
        if chunk.len == 0 {
            // `seq` may be past the shutdown, beyond the buffer.
            return;
        }
        let start = (chunk.seq - self.base) as usize;
        out.extend(self.buf.range(start..start + chunk.len));
    }

    /// Records that a frame covering `span` positions from the send position
    /// has been produced.
    pub fn advance(&mut self, span: u64) {
        self.pos += span;
        self.max_sent = self.max_sent.max(self.pos);
    }
}
