//! Protocol errors detected by the core.

use thiserror::Error;

use crate::frame::FrameError;

/// A violation of the transport protocol by the peer (see "Validation" in
/// `doc/framing.md`). Any protocol error is fatal to the stream: once one has
/// been returned, the [`Session`](super::Session) is dead.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    /// Rule 1: the frame header has unknown flag bits or a non-zero reserved
    /// byte.
    #[error("malformed frame header: {0}")]
    Header(#[from] FrameError),

    /// Rule 2: the first frame on a channel did not have `FIRST` set.
    #[error("first frame on channel does not have the FIRST flag set")]
    MissingFirst,

    /// Rule 2: a frame other than the first on a channel had `FIRST` set.
    #[error("FIRST flag set on frame {index} of the channel (only frame 0 may have it)")]
    UnexpectedFirst {
        /// Zero-based index of the offending frame on its channel.
        index: u64,
    },

    /// Rule 3: a frame starts beyond the receive high-water mark.
    #[error("gap in stream: frame starts at offset {seq}, beyond high-water mark {hwm}")]
    Gap {
        /// The frame's `seq`.
        seq: u64,
        /// The receiver's high-water mark.
        hwm: u64,
    },

    /// Rule 3: once the channel has resynchronized, a frame starts below the
    /// receive high-water mark (duplicate or out-of-order traffic).
    #[error(
        "frame starts at offset {seq}, below high-water mark {hwm}, after the channel resynchronized"
    )]
    BehindHighWaterMark {
        /// The frame's `seq`.
        seq: u64,
        /// The receiver's high-water mark.
        hwm: u64,
    },

    /// Rule 4: a frame covers a position beyond a shutdown already received,
    /// or carries data at the shutdown's position.
    #[error("frame [{seq}, {end}) extends beyond the peer's shutdown at offset {shutdown}")]
    BeyondShutdown {
        /// The frame's `seq`.
        seq: u64,
        /// The end of the frame's data (`seq + length`).
        end: u64,
        /// Position of the shutdown already received.
        shutdown: u64,
    },

    /// A frame places a shutdown at a position which contradicts what has
    /// already been received: before data already received, or at a
    /// different position from a shutdown already received.
    #[error("frame places a shutdown at offset {position}, contradicting data already received")]
    ConflictingShutdown {
        /// Position of the shutdown the frame claims.
        position: u64,
    },

    /// Rule 5: the new data in a frame does not fit in the receive buffer.
    #[error("receive buffer overflow: frame carries {len} new bytes but only {free} are free")]
    ReceiveOverflow {
        /// Number of new (non-duplicate) data bytes in the frame.
        len: u64,
        /// Free space in the receive buffer.
        free: u64,
    },

    /// Rule 6: a frame acknowledges a position we have never sent.
    #[error("acknowledgement {ack} is beyond the highest offset sent ({sent})")]
    AckBeyondSent {
        /// The frame's `ack`.
        ack: u64,
        /// The highest offset we have ever sent.
        sent: u64,
    },

    /// Rule 6: a frame's acknowledgement is below one previously received.
    #[error("acknowledgement went backwards from {previous} to {ack}")]
    AckWentBackwards {
        /// The frame's `ack`.
        ack: u64,
        /// The highest acknowledgement previously received.
        previous: u64,
    },

    /// Rule 7: a frame would decrease our send limit.
    #[error("send limit decreased from {previous} to {limit}")]
    LimitDecreased {
        /// `ack + window` from the frame.
        limit: u64,
        /// Our current send limit.
        previous: u64,
    },
}
