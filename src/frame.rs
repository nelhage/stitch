//! The transport frame format (see `doc/framing.md`).
//!
//! A frame is a fixed [`HEADER_LEN`]-byte header followed by `length` bytes of
//! payload. This module encodes and decodes headers, validates the parts of a
//! header which can be checked in isolation (validation rule 1 of
//! `framing.md`), and provides [`FrameDecoder`], which reassembles frames from
//! a byte stream delivered in arbitrary chunks.
//!
//! Everything here is pure; the semantic checks (rules 2–7) live in
//! [`crate::core`].

use thiserror::Error;

/// Size of the fixed frame header, in bytes.
pub const HEADER_LEN: usize = 24;

/// Maximum payload length of a single frame (the `length` field is a `u16`).
pub const MAX_PAYLOAD: usize = u16::MAX as usize;

/// `flags` bit 0: this is the first frame on its channel.
pub const FLAG_FIRST: u8 = 0x01;

/// `flags` bit 1: the sender's stream ends immediately after this frame's
/// payload.
pub const FLAG_SHUTDOWN: u8 = 0x02;

/// All `flags` bits which have a defined meaning.
pub const KNOWN_FLAGS: u8 = FLAG_FIRST | FLAG_SHUTDOWN;

/// A header which violates validation rule 1 of `framing.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FrameError {
    /// A bit other than `FIRST` or `SHUTDOWN` is set in `flags`.
    #[error("unknown flag bits set in frame header (flags = {0:#04x})")]
    UnknownFlags(u8),
    /// The `reserved` byte is not zero.
    #[error("reserved byte in frame header is non-zero ({0:#04x})")]
    ReservedNonZero(u8),
}

/// A decoded frame header.
///
/// `seq` refers to the stream flowing in the same direction as the frame;
/// `ack` and `window` refer to the stream flowing in the opposite direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameHeader {
    /// `FIRST`: this is the first frame on this channel.
    pub first: bool,
    /// `SHUTDOWN`: the sender's stream ends right after this frame's payload;
    /// the shutdown occupies one phantom position at `seq + length`.
    pub shutdown: bool,
    /// Number of payload bytes following the header.
    pub length: u16,
    /// Bytes free in the sender's receive buffer.
    pub window: u32,
    /// Stream offset of the first payload byte (for a zero-length frame, the
    /// sender's current send position).
    pub seq: u64,
    /// The next stream offset the sender expects from its peer.
    pub ack: u64,
}

impl FrameHeader {
    /// The raw `flags` byte for this header.
    pub fn flags(&self) -> u8 {
        (if self.first { FLAG_FIRST } else { 0 }) | (if self.shutdown { FLAG_SHUTDOWN } else { 0 })
    }

    /// Number of stream positions this frame covers: its payload length,
    /// plus one for the phantom shutdown position if `SHUTDOWN` is set.
    pub fn span(&self) -> u64 {
        u64::from(self.length) + u64::from(self.shutdown)
    }

    /// The peer's send limit implied by this frame: `ack + window`
    /// (saturating, so that a hostile header cannot cause an overflow).
    pub fn limit(&self) -> u64 {
        self.ack.saturating_add(u64::from(self.window))
    }

    /// Encodes the header into its 24-byte wire form.
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0] = self.flags();
        b[1] = 0;
        b[2..4].copy_from_slice(&self.length.to_be_bytes());
        b[4..8].copy_from_slice(&self.window.to_be_bytes());
        b[8..16].copy_from_slice(&self.seq.to_be_bytes());
        b[16..24].copy_from_slice(&self.ack.to_be_bytes());
        b
    }

    /// Appends the encoded header to `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.encode());
    }

    /// Decodes a header from exactly [`HEADER_LEN`] bytes, checking
    /// validation rule 1 (no unknown flag bits; reserved byte zero).
    pub fn decode(bytes: &[u8; HEADER_LEN]) -> Result<FrameHeader, FrameError> {
        let flags = bytes[0];
        if flags & !KNOWN_FLAGS != 0 {
            return Err(FrameError::UnknownFlags(flags));
        }
        if bytes[1] != 0 {
            return Err(FrameError::ReservedNonZero(bytes[1]));
        }
        let u64_at = |i: usize| u64::from_be_bytes(bytes[i..i + 8].try_into().unwrap());
        Ok(FrameHeader {
            first: flags & FLAG_FIRST != 0,
            shutdown: flags & FLAG_SHUTDOWN != 0,
            length: u16::from_be_bytes([bytes[2], bytes[3]]),
            window: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            seq: u64_at(8),
            ack: u64_at(16),
        })
    }
}

/// Appends a complete frame (header followed by `payload`) to `out`.
///
/// # Panics
/// If `payload.len()` does not equal `header.length`.
pub fn encode_frame(header: &FrameHeader, payload: &[u8], out: &mut Vec<u8>) {
    assert_eq!(
        payload.len(),
        usize::from(header.length),
        "frame payload length does not match header"
    );
    header.encode_into(out);
    out.extend_from_slice(payload);
}

/// Incrementally reassembles frames from a byte stream.
///
/// Bytes are fed in with [`push`](FrameDecoder::push), in chunks split at any
/// position; complete frames are taken out with
/// [`next_frame`](FrameDecoder::next_frame). A header is validated (rule 1) as
/// soon as all 24 of its bytes are available, before its payload arrives.
///
/// The decoder buffers at most what has been pushed but not yet returned as a
/// frame; callers which drain frames after every push hold at most one
/// partial frame plus one pushed chunk.
#[derive(Debug, Clone, Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
    /// Bytes of `buf` before this index have already been returned.
    start: usize,
}

impl FrameDecoder {
    /// Creates an empty decoder.
    pub fn new() -> FrameDecoder {
        FrameDecoder::default()
    }

    /// Appends bytes read from the stream.
    pub fn push(&mut self, data: &[u8]) {
        self.compact();
        self.buf.extend_from_slice(data);
    }

    /// Returns the next complete frame, if one is buffered: its header and a
    /// slice holding exactly `header.length` payload bytes.
    ///
    /// Returns `Ok(None)` if more bytes are needed. An error means the
    /// buffered header is invalid; the stream cannot be decoded further, and
    /// subsequent calls return the same error.
    pub fn next_frame(&mut self) -> Result<Option<(FrameHeader, &[u8])>, FrameError> {
        let avail = &self.buf[self.start..];
        let Some(hdr_bytes) = avail.first_chunk::<HEADER_LEN>() else {
            return Ok(None);
        };
        let header = FrameHeader::decode(hdr_bytes)?;
        let total = HEADER_LEN + usize::from(header.length);
        if avail.len() < total {
            return Ok(None);
        }
        let begin = self.start + HEADER_LEN;
        self.start += total;
        Ok(Some((header, &self.buf[begin..self.start])))
    }

    /// Number of bytes buffered which have not yet been returned in a frame
    /// (i.e. the size of the partial frame, if any).
    pub fn buffered(&self) -> usize {
        self.buf.len() - self.start
    }

    /// Discards all buffered bytes, including any partial frame.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.start = 0;
    }

    fn compact(&mut self) {
        if self.start == self.buf.len() {
            self.buf.clear();
        } else if self.start > 0 {
            self.buf.drain(..self.start);
        }
        self.start = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn header_strategy() -> impl Strategy<Value = FrameHeader> {
        (
            any::<bool>(),
            any::<bool>(),
            any::<u16>(),
            any::<u32>(),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(|(first, shutdown, length, window, seq, ack)| FrameHeader {
                first,
                shutdown,
                length,
                window,
                seq,
                ack,
            })
    }

    #[test]
    fn encode_layout() {
        let h = FrameHeader {
            first: true,
            shutdown: true,
            length: 0x0102,
            window: 0x0304_0506,
            seq: 0x0708_090a_0b0c_0d0e,
            ack: 0x1112_1314_1516_1718,
        };
        let b = h.encode();
        assert_eq!(
            b,
            [
                0x03, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                0x0d, 0x0e, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18
            ]
        );
        assert_eq!(FrameHeader::decode(&b), Ok(h));
        assert_eq!(h.span(), 0x0103);
    }

    #[test]
    fn rejects_unknown_flags_and_reserved() {
        let mut b = FrameHeader::default().encode();
        b[0] = 0x04;
        assert_eq!(FrameHeader::decode(&b), Err(FrameError::UnknownFlags(0x04)));
        b[0] = 0x80 | FLAG_FIRST;
        assert_eq!(FrameHeader::decode(&b), Err(FrameError::UnknownFlags(0x81)));
        b[0] = 0;
        b[1] = 1;
        assert_eq!(FrameHeader::decode(&b), Err(FrameError::ReservedNonZero(1)));
    }

    #[test]
    fn decoder_validates_header_before_payload() {
        let mut d = FrameDecoder::new();
        let mut b = FrameHeader {
            length: 100,
            ..Default::default()
        }
        .encode();
        b[1] = 7;
        d.push(&b);
        assert_eq!(d.next_frame(), Err(FrameError::ReservedNonZero(7)));
        // Sticky.
        assert_eq!(d.next_frame(), Err(FrameError::ReservedNonZero(7)));
    }

    #[test]
    fn decoder_byte_at_a_time() {
        let h = FrameHeader {
            length: 3,
            seq: 9,
            ..Default::default()
        };
        let mut wire = Vec::new();
        encode_frame(&h, b"abc", &mut wire);
        encode_frame(&FrameHeader::default(), b"", &mut wire);
        let mut d = FrameDecoder::new();
        let mut got = Vec::new();
        for b in &wire {
            d.push(std::slice::from_ref(b));
            while let Some((h, p)) = d.next_frame().unwrap() {
                got.push((h, p.to_vec()));
            }
        }
        assert_eq!(
            got,
            vec![(h, b"abc".to_vec()), (FrameHeader::default(), vec![])]
        );
        assert_eq!(d.buffered(), 0);
    }

    #[test]
    fn decoder_clear_discards_partial() {
        let mut d = FrameDecoder::new();
        d.push(&[0u8; 10]);
        assert_eq!(d.buffered(), 10);
        d.clear();
        assert_eq!(d.buffered(), 0);
        let mut wire = Vec::new();
        encode_frame(&FrameHeader::default(), b"", &mut wire);
        d.push(&wire);
        assert_eq!(d.next_frame().unwrap().unwrap().0, FrameHeader::default());
    }

    proptest! {
        #[test]
        fn header_round_trip(h in header_strategy()) {
            prop_assert_eq!(FrameHeader::decode(&h.encode()), Ok(h));
        }

        #[test]
        fn decode_arbitrary_header_never_panics(b in any::<[u8; HEADER_LEN]>()) {
            let r = FrameHeader::decode(&b);
            if let Ok(h) = r {
                prop_assert_eq!(h.encode(), b);
            }
        }

        #[test]
        fn decoder_arbitrary_bytes_never_panic(
            chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..200), 0..20)
        ) {
            let mut d = FrameDecoder::new();
            for c in &chunks {
                d.push(c);
                while let Ok(Some((h, p))) = d.next_frame() {
                    prop_assert_eq!(p.len(), usize::from(h.length));
                }
            }
        }

        #[test]
        fn decoder_reassembles_any_split(
            frames in prop::collection::vec(
                (header_strategy(), prop::collection::vec(any::<u8>(), 0..300)), 0..8),
            cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..10),
        ) {
            let frames: Vec<(FrameHeader, Vec<u8>)> = frames
                .into_iter()
                .map(|(mut h, p)| { h.length = p.len() as u16; (h, p) })
                .collect();
            let mut wire = Vec::new();
            for (h, p) in &frames {
                encode_frame(h, p, &mut wire);
            }
            let mut cuts: Vec<usize> = cuts.iter().map(|i| i.index(wire.len() + 1)).collect();
            cuts.push(0);
            cuts.push(wire.len());
            cuts.sort();
            let mut d = FrameDecoder::new();
            let mut got = Vec::new();
            for w in cuts.windows(2) {
                d.push(&wire[w[0]..w[1]]);
                while let Some((h, p)) = d.next_frame().unwrap() {
                    got.push((h, p.to_vec()));
                }
            }
            prop_assert_eq!(got, frames);
            prop_assert_eq!(d.buffered(), 0);
        }
    }
}
