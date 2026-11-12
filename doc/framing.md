This document specifies the wire format of the `stitch` transport protocol: the frames which the two proxies exchange over an `ssh` channel once the JSON setup handshake has completed. See `stitch.md` for the overall design, and in particular for the semantics of acknowledgement, resumption, flow control, and shutdown, which this document only summarizes.

# Frame layout
There is a single frame type, consisting of a fixed 24-byte header followed by a payload. All integers are big-endian.

```
 0               1               2               3
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+---------------+---------------+-------------------------------+
|     flags     |   reserved    |            length             |
+---------------+---------------+-------------------------------+
|                            window                             |
+---------------------------------------------------------------+
|                                                               |
+                              seq                              +
|                                                               |
+---------------------------------------------------------------+
|                                                               |
+                              ack                              +
|                                                               |
+---------------------------------------------------------------+
|                     payload (length bytes)                    |
+---------------------------------------------------------------+
```

| Field | Size | Meaning |
|---|---|---|
| `flags` | u8 | Bit 0, `FIRST`: this is the first frame on this channel. Bit 1, `SHUTDOWN`: the sender's stream ends immediately after this frame's payload. All other bits must be zero. |
| `reserved` | u8 | Must be zero. |
| `length` | u16 | Number of payload bytes, 0–65535. A zero-length frame serves as a heartbeat, or as a pure acknowledgement or window update. |
| `window` | u32 | Number of bytes free in the sender's receive buffer. The peer's send limit is `ack + window`. |
| `seq` | u64 | Stream offset of the first payload byte. For a zero-length frame, the sender's current send position. |
| `ack` | u64 | The next stream offset the sender expects from its peer; everything below it has been received. |

`seq` refers to the stream flowing in the same direction as the frame; `ack` and `window` refer to the stream flowing in the opposite direction.

# Sequence space
Each direction of the stream is numbered from offset zero. A frame covers `length + (SHUTDOWN ? 1 : 0)` positions, starting at `seq`: the shutdown occupies one phantom position following the final byte of data.

Treating the shutdown as part of the span of the frame means that it needs no special handling elsewhere:
- `SHUTDOWN` may be set on the final data frame, or on a zero-length frame.
- A shutdown is acknowledged like data: if the stream carried `N` bytes, then an `ack` of `N + 1` acknowledges the shutdown.
- After sending a shutdown, the sender's position is `N + 1`. Any later frames (heartbeats and acknowledgements) have `seq = N + 1`, a zero length, and do not set `SHUTDOWN`.
- If a shutdown is unacknowledged at a channel swap, it is rewound and re-sent like any other unacknowledged position. A receiver which has already seen it discards it as an ordinary duplicate below its high-water mark.

The shutdown does not occupy space in the receive buffer, and is not subject to the send limit.

# Validation
A receiver checks each frame as follows. Any failure is a protocol error, and closes the stream.

1. No unknown bits are set in `flags`, and `reserved` is zero.
2. `FIRST` is set if, and only if, this is the first frame read from the channel.
3. `seq` is not above the receiver's high-water mark (a gap). Once the channel has resynchronized -- that is, once a frame on it has reached the high-water mark -- `seq` is also not below the high-water mark.
4. The frame does not cover any position beyond a shutdown which the receiver has already received.
5. After discarding any portion of the payload below the high-water mark, the remainder fits in the free space of the receive buffer.
6. `ack` is not above the highest offset the receiver has ever sent, and is not below any `ack` previously received.
7. `ack + window` is not below the receiver's current send limit.

# Channel start
Frames begin immediately after the JSON handshake, in each direction. The server's first frame directly follows its JSON success reply; the client sends its first frame once it has read that reply. This applies equally to the initial channel and to each resumed channel.

Neither end may send data until it has learned its send limit from the peer's first frame, so the first frame from each end at the start of a session is necessarily zero-length.

# Example
A client starts a session and sends the five bytes `hello`. Each end has a 4MiB receive buffer.

```
C→S  flags=FIRST len=0 window=4194304 seq=0 ack=0
S→C  flags=FIRST len=0 window=4194304 seq=0 ack=0
C→S  flags=0     len=5 window=4194304 seq=0 ack=0  "hello"
S→C  flags=0     len=0 window=4194299 seq=0 ack=5
```

The final frame acknowledges the data. Its window is five bytes smaller because the server's application has not yet read the data; once it does, the window returns to 4194304, although the server will not send a frame solely to announce so small a change.

# Design notes
- **Fixed-size header.** A variable-length encoding would save a few bytes per frame, but 24 bytes is small next to the overhead of the `ssh` packets on either side of it, and a fixed header keeps both the read loop and the tests simple.
- **`seq` on every frame.** It is strictly required only on the first frame of a channel, since frames within a channel arrive in order. Carrying it on every frame gives a continuous consistency check.
- **64-bit offsets.** Sessions may be very long-lived; 64-bit offsets mean we never need to consider wraparound.
- **No version, magic number, or checksum.** Version negotiation belongs to the JSON handshake, which must also gate the use of any new flag bits. Today that is a single version, which every receiver of a handshake message checks, refusing any other before a frame is exchanged (see "Setup handshake" in `implementation.md`); a frame format change must come with a new handshake version. As for checksums, `ssh` already guarantees the integrity of the channel.
- **16-bit length.** This fixes the maximum payload at 65535 bytes, which ensures that a fresh `ack` and `window` are never delayed behind a single very large frame.
- **32-bit window.** This limits the receive buffer to just under 4GiB, far more than is useful.
