//! Tunable parameters of a [`Session`](super::Session).

use std::time::Duration;

use crate::frame::MAX_PAYLOAD;

/// Configuration of one end of a stream. None of these need to agree between
/// the two ends; each end simply advertises what it has (although the peer's
/// timeout should comfortably exceed our heartbeat interval).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Capacity of the send buffer, in bytes: data accepted from the local
    /// application which has not yet been acknowledged by the peer (whether
    /// or not it has been sent). When it is full, the driver should stop
    /// reading from the local application.
    pub send_buffer: usize,
    /// Capacity of the receive buffer, in bytes: data received from the peer
    /// which the local application has not yet accepted. This is what we
    /// advertise as our window; it must fit in a `u32`.
    pub recv_buffer: usize,
    /// Largest payload we put in a single frame; 1 to
    /// [`MAX_PAYLOAD`](crate::frame::MAX_PAYLOAD).
    pub max_payload: usize,
    /// Send a (possibly zero-length) frame if none has been sent on the
    /// current channel for this long.
    pub heartbeat_interval: Duration,
    /// Report the transport as timed out if nothing has been received on the
    /// current channel for this long, or if pending output has made no
    /// progress for this long.
    pub timeout: Duration,
    /// Send a window update once the send limit we would advertise exceeds
    /// the one last advertised by at least this many bytes.
    pub window_update_threshold: usize,
    /// Send an acknowledgement once at least this many received bytes are
    /// unacknowledged.
    pub ack_threshold: usize,
    /// Send an acknowledgement once received data has been unacknowledged
    /// for this long.
    pub ack_delay: Duration,
}

impl Config {
    /// Default send buffer capacity: 16 MiB.
    pub const DEFAULT_SEND_BUFFER: usize = 16 << 20;
    /// Default receive buffer capacity: 4 MiB.
    pub const DEFAULT_RECV_BUFFER: usize = 4 << 20;
    /// Default heartbeat interval: 10 s.
    pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
    /// Default transport timeout: 20 s.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
    /// Default ack threshold: 256 KiB.
    pub const DEFAULT_ACK_THRESHOLD: usize = 256 << 10;
    /// Default ack delay: 200 ms.
    pub const DEFAULT_ACK_DELAY: Duration = Duration::from_millis(200);

    /// Returns the default configuration, but with the given receive buffer
    /// capacity and a window update threshold of a quarter of it.
    pub fn with_recv_buffer(recv_buffer: usize) -> Config {
        Config {
            recv_buffer,
            window_update_threshold: recv_buffer / 4,
            ..Config::default()
        }
    }

    /// Panics if the configuration is unusable.
    pub(crate) fn validate(&self) {
        assert!(
            u32::try_from(self.recv_buffer).is_ok(),
            "recv_buffer must fit in a u32"
        );
        assert!(
            (1..=MAX_PAYLOAD).contains(&self.max_payload),
            "max_payload must be between 1 and {MAX_PAYLOAD}"
        );
    }
}

impl Default for Config {
    /// The defaults from `doc/implementation.md`.
    fn default() -> Config {
        Config {
            send_buffer: Config::DEFAULT_SEND_BUFFER,
            recv_buffer: Config::DEFAULT_RECV_BUFFER,
            max_payload: MAX_PAYLOAD,
            heartbeat_interval: Config::DEFAULT_HEARTBEAT_INTERVAL,
            timeout: Config::DEFAULT_TIMEOUT,
            window_update_threshold: Config::DEFAULT_RECV_BUFFER / 4,
            ack_threshold: Config::DEFAULT_ACK_THRESHOLD,
            ack_delay: Config::DEFAULT_ACK_DELAY,
        }
    }
}
