//! The pure, IO-free protocol state machine for one end of a stream.
//!
//! See "The protocol core" in `doc/implementation.md`, and `doc/stitch.md`
//! and `doc/framing.md` for the semantics implemented here.
//!
//! A [`Session`] never performs IO and never reads the clock; every method
//! which depends on time takes `now: Instant`. A driver moves bytes between
//! the session and real file descriptors:
//!
//! - bytes read from the local application go in with
//!   [`write_app`](Session::write_app) (up to [`send_space`](Session::send_space));
//!   EOF from it is reported with [`close_app_send`](Session::close_app_send);
//! - bytes for the local application come out of
//!   [`app_data`](Session::app_data) / [`consume_app`](Session::consume_app);
//!   once [`app_eof`](Session::app_eof) is true, the driver closes the
//!   application's write side;
//! - bytes read from the channel go in with [`receive`](Session::receive);
//! - bytes for the channel come out of
//!   [`transmit_data`](Session::transmit_data) /
//!   [`consume_transmit`](Session::consume_transmit);
//! - the driver sleeps until [`poll_timeout`](Session::poll_timeout) and
//!   then calls [`handle_timeout`](Session::handle_timeout).
//!
//! After anything changes the session's state (any of the calls above), the
//! driver should check `transmit_data` again: frames are produced lazily, one
//! at a time, when the channel can take them.

mod config;
mod error;
mod recv;
mod send;
#[cfg(test)]
mod tests;

use std::time::Instant;

pub use config::Config;
pub use error::ProtocolError;

use crate::frame::{FrameDecoder, FrameHeader};
use recv::RecvSide;
use send::{Chunk, SendSide};

/// Per-channel state, discarded on every [`Session::swap_channel`].
#[derive(Debug)]
struct Channel {
    /// Partially received input.
    decoder: FrameDecoder,
    /// Number of frames received on this channel.
    frames_received: u64,
    /// Whether our `FIRST` frame has been produced on this channel.
    first_sent: bool,
    /// Whether a frame on this channel has reached the receive high-water
    /// mark; from then on frames must start exactly at it.
    resynced: bool,
    /// When bytes were last received (or the channel was installed).
    last_recv: Instant,
    /// When a frame was last produced on this channel (or it was installed).
    last_frame_at: Instant,
    /// When pending output last made progress (or was produced).
    tx_progress: Instant,
}

impl Channel {
    fn new(now: Instant) -> Channel {
        Channel {
            decoder: FrameDecoder::new(),
            frames_received: 0,
            first_sent: false,
            resynced: false,
            last_recv: now,
            last_frame_at: now,
            tx_progress: now,
        }
    }
}

/// One end of a `stitch` stream: the protocol state machine.
///
/// The session is symmetric; it has no notion of client or server, or of
/// resumption. Channels (including the very first) are installed with
/// [`swap_channel`](Session::swap_channel). See the [module
/// documentation](self) for how a driver uses it.
#[derive(Debug)]
pub struct Session {
    config: Config,
    send: SendSide,
    recv: RecvSide,
    channel: Option<Channel>,
    /// Encoded output not yet written to the channel: at most one frame.
    tx: Vec<u8>,
    /// Bytes of `tx` already written.
    tx_off: usize,
    /// `ack` carried by the most recently produced frame.
    ack_sent: u64,
    /// `ack + window` carried by the most recently produced frame.
    advertised_limit: u64,
    /// When received data first became unacknowledged, if it is.
    unacked_since: Option<Instant>,
    /// Latched by `handle_timeout`; see `transport_timed_out`.
    timed_out: bool,
    /// Set once a protocol error has occurred; the session is then dead.
    error: Option<ProtocolError>,
    frames_sent: u64,
    frames_received: u64,
}

impl Session {
    /// Creates a session with no channel installed.
    ///
    /// # Panics
    /// If `config.recv_buffer` does not fit in a `u32`, or
    /// `config.max_payload` is not between 1 and
    /// [`MAX_PAYLOAD`](crate::frame::MAX_PAYLOAD).
    pub fn new(config: Config) -> Session {
        config.validate();
        Session {
            send: SendSide::new(config.send_buffer),
            recv: RecvSide::new(config.recv_buffer),
            config,
            channel: None,
            tx: Vec::new(),
            tx_off: 0,
            ack_sent: 0,
            advertised_limit: 0,
            unacked_since: None,
            timed_out: false,
            error: None,
            frames_sent: 0,
            frames_received: 0,
        }
    }

    /// The session's configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    // ----------------------------------------------------------------
    // Application side
    // ----------------------------------------------------------------

    /// How many more bytes [`write_app`](Session::write_app) would accept
    /// now. Zero once the send buffer is full (the driver should stop reading
    /// from the application), after
    /// [`close_app_send`](Session::close_app_send), or after a protocol
    /// error.
    pub fn send_space(&self) -> usize {
        if self.error.is_some() {
            0
        } else {
            self.send.space()
        }
    }

    /// Accepts bytes read from the local application into the send buffer,
    /// returning how many were accepted (at most
    /// [`send_space`](Session::send_space)).
    ///
    /// # Panics
    /// If called after [`close_app_send`](Session::close_app_send) with a
    /// non-empty `data`.
    pub fn write_app(&mut self, data: &[u8]) -> usize {
        if self.error.is_some() || data.is_empty() {
            return 0;
        }
        self.send.write(data)
    }

    /// The local application has reached EOF: queue our `SHUTDOWN` after all
    /// data written so far. Idempotent.
    pub fn close_app_send(&mut self) {
        self.send.close();
    }

    /// Data received from the peer awaiting delivery to the local
    /// application. This may be only a prefix of the pending data; it is
    /// non-empty whenever any data is pending.
    pub fn app_data(&self) -> &[u8] {
        self.recv.data()
    }

    /// Marks the first `n` bytes of [`app_data`](Session::app_data) as
    /// delivered to the local application, freeing receive buffer space.
    ///
    /// # Panics
    /// If `n` exceeds the pending data.
    pub fn consume_app(&mut self, n: usize) {
        self.recv.consume(n);
    }

    /// The peer's shutdown has been received and all data before it has been
    /// delivered: the driver should close the local application's write
    /// side.
    pub fn app_eof(&self) -> bool {
        self.recv.eof()
    }

    /// The local application can no longer accept data (e.g. writing to it
    /// failed with `EPIPE`). Pending data is discarded, and from now on
    /// incoming data is discarded on arrival while still being acknowledged,
    /// and a full window is advertised (see "Local application closes its
    /// read side" in `stitch.md`). Idempotent.
    pub fn close_app_read(&mut self) {
        self.recv.close_read();
    }

    // ----------------------------------------------------------------
    // Transport side
    // ----------------------------------------------------------------

    /// Installs a new channel, replacing the current one if any; this is also
    /// how the initial channel is installed.
    ///
    /// Any partially received frame and any output not yet written are
    /// discarded; the send position is rewound to the first unacknowledged
    /// position (including an unacknowledged shutdown); and a `FIRST` frame
    /// is queued for the new channel. Nothing is written by this call itself.
    /// The receive and transmit timeouts restart from `now`.
    ///
    /// Does nothing after a protocol error.
    pub fn swap_channel(&mut self, now: Instant) {
        if self.error.is_some() {
            return;
        }
        self.tx.clear();
        self.tx_off = 0;
        self.send.rewind();
        self.channel = Some(Channel::new(now));
        self.timed_out = false;
    }

    /// Feeds bytes read from the current channel. They may split frames at
    /// any position. Every complete frame is validated and processed.
    ///
    /// On a protocol error the session becomes dead, and this and every later
    /// call to `receive` returns the error.
    ///
    /// # Panics
    /// If no channel has been installed.
    pub fn receive(&mut self, data: &[u8], now: Instant) -> Result<(), ProtocolError> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        let ch = self
            .channel
            .as_mut()
            .expect("Session::receive called before swap_channel");
        if data.is_empty() {
            return Ok(());
        }
        ch.last_recv = now;
        ch.decoder.push(data);
        let mut decoder = std::mem::take(&mut ch.decoder);
        let result = self.process_frames(&mut decoder, now);
        if let Some(ch) = self.channel.as_mut() {
            ch.decoder = decoder;
        }
        if let Err(e) = &result {
            self.error = Some(e.clone());
            self.tx.clear();
            self.tx_off = 0;
        } else if self.timed_out {
            self.timed_out = self.timeout_condition(now);
        }
        result
    }

    /// Output to write to the current channel. Empty if there is nothing to
    /// send (or no channel, or after a protocol error).
    ///
    /// If no output is pending and a frame should be sent (see "Frame-sending
    /// policy" in `doc/implementation.md`), one frame is encoded first;
    /// output is thus produced lazily, about one frame at a time, so that
    /// little is discarded on a channel swap. After writing some of it,
    /// report how much with [`consume_transmit`](Session::consume_transmit).
    pub fn transmit_data(&mut self, now: Instant) -> &[u8] {
        if self.error.is_none() && self.tx_pending() == 0 && self.frame_wanted(now) {
            self.encode_frame(now);
        }
        &self.tx[self.tx_off..]
    }

    /// Records that the first `n` bytes of
    /// [`transmit_data`](Session::transmit_data) have been written to the
    /// channel.
    ///
    /// # Panics
    /// If `n` exceeds the pending output.
    pub fn consume_transmit(&mut self, n: usize, now: Instant) {
        assert!(
            n <= self.tx_pending(),
            "consume_transmit beyond pending output"
        );
        if n == 0 {
            return;
        }
        self.tx_off += n;
        if self.tx_off == self.tx.len() {
            self.tx.clear();
            self.tx_off = 0;
        }
        if let Some(ch) = self.channel.as_mut() {
            ch.tx_progress = now;
        }
        if self.timed_out {
            self.timed_out = self.timeout_condition(now);
        }
    }

    // ----------------------------------------------------------------
    // Time
    // ----------------------------------------------------------------

    /// The earliest instant at which [`handle_timeout`](Session::handle_timeout)
    /// would do something: a heartbeat or delayed acknowledgement falling
    /// due, or the transport timing out. `None` if there is no channel, after
    /// a protocol error, or (in principle) if nothing is scheduled.
    ///
    /// The returned instant may be in the past, if `handle_timeout` has not
    /// been called since it passed. Deadlines which have already been acted
    /// on are not returned again, so a driver which calls `handle_timeout`
    /// whenever this instant is reached never spins.
    ///
    /// Immediate work (e.g. data ready to send) is not reflected here; it is
    /// picked up by [`transmit_data`](Session::transmit_data).
    pub fn poll_timeout(&self) -> Option<Instant> {
        if self.error.is_some() {
            return None;
        }
        let ch = self.channel.as_ref()?;
        let mut deadlines = Vec::with_capacity(4);
        if !self.timed_out {
            deadlines.push(ch.last_recv.checked_add(self.config.timeout));
            if self.tx_pending() > 0 {
                deadlines.push(ch.tx_progress.checked_add(self.config.timeout));
            }
        }
        if self.tx_pending() == 0 {
            deadlines.push(self.heartbeat_deadline(ch));
            deadlines.push(self.ack_deadline());
        }
        deadlines.into_iter().flatten().min()
    }

    /// Acts on any deadlines which have passed by `now`: detects a timed-out
    /// transport (see [`transport_timed_out`](Session::transport_timed_out)),
    /// and, if no output is pending, queues a frame if one is due (e.g. a
    /// heartbeat or delayed acknowledgement). Calling it early is harmless.
    pub fn handle_timeout(&mut self, now: Instant) {
        if self.error.is_some() || self.channel.is_none() {
            return;
        }
        if self.timeout_condition(now) {
            self.timed_out = true;
        }
        if self.tx_pending() == 0 && self.frame_wanted(now) {
            self.encode_frame(now);
        }
    }

    /// Whether the current channel has been found dead by
    /// [`handle_timeout`](Session::handle_timeout): nothing has been
    /// received on it for [`Config::timeout`], or pending output has made no
    /// progress for [`Config::timeout`].
    ///
    /// The flag is latched by `handle_timeout`, and cleared by a channel swap
    /// or once traffic on the channel means the condition no longer holds.
    /// Never true before a channel is installed.
    pub fn transport_timed_out(&self) -> bool {
        self.timed_out
    }

    /// Whether the stream is complete: both directions have shut down (our
    /// shutdown acknowledged, the peer's received and acknowledged in a frame
    /// on the current channel) and the receive buffer has been delivered. The
    /// driver should flush any pending [`transmit_data`](Session::transmit_data)
    /// and exit.
    pub fn is_finished(&self) -> bool {
        self.error.is_none()
            && self.channel.as_ref().is_some_and(|c| c.first_sent)
            && self.send.shutdown_acked()
            && self.recv.eof()
            && self.ack_sent == self.recv.hwm()
    }

    /// The protocol error which killed the session, if any.
    pub fn error(&self) -> Option<&ProtocolError> {
        self.error.as_ref()
    }

    // ----------------------------------------------------------------
    // Introspection (for tests and diagnostics)
    // ----------------------------------------------------------------

    /// Whether a channel has been installed.
    pub fn has_channel(&self) -> bool {
        self.channel.is_some()
    }

    /// Our send limit: we may send data only at offsets below it.
    pub fn send_limit(&self) -> u64 {
        self.send.limit()
    }

    /// The next outgoing stream position to be put in a frame.
    pub fn send_position(&self) -> u64 {
        self.send.pos()
    }

    /// One past the highest outgoing stream position ever put in a frame
    /// (including the shutdown position).
    pub fn max_sent(&self) -> u64 {
        self.send.max_sent()
    }

    /// The highest acknowledgement received: the first outgoing position
    /// not known to be received by the peer.
    pub fn peer_ack(&self) -> u64 {
        self.send.peer_ack()
    }

    /// Total outgoing data accepted from the application (the offset at
    /// which our shutdown sits, once closed).
    pub fn send_data_end(&self) -> u64 {
        self.send.data_end()
    }

    /// Bytes held in the send buffer (unacknowledged, sent or not).
    pub fn send_buffered(&self) -> usize {
        self.send.buffered()
    }

    /// Capacity of the send buffer.
    pub fn send_capacity(&self) -> usize {
        self.send.capacity()
    }

    /// Whether [`close_app_send`](Session::close_app_send) has been called.
    pub fn send_closed(&self) -> bool {
        self.send.closed()
    }

    /// Whether our shutdown has been put in a frame at least once.
    pub fn shutdown_sent(&self) -> bool {
        self.send.shutdown_sent()
    }

    /// Whether the peer has acknowledged our shutdown.
    pub fn shutdown_acked(&self) -> bool {
        self.send.shutdown_acked()
    }

    /// The receive high-water mark: the next incoming position expected,
    /// i.e. the acknowledgement we would send now.
    pub fn recv_high_water_mark(&self) -> u64 {
        self.recv.hwm()
    }

    /// Incoming data delivered to (or, after
    /// [`close_app_read`](Session::close_app_read), discarded for) the
    /// application.
    pub fn delivered(&self) -> u64 {
        self.recv.delivered()
    }

    /// Bytes held in the receive buffer.
    pub fn recv_buffered(&self) -> usize {
        self.recv.buffered()
    }

    /// Capacity of the receive buffer.
    pub fn recv_capacity(&self) -> usize {
        self.recv.capacity()
    }

    /// The window we would advertise now (free receive buffer space).
    pub fn window(&self) -> u32 {
        self.recv.window()
    }

    /// The `ack` carried by the most recently produced frame.
    pub fn ack_sent(&self) -> u64 {
        self.ack_sent
    }

    /// The `ack + window` carried by the most recently produced frame.
    pub fn advertised_limit(&self) -> u64 {
        self.advertised_limit
    }

    /// The position of the peer's shutdown, once received.
    pub fn peer_shutdown(&self) -> Option<u64> {
        self.recv.shutdown()
    }

    /// Whether [`close_app_read`](Session::close_app_read) has been called.
    pub fn app_read_is_closed(&self) -> bool {
        self.recv.read_closed()
    }

    /// Whether the current channel has resynchronized (a frame on it has
    /// reached the receive high-water mark). False with no channel.
    pub fn channel_resynced(&self) -> bool {
        self.channel.as_ref().is_some_and(|c| c.resynced)
    }

    /// Bytes of encoded output not yet written to the channel.
    pub fn tx_pending(&self) -> usize {
        self.tx.len() - self.tx_off
    }

    /// Total number of frames produced, over all channels.
    pub fn frames_sent(&self) -> u64 {
        self.frames_sent
    }

    /// Total number of valid frames received, over all channels.
    pub fn frames_received(&self) -> u64 {
        self.frames_received
    }

    // ----------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------

    fn process_frames(
        &mut self,
        decoder: &mut FrameDecoder,
        now: Instant,
    ) -> Result<(), ProtocolError> {
        while let Some((header, payload)) = decoder.next_frame()? {
            self.process_frame(&header, payload, now)?;
        }
        Ok(())
    }

    /// Validates (rules 2–7) and then applies one frame. Nothing is changed
    /// if validation fails.
    fn process_frame(
        &mut self,
        h: &FrameHeader,
        payload: &[u8],
        now: Instant,
    ) -> Result<(), ProtocolError> {
        let ch = self.channel.as_mut().expect("channel installed");
        let index = ch.frames_received;
        if index == 0 && !h.first {
            return Err(ProtocolError::MissingFirst);
        }
        if index > 0 && h.first {
            return Err(ProtocolError::UnexpectedFirst { index });
        }
        self.recv.validate(h, ch.resynced)?;
        self.send.validate_ack(h.ack, h.limit())?;

        ch.frames_received += 1;
        self.frames_received += 1;
        let accepted = self.recv.accept(h, payload);
        if accepted.reached_hwm {
            ch.resynced = true;
        }
        if accepted.advanced && self.unacked_since.is_none() {
            self.unacked_since = Some(now);
        }
        self.send.apply_ack(h.ack, h.limit());
        Ok(())
    }

    fn heartbeat_deadline(&self, ch: &Channel) -> Option<Instant> {
        ch.last_frame_at.checked_add(self.config.heartbeat_interval)
    }

    fn ack_deadline(&self) -> Option<Instant> {
        self.unacked_since?.checked_add(self.config.ack_delay)
    }

    /// Whether the current channel meets the timeout condition at `now`.
    fn timeout_condition(&self, now: Instant) -> bool {
        let Some(ch) = &self.channel else {
            return false;
        };
        let passed = |since: Instant| {
            since
                .checked_add(self.config.timeout)
                .is_some_and(|t| now >= t)
        };
        passed(ch.last_recv) || (self.tx_pending() > 0 && passed(ch.tx_progress))
    }

    /// The frame-sending policy: whether a frame should be produced now.
    fn frame_wanted(&self, now: Instant) -> bool {
        let Some(ch) = &self.channel else {
            return false;
        };
        if !ch.first_sent {
            return true;
        }
        if self.send.next_chunk(self.config.max_payload).is_some() {
            return true;
        }
        let hwm = self.recv.hwm();
        // Acknowledge a received shutdown immediately.
        if self.recv.shutdown().is_some() && self.ack_sent < hwm {
            return true;
        }
        let limit = self.recv.limit();
        if limit > self.advertised_limit
            && limit - self.advertised_limit >= self.config.window_update_threshold as u64
        {
            return true;
        }
        if hwm > self.ack_sent && hwm - self.ack_sent >= self.config.ack_threshold as u64 {
            return true;
        }
        let due = |t: Option<Instant>| t.is_some_and(|t| now >= t);
        due(self.ack_deadline()) || due(self.heartbeat_deadline(ch))
    }

    /// Encodes the next frame into the (empty) transmit buffer.
    fn encode_frame(&mut self, now: Instant) {
        debug_assert_eq!(self.tx_pending(), 0);
        let ch = self.channel.as_mut().expect("channel installed");
        let chunk = self
            .send
            .next_chunk(self.config.max_payload)
            .unwrap_or(Chunk {
                seq: self.send.pos(),
                len: 0,
                shutdown: false,
            });
        let header = FrameHeader {
            first: !ch.first_sent,
            shutdown: chunk.shutdown,
            length: u16::try_from(chunk.len).expect("chunk within max_payload"),
            window: self.recv.window(),
            seq: chunk.seq,
            ack: self.recv.hwm(),
        };
        self.tx.clear();
        self.tx_off = 0;
        header.encode_into(&mut self.tx);
        self.send.copy_chunk(chunk, &mut self.tx);
        self.send.advance(header.span());

        ch.first_sent = true;
        ch.last_frame_at = now;
        ch.tx_progress = now;
        self.ack_sent = header.ack;
        self.advertised_limit = header.limit();
        self.unacked_since = None;
        self.frames_sent += 1;
    }
}
