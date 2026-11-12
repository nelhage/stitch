//! When the client resumes: its reconnection policy, as a state machine
//! which, like [`core::Session`](crate::core::Session), never reads the
//! clock: every method which depends on time takes `now`. The client's main
//! loop feeds it what happens (the active `ssh` exits, an attempt fails, a
//! `SIGHUP` arrives, ...) and asks it what to do; this keeps the timing
//! rules testable without real time. See "Client side" in
//! `doc/implementation.md`.

use std::time::{Duration, Instant};

/// Delay before retrying after the first failed resume attempt; it doubles
/// with each further failure, up to [`MAX_BACKOFF`].
pub const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// The longest delay between resume attempts.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How long the channel may outlive the active `ssh` before we treat it as
/// lost. An `ssh` normally exits only after writing everything it received
/// to the channel, so we keep reading until EOF; this bounds the wait in
/// case some other process (a child of `ssh`, say) holds the channel open.
pub const SSH_EXIT_GRACE: Duration = Duration::from_millis(1500);

/// How long we allow the session to finish after `SIGHUP`, before giving
/// up on it.
pub const HANGUP_TIMEOUT: Duration = Duration::from_secs(10);

/// The client's reconnection policy. See the [module documentation](self).
///
/// The owner's loop should, each time round: call
/// [`handle_timeout`](Self::handle_timeout); give up if
/// [`hangup_expired`](Self::hangup_expired); start an attempt (and call
/// [`attempt_started`](Self::attempt_started)) if
/// [`should_attempt`](Self::should_attempt); and then wait for an event, or
/// until [`poll_timeout`](Self::poll_timeout), whichever comes first.
#[derive(Debug)]
pub struct ResumePolicy {
    /// The delay before the next attempt, should the current one fail.
    backoff: Duration,
    /// When the next attempt may start, after a failure.
    retry_at: Option<Instant>,
    /// Whether a resume attempt is outstanding.
    attempting: bool,
    /// When the active `ssh` was seen to exit.
    ssh_exited_at: Option<Instant>,
    /// The server rejected a resume after we had received everything we
    /// need from it: there is no point in trying again.
    server_gone: bool,
    /// After `SIGHUP`, when we give up on finishing the session.
    hangup_deadline: Option<Instant>,
}

impl Default for ResumePolicy {
    fn default() -> Self {
        ResumePolicy::new()
    }
}

impl ResumePolicy {
    /// The policy for a session just opened: its `ssh` running, no attempt
    /// made yet.
    pub fn new() -> ResumePolicy {
        ResumePolicy {
            backoff: INITIAL_BACKOFF,
            retry_at: None,
            attempting: false,
            ssh_exited_at: None,
            server_gone: false,
            hangup_deadline: None,
        }
    }

    /// Whether to start a resume attempt now, given the state of the
    /// channel: whether it has been lost (EOF or an error), and whether the
    /// session considers the transport timed out.
    ///
    /// The triggers are levels, not edges: the channel has been lost, the
    /// transport is timed out, or the active `ssh` exited more than
    /// [`SSH_EXIT_GRACE`] ago. They count only while no attempt is
    /// outstanding, no backoff delay is pending, and the server has not
    /// been found gone.
    pub fn should_attempt(&self, now: Instant, channel_lost: bool, timed_out: bool) -> bool {
        !self.attempting
            && self.retry_at.is_none()
            && !self.server_gone
            && (channel_lost || timed_out || self.ssh_exit_deadline().is_some_and(|t| t <= now))
    }

    /// An attempt has been started.
    pub fn attempt_started(&mut self) {
        debug_assert!(!self.attempting);
        self.attempting = true;
    }

    /// The outstanding attempt succeeded: its `ssh` is now the active one.
    pub fn attempt_succeeded(&mut self) {
        self.attempting = false;
        self.backoff = INITIAL_BACKOFF;
        self.retry_at = None;
        self.ssh_exited_at = None;
    }

    /// The outstanding attempt failed in a way which another attempt might
    /// not. Returns the delay before the next attempt may start.
    pub fn attempt_failed(&mut self, now: Instant) -> Duration {
        self.attempting = false;
        let delay = self.backoff;
        self.retry_at = Some(now + delay);
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
        delay
    }

    /// The outstanding attempt found that the server's session has gone,
    /// but we have received everything we need from it: make no further
    /// attempts.
    pub fn server_gone(&mut self) {
        self.attempting = false;
        self.server_gone = true;
    }

    /// The active `ssh` has exited.
    pub fn ssh_exited(&mut self, now: Instant) {
        self.ssh_exited_at.get_or_insert(now);
    }

    /// When the exit of the active `ssh` becomes a resume trigger, if it has
    /// exited.
    fn ssh_exit_deadline(&self) -> Option<Instant> {
        self.ssh_exited_at.map(|t| t + SSH_EXIT_GRACE)
    }

    /// We were sent `SIGHUP`: the session has [`HANGUP_TIMEOUT`] to finish.
    /// A later hangup does not extend that.
    pub fn hangup(&mut self, now: Instant) {
        self.hangup_deadline.get_or_insert(now + HANGUP_TIMEOUT);
    }

    /// Whether we have been sent `SIGHUP`.
    pub fn hung_up(&self) -> bool {
        self.hangup_deadline.is_some()
    }

    /// Whether, after a hangup, the session has failed to finish in time.
    pub fn hangup_expired(&self, now: Instant) -> bool {
        self.hangup_deadline.is_some_and(|t| t <= now)
    }

    /// Acts on deadlines which have passed: the end of a backoff delay.
    pub fn handle_timeout(&mut self, now: Instant) {
        if self.retry_at.is_some_and(|t| t <= now) {
            self.retry_at = None;
        }
    }

    /// The earliest instant at which the policy's answers may change with
    /// no event: the end of a backoff delay or of the active `ssh`'s grace
    /// period, while those matter, or the hangup deadline. Once
    /// [`handle_timeout`](Self::handle_timeout) has been called, and any
    /// attempt [`should_attempt`](Self::should_attempt) called for has been
    /// started, it is in the future.
    pub fn poll_timeout(&self) -> Option<Instant> {
        let resume = if self.attempting || self.server_gone {
            None
        } else {
            self.retry_at.or(self.ssh_exit_deadline())
        };
        match (resume, self.hangup_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}
