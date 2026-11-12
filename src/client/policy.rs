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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const MS: Duration = Duration::from_millis(1);

    /// A stand-in for the client's main loop, on a simulated clock.
    struct Driver {
        p: ResumePolicy,
        start: Instant,
        now: Instant,
        channel_lost: bool,
        timed_out: bool,
        /// When each attempt was started, relative to `start`.
        attempts: Vec<Duration>,
    }

    impl Driver {
        fn new() -> Driver {
            let start = Instant::now();
            Driver {
                p: ResumePolicy::new(),
                start,
                now: start,
                channel_lost: false,
                timed_out: false,
                attempts: vec![],
            }
        }

        fn elapsed(&self) -> Duration {
            self.now - self.start
        }

        /// One turn of the loop, up to waiting: starts an attempt if one is
        /// called for.
        fn step(&mut self) {
            self.p.handle_timeout(self.now);
            if self
                .p
                .should_attempt(self.now, self.channel_lost, self.timed_out)
            {
                self.p.attempt_started();
                self.attempts.push(self.elapsed());
            }
            if !self.p.hangup_expired(self.now) {
                assert!(
                    self.p.poll_timeout().is_none_or(|t| t > self.now),
                    "{:?} at {:?}",
                    self.p,
                    self.elapsed()
                );
            }
        }

        /// Lets `d` pass with no event, waking up whenever the policy asks.
        fn advance(&mut self, d: Duration) {
            let end = self.now + d;
            self.step();
            // Once the hangup deadline has passed, the client gives up.
            while let Some(t) = self.p.poll_timeout().filter(|&t| t <= end) {
                if self.p.hangup_expired(self.now) {
                    break;
                }
                self.now = t;
                self.step();
            }
            self.now = end;
            self.step();
        }

        fn lose_channel(&mut self) {
            self.channel_lost = true;
            self.step();
        }

        fn ssh_exits(&mut self) {
            self.p.ssh_exited(self.now);
            self.step();
        }

        /// The outstanding attempt succeeds, with a new, working channel.
        fn succeed(&mut self) {
            self.p.attempt_succeeded();
            self.channel_lost = false;
            self.timed_out = false;
            self.step();
        }

        fn fail(&mut self) -> Duration {
            let delay = self.p.attempt_failed(self.now);
            self.step();
            delay
        }
    }

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    #[test]
    fn healthy_channel_never_resumes() {
        let mut d = Driver::new();
        d.advance(secs(3600.0));
        assert_eq!(d.attempts, []);
        assert_eq!(d.p.poll_timeout(), None);
    }

    #[test]
    fn lost_channel_resumes_at_once_one_attempt_at_a_time() {
        let mut d = Driver::new();
        d.advance(secs(5.0));
        d.lose_channel();
        assert_eq!(d.attempts, [secs(5.0)]);
        // Nothing more while the attempt is outstanding, whatever happens.
        d.timed_out = true;
        d.ssh_exits();
        d.advance(secs(60.0));
        assert_eq!(d.attempts, [secs(5.0)]);
    }

    #[test]
    fn timeout_resumes_at_once() {
        let mut d = Driver::new();
        d.timed_out = true;
        d.step();
        assert_eq!(d.attempts, [Duration::ZERO]);
    }

    #[test]
    fn ssh_exit_resumes_only_after_the_grace_period() {
        let mut d = Driver::new();
        d.advance(secs(2.0));
        d.ssh_exits();
        assert_eq!(d.p.poll_timeout(), Some(d.now + SSH_EXIT_GRACE));
        d.advance(SSH_EXIT_GRACE - MS);
        assert_eq!(d.attempts, []);
        d.advance(MS);
        assert_eq!(d.attempts, [secs(2.0) + SSH_EXIT_GRACE]);
    }

    #[test]
    fn channel_lost_within_the_grace_period_resumes_at_once() {
        let mut d = Driver::new();
        d.ssh_exits();
        d.advance(secs(1.0));
        d.lose_channel();
        assert_eq!(d.attempts, [secs(1.0)]);
    }

    #[test]
    fn a_new_ssh_gets_a_new_grace_period() {
        let mut d = Driver::new();
        d.ssh_exits();
        d.lose_channel();
        d.succeed();
        // The old ssh's exit is forgotten.
        d.advance(secs(60.0));
        assert_eq!(d.attempts, [Duration::ZERO]);
        d.ssh_exits();
        d.advance(SSH_EXIT_GRACE);
        assert_eq!(d.attempts, [Duration::ZERO, secs(60.0) + SSH_EXIT_GRACE]);
    }

    #[test]
    fn failures_back_off_exponentially() {
        let mut d = Driver::new();
        d.lose_channel();
        let mut delays = vec![];
        for _ in 0..7 {
            let n = d.attempts.len();
            let delay = d.fail();
            delays.push(delay.as_secs());
            // Even though the channel is still lost, the next attempt waits
            // for exactly the delay.
            d.advance(delay - MS);
            assert_eq!(d.attempts.len(), n);
            d.advance(MS);
            assert_eq!(d.attempts.len(), n + 1);
            assert_eq!(d.attempts[n], d.elapsed());
        }
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30]);

        // Success starts the backoff afresh.
        d.succeed();
        d.lose_channel();
        assert_eq!(d.fail(), INITIAL_BACKOFF);
    }

    #[test]
    fn a_failure_is_retried_only_if_the_channel_still_needs_it() {
        let mut d = Driver::new();
        d.timed_out = true;
        d.step();
        d.fail();
        // Traffic resumed on the old channel meanwhile.
        d.timed_out = false;
        d.advance(secs(3600.0));
        assert_eq!(d.attempts, [Duration::ZERO]);
    }

    #[test]
    fn ssh_exit_during_backoff_resumes_once_both_have_passed() {
        let mut d = Driver::new();
        d.timed_out = true;
        d.step();
        assert_eq!(d.fail(), secs(1.0));
        d.timed_out = false;
        d.advance(secs(0.5));
        d.ssh_exits();
        // The backoff ends at 1 s, the grace period at 2 s.
        d.advance(secs(10.0));
        assert_eq!(d.attempts, [Duration::ZERO, secs(0.5) + SSH_EXIT_GRACE]);
    }

    #[test]
    fn server_gone_stops_all_attempts() {
        let mut d = Driver::new();
        d.lose_channel();
        d.p.server_gone();
        d.ssh_exits();
        d.timed_out = true;
        d.advance(secs(3600.0));
        assert_eq!(d.attempts, [Duration::ZERO]);
        assert_eq!(d.p.poll_timeout(), None);
    }

    #[test]
    fn hangup_gives_up_after_its_timeout() {
        let mut d = Driver::new();
        d.advance(secs(1.0));
        d.p.hangup(d.now);
        assert!(d.p.hung_up());
        assert_eq!(d.p.poll_timeout(), Some(d.now + HANGUP_TIMEOUT));
        d.advance(secs(5.0));
        // A second hangup does not extend the deadline.
        d.p.hangup(d.now);
        d.advance(secs(5.0) - MS);
        assert!(!d.p.hangup_expired(d.now));
        d.advance(MS);
        assert!(d.p.hangup_expired(d.now));
    }

    #[derive(Debug, Clone)]
    enum Op {
        Advance(u64),
        LoseChannel,
        TimedOut(bool),
        SshExits,
        Succeed,
        Fail,
        ServerGone,
        Hangup,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0..40_000u64).prop_map(Op::Advance),
            Just(Op::LoseChannel),
            any::<bool>().prop_map(Op::TimedOut),
            Just(Op::SshExits),
            Just(Op::Succeed),
            Just(Op::Fail),
            Just(Op::ServerGone),
            Just(Op::Hangup),
        ]
    }

    proptest! {
        /// Whatever happens, the policy never asks to be woken in the past
        /// (which would make the loop spin; checked by `Driver::step`), and
        /// starts an attempt exactly when a trigger holds and nothing
        /// holds it back.
        #[test]
        fn policy_invariants(ops in prop::collection::vec(op(), 0..60)) {
            let mut d = Driver::new();
            let mut ssh_exited: Option<Instant> = None;
            let mut gone = false;
            for op in ops {
                let attempting = d.p.attempting;
                match op {
                    Op::Advance(ms) => d.advance(Duration::from_millis(ms)),
                    Op::LoseChannel => d.lose_channel(),
                    Op::TimedOut(t) => {
                        d.timed_out = t;
                        d.step();
                    }
                    Op::SshExits => {
                        ssh_exited.get_or_insert(d.now);
                        d.ssh_exits();
                    }
                    Op::Succeed if attempting => {
                        ssh_exited = None;
                        d.succeed();
                    }
                    Op::Fail if attempting => {
                        d.fail();
                    }
                    Op::ServerGone if attempting => {
                        gone = true;
                        d.p.server_gone();
                        d.step();
                    }
                    Op::Hangup => {
                        d.p.hangup(d.now);
                        d.step();
                    }
                    Op::Succeed | Op::Fail | Op::ServerGone => {}
                }
                // After each turn of the loop, either an attempt is
                // outstanding, or none is called for.
                let triggered = d.channel_lost
                    || d.timed_out
                    || ssh_exited.is_some_and(|t| t + SSH_EXIT_GRACE <= d.now);
                if !d.p.attempting && !gone && d.p.retry_at.is_none() {
                    prop_assert!(!triggered, "{:?}", d.p);
                }
                if gone {
                    prop_assert!(!d.p.attempting);
                }
            }
        }
    }
}
