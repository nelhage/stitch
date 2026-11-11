//! Deterministic simulation testing of the protocol core (`stitch::core`).
//!
//! Two [`Session`](stitch::core::Session)s, a client and a server, are driven
//! through a simulated world with a virtual clock and a seeded RNG: channels
//! with latency, bandwidth and bounded capacity which can be cut, stalled
//! and replaced; a model of the client's resume logic (including speculative,
//! failing, abandoned and stale attempts) and of the server's acceptance of
//! resumes; and applications which write and read at random rates, stall,
//! close their read sides and shut down. Invariants are checked after every
//! step (see `sim/world.rs`), and progress/liveness at the end.
//!
//! Every simulation is determined by a [`Case`] (`family:seed:size:chaos`).
//! A failure prints it; replay it with
//!
//! ```text
//! SIM_CASE=chaos:123:50:80 cargo test --test sim replay -- --nocapture
//! ```
//!
//! adding `SIM_TRACE=1` for a full event trace.
//!
//! Case counts: `SIM_CASES` overrides the case count of each default
//! simulation test, and `SIM_FUZZ_CASES` that of the fuzz tests (default
//! 256); the `#[ignore]`d soak tests use `SIM_SOAK_CASES` (default 5000),
//! e.g.
//!
//! ```text
//! SIM_SOAK_CASES=20000 cargo test --release --test sim soak -- --ignored
//! ```

// The client and server are indices 0 and 1 throughout, and are often
// paired with their peer as `1 - x`; index loops read best.
#![allow(clippy::needless_range_loop)]

#[path = "sim/adversarial.rs"]
mod adversarial;
#[path = "sim/net.rs"]
mod net;
#[path = "sim/rng.rs"]
mod rng;
#[path = "sim/scenario.rs"]
mod scenario;
#[path = "sim/world.rs"]
mod world;

use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;

use scenario::{Case, Family};

fn env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// Default-run case count: `default`, or `SIM_CASES` if set.
fn cases(default: u32) -> ProptestConfig {
    ProptestConfig {
        cases: env_u32("SIM_CASES").unwrap_or(default),
        // A shrink step re-runs a whole simulation; keep it bounded.
        max_shrink_iters: 64,
        // (Proptest cannot place regression files for integration tests;
        // the case printed on failure is the way to reproduce.)
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn soak_cases() -> ProptestConfig {
    ProptestConfig {
        cases: env_u32("SIM_SOAK_CASES").unwrap_or(5000),
        max_shrink_iters: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn case(family: Family, seed: u64, size: u32, chaos: u32) -> Case {
    Case {
        family,
        seed,
        size,
        chaos,
    }
}

fn run(c: Case) {
    world::run(&c, false);
}

proptest! {
    #![proptest_config(cases(256))]

    /// No faults (but possibly speculative resumes): both streams arrive
    /// intact, both sessions finish, and nothing ever times out.
    #[test]
    fn sim_clean(seed in any::<u64>(), size in 1u32..=100, chaos in 0u32..=100) {
        run(case(Family::Clean, seed, size, chaos));
    }
}

proptest! {
    #![proptest_config(cases(512))]

    /// Cuts, stalls, failing/slow/stale resume attempts and stalled readers;
    /// then the network heals and everything must complete.
    #[test]
    fn sim_chaos(seed in any::<u64>(), size in 1u32..=100, chaos in 0u32..=100) {
        run(case(Family::Chaos, seed, size, chaos));
    }
}

proptest! {
    #![proptest_config(cases(128))]

    /// A healthy channel with one application that never reads: no
    /// timeouts, no resumes, heartbeats flow, the other direction is
    /// unaffected.
    #[test]
    fn sim_liveness(seed in any::<u64>()) {
        run(case(Family::Liveness, seed, 50, 0));
    }
}

proptest! {
    #![proptest_config(soak_cases())]

    #[test]
    #[ignore = "soak test; run with --ignored (see SIM_SOAK_CASES)"]
    fn soak_chaos(seed in any::<u64>(), size in 1u32..=100, chaos in 0u32..=100) {
        run(case(Family::Chaos, seed, size, chaos));
    }

    #[test]
    #[ignore = "soak test; run with --ignored (see SIM_SOAK_CASES)"]
    fn soak_clean_and_liveness(seed in any::<u64>(), size in 1u32..=100, chaos in 0u32..=100) {
        run(case(Family::Clean, seed, size, chaos));
        run(case(Family::Liveness, seed, size, 0));
    }
}

/// Replays the case in `SIM_CASE` (does nothing if it is unset), printing a
/// trace if `SIM_TRACE` is set.
#[test]
fn replay() {
    let Ok(s) = std::env::var("SIM_CASE") else {
        return;
    };
    let c = Case::parse(&s).unwrap();
    let verbose = std::env::var_os("SIM_TRACE").is_some();
    let stats = world::run(&c, verbose);
    println!("{c}: {stats:#?}");
}

/// The scenarios really exercise what they are meant to: over a fixed set
/// of seeds, each interesting situation occurs at least once. (Guards
/// against a harness that silently stops testing anything.)
#[test]
fn scenarios_cover_interesting_cases() {
    let mut total = world::Stats::default();
    for seed in 0..40 {
        total.add(&world::run(&case(Family::Chaos, seed, 20, 90), false));
    }
    for seed in 0..20 {
        total.add(&world::run(&case(Family::Clean, seed, 20, 100), false));
    }
    println!("{total:#?}");
    let checks = [
        ("client swaps", total.client_swaps),
        ("speculative swaps", total.speculative_swaps),
        (
            "data on the old channel during a resume",
            total.data_during_resume,
        ),
        ("re-sent duplicates after a rewind", total.rewind_duplicates),
        ("abandoned attempts", total.abandoned),
        ("stale epochs rejected", total.stale_rejected),
        (
            "resumes accepted on abandoned channels",
            total.zombie_accepts,
        ),
        ("transport timeouts", total.timeouts),
        ("cuts", total.cuts),
        ("stalls", total.stalls),
        ("partial writes", total.partial_writes),
        ("sessions finishing", total.both_finished),
    ];
    let missing: Vec<&str> = checks
        .iter()
        .filter(|(_, n)| *n == 0)
        .map(|(name, _)| *name)
        .collect();
    assert!(missing.is_empty(), "never exercised: {missing:?}");
}
