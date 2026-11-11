//! A small deterministic PRNG (SplitMix64), so that a simulation is entirely
//! determined by its seed, independently of any external crate's algorithm.

#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`; zero if `n` is zero.
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform in `[lo, hi]`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "empty range {lo}..={hi}");
        lo + self.below((hi - lo).saturating_add(1))
    }

    pub fn usize_range(&mut self, lo: usize, hi: usize) -> usize {
        self.range(lo as u64, hi as u64) as usize
    }

    /// `true` with probability `num / den`.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }

    /// Roughly log-uniform in `[lo, hi]` (`lo >= 1`): a uniformly chosen
    /// power of two, then uniform within it. Integer-only, so fully
    /// reproducible across platforms.
    pub fn log_range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(1 <= lo && lo <= hi, "bad log range {lo}..={hi}");
        let lo_bits = 63 - lo.leading_zeros();
        let hi_bits = 63 - hi.leading_zeros();
        let b = self.range(u64::from(lo_bits), u64::from(hi_bits)) as u32;
        let base = 1u64 << b;
        self.range(base, base.saturating_mul(2) - 1).clamp(lo, hi)
    }

    pub fn usize_log(&mut self, lo: usize, hi: usize) -> usize {
        self.log_range(lo as u64, hi as u64) as usize
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n + 8);
        while out.len() < n {
            out.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        out.truncate(n);
        out
    }
}
