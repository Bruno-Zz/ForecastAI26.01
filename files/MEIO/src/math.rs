//! Statistical math utilities used by the MEIO fill-rate calculations.
//!
//! All functions operate on demand expressed in **line-units** (i.e. already
//! divided by avg_size where appropriate).  The caller is responsible for unit
//! conversion before calling these functions.

use statrs::distribution::{ContinuousCDF, DiscreteCDF};

// ── Normal distribution ────────────────────────────────────────────────────

/// Standard normal CDF: Φ(x) = P(Z ≤ x).
///
/// Equivalent to SciPy `scipy.special.ndtr(x)`.
#[inline]
pub fn normal_cdf(x: f64) -> f64 {
    if !x.is_finite() {
        return if x > 0.0 { 1.0 } else { 0.0 };
    }
    statrs::distribution::Normal::new(0.0, 1.0)
        .expect("standard normal params are always valid")
        .cdf(x)
}

/// Standard normal quantile / inverse CDF: Φ⁻¹(p).
///
/// Equivalent to SciPy `scipy.special.ndtri(p)`.
/// Returns `f64::NEG_INFINITY` for p ≤ 0 and `f64::INFINITY` for p ≥ 1.
#[inline]
pub fn normal_inv(p: f64) -> f64 {
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    statrs::distribution::Normal::new(0.0, 1.0)
        .expect("standard normal params are always valid")
        .inverse_cdf(p)
}

// ── Poisson distribution ──────────────────────────────────────────────────

/// Poisson CDF: P(X ≤ k | λ).
///
/// `k` is treated as `floor(k)`.  Returns 1.0 when λ ≤ 0.
///
/// Equivalent to SciPy `scipy.stats.poisson.cdf(k, lambda)`.
#[inline]
pub fn poisson_cdf(k: f64, lambda: f64) -> f64 {
    if lambda <= 0.0 {
        return 1.0;
    }
    if k < 0.0 {
        return 0.0;
    }
    statrs::distribution::Poisson::new(lambda)
        .expect("lambda > 0 is valid for Poisson")
        .cdf(k.floor() as u64)
}

/// Poisson PMF: P(X = k | λ).
///
/// Equivalent to SciPy `scipy.stats.poisson.pmf(k, lambda)`.
#[inline]
pub fn poisson_pmf(k: f64, lambda: f64) -> f64 {
    if lambda <= 0.0 {
        return if k == 0.0 { 1.0 } else { 0.0 };
    }
    if k < 0.0 {
        return 0.0;
    }
    use statrs::distribution::Discrete;
    statrs::distribution::Poisson::new(lambda)
        .expect("lambda > 0 is valid for Poisson")
        .pmf(k.floor() as u64)
}

// ── Gamma distribution ────────────────────────────────────────────────────

/// Gamma CDF: P(X ≤ x | shape, scale).
///
/// Note: `statrs` uses **rate** = 1/scale internally.
#[inline]
pub fn gamma_cdf(x: f64, shape: f64, scale: f64) -> f64 {
    if x <= 0.0 || shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }
    let rate = 1.0 / scale;
    statrs::distribution::Gamma::new(shape, rate)
        .map(|d| d.cdf(x))
        .unwrap_or(0.0)
}

// ── LogNormal distribution ─────────────────────────────────────────────────

/// Log-Normal CDF: P(X ≤ x | μ, σ) where μ and σ are parameters of the
/// underlying normal (i.e. log-space mean and std-dev).
#[inline]
pub fn lognormal_cdf(x: f64, mu: f64, sigma: f64) -> f64 {
    if x <= 0.0 || sigma <= 0.0 {
        return 0.0;
    }
    statrs::distribution::LogNormal::new(mu, sigma)
        .map(|d| d.cdf(x))
        .unwrap_or(0.0)
}

// ── Weibull distribution ──────────────────────────────────────────────────

/// Weibull CDF: P(X ≤ x | shape k, scale λ).
#[inline]
pub fn weibull_cdf(x: f64, shape: f64, scale: f64) -> f64 {
    if x <= 0.0 || shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }
    statrs::distribution::Weibull::new(shape, scale)
        .map(|d| d.cdf(x))
        .unwrap_or(0.0)
}

// ── Negative Binomial distribution ────────────────────────────────────────

/// Negative Binomial CDF: P(X ≤ k | r, p).
///
/// Convention: r = number of successes (can be non-integer), p = success probability.
/// Returns `statrs` NegativeBinomial which uses the alternative parameterisation
/// (number of failures until r successes).
#[inline]
pub fn neg_binomial_cdf(k: f64, r: f64, p: f64) -> f64 {
    if r <= 0.0 || !(0.0..=1.0).contains(&p) {
        return 0.0;
    }
    if k < 0.0 {
        return 0.0;
    }
    statrs::distribution::NegativeBinomial::new(r, p)
        .map(|d| d.cdf(k.floor() as u64))
        .unwrap_or(0.0)
}

// ── Erlang telephony formulas ─────────────────────────────────────────────
//
// Used for asset / rotable pool sizing.
// rho = offered load = failure_rate × fleet_size × repair_tat_mean

/// Erlang-B: probability that an arriving demand finds all `n` servers busy
/// (loss system — no queuing).
///
/// Computed via the iterative recurrence in log-space to avoid overflow:
///   B(0, ρ) = 1
///   B(n, ρ) = (ρ/n · B(n-1, ρ)) / (1 + ρ/n · B(n-1, ρ))
///
/// Equivalent to SciPy's `scipy.special.erlangs` (loss formula).
pub fn erlang_b(n: u32, rho: f64) -> f64 {
    if rho <= 0.0 {
        return 0.0;
    }
    // Iterative form — numerically stable for large n and rho
    let mut b = 1.0_f64;
    for k in 1..=(n as u64) {
        let term = rho / k as f64;
        b = term * b / (1.0 + term * b);
    }
    b
}

/// Erlang-C: probability that an arriving demand must wait (delay system —
/// infinite queue capacity).
///
/// Formula:  C(n, ρ) = B(n, ρ) / (1 - (ρ/n)(1 - B(n, ρ)))
/// where B is Erlang-B.
///
/// Returns 1.0 when ρ ≥ n (system saturated; all calls wait).
pub fn erlang_c(n: u32, rho: f64) -> f64 {
    if n == 0 || rho <= 0.0 {
        return 0.0;
    }
    let n_f = n as f64;
    if rho >= n_f {
        return 1.0;
    }
    let b = erlang_b(n, rho);
    let num = b;
    let den = 1.0 - (rho / n_f) * (1.0 - b);
    if den <= 0.0 { 1.0 } else { (num / den).min(1.0) }
}

/// Smallest pool size `S` such that `erlang_b(S, rho) ≤ p_target`.
///
/// Searches upward from 1.  Caps at 10 000 to prevent runaway loops.
pub fn min_pool_size_loss(rho: f64, p_target: f64) -> u32 {
    for s in 1..=10_000_u32 {
        if erlang_b(s, rho) <= p_target {
            return s;
        }
    }
    10_000
}

/// Smallest pool size `S` such that `erlang_c(S, rho) ≤ p_target`.
///
/// Searches upward from 1.  Caps at 10 000.
pub fn min_pool_size_queue(rho: f64, p_target: f64) -> u32 {
    for s in 1..=10_000_u32 {
        if erlang_c(s, rho) <= p_target {
            return s;
        }
    }
    10_000
}

// ── EOQ loop step helper ──────────────────────────────────────────────────

/// Compute the step size for the EOQ integration loop.
///
/// Matches the Python formula:
/// `step = int(max(1, avgsize, math.ceil(eoq / maxEoqFreq)))` with `maxEoqFreq = 10`.
#[inline]
pub fn eoq_step(avg_size: f64, eoq: f64) -> u64 {
    const MAX_EOQ_FREQ: u64 = 10;
    let ceil_eoq_slice = (eoq / MAX_EOQ_FREQ as f64).ceil() as u64;
    1_u64.max(avg_size as u64).max(ceil_eoq_slice)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_cdf_symmetry() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-10);
        assert!((normal_cdf(1.96) - 0.975).abs() < 0.001);
        assert!((normal_cdf(-1.96) - 0.025).abs() < 0.001);
    }

    #[test]
    fn normal_inv_roundtrip() {
        for p in [0.1, 0.25, 0.5, 0.75, 0.9, 0.99_f64] {
            let z = normal_inv(p);
            let back = normal_cdf(z);
            assert!((back - p).abs() < 1e-9, "roundtrip failed for p={p}: got {back}");
        }
    }

    #[test]
    fn poisson_cdf_basic() {
        // P(X ≤ 2 | λ=1) ≈ 0.9197
        let v = poisson_cdf(2.0, 1.0);
        assert!((v - 0.9197).abs() < 0.001, "got {v}");
    }

    #[test]
    fn erlang_b_zero_servers() {
        // With 0 load all calls are blocked? — we guard rho<=0 → 0.0
        assert_eq!(erlang_b(0, 1.0), 1.0); // B(0,ρ) = 1 by convention (loop skips)
        assert_eq!(erlang_b(5, 0.0), 0.0);
    }

    #[test]
    fn erlang_b_known_values() {
        // B(1, 1.0) = 0.5  (one server, load = 1)
        let v = erlang_b(1, 1.0);
        assert!((v - 0.5).abs() < 0.001, "B(1,1)={v}");
        // B(5, 5.0) ≈ 0.1727
        let v5 = erlang_b(5, 5.0);
        assert!((v5 - 0.1727).abs() < 0.001, "B(5,5)={v5}");
    }

    #[test]
    fn erlang_c_saturated() {
        // rho >= n → C = 1.0
        assert_eq!(erlang_c(3, 3.0), 1.0);
        assert_eq!(erlang_c(3, 5.0), 1.0);
    }

    #[test]
    fn min_pool_size_loss_basic() {
        // rho=2, target=0.05 → need enough servers to get B ≤ 5%
        let s = min_pool_size_loss(2.0, 0.05);
        assert!(erlang_b(s, 2.0) <= 0.05, "s={s}, B={}", erlang_b(s, 2.0));
        assert!(s > 0);
    }

    #[test]
    fn eoq_step_matches_python() {
        // max(1, 5, ceil(100/10)) = max(1,5,10) = 10
        assert_eq!(eoq_step(5.0, 100.0), 10);
        // max(1, 1, ceil(3/10)) = max(1,1,1) = 1
        assert_eq!(eoq_step(1.0, 3.0), 1);
    }
}
