//! Distribution types and fill-rate calculation.
//!
//! ## Priority hierarchy
//!
//! 1. **Fitted distribution** from the Distributions pipeline step (Gamma, LogNormal,
//!    Weibull, NegativeBinomial) — used when available.
//! 2. **Legacy Normal** ("golden formula") — when `sku_count > distribution_threshold`.
//! 3. **Legacy Poisson** — when `sku_count ≤ distribution_threshold`.
//!
//! All fill-rate functions integrate over the EOQ range using the same
//! trapezoidal summation pattern as the original Python code.

use serde::{Deserialize, Serialize};

use crate::math::{
    eoq_step, gamma_cdf, lognormal_cdf, neg_binomial_cdf, normal_cdf, normal_inv, poisson_cdf,
    weibull_cdf,
};

// ── Distribution type ─────────────────────────────────────────────────────

/// The demand distribution used to map a reorder-point buffer to a fill rate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DistributionType {
    /// Legacy normal ("golden formula") — selected when `sku_count > threshold`.
    Normal,
    /// Legacy Poisson — selected when `sku_count ≤ threshold`.
    Poisson,
    /// Fitted Gamma distribution (shape α, scale θ).
    Gamma { shape: f64, scale: f64 },
    /// Fitted Log-Normal distribution (log-space mean μ, log-space σ).
    LogNormal { mu: f64, sigma: f64 },
    /// Fitted Weibull distribution (shape k, scale λ).
    Weibull { shape: f64, scale: f64 },
    /// Fitted Negative Binomial (successes r, success-probability p).
    NegativeBinomial { r: f64, p: f64 },
}

impl DistributionType {
    /// Resolve the distribution for a SKU:
    /// - If a fitted distribution is provided, use it.
    /// - Otherwise fall back to `Normal` or `Poisson` depending on `sku_count`.
    pub fn resolve(
        fitted: Option<&DistributionType>,
        sku_count: i64,
        distribution_threshold: i64,
    ) -> DistributionType {
        if let Some(f) = fitted {
            return f.clone();
        }
        if sku_count > distribution_threshold {
            DistributionType::Normal
        } else {
            DistributionType::Poisson
        }
    }
}

// ── Fill-rate calculation ─────────────────────────────────────────────────

/// Parameters needed to compute fill rate from a reorder-point buffer.
///
/// These map one-to-one to the arguments of the Python `new_fill_rate_calc`.
#[derive(Debug, Clone)]
pub struct FillRateParams {
    /// Buffer / reorder-point (ROP), in **line units**.
    pub buffer: f64,
    /// Forecast over lead time, in line units.
    pub forecast_over_lt: f64,
    /// Economic order quantity.
    pub eoq: f64,
    /// Average order / transaction size.
    pub avg_size: f64,
    /// Leg lead time (days).
    pub lead_time: f64,
    /// Standard deviation of lead time (days).  May be `f64::NAN` if unknown.
    pub lead_time_stddev: f64,
    /// Standard deviation of demand per period.  May be 0 if using MAD.
    pub dmd_stddev: f64,
    /// Mean Absolute Deviation of demand.  May be `f64::NAN` if not available.
    pub mad: f64,
}

/// Compute the fill-rate (service level) for a given ROP buffer.
///
/// Dispatches to the appropriate sub-function based on `distribution`.
/// Matches the Python `new_fill_rate_calc` dispatch exactly.
pub fn fill_rate_for_rop(dist: &DistributionType, p: &FillRateParams) -> f64 {
    let fr = match dist {
        DistributionType::Normal => golden_formula_rop_to_fr(p),
        DistributionType::Poisson => poisson_rop_to_fr(p),
        DistributionType::Gamma { shape, scale } => {
            fitted_cdf_rop_to_fr(p, |x| gamma_cdf(x, *shape, *scale))
        }
        DistributionType::LogNormal { mu, sigma } => {
            fitted_cdf_rop_to_fr(p, |x| lognormal_cdf(x, *mu, *sigma))
        }
        DistributionType::Weibull { shape, scale } => {
            fitted_cdf_rop_to_fr(p, |x| weibull_cdf(x, *shape, *scale))
        }
        DistributionType::NegativeBinomial { r, p: prob } => {
            fitted_cdf_rop_to_fr(p, |x| neg_binomial_cdf(x, *r, *prob))
        }
    };
    fr.clamp(0.0, 1.0)
}

// ── Internal helpers ──────────────────────────────────────────────────────

/// "Golden formula" normal fill-rate calculation.
///
/// Mirrors Python `golden_formula_ROP2FR`.
fn golden_formula_rop_to_fr(p: &FillRateParams) -> f64 {
    // Handle degenerate case: no variability → use simple normal CDF
    if p.mad.is_nan() && p.dmd_stddev == 0.0 {
        return normal_rop_to_fr(p);
    }

    let step = eoq_step(p.avg_size, p.eoq) as f64;
    let lead_time_month = p.lead_time / 30.0;
    let daily_rate = p.forecast_over_lt * p.avg_size / p.lead_time.max(1e-9);

    let lt_stddev_safe = if p.lead_time_stddev.is_nan() { 0.0 } else { p.lead_time_stddev };
    let lead_time_deviation_portion = (daily_rate * lt_stddev_safe).powi(2);

    let mut total_fill_rate = 0.0_f64;
    let mut loop_counter = 0_u64;
    let mut tested_eoq = 0.0_f64;

    while tested_eoq < p.eoq + step {
        let safety_stock = p.buffer + tested_eoq - p.forecast_over_lt * p.avg_size;

        let demand_variation = if p.mad.is_nan() || p.mad == 0.0 {
            p.dmd_stddev
        } else {
            1.25 * p.mad * p.forecast_over_lt * p.avg_size
                * (1.0 / lead_time_month.max(1e-9)).sqrt()
        };

        let demand_variation_portion = (demand_variation.powi(2) * p.lead_time).sqrt();
        let racine = (demand_variation_portion + lead_time_deviation_portion).sqrt();

        let fill_rate = if racine > 1e-12 {
            normal_cdf(safety_stock / racine)
        } else {
            if safety_stock >= 0.0 { 1.0 } else { 0.0 }
        };

        total_fill_rate += fill_rate;
        tested_eoq += step;
        loop_counter += 1;
    }

    if loop_counter == 0 {
        0.0
    } else {
        total_fill_rate / loop_counter as f64
    }
}

/// Simple normal CDF fill-rate (fallback when MAD and dmd_stddev are both zero).
///
/// Mirrors Python `normal_distribution_ROP2FR`.
fn normal_rop_to_fr(p: &FillRateParams) -> f64 {
    let step = eoq_step(p.avg_size, p.eoq) as f64;
    let mut total_fill_rate = 0.0_f64;
    let mut loop_counter = 0_u64;
    let mut tested_eoq = 0.0_f64;

    while tested_eoq < p.eoq + step {
        let max_eoq = p.eoq.min(tested_eoq);
        // norm.cdf((ROP + max_eoq) / avg_size, mean=forecastlt, std=dmd_stddev)
        // Here dmd_stddev == 0 so we default std to forecast (as Python does)
        let std = if p.dmd_stddev > 1e-12 { p.dmd_stddev } else { p.forecast_over_lt.max(1e-9) };
        let z = ((p.buffer + max_eoq) / p.avg_size.max(1e-9) - p.forecast_over_lt) / std;
        total_fill_rate += normal_cdf(z);
        tested_eoq += step;
        loop_counter += 1;
    }

    if loop_counter == 0 { 0.0 } else { total_fill_rate / loop_counter as f64 }
}

/// Poisson fill-rate calculation.
///
/// Mirrors Python `poisson_distribution_ROP2FR`.
fn poisson_rop_to_fr(p: &FillRateParams) -> f64 {
    let step = eoq_step(p.avg_size, p.eoq) as f64;
    let mut total_fill_rate = 0.0_f64;
    let mut loop_counter = 0_u64;
    let mut tested_eoq = 0.0_f64;

    while tested_eoq < p.eoq + step {
        let max_eoq = p.eoq.min(tested_eoq);
        // poisson.cdf((rop + max_eoq) / avg_size, forecastlt)
        let k = (p.buffer + max_eoq) / p.avg_size.max(1e-9);
        total_fill_rate += poisson_cdf(k, p.forecast_over_lt);
        tested_eoq += step;
        loop_counter += 1;
    }

    if loop_counter == 0 { 0.0 } else { total_fill_rate / loop_counter as f64 }
}

/// Generic fitted-distribution fill-rate: same EOQ loop but using an arbitrary CDF.
fn fitted_cdf_rop_to_fr<F>(p: &FillRateParams, cdf: F) -> f64
where
    F: Fn(f64) -> f64,
{
    let step = eoq_step(p.avg_size, p.eoq) as f64;
    let mut total_fill_rate = 0.0_f64;
    let mut loop_counter = 0_u64;
    let mut tested_eoq = 0.0_f64;

    while tested_eoq < p.eoq + step {
        let max_eoq = p.eoq.min(tested_eoq);
        let x = (p.buffer + max_eoq) / p.avg_size.max(1e-9);
        total_fill_rate += cdf(x);
        tested_eoq += step;
        loop_counter += 1;
    }

    if loop_counter == 0 { 0.0 } else { total_fill_rate / loop_counter as f64 }
}

/// Inverse Poisson fill-rate: find the ROP that achieves a target fill rate.
///
/// Mirrors Python `poisson_distribution_FR2ROP`.
pub fn poisson_fr_to_rop(target_fr: f64, eoq: f64, forecast_lt: f64, avg_size: f64) -> f64 {
    use statrs::distribution::{DiscreteCDF, Poisson};
    if forecast_lt <= 0.0 {
        return 0.0;
    }
    // ppf = Poisson.ppf(target_fr, mean) * avg_size - eoq/2
    let dist = Poisson::new(forecast_lt).unwrap_or_else(|_| panic!("forecast_lt={forecast_lt}"));
    // Binary search for k such that P(X ≤ k) >= target_fr
    let mut k = 0u64;
    while dist.cdf(k) < target_fr && k < 1_000_000 {
        k += 1;
    }
    k as f64 * avg_size - eoq / 2.0
}

/// Inverse normal fill-rate: find the ROP that achieves a target fill rate.
///
/// Mirrors Python `normal_distribution_FR2ROP`.
pub fn normal_fr_to_rop(
    target_fr: f64,
    _lead_time: f64,
    forecast_lt: f64,
    avg_size: f64,
    lead_time_stddev: f64,
    _dmd_stddev: f64,
) -> f64 {
    // norm.ppf(FR, loc=forecastlt, scale=lead_time_stddev) * avg_size
    let lt_std = if lead_time_stddev.is_nan() || lead_time_stddev <= 0.0 {
        forecast_lt.max(1e-9)
    } else {
        lead_time_stddev
    };
    (forecast_lt + normal_inv(target_fr) * lt_std) * avg_size
}

impl DistributionType {
    /// CDF dispatch used internally — thin wrapper around `fill_rate_for_rop`.
    pub fn cdf_at(&self, x: f64, avg_size: f64, eoq: f64, forecast_lt: f64) -> f64 {
        let params = FillRateParams {
            buffer: x * avg_size,
            forecast_over_lt: forecast_lt,
            eoq,
            avg_size,
            lead_time: 30.0, // not used for fitted; irrelevant
            lead_time_stddev: f64::NAN,
            dmd_stddev: 0.0,
            mad: f64::NAN,
        };
        fill_rate_for_rop(self, &params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_params() -> FillRateParams {
        FillRateParams {
            buffer: 10.0,
            forecast_over_lt: 5.0,
            eoq: 10.0,
            avg_size: 1.0,
            lead_time: 30.0,
            lead_time_stddev: f64::NAN,
            dmd_stddev: 2.0,
            mad: f64::NAN,
        }
    }

    #[test]
    fn poisson_fill_rate_in_range() {
        let p = default_params();
        let fr = fill_rate_for_rop(&DistributionType::Poisson, &p);
        assert!(fr > 0.0 && fr <= 1.0, "fr={fr}");
    }

    #[test]
    fn normal_fill_rate_in_range() {
        let p = default_params();
        let fr = fill_rate_for_rop(&DistributionType::Normal, &p);
        assert!(fr > 0.0 && fr <= 1.0, "fr={fr}");
    }

    #[test]
    fn higher_buffer_gives_higher_fill_rate() {
        let low = FillRateParams { buffer: 5.0, ..default_params() };
        let high = FillRateParams { buffer: 20.0, ..default_params() };
        let fr_low = fill_rate_for_rop(&DistributionType::Poisson, &low);
        let fr_high = fill_rate_for_rop(&DistributionType::Poisson, &high);
        assert!(fr_high >= fr_low, "fr_high={fr_high} fr_low={fr_low}");
    }
}
