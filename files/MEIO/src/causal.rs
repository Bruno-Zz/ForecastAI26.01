//! Monte-Carlo uncertainty propagation for causal demand (Phase 2).
//!
//! When utilisation projections are themselves forecasted (not given), they
//! carry forecast uncertainty.  This module draws `n_samples` from the
//! utilisation distribution for each asset, multiplies by the MDFH (also
//! sampled), sums across assets, and fits a parametric distribution to the
//! resulting demand histogram.

use serde::{Deserialize, Serialize};

/// Per-(item, site, scenario) inputs for Monte-Carlo causal uncertainty.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalUncertaintyParams {
    pub item_id:     i64,
    pub site_id:     i64,
    pub scenario_id: Option<i64>,

    /// Mean of total utilisation (sum over all assets at this site for this driver)
    pub util_mean:   f64,
    /// Std dev of total utilisation (from forecast interval)
    pub util_stddev: f64,

    /// Mean of the MDFH (demand per unit utilisation)
    pub mdfh_mean:   f64,
    /// Std dev of MDFH (from fitter uncertainty)
    pub mdfh_stddev: f64,

    /// Deterministic scheduled demand (added after MC draw)
    pub scheduled_demand: f64,
}

/// Output record for one (item, site, scenario).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalUncertaintyResult {
    pub item_id:           i64,
    pub site_id:           i64,
    pub scenario_id:       Option<i64>,
    pub demand_mean:       f64,
    pub demand_stddev:     f64,
    pub demand_cv:         f64,
    /// JSON string of the fitted DistributionType (passed straight to SkuRecord)
    pub distribution_json: String,
}

/// Run Monte-Carlo uncertainty propagation for a single parameter set.
///
/// Draws `n_samples` from Normal(util_mean, util_stddev).max(0) ×
/// Normal(mdfh_mean, mdfh_stddev).max(0), adds scheduled_demand, and
/// returns the sample statistics.
pub fn run_mc(params: &CausalUncertaintyParams, n_samples: u32) -> CausalUncertaintyResult {
    use rand::SeedableRng;
    use rand_distr::{Distribution, Normal};

    let n = n_samples.max(10) as usize;
    let mut rng = rand::rngs::SmallRng::from_entropy();

    // Build distributions; fall back to degenerate (delta at mean) if stddev <= 0
    let util_dist: Option<Normal<f64>> = if params.util_stddev > 0.0 {
        Normal::new(params.util_mean, params.util_stddev).ok()
    } else {
        None
    };

    let mdfh_dist: Option<Normal<f64>> = if params.mdfh_stddev > 0.0 {
        Normal::new(params.mdfh_mean, params.mdfh_stddev).ok()
    } else {
        None
    };

    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let u = match &util_dist {
            Some(d) => d.sample(&mut rng).max(0.0),
            None    => params.util_mean.max(0.0),
        };
        let m = match &mdfh_dist {
            Some(d) => d.sample(&mut rng).max(0.0),
            None    => params.mdfh_mean.max(0.0),
        };
        samples.push(u * m + params.scheduled_demand);
    }

    // Compute mean and stddev from samples
    let n_f = n as f64;
    let mean = samples.iter().sum::<f64>() / n_f;
    let var  = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n_f;
    let stddev = var.sqrt();
    let cv   = if mean > 1e-12 { stddev / mean } else { 0.0 };

    // Fit distribution: use Gamma when mean > 0 and CV is in a reasonable range
    let distribution_json = if mean > 0.0 && cv > 0.01 && cv < 10.0 {
        // Method-of-moments Gamma: shape = (mean/stddev)^2, scale = stddev^2/mean
        let shape = (mean / stddev.max(1e-12)).powi(2);
        let scale = stddev.powi(2) / mean.max(1e-12);
        format!(r#"{{"Gamma":{{"shape":{shape:.6},"scale":{scale:.6}}}}}"#)
    } else if mean > 0.0 {
        format!(r#"{{"Normal":{{"mean":{mean:.6},"std_dev":{std:.6}}}}}"#,
                mean = mean, std = stddev)
    } else {
        r#"{"Poisson":{"lambda":0.0}}"#.to_string()
    };

    CausalUncertaintyResult {
        item_id:           params.item_id,
        site_id:           params.site_id,
        scenario_id:       params.scenario_id,
        demand_mean:       mean,
        demand_stddev:     stddev,
        demand_cv:         cv,
        distribution_json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mc_deterministic_when_stddev_zero() {
        let p = CausalUncertaintyParams {
            item_id: 1, site_id: 1, scenario_id: Some(0),
            util_mean: 100.0, util_stddev: 0.0,
            mdfh_mean: 0.05, mdfh_stddev: 0.0,
            scheduled_demand: 2.0,
        };
        let r = run_mc(&p, 500);
        // demand = 100 * 0.05 + 2 = 7; stddev should be ~0
        assert!((r.demand_mean - 7.0).abs() < 0.01, "mean={}", r.demand_mean);
        assert!(r.demand_stddev < 0.01, "stddev={}", r.demand_stddev);
    }

    #[test]
    fn mc_positive_uncertainty() {
        let p = CausalUncertaintyParams {
            item_id: 2, site_id: 1, scenario_id: None,
            util_mean: 100.0, util_stddev: 10.0,
            mdfh_mean: 0.05, mdfh_stddev: 0.005,
            scheduled_demand: 0.0,
        };
        let r = run_mc(&p, 2000);
        assert!(r.demand_mean > 0.0);
        assert!(r.demand_stddev > 0.0);
    }
}
