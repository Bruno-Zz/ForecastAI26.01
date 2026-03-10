//! `meio_optimizer` — PyO3 Python extension.
//!
//! ## Usage from Python
//!
//! ```python
//! import meio_optimizer
//!
//! result = meio_optimizer.run_optimization(
//!     skus_json=json.dumps(sku_list),           # list of SKU dicts
//!     config_json=json.dumps(config_dict),       # MeioConfig dict
//!     group_targets_json=json.dumps(targets),    # list of GroupTarget dicts
//! )
//! # result is a dict: {"sku_results": [...], "group_results": [...], "iterations": N}
//!
//! # Or: check method compatibility
//! compat = meio_optimizer.check_compatibility(
//!     skus_json=...,
//!     config_json=...,
//! )
//! ```
//!
//! ## Data contract
//!
//! ### Input: `skus_json`
//! JSON array of objects matching `SkuRecord` (see `sku.rs`).
//! Each object must include at minimum:
//! - `item_id`, `site_id`
//! - `total_demand_rate`, `direct_demand_rate`, `avg_size`, `eoq`
//! - `leg_lead_time`, `total_lead_time`
//! - `unit_cost`, `sku_count`
//! - `sku_max_fill_rate`, `sku_min_fill_rate`
//! - `j_target_groups`: array of `{io_tgt_group: str, group_participation: f64}`
//! - Optional: `fitted_distribution` — if present, overrides Normal/Poisson selection
//!
//! ### Input: `config_json`
//! JSON object matching `MeioConfig`.  Key fields:
//! - `parallel_workers`: int (0 = auto)
//! - `distribution_threshold`: int (default 25)
//! - `precision_jump`: float (0 = per-unit steps)
//! - `big_jump_threshold`: float (default 0.95)
//!
//! ### Input: `group_targets_json`
//! JSON array of `{group_name: str, fill_rate_target: float, max_budget: float}`.
//!
//! ### Output
//! JSON string: `{"sku_results": [...], "group_results": [...], "iterations": N}`

// These allow attributes suppress warnings for utility functions and types that
// are part of the public API but not yet exercised from within the crate itself.
#![allow(dead_code)]
#![allow(unused_imports)]

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

mod config;
mod distributions;
mod marginal;
mod math;
mod optimizer;
mod sku;
mod target;

use crate::config::{GroupTarget, MeioConfig};
use crate::optimizer::Optimizer;
use crate::sku::skus_from_json;

// ── run_optimization ──────────────────────────────────────────────────────

/// Run the MEIO greedy optimization.
///
/// All inputs and outputs are JSON strings for maximum interoperability with
/// the Python pipeline.
///
/// Returns a JSON string with keys `sku_results`, `group_results`, `iterations`.
#[pyfunction]
#[pyo3(signature = (skus_json, config_json, group_targets_json))]
fn run_optimization(
    skus_json: &str,
    config_json: &str,
    group_targets_json: &str,
) -> PyResult<String> {
    // 1. Parse inputs
    let skus = skus_from_json(skus_json)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse skus_json: {e}")))?;

    let cfg: MeioConfig = serde_json::from_str(config_json)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse config_json: {e}")))?;

    let group_targets: Vec<GroupTarget> = serde_json::from_str(group_targets_json)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse group_targets_json: {e}")))?;

    // 2. Configure Rayon thread pool if workers is explicitly set
    if cfg.parallel_workers > 0 {
        // Try to build a dedicated pool; fall back to global if already built
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(cfg.parallel_workers)
            .build_global();
    }

    // 3. Run optimization
    let optimizer = Optimizer::new(cfg, group_targets);
    let result = optimizer
        .run(skus)
        .map_err(|e| PyValueError::new_err(format!("Optimization failed: {e}")))?;

    // 4. Serialize output
    let out = serde_json::json!({
        "sku_results":   result.sku_results,
        "group_results": result.group_results,
        "iterations":    result.iterations,
    });
    serde_json::to_string(&out)
        .map_err(|e| PyValueError::new_err(format!("Failed to serialize result: {e}")))
}

// ── run_optimization_batch ────────────────────────────────────────────────

/// Run the MEIO optimization for multiple scopes in parallel.
///
/// `batches_json` is a JSON array of objects, each containing:
/// - `skus_json`: string (JSON array of SKUs for this scope)
/// - `config_json`: string (MeioConfig for this scope)
/// - `group_targets_json`: string (GroupTargets for this scope)
///
/// Returns a JSON array of result objects (same structure as `run_optimization`).
#[pyfunction]
#[pyo3(signature = (batches_json))]
fn run_optimization_batch(batches_json: &str) -> PyResult<String> {
    use rayon::prelude::*;

    #[derive(serde::Deserialize)]
    struct Batch {
        skus_json: String,
        config_json: String,
        group_targets_json: String,
    }

    let batches: Vec<Batch> = serde_json::from_str(batches_json)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse batches_json: {e}")))?;

    let results: Result<Vec<String>, PyErr> = batches
        .par_iter()
        .map(|b| run_optimization(&b.skus_json, &b.config_json, &b.group_targets_json))
        .collect();

    let results = results?;

    // Wrap as a JSON array of already-serialized JSON strings → parse and re-serialize
    let parsed: Vec<serde_json::Value> = results
        .iter()
        .map(|s| serde_json::from_str(s).unwrap_or(serde_json::Value::Null))
        .collect();

    serde_json::to_string(&parsed)
        .map_err(|e| PyValueError::new_err(format!("Failed to serialize batch results: {e}")))
}

// ── fill_rate_for_buffer ──────────────────────────────────────────────────

/// Compute the fill rate for a given buffer/ROP.
///
/// Exposed to Python for inspection / simulation purposes.
///
/// Arguments:
/// - `buffer`: ROP in absolute units
/// - `forecast_over_lt`: forecast over lead time (line units)
/// - `eoq`: economic order quantity
/// - `avg_size`: average order size
/// - `lead_time`: lead time in days
/// - `lead_time_stddev`: std dev of lead time (NaN if unknown)
/// - `dmd_stddev`: std dev of demand
/// - `mad`: mean absolute deviation (NaN if unknown)
/// - `distribution_json`: JSON string of a `DistributionType` variant
///
/// Returns the fill rate as a float.
#[pyfunction]
#[pyo3(signature = (
    buffer,
    forecast_over_lt,
    eoq,
    avg_size,
    lead_time,
    lead_time_stddev,
    dmd_stddev,
    mad,
    distribution_json
))]
fn fill_rate_for_buffer(
    buffer: f64,
    forecast_over_lt: f64,
    eoq: f64,
    avg_size: f64,
    lead_time: f64,
    lead_time_stddev: f64,
    dmd_stddev: f64,
    mad: f64,
    distribution_json: &str,
) -> PyResult<f64> {
    use crate::distributions::{fill_rate_for_rop, DistributionType, FillRateParams};

    let dist: DistributionType = serde_json::from_str(distribution_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid distribution_json: {e}")))?;

    let params = FillRateParams {
        buffer,
        forecast_over_lt,
        eoq,
        avg_size,
        lead_time,
        lead_time_stddev,
        dmd_stddev,
        mad,
    };

    Ok(fill_rate_for_rop(&dist, &params))
}

// ── Module definition ─────────────────────────────────────────────────────

/// Python module definition.
#[pymodule]
fn meio_optimizer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_optimization, m)?)?;
    m.add_function(wrap_pyfunction!(run_optimization_batch, m)?)?;
    m.add_function(wrap_pyfunction!(fill_rate_for_buffer, m)?)?;

    // Expose version constant
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
