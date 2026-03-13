//! Configuration tree for the MEIO optimizer.
//!
//! ## Hierarchy
//!
//! ```text
//! MeioConfig
//! ├── OptimizationScope      (1 or more scopes)
//! │   ├── segment_ids        → which segments belong to this scope
//! │   └── param_ids          → which OptimizationParameters apply
//! └── OptimizationParameters (1 or more param sets)
//!     └── targets: HashMap<SkuKey, SkuTarget>
//! ```
//!
//! Each SKU belongs to exactly **one** scope (resolved via segment membership).
//! Each SKU can have **multiple** target-group entries (from different
//! `OptimizationParameters`), with a priority to handle budget conflicts.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Top-level configuration passed from Python into `run_optimization`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeioConfig {
    /// Named optimization scopes.
    pub scopes: Vec<OptimizationScope>,

    /// Named optimization parameter sets.
    pub optimization_params: Vec<OptimizationParameters>,

    /// Number of parallel workers (= CPUs).  0 means auto-detect.
    #[serde(default)]
    pub parallel_workers: usize,

    /// SKU count threshold: if `sku_count > threshold` use Normal else Poisson.
    /// Only used when no fitted distribution is available.
    #[serde(default = "default_distribution_threshold")]
    pub distribution_threshold: i64,

    /// Whether to account for Economic Order Quantity in fill-rate calculation.
    #[serde(default = "default_true")]
    pub consider_eoq: bool,

    /// Whether to use line fill rate (demand divided by avg_size).
    #[serde(default = "default_true")]
    pub line_fill_rate: bool,

    /// Precision-jump parameter.  0 = small per-unit jumps (slower but accurate).
    /// > 0 enables the "long-jump" heuristic that leaps towards `big_jump_threshold`
    /// in one step when the current fill rate is below the threshold.
    #[serde(default)]
    pub precision_jump: f64,

    /// Fill-rate threshold below which the long-jump heuristic is applied.
    #[serde(default = "default_big_jump")]
    pub big_jump_threshold: f64,

    /// Asset pool configurations (for rotable / capital asset sizing).
    /// Processed independently from the main greedy loop via Erlang-B/C.
    #[serde(default)]
    pub asset_targets: Vec<AssetTarget>,
}

fn default_distribution_threshold() -> i64 { 25 }
fn default_true() -> bool { true }
fn default_big_jump() -> f64 { 0.95 }

impl MeioConfig {
    /// Resolve the number of parallel workers (1 if precision_jump > 0 implies
    /// long-jump and the caller may want deterministic results; otherwise the
    /// configured value or the number of logical CPUs).
    pub fn effective_workers(&self) -> usize {
        if self.parallel_workers == 0 {
            num_cpus()
        } else {
            self.parallel_workers.max(1)
        }
    }
}

/// Determines how many SKUs to evaluate in parallel on each greedy step.
fn num_cpus() -> usize {
    // Use rayon's global thread pool size as the default.
    rayon::current_num_threads().max(1)
}

// ── OptimizationScope ─────────────────────────────────────────────────────

/// Groups SKUs by segment and links them to optimization parameter sets.
///
/// A scope represents a "planning scenario" — for example "all warehouse A
/// SKUs using the Q2-budget constraint".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationScope {
    /// Unique identifier for this scope.
    pub scope_id: String,

    /// Segment IDs that belong to this scope.
    /// SKU → scope resolution: the first scope whose segment list contains
    /// the SKU's segment wins.
    pub segment_ids: Vec<String>,

    /// Optimization parameter set IDs that apply to this scope.
    /// Multiple param sets allow a SKU to chase several targets simultaneously
    /// (e.g. "fill-rate target" + "budget cap").
    pub param_ids: Vec<String>,
}

// ── OptimizationParameters ────────────────────────────────────────────────

/// A named set of per-SKU inventory targets.
///
/// Because a SKU can belong to multiple `OptimizationParameters` at the same
/// time (multi-target), targets are weighted by `priority` when constraints
/// conflict.  Higher priority = enforced first.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationParameters {
    /// Unique identifier for this parameter set.
    pub param_id: String,

    /// Human-readable name.
    pub name: String,

    /// Per-SKU target overrides.
    /// Key: `"item_id:site_id"` string.
    #[serde(default)]
    pub sku_targets: HashMap<String, SkuTarget>,

    /// Default target applied to SKUs not in `sku_targets`.
    #[serde(default)]
    pub default_target: Option<SkuTarget>,

    /// Group-level targets (fill rate + budget).
    #[serde(default)]
    pub group_targets: Vec<GroupTarget>,
}

impl OptimizationParameters {
    /// Resolve the target for a specific SKU key (`"item_id:site_id"`).
    pub fn target_for(&self, key: &str) -> Option<&SkuTarget> {
        self.sku_targets
            .get(key)
            .or(self.default_target.as_ref())
    }
}

// ── Per-SKU target ────────────────────────────────────────────────────────

/// Constraints and objectives for a single SKU.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SkuTarget {
    /// Priority: higher value = enforced first when budgets are tight.
    #[serde(default)]
    pub priority: i32,

    /// Minimum allowed fill rate.  0 = no minimum.
    #[serde(default)]
    pub min_fill_rate: f64,

    /// Maximum allowed fill rate.  1 = no cap.
    #[serde(default = "default_one")]
    pub max_fill_rate: f64,

    /// Maximum ROP quantity (absolute units).
    #[serde(default = "default_large")]
    pub max_sl_qty: f64,

    /// Minimum ROP quantity (absolute units).
    #[serde(default)]
    pub min_sl_qty: f64,

    /// Maximum stock cover in calendar months.
    #[serde(default = "default_large")]
    pub max_sl_slices: f64,

    /// Minimum stock cover in calendar months.
    #[serde(default)]
    pub min_sl_slices: f64,

    /// Whether to count existing on-hand inventory towards the minimum.
    #[serde(default)]
    pub use_existing_inventory: bool,
}

fn default_one() -> f64 { 1.0 }
fn default_large() -> f64 { 99_999_999.0 }

// ── Group-level target ────────────────────────────────────────────────────

/// A service-level target for a group of SKUs (e.g. a product family or
/// an ABC classification).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupTarget {
    /// Unique name for this group, matching `j_target_groups[].io_tgt_group`
    /// in the SKU attributes.
    pub group_name: String,

    /// Target fill rate for the group (weighted average across all member SKUs).
    pub fill_rate_target: f64,

    /// Maximum total investment (budget) for this group.
    /// `f64::INFINITY` means no budget cap.
    #[serde(default = "default_inf")]
    pub max_budget: f64,
}

fn default_inf() -> f64 { f64::INFINITY }

// ── Asset pool target ─────────────────────────────────────────────────────

/// Configuration for a rotable / capital asset pool.
///
/// Pool sizing uses Erlang-B (loss system) or Erlang-C (delay system) to
/// determine the minimum pool size that achieves `target_availability`.
/// The offered load is `failure_rate_per_unit × fleet_size × repair_tat_mean`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetTarget {
    /// Unique asset identifier (matches `item_id:site_id` or a custom string).
    pub asset_id: String,

    /// Known fleet size.  `None` means the optimizer determines the minimum
    /// pool size required to achieve `target_availability`.
    #[serde(default)]
    pub fleet_size: Option<f64>,

    /// Expected failure events per unit per period (same units as `leg_lead_time`).
    #[serde(default)]
    pub failure_rate_per_unit: f64,

    /// Mean repair turnaround time (same units as `leg_lead_time`).
    pub repair_tat_mean: f64,

    /// Coefficient of variation of repair TAT.  0 = deterministic TAT.
    #[serde(default)]
    pub repair_tat_cv: f64,

    /// Target availability: P(asset available when demanded).  E.g. 0.95.
    #[serde(default = "default_availability")]
    pub target_availability: f64,

    /// Unit cost used for investment reporting.
    #[serde(default)]
    pub unit_cost: f64,

    /// Criticality multiplier applied to the marginal-value ranking.
    /// Values > 1 prioritise this asset over cheaper items in the greedy loop.
    #[serde(default = "default_one")]
    pub criticality: f64,

    /// Site IDs that share this asset pool (pooled stocking).
    #[serde(default)]
    pub pooled_sites: Vec<i64>,
}

fn default_availability() -> f64 { 0.95 }

/// Result of the asset pool sizing calculation for one asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetPoolResult {
    pub asset_id: String,
    pub recommended_pool_size: u32,
    /// Achieved availability at `recommended_pool_size` (Erlang-B shortage probability subtracted from 1).
    pub achieved_availability: f64,
    /// Investment at `unit_cost × recommended_pool_size`.
    pub investment: f64,
}

// ── Serialization helpers ─────────────────────────────────────────────────

impl MeioConfig {
    /// Deserialize from a JSON string (as passed from Python).
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Find the `OptimizationParameters` for a given param_id.
    pub fn find_params(&self, param_id: &str) -> Option<&OptimizationParameters> {
        self.optimization_params.iter().find(|p| p.param_id == param_id)
    }

    /// Collect all `GroupTarget` entries from all parameter sets.
    pub fn all_group_targets(&self) -> Vec<&GroupTarget> {
        self.optimization_params
            .iter()
            .flat_map(|p| p.group_targets.iter())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_roundtrip() {
        let cfg = MeioConfig {
            scopes: vec![],
            optimization_params: vec![],
            parallel_workers: 4,
            distribution_threshold: 25,
            consider_eoq: true,
            line_fill_rate: true,
            precision_jump: 0.0,
            big_jump_threshold: 0.95,
            asset_targets: vec![],
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: MeioConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.parallel_workers, 4);
        assert_eq!(back.distribution_threshold, 25);
    }

    #[test]
    fn effective_workers_auto_detect() {
        let cfg = MeioConfig {
            scopes: vec![],
            optimization_params: vec![],
            parallel_workers: 0,
            distribution_threshold: 25,
            consider_eoq: true,
            line_fill_rate: true,
            precision_jump: 0.0,
            big_jump_threshold: 0.95,
            asset_targets: vec![],
        };
        assert!(cfg.effective_workers() >= 1);
    }
}
