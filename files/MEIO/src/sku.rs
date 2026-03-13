//! SKU data structures for the MEIO optimizer.
//!
//! A `SkuRecord` holds all per-SKU data that the optimizer reads or writes
//! during a run.  The fields map closely to the columns returned by the
//! SQL query in the Python `MEIO.MainLoop` method.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::distributions::DistributionType;

/// Composite key for a SKU: (item_id, site_id).
pub type SkuKey = (i64, i64);

// ── RepairFlow ─────────────────────────────────────────────────────────────

/// Repair / return flow parameters for a SKU.
///
/// When populated, the optimizer uses net demand (gross demand minus
/// the serviceable return stream) and credits WIP units in repair
/// against the required safety stock.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RepairFlow {
    /// Fraction of demand units that are returned for repair (0–1).
    #[serde(default)]
    pub return_rate: f64,
    /// Fraction of returned units that pass repair and re-enter
    /// serviceable inventory.  `1 − repair_yield` = condemnation rate.
    #[serde(default = "default_one")]
    pub repair_yield: f64,
    /// Mean repair turnaround time in the same units as `leg_lead_time`.
    #[serde(default)]
    pub repair_tat_mean: f64,
    /// Coefficient of variation of repair TAT.
    #[serde(default)]
    pub repair_tat_cv: f64,
    /// Units currently in the repair pipeline (WIP). Counted as
    /// expected serviceable returns and credited against the ROP.
    #[serde(default)]
    pub wip_qty: f64,
}

/// Convenience: format a `SkuKey` as `"item_id:site_id"` for logging / maps.
pub fn key_to_string(k: &SkuKey) -> String {
    format!("{}:{}", k.0, k.1)
}

// ── DependantChange ───────────────────────────────────────────────────────

/// A pending update to a dependant SKU that was computed during
/// `MarginalValue` and will be committed once the main SKU is chosen.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependantChange {
    /// Which SKU is affected.
    pub key: SkuKey,
    /// The new fill rate for this dependant SKU.
    pub new_fill_rate: f64,
    /// The new effective wait time (probabilistic lead-time contribution).
    pub new_wait_time: f64,
}

// ── TargetGroupRef ────────────────────────────────────────────────────────

/// A reference to the target group that this SKU belongs to, together with
/// the SKU's weight within the group.
///
/// Corresponds to one element of the `j_target_groups` JSON array on the SKU.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetGroupRef {
    /// Name of the group (matches `GroupTarget::group_name`).
    pub io_tgt_group: String,
    /// This SKU's participation weight in the group (fraction of group demand).
    #[serde(default = "default_one")]
    pub group_participation: f64,
}

fn default_one() -> f64 { 1.0 }

// ── SkuRecord ─────────────────────────────────────────────────────────────

/// All per-SKU data consumed and produced by the MEIO greedy algorithm.
///
/// Field names and ordering follow the Python pandas DataFrame columns to
/// make cross-referencing the original code straightforward.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkuRecord {
    // ── Identity ──────────────────────────────────────────────────────────
    pub item_id: i64,
    pub site_id: i64,

    // ── Demand ────────────────────────────────────────────────────────────
    /// Total demand rate (line units per day).
    pub total_demand_rate: f64,
    /// Direct (own) demand rate (line units per day).
    pub direct_demand_rate: f64,
    /// Indirect demand rate driven by kits / parent assemblies.
    pub indirect_demand_rate: f64,
    /// Average transaction / order size.
    pub avg_size: f64,

    // ── Supply ────────────────────────────────────────────────────────────
    /// Economic order quantity.
    pub eoq: f64,
    /// Leg (own echelon) lead time in days.
    pub leg_lead_time: f64,
    /// Total lead time (leg + upstream wait) in days.
    pub total_lead_time: f64,
    /// Wait time coming from the upstream replenishment site.
    pub wait_time: f64,
    /// Current effective wait time (updated as algorithm commits upstream SKUs).
    pub current_wait_time: f64,

    // ── Variability ───────────────────────────────────────────────────────
    /// Standard deviation of demand.
    pub dmd_stddev: f64,
    /// Standard deviation of lead time.
    pub lt_stddev: f64,
    /// Mean Absolute Deviation of demand.
    pub mad: f64,
    /// Coefficient of variation of demand.
    pub dmd_coefficient_of_variation: f64,
    /// Cap on the coefficient of variation.
    pub varcoeff_max: f64,

    // ── Financials ────────────────────────────────────────────────────────
    pub unit_cost: f64,

    // ── Distribution ─────────────────────────────────────────────────────
    /// Number of SKUs in the distribution sample (used for Normal/Poisson selection).
    pub sku_count: i64,
    /// Monthly forecast quantity (line units).
    pub total_fcst_monthly: f64,
    /// Current on-hand inventory.
    pub on_hand: f64,

    // ── Fitted distribution (from Distributions pipeline step) ────────────
    /// If the Distributions step produced a fitted distribution for this SKU,
    /// it is stored here.  When `Some(...)`, Normal/Poisson fall-back is skipped.
    #[serde(default)]
    pub fitted_distribution: Option<DistributionType>,

    // ── Network topology ─────────────────────────────────────────────────
    /// Site IDs that this SKU replenishes from (downstream replenishment chain).
    #[serde(default)]
    pub repl_site_ids: Vec<i64>,
    /// Item IDs that are kits composed (partially) of this SKU.
    #[serde(default)]
    pub kits: Vec<i64>,
    /// Component item IDs that compose this kit SKU.
    #[serde(default)]
    pub components: Vec<i64>,
    /// Parent site IDs (upstream supply locations).
    #[serde(default)]
    pub parent_ids: Vec<i64>,

    // ── Targets ───────────────────────────────────────────────────────────
    pub sku_max_fill_rate: f64,
    pub sku_min_fill_rate: f64,
    #[serde(default = "default_large")]
    pub sku_max_sl_qty: f64,
    #[serde(default)]
    pub sku_min_sl_qty: f64,
    #[serde(default = "default_large")]
    pub sku_max_sl_slices: f64,
    #[serde(default)]
    pub sku_min_sl_slices: f64,
    #[serde(default)]
    pub use_existing_inventory: bool,
    /// Pre-computed: min(max_fill_rate, min_fill_rate) — the effective target.
    pub sku_tgt_fillrate: f64,

    // ── Group membership ──────────────────────────────────────────────────
    /// Groups this SKU belongs to, with per-group participation weights.
    #[serde(default)]
    pub j_target_groups: Vec<TargetGroupRef>,
    /// Overall group participation weight.
    #[serde(default = "default_one")]
    pub group_participation: f64,

    // ── Optimizer state (mutable during the run) ──────────────────────────
    /// Buffer currently committed (absolute units).
    #[serde(default)]
    pub committed_buffer: f64,
    /// Proposed new buffer being evaluated.
    #[serde(default = "default_neg_one")]
    pub new_buffer: f64,
    /// Current fill rate (after all committed steps so far).
    #[serde(default)]
    pub current_fill_rate: f64,
    /// Proposed fill rate at `new_buffer`.
    #[serde(default = "default_neg_one")]
    pub new_fill_rate: f64,
    /// Marginal value: gain / investment — the greedy selection criterion.
    #[serde(default)]
    pub marginal_value: f64,
    /// Pending dependant changes associated with the proposed new_buffer.
    #[serde(default)]
    pub dependant_changes: Vec<DependantChange>,
    /// Whether this SKU's initial ASL (min fill rate / min qty) has been set.
    #[serde(default)]
    pub sku_init_set: bool,

    /// Scenario ID (passed through to output for traceability).
    pub scenario_id: Option<i64>,

    // ── Repair / return flow ──────────────────────────────────────────────
    /// Optional repair flow parameters.  When `Some`, the optimizer
    /// adjusts effective demand and credits WIP inventory.
    #[serde(default)]
    pub repair_flow: Option<RepairFlow>,

    // ── Asset pool mode ───────────────────────────────────────────────────
    /// When `true`, this SKU is treated as a rotable / capital asset.
    /// The optimizer uses Erlang-B pool-shortage probability instead of
    /// the standard fill-rate formula.
    #[serde(default)]
    pub asset_mode: bool,
    /// Criticality multiplier applied to the marginal-value ranking in
    /// asset mode.  Values > 1 elevate this asset above cheaper items.
    #[serde(default = "default_one")]
    pub criticality: f64,
}

fn default_neg_one() -> f64 { -1.0 }
fn default_large() -> f64 { 99_999_999.0 }

impl SkuRecord {
    /// Composite key for this record.
    #[inline]
    pub fn key(&self) -> SkuKey {
        (self.item_id, self.site_id)
    }

    /// Resolve the active distribution type for this SKU.
    pub fn effective_distribution(&self, distribution_threshold: i64) -> DistributionType {
        DistributionType::resolve(
            self.fitted_distribution.as_ref(),
            self.sku_count,
            distribution_threshold,
        )
    }
}

// ── SkuResult (output record) ────────────────────────────────────────────

/// Result produced for one SKU after the optimizer run completes.
/// Mirrors the columns written to `custom.io_sku_output` in the Python code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkuResult {
    pub item_id: i64,
    pub site_id: i64,
    pub committed_buffer: f64,
    pub current_fill_rate: f64,
    pub marginal_value: f64,
    pub scenario_id: Option<i64>,
    /// Total inventory investment: `committed_buffer × unit_cost`.
    pub inventory_value: f64,
}

impl SkuResult {
    pub fn from_record(r: &SkuRecord) -> Self {
        SkuResult {
            item_id: r.item_id,
            site_id: r.site_id,
            committed_buffer: r.committed_buffer,
            current_fill_rate: r.current_fill_rate,
            marginal_value: r.marginal_value,
            scenario_id: r.scenario_id,
            inventory_value: r.committed_buffer * r.unit_cost,
        }
    }
}

// ── GroupResult (output record) ──────────────────────────────────────────

/// Result produced for one target group after the optimizer run completes.
/// Mirrors the columns written to `custom.io_group_output`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupResult {
    pub group_name: String,
    pub achieved_fill_rate: f64,
    pub fill_rate_target: f64,
    pub achieved_budget: f64,
    pub max_budget: f64,
    pub completed: bool,
}

// ── SkuStore ─────────────────────────────────────────────────────────────

/// The in-memory store for all SKU records, indexed by `SkuKey`.
pub type SkuStore = HashMap<SkuKey, SkuRecord>;

/// Deserialize a list of SKU records from a JSON array string.
pub fn skus_from_json(json: &str) -> Result<SkuStore, serde_json::Error> {
    let records: Vec<SkuRecord> = serde_json::from_str(json)?;
    Ok(records.into_iter().map(|r| (r.key(), r)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sku() -> SkuRecord {
        SkuRecord {
            item_id: 1,
            site_id: 10,
            total_demand_rate: 5.0,
            direct_demand_rate: 5.0,
            indirect_demand_rate: 0.0,
            avg_size: 1.0,
            eoq: 10.0,
            leg_lead_time: 30.0,
            total_lead_time: 30.0,
            wait_time: 0.0,
            current_wait_time: 0.0,
            dmd_stddev: 2.0,
            lt_stddev: 0.0,
            mad: f64::NAN,
            dmd_coefficient_of_variation: 0.4,
            varcoeff_max: 2.0,
            unit_cost: 100.0,
            sku_count: 30,
            total_fcst_monthly: 150.0,
            on_hand: 0.0,
            fitted_distribution: None,
            repl_site_ids: vec![],
            kits: vec![],
            components: vec![],
            parent_ids: vec![],
            sku_max_fill_rate: 1.0,
            sku_min_fill_rate: 0.0,
            sku_max_sl_qty: 99_999_999.0,
            sku_min_sl_qty: 0.0,
            sku_max_sl_slices: 99_999_999.0,
            sku_min_sl_slices: 0.0,
            use_existing_inventory: false,
            sku_tgt_fillrate: 0.0,
            j_target_groups: vec![],
            group_participation: 1.0,
            committed_buffer: 0.0,
            new_buffer: -1.0,
            current_fill_rate: 0.0,
            new_fill_rate: -1.0,
            marginal_value: 0.0,
            dependant_changes: vec![],
            sku_init_set: false,
            scenario_id: Some(1),
            repair_flow: None,
            asset_mode: false,
            criticality: 1.0,
        }
    }

    #[test]
    fn key_roundtrip() {
        let s = make_sku();
        assert_eq!(s.key(), (1, 10));
        assert_eq!(key_to_string(&s.key()), "1:10");
    }

    #[test]
    fn effective_distribution_normal() {
        let s = make_sku(); // sku_count=30, threshold=25 → Normal
        let dist = s.effective_distribution(25);
        assert!(matches!(dist, DistributionType::Normal));
    }

    #[test]
    fn effective_distribution_poisson() {
        let mut s = make_sku();
        s.sku_count = 10; // < 25 → Poisson
        let dist = s.effective_distribution(25);
        assert!(matches!(dist, DistributionType::Poisson));
    }

    #[test]
    fn effective_distribution_fitted_wins() {
        let mut s = make_sku();
        s.fitted_distribution = Some(DistributionType::Gamma { shape: 2.0, scale: 1.5 });
        let dist = s.effective_distribution(25);
        assert!(matches!(dist, DistributionType::Gamma { .. }));
    }
}
