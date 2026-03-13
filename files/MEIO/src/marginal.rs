//! Core "MarginalValue" algorithm.
//!
//! This module is a **pure, side-effect-free** translation of the Python
//! `MEIO.MarginalValue` + `MEIO.group_completion_for_sku` methods.
//!
//! The key design difference from the Python version:
//! - Python mutates the DataFrame in-place inside `MarginalValue`.
//! - Rust returns an immutable `MarginalValueResult` — the optimizer applies
//!   the result to the store only when the SKU is chosen for commitment.
//!
//! This makes it safe to call `compute_marginal_value` in parallel for N
//! candidate SKUs without any locking.

use crate::config::MeioConfig;
use crate::distributions::{fill_rate_for_rop, poisson_fr_to_rop, FillRateParams};
use crate::math::{erlang_b, normal_inv};
use crate::sku::{DependantChange, SkuKey, SkuRecord, SkuStore};
use crate::target::TargetDictionary;

const LARGE_QUANTITY: f64 = 99_999_999.0;

// ── Result types ──────────────────────────────────────────────────────────

/// Everything computed by one call to `compute_marginal_value` for the
/// *main* (directly chosen) SKU.
#[derive(Debug, Clone)]
pub struct MarginalValueResult {
    pub key: SkuKey,
    /// Proposed new buffer for this SKU.
    pub new_buffer: f64,
    /// Proposed new fill rate at `new_buffer`.
    pub new_fill_rate: f64,
    /// Marginal gain / investment (the selection score).
    pub marginal_value: f64,
    /// Pending updates to dependant SKUs.
    pub dependant_changes: Vec<DependantChange>,
    /// True if the SKU has no remaining potential (max fill rate reached, etc.)
    pub exhausted: bool,
}

// ── Min/max ROP helpers ───────────────────────────────────────────────────

/// Compute the target min/max ROP quantities.
///
/// Mirrors Python `MEIO.minMaxROPQty`.
pub fn min_max_rop_qty(
    sku_max_sl_slices: f64,
    total_demand_rate: f64,
    avg_size: f64,
    sku_max_sl_qty: f64,
    sku_min_sl_slices: f64,
    use_existing_inventory: bool,
    on_hand: f64,
    sku_min_sl_qty: f64,
    effective_total_lt_fcst: f64,
) -> (f64, f64) {
    // Max slice quantity
    let sku_max_sl_slices_qty = if sku_max_sl_slices < LARGE_QUANTITY && sku_max_sl_slices > 0.0 {
        // Approximation: 1 slice ≈ 30 days
        let max_days = sku_max_sl_slices * 30.0;
        max_days * total_demand_rate * avg_size
    } else {
        LARGE_QUANTITY
    };
    let tgt_max_sl_qty = sku_max_sl_qty.min(sku_max_sl_slices_qty);

    // Min slice quantity
    let sku_min_sl_slices_qty = if sku_min_sl_slices > 0.0 {
        let min_days = sku_min_sl_slices * 30.0;
        min_days * total_demand_rate * avg_size
    } else {
        0.0
    };
    let onhand_min_qty = if use_existing_inventory { on_hand } else { 0.0 };
    let min_sl_qty = sku_min_sl_qty.max(onhand_min_qty).max(sku_min_sl_slices_qty);
    // Cap the min by the max
    let tgt_min_sl_qty = if min_sl_qty < tgt_max_sl_qty { min_sl_qty } else { tgt_max_sl_qty };

    let tgt_min_rop_qty = if tgt_min_sl_qty > 0.0 {
        tgt_min_sl_qty + effective_total_lt_fcst
    } else {
        0.0
    };
    let tgt_max_rop_qty = if tgt_max_sl_qty < LARGE_QUANTITY {
        tgt_max_sl_qty + effective_total_lt_fcst
    } else {
        LARGE_QUANTITY
    };

    (tgt_min_rop_qty, tgt_max_rop_qty)
}

// ── Effective LT forecast ─────────────────────────────────────────────────

/// Compute effective lead-time forecast and effective direct LT forecast.
///
/// Mirrors Python `MEIO.EffectiveLtFcst`.
#[inline]
pub fn effective_lt_fcst(
    total_demand_rate: f64,
    direct_demand_rate: f64,
    leg_lead_time: f64,
    parent_wait_time: f64,
) -> (f64, f64) {
    let total_lead_time = leg_lead_time + parent_wait_time;
    (
        total_demand_rate * total_lead_time,
        direct_demand_rate * total_lead_time,
    )
}

// ── Replenishment wait time ───────────────────────────────────────────────

/// Probabilistic wait contribution from a replenishment location.
///
/// Mirrors Python `MEIO.Replenishingloclt`.
#[inline]
pub fn replenishment_wait_time(total_wait_time: f64, new_fill_rate: f64) -> f64 {
    total_wait_time * (1.0 - new_fill_rate)
}

// ── Standard-deviation cap ────────────────────────────────────────────────

/// Cap standard deviation at coefficient-of-variation cap.
///
/// Mirrors Python `MEIO.capStandardDeviation`.
pub fn cap_std_dev(
    hist_std_dev: f64,
    monthly_forecast: f64,
    coeff_var_cap: f64,
    lead_time_stddev: f64,
    lead_time: f64,
) -> (f64, f64) {
    // Demand std dev cap
    let capped_dmd = hist_std_dev
        .min(monthly_forecast * coeff_var_cap)
        .max(monthly_forecast);

    // Lead-time std dev cap
    let capped_lt = lead_time_stddev
        .min(lead_time * coeff_var_cap)
        .max(lead_time);

    (capped_dmd, capped_lt)
}

// ── Initial jump ─────────────────────────────────────────────────────────

/// Compute the initial buffer jump for a SKU (sets the first non-zero buffer).
///
/// Mirrors Python `MEIO.initialJump`.
pub fn initial_jump(
    sku: &SkuRecord,
    effective_total_lt_fcst: f64,
    dmd_stddev: f64,
    lt_stddev: f64,
    dist_threshold: i64,
) -> (f64, f64) {
    let dist = sku.effective_distribution(dist_threshold);
    let avg_size = sku.avg_size.max(1.0);
    let eoq = sku.eoq.max(0.0);

    // First step: at least avg_size or (fcstlt * avg_size − eoq/2)
    let first_buf = avg_size.max(
        (effective_total_lt_fcst * avg_size - (eoq / 2.0).ceil()).ceil().max(avg_size),
    );

    let fr_params = FillRateParams {
        buffer: first_buf,
        forecast_over_lt: effective_total_lt_fcst,
        eoq,
        avg_size,
        lead_time: sku.total_lead_time,
        lead_time_stddev: lt_stddev,
        dmd_stddev,
        mad: sku.mad,
    };

    let fr = fill_rate_for_rop(&dist, &fr_params);
    (first_buf, fr)
}

// ── Further jump (long-jump heuristic) ────────────────────────────────────

/// Determine the buffer increment for subsequent steps.
///
/// When `precision_jump > 0` and `current_fill_rate < big_jump_threshold`,
/// jumps directly towards the target fill rate.  Otherwise increments by one
/// avg_size.
///
/// Mirrors Python `MEIO.further_jump`.
pub fn further_jump(
    sku: &SkuRecord,
    current_fill_rate: f64,
    committed_buffer: f64,
    effective_total_lt_fcst: f64,
    dmd_stddev: f64,
    lt_stddev: f64,
    precision_jump: f64,
    big_jump_threshold: f64,
    dist_threshold: i64,
) -> f64 {
    let avg_size = sku.avg_size.max(1.0);

    if precision_jump > 0.0 && current_fill_rate < big_jump_threshold {
        let dist = sku.effective_distribution(dist_threshold);
        let lead_time_month = sku.total_lead_time / 30.0;
        let daily_rate = effective_total_lt_fcst / sku.total_lead_time.max(1.0);
        let lt_std_safe = if lt_stddev.is_nan() { 0.0 } else { lt_stddev };
        let lead_time_deviation_portion = (daily_rate * lt_std_safe).powi(2);

        let next_jump = match dist {
            crate::distributions::DistributionType::Poisson => {
                let new_buf = poisson_fr_to_rop(
                    current_fill_rate + precision_jump,
                    sku.eoq,
                    effective_total_lt_fcst,
                    avg_size,
                );
                let raw = new_buf - committed_buffer;
                let j = raw.max(avg_size);
                (j / avg_size).ceil() * avg_size
            }
            _ => {
                // Golden formula inverse for Normal / fitted distributions
                let mad_safe = if sku.mad.is_nan() { 0.0 } else { sku.mad };
                let demand_variation = if mad_safe > 0.0 {
                    1.25 * mad_safe * effective_total_lt_fcst
                        * (1.0 / lead_time_month.max(1e-9)).sqrt()
                } else {
                    dmd_stddev
                };
                let demand_var_portion = (demand_variation.powi(2) * sku.total_lead_time).sqrt();
                let racine = (demand_var_portion + lead_time_deviation_portion).sqrt();
                let zscore = normal_inv(current_fill_rate + precision_jump);
                let new_line_ss = zscore * racine;
                let raw = new_line_ss + effective_total_lt_fcst * avg_size - committed_buffer;
                let j = raw.max(avg_size);
                (j / avg_size).ceil() * avg_size
            }
        };
        next_jump
    } else {
        avg_size
    }
}

// ── Apply SKU minimum ─────────────────────────────────────────────────────

/// Find the buffer (and corresponding fill rate) that satisfies the SKU's
/// minimum fill-rate and minimum ROP-quantity constraints.
///
/// Mirrors Python `MEIO.applySkuMin`.
pub fn apply_sku_min(
    sku: &SkuRecord,
    effective_total_lt_fcst: f64,
    tgt_min_rop_qty: f64,
    tgt_max_rop_qty: f64,
    dmd_stddev: f64,
    lt_stddev: f64,
    dist_threshold: i64,
) -> (f64, f64) {
    let dist = sku.effective_distribution(dist_threshold);
    let avg_size = sku.avg_size.max(1.0);

    let start_buf = ((tgt_min_rop_qty / avg_size).ceil() * avg_size).max(avg_size);
    let mut buffer = start_buf;
    let mut last_fr = 0.0_f64;
    let mut counter = 0_u64;

    loop {
        let fr_params = FillRateParams {
            buffer,
            forecast_over_lt: effective_total_lt_fcst,
            eoq: sku.eoq,
            avg_size,
            lead_time: sku.total_lead_time,
            lead_time_stddev: lt_stddev,
            dmd_stddev,
            mad: sku.mad,
        };
        let fr = fill_rate_for_rop(&dist, &fr_params);

        if fr > sku.sku_min_fill_rate || buffer > tgt_max_rop_qty {
            if counter == 0 {
                last_fr = fr;
            }
            break;
        }
        last_fr = fr;
        buffer += avg_size;
        counter += 1;

        // Safety: avoid infinite loop for edge cases
        if counter > 1_000_000 {
            break;
        }
    }

    (buffer, last_fr)
}

// ── Kit probabilistic wait time ───────────────────────────────────────────

/// Probabilistic wait time for a kit SKU based on the fill rates of its
/// components.  Returns the expected lead time weighted by component outage
/// probabilities.
///
/// Mirrors Python `MEIO.kit_probabilistic_waiting_Lt`.
pub fn kit_probabilistic_wait_time(
    kit_key: &SkuKey,
    modified_component_item_id: i64,
    new_fill_rate_of_modified: f64,
    skus: &SkuStore,
) -> f64 {
    let kit_sku = match skus.get(kit_key) {
        Some(s) => s,
        None => return 0.0,
    };

    let kit_site_id = kit_key.1;

    // Gather all component records for this kit
    let mut component_skus: Vec<(&SkuRecord, f64)> = kit_sku
        .components
        .iter()
        .filter_map(|&comp_item_id| {
            let comp_key = (comp_item_id, kit_site_id);
            skus.get(&comp_key).map(|s| {
                let fr = if comp_item_id == modified_component_item_id {
                    new_fill_rate_of_modified
                } else {
                    s.current_fill_rate
                };
                (s, fr)
            })
        })
        .collect();

    // Sort by leg_lead_time ascending (matches Python sort)
    component_skus.sort_by(|a, b| a.0.leg_lead_time.partial_cmp(&b.0.leg_lead_time).unwrap());

    let mut overall_lt_weighted = 0.0_f64;

    for i in 0..component_skus.len() {
        let (comp, comp_fr) = &component_skus[i];
        let current_comp_leg_lt = comp.leg_lead_time;

        // P(component i is the bottleneck) = (1 - FR_i) * Π_{j: LT_j > LT_i} FR_j
        let mut prob = 1.0 - comp_fr;

        for j in 0..component_skus.len() {
            if component_skus[j].0.leg_lead_time > current_comp_leg_lt {
                prob *= component_skus[j].1; // FR of higher-LT component
            }
        }

        overall_lt_weighted += prob * current_comp_leg_lt;
    }

    overall_lt_weighted
}

// ── Asset mode: Erlang-B pool sizing marginal value ───────────────────────

/// Compute marginal value for an asset-mode SKU using Erlang-B.
///
/// The "buffer" in asset mode represents the pool size (number of units owned).
/// The fill rate analogue is `1 − erlang_b(pool_size, rho)`.
/// Marginal value = criticality × Δavailability / unit_cost.
fn compute_asset_marginal_value(key: &SkuKey, sku: &SkuRecord) -> Option<MarginalValueResult> {
    let current_pool = sku.committed_buffer.floor() as u32;
    let next_pool    = current_pool + 1;

    // Offered load: λ × μ  (failure rate × repair TAT)
    // Use total_demand_rate as failure rate; leg_lead_time as repair TAT.
    let rho = sku.total_demand_rate * sku.leg_lead_time;

    let current_shortage = erlang_b(current_pool, rho);
    let next_shortage    = erlang_b(next_pool,    rho);

    let current_avail = 1.0 - current_shortage;
    let next_avail    = 1.0 - next_shortage;

    // Already at or above max fill rate (used as max availability cap)
    if next_avail <= current_avail || current_avail >= sku.sku_max_fill_rate {
        return Some(MarginalValueResult {
            key: *key,
            new_buffer:    next_pool as f64,
            new_fill_rate: next_avail,
            marginal_value: -LARGE_QUANTITY,
            dependant_changes: vec![],
            exhausted: true,
        });
    }

    let delta_avail = next_avail - current_avail;
    let investment  = sku.unit_cost.max(1e-9);
    let criticality = if sku.criticality > 0.0 { sku.criticality } else { 1.0 };
    let mv = criticality * delta_avail / investment;

    Some(MarginalValueResult {
        key: *key,
        new_buffer:    next_pool as f64,
        new_fill_rate: next_avail,
        marginal_value: mv,
        dependant_changes: vec![],
        exhausted: false,
    })
}

// ── Core: compute_marginal_value ──────────────────────────────────────────

/// Compute the marginal value (gain / investment) for increasing one SKU's buffer.
///
/// This is a **pure function** — it reads from `skus` and `targets` but does
/// not mutate them.  Results are returned as `MarginalValueResult` and applied
/// by the caller.
///
/// Mirrors Python `MEIO.MarginalValue(indexrow, mainindexrow, ...)`.
///
/// # Arguments
/// * `key`          — The SKU being evaluated (main SKU).
/// * `skus`         — Snapshot of the full SKU store (read-only).
/// * `targets`      — The target dictionary (read-only for gain calculation).
/// * `cfg`          — Optimizer configuration.
pub fn compute_marginal_value(
    key: &SkuKey,
    skus: &SkuStore,
    targets: &TargetDictionary,
    cfg: &MeioConfig,
) -> Option<MarginalValueResult> {
    let _sku = skus.get(key)?;   // validate key exists before delegating

    compute_mv_recursive(
        key,
        key,
        None,  // main_investment: computed inside
        None,  // override_wait_time: none for main SKU
        skus,
        targets,
        cfg,
    )
}

/// Internal recursive implementation that handles both main SKUs and dependants.
///
/// Returns `None` if the SKU is not found.
fn compute_mv_recursive(
    key: &SkuKey,
    main_key: &SkuKey,
    main_investment: Option<f64>,
    override_wait_time: Option<f64>,
    skus: &SkuStore,
    targets: &TargetDictionary,
    cfg: &MeioConfig,
) -> Option<MarginalValueResult> {
    let sku = skus.get(key)?;

    let is_main = key == main_key;
    let avg_size = sku.avg_size.max(1.0);
    let dist_threshold = cfg.distribution_threshold;

    // ── Asset mode: use Erlang-B pool-shortage instead of fill-rate formula ──
    if sku.asset_mode && is_main {
        return compute_asset_marginal_value(key, sku);
    }

    // ── Repair flow: compute net demand rate and WIP inventory credit ──────
    // These are local adjustments only — the SkuRecord is not mutated.
    let (effective_total_demand_rate, effective_direct_demand_rate, effective_on_hand) =
        if let Some(ref rf) = sku.repair_flow {
            // Net serviceable return rate per period
            let return_supply_rate = sku.total_demand_rate * rf.return_rate * rf.repair_yield;
            let net_total  = (sku.total_demand_rate  - return_supply_rate).max(0.0);
            let net_direct = (sku.direct_demand_rate - return_supply_rate.min(sku.direct_demand_rate)).max(0.0);
            // WIP units already in repair that will eventually return serviceable
            let wip_credit = rf.wip_qty * rf.repair_yield;
            (net_total, net_direct, sku.on_hand + wip_credit)
        } else {
            (sku.total_demand_rate, sku.direct_demand_rate, sku.on_hand)
        };

    // Effective wait time: use override (from parent's fill-rate change) or current
    let wait_time = override_wait_time.unwrap_or(sku.current_wait_time);
    let (effective_total_lt_fcst, _effective_direct_lt_fcst) =
        effective_lt_fcst(effective_total_demand_rate, effective_direct_demand_rate, sku.leg_lead_time, wait_time);

    // Cap standard deviations
    let monthly_rate = 30.0 * (effective_total_demand_rate + effective_direct_demand_rate);
    let (dmd_stddev, lt_stddev) = cap_std_dev(
        sku.dmd_stddev,
        monthly_rate,
        sku.varcoeff_max,
        sku.lt_stddev,
        sku.leg_lead_time + wait_time,
    );

    let dist = sku.effective_distribution(dist_threshold);

    // ── Determine the proposed new buffer ────────────────────────────────
    let (new_tested_buffer, initial_fill_rate) = if is_main {
        if sku.committed_buffer == 0.0 {
            // First time: compute initial jump from zero.
            // tgt_max is used to clamp the initial buffer so we don't over-shoot
            // any hard maximum ROP quantity constraint.
            let (_tgt_min, tgt_max) = min_max_rop_qty(
                sku.sku_max_sl_slices,
                effective_total_demand_rate,
                avg_size,
                sku.sku_max_sl_qty,
                sku.sku_min_sl_slices,
                sku.use_existing_inventory,
                effective_on_hand,
                sku.sku_min_sl_qty,
                effective_total_lt_fcst,
            );
            let (raw_buf, fr) = initial_jump(sku, effective_total_lt_fcst, dmd_stddev, lt_stddev, dist_threshold);
            // Clamp to max ROP quantity
            let buf = raw_buf.min(tgt_max);
            (buf, Some(fr))
        } else {
            // Subsequent step: advance by one jump increment
            let jump = further_jump(
                sku,
                sku.current_fill_rate,
                sku.committed_buffer,
                effective_total_lt_fcst,
                dmd_stddev,
                lt_stddev,
                cfg.precision_jump,
                cfg.big_jump_threshold,
                dist_threshold,
            );
            let new_buf = sku.new_buffer.max(sku.committed_buffer + jump);
            (new_buf, None)
        }
    } else {
        // Dependant SKU: buffer does not change; fill rate changes due to
        // parent's wait-time adjustment.
        (sku.committed_buffer, None)
    };

    // ── Compute new fill rate ─────────────────────────────────────────────
    let new_sku_fill_rate = if let Some(fr) = initial_fill_rate {
        fr
    } else {
        let fr_params = FillRateParams {
            buffer: new_tested_buffer,
            forecast_over_lt: effective_total_lt_fcst,
            eoq: sku.eoq,
            avg_size,
            lead_time: sku.total_lead_time,
            lead_time_stddev: lt_stddev,
            dmd_stddev,
            mad: sku.mad,
        };
        fill_rate_for_rop(&dist, &fr_params)
    };

    // ── Main investment cost for this step ────────────────────────────────
    let investment = main_investment.unwrap_or_else(|| {
        (new_tested_buffer - sku.committed_buffer) * sku.unit_cost
    });
    let investment = investment.max(1e-9); // guard against division by zero

    // ── Marginal group gain ───────────────────────────────────────────────
    // Use effective (net) direct demand rate and apply criticality weight.
    let criticality = if sku.criticality > 0.0 { sku.criticality } else { 1.0 };
    let mut total_gain = criticality * targets.marginal_group_gain(
        &sku.j_target_groups,
        effective_direct_demand_rate,
        new_sku_fill_rate,
        sku.current_fill_rate,
        sku.group_participation,
        sku.sku_max_fill_rate,
    );

    // ── Recurse into replenishment dependants ────────────────────────────
    let mut dependant_changes: Vec<DependantChange> = Vec::new();

    if !sku.repl_site_ids.is_empty() {
        let parent_wait = replenishment_wait_time(sku.total_lead_time, new_sku_fill_rate);
        for &dep_site_id in &sku.repl_site_ids {
            let dep_key = (sku.item_id, dep_site_id);
            if let Some(dep_result) = compute_mv_recursive(
                &dep_key,
                main_key,
                Some(investment),
                Some(parent_wait),
                skus,
                targets,
                cfg,
            ) {
                total_gain += dep_result.marginal_value * investment; // re-weight
                dependant_changes.extend(dep_result.dependant_changes);
                // Add this dependant's fill-rate change
                dependant_changes.push(DependantChange {
                    key: dep_key,
                    new_fill_rate: dep_result.new_fill_rate,
                    new_wait_time: parent_wait,
                });
            }
        }
    }

    // ── Recurse into kit dependants ───────────────────────────────────────
    if !sku.kits.is_empty() {
        for &kit_item_id in &sku.kits {
            let kit_key = (kit_item_id, key.1);
            let kit_wait = kit_probabilistic_wait_time(&kit_key, key.0, new_sku_fill_rate, skus);
            if let Some(kit_result) = compute_mv_recursive(
                &kit_key,
                main_key,
                Some(investment),
                Some(kit_wait),
                skus,
                targets,
                cfg,
            ) {
                total_gain += kit_result.marginal_value * investment;
                dependant_changes.extend(kit_result.dependant_changes);
                dependant_changes.push(DependantChange {
                    key: kit_key,
                    new_fill_rate: kit_result.new_fill_rate,
                    new_wait_time: kit_wait,
                });
            }
        }
    }

    // ── Compute final marginal value ──────────────────────────────────────
    if is_main {
        // Check feasibility
        let fr_increased = new_sku_fill_rate > sku.current_fill_rate;
        let below_max = new_sku_fill_rate <= sku.sku_max_fill_rate;

        if !fr_increased || !below_max {
            return Some(MarginalValueResult {
                key: *key,
                new_buffer: new_tested_buffer,
                new_fill_rate: new_sku_fill_rate,
                marginal_value: -LARGE_QUANTITY,
                dependant_changes: vec![],
                exhausted: true,
            });
        }

        let mv = total_gain / investment;

        Some(MarginalValueResult {
            key: *key,
            new_buffer: new_tested_buffer,
            new_fill_rate: new_sku_fill_rate,
            marginal_value: mv,
            dependant_changes,
            exhausted: false,
        })
    } else {
        // Not the main SKU — return as a dependant result
        // The marginal "value" here is just the gain per investment for the
        // recursive calls to aggregate.
        let mv = if investment > 0.0 { total_gain / investment } else { 0.0 };
        Some(MarginalValueResult {
            key: *key,
            new_buffer: new_tested_buffer,
            new_fill_rate: new_sku_fill_rate,
            marginal_value: mv,
            dependant_changes,
            exhausted: false,
        })
    }
}

// ── Initialization: compute marginal values for all SKUs ─────────────────

/// Initialize the marginal values for all SKUs in the store.
///
/// Mirrors Python `MEIO.initialize_marginal_value_fast` (second pass —
/// the full MarginalValue computation after ASL initialisation is done
/// by the optimizer's `apply_initial_asl` step).
pub fn initialize_all_marginal_values(
    skus: &mut SkuStore,
    targets: &TargetDictionary,
    cfg: &MeioConfig,
) {
    let keys: Vec<SkuKey> = skus.keys().cloned().collect();

    for key in keys {
        // Build a read-only snapshot for the computation
        let result = compute_marginal_value(&key, skus, targets, cfg);
        if let Some(r) = result {
            if let Some(sku) = skus.get_mut(&key) {
                sku.marginal_value = r.marginal_value;
                sku.new_buffer = r.new_buffer;
                sku.new_fill_rate = r.new_fill_rate;
                sku.dependant_changes = r.dependant_changes;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{GroupTarget, MeioConfig};
    use crate::sku::{SkuRecord, TargetGroupRef};
    use crate::target::TargetDictionary;
    use std::collections::HashMap;

    fn make_simple_sku(item_id: i64, site_id: i64) -> SkuRecord {
        SkuRecord {
            item_id,
            site_id,
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
            j_target_groups: vec![TargetGroupRef {
                io_tgt_group: "group_A".to_string(),
                group_participation: 1.0,
            }],
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

    fn make_config() -> MeioConfig {
        MeioConfig {
            scopes: vec![],
            optimization_params: vec![],
            parallel_workers: 1,
            distribution_threshold: 25,
            consider_eoq: true,
            line_fill_rate: true,
            precision_jump: 0.0,
            big_jump_threshold: 0.95,
        }
    }

    #[test]
    fn marginal_value_is_positive_for_fresh_sku() {
        let sku = make_simple_sku(1, 10);
        let key = sku.key();
        let mut skus: SkuStore = HashMap::new();
        skus.insert(key, sku);

        let targets = TargetDictionary::new(
            &[GroupTarget {
                group_name: "group_A".to_string(),
                fill_rate_target: 0.95,
                max_budget: f64::INFINITY,
            }],
            &skus,
        );

        let result = compute_marginal_value(&key, &skus, &targets, &make_config()).unwrap();
        assert!(!result.exhausted, "Should not be exhausted on first call");
        assert!(result.marginal_value > 0.0, "Expected positive MV, got {}", result.marginal_value);
        assert!(result.new_fill_rate > 0.0, "Expected FR > 0, got {}", result.new_fill_rate);
    }

    #[test]
    fn effective_lt_fcst_basic() {
        let (total, direct) = effective_lt_fcst(10.0, 8.0, 30.0, 5.0);
        assert!((total - 350.0).abs() < 1e-9); // (30+5)*10
        assert!((direct - 280.0).abs() < 1e-9); // (30+5)*8
    }
}
