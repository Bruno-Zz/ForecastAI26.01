//! Parallel MEIO greedy optimizer.
//!
//! ## Algorithm
//!
//! The standard single-threaded MEIO greedy loop is:
//! ```text
//! repeat:
//!   i = argmax(marginal_value)
//!   commit(i)          ← update buffer, fill rate, group targets
//! until all groups done
//! ```
//!
//! The parallel version batches **N candidates** per iteration (N = CPU count):
//! ```text
//! repeat:
//!   top_n = top_n_by_marginal_value(N)
//!   results = top_n.par_iter().map(|k| compute_marginal_value(k)).collect()   ← parallel
//!   for result in results:
//!     commit(result)    ← serial
//!   recompute MV for affected SKUs
//! until all groups done
//! ```
//!
//! The parallelism is a deliberate **approximation**: candidates 2..N are
//! evaluated against the state *before* candidate 1 is committed.  This means
//! the same "investment dollar" may be counted twice in one batch.  In practice
//! the deviation from the true greedy solution is negligible for large SKU
//! counts.

use rayon::prelude::*;

use crate::config::{GroupTarget, MeioConfig};
use crate::marginal::{
    apply_sku_min, cap_std_dev, compute_marginal_value, effective_lt_fcst,
    initialize_all_marginal_values, min_max_rop_qty, MarginalValueResult,
};
use crate::sku::{GroupResult, SkuKey, SkuResult, SkuStore};
use crate::target::TargetDictionary;

const LARGE_QUANTITY: f64 = 99_999_999.0;

// ── OptimizerResult ───────────────────────────────────────────────────────

/// Output of one optimizer run.
pub struct OptimizerResult {
    /// Per-SKU output records (committed buffer, fill rate, etc.).
    pub sku_results: Vec<SkuResult>,
    /// Per-group outcome (achieved fill rate, budget, completion flag).
    pub group_results: Vec<GroupResult>,
    /// Number of greedy iterations performed.
    pub iterations: u64,
}

// ── Optimizer ─────────────────────────────────────────────────────────────

/// MEIO greedy optimizer.
pub struct Optimizer {
    pub cfg: MeioConfig,
    pub group_targets: Vec<GroupTarget>,
}

impl Optimizer {
    // ── Constructor ───────────────────────────────────────────────────────

    pub fn new(cfg: MeioConfig, group_targets: Vec<GroupTarget>) -> Self {
        Optimizer { cfg, group_targets }
    }

    // ── Main entry point ──────────────────────────────────────────────────

    /// Run the MEIO optimization over the provided SKU store.
    ///
    /// # Returns
    /// `Ok(OptimizerResult)` on success, `Err(msg)` on fatal error.
    pub fn run(&self, mut skus: SkuStore) -> Result<OptimizerResult, String> {
        // 1. Build the target dictionary (initialised with demand totals)
        let mut targets = TargetDictionary::new(&self.group_targets, &skus);

        // 2. Phase 1 – Apply ASL (absolute stock lower bounds) for SKUs with
        //    minimum fill-rate / minimum quantity constraints.
        self.apply_initial_asl(&mut skus, &mut targets)?;

        // 3. Phase 2 – Compute initial marginal values for all SKUs.
        initialize_all_marginal_values(&mut skus, &targets, &self.cfg);

        // 4. Phase 3 – Parallel greedy main loop.
        let iterations = self.greedy_loop(&mut skus, &mut targets)?;

        // 5. Collect results.
        let sku_results: Vec<SkuResult> = skus.values().map(SkuResult::from_record).collect();
        let group_results = targets.to_results();

        Ok(OptimizerResult { sku_results, group_results, iterations })
    }

    // ── Phase 1: Apply initial ASL ────────────────────────────────────────

    /// For every SKU that has a minimum fill-rate or minimum quantity constraint,
    /// immediately set its buffer to satisfy that constraint (independent of the
    /// greedy order).
    ///
    /// Mirrors Python `MEIO.initialize_marginal_value_fast` — first loop.
    fn apply_initial_asl(
        &self,
        skus: &mut SkuStore,
        targets: &mut TargetDictionary,
    ) -> Result<(), String> {
        let keys: Vec<SkuKey> = skus.keys().cloned().collect();

        // Process in an arbitrary order — same as Python (no ordering by `lvl`
        // here; complex kit hierarchies should be ordered by the caller if needed).
        for key in keys {
            let sku = match skus.get(&key) {
                Some(s) => s,
                None => continue,
            };

            // Only process SKUs that have at least one minimum constraint
            let needs_asl = sku.sku_min_fill_rate > 0.0
                || sku.sku_min_sl_qty > 0.0
                || sku.sku_min_sl_slices > 0.0
                || (sku.use_existing_inventory && sku.on_hand > 0.0);

            if !needs_asl {
                continue;
            }

            let avg_size = sku.avg_size.max(1.0);
            let wait_time = sku.current_wait_time;
            let (eff_total, _eff_direct) = effective_lt_fcst(
                sku.total_demand_rate,
                sku.direct_demand_rate,
                sku.leg_lead_time,
                wait_time,
            );
            let monthly_rate = 30.0 * (sku.total_demand_rate + sku.direct_demand_rate);
            let (dmd_stddev, lt_stddev) = cap_std_dev(
                sku.dmd_stddev,
                monthly_rate,
                sku.varcoeff_max,
                sku.lt_stddev,
                sku.leg_lead_time + wait_time,
            );
            let (tgt_min, tgt_max) = min_max_rop_qty(
                sku.sku_max_sl_slices,
                sku.total_demand_rate,
                avg_size,
                sku.sku_max_sl_qty,
                sku.sku_min_sl_slices,
                sku.use_existing_inventory,
                sku.on_hand,
                sku.sku_min_sl_qty,
                eff_total,
            );

            let (min_buf, min_fr) = apply_sku_min(
                sku,
                eff_total,
                tgt_min,
                tgt_max,
                dmd_stddev,
                lt_stddev,
                self.cfg.distribution_threshold,
            );

            // Commit the ASL buffer to the SKU
            {
                let sku_mut = skus.get_mut(&key).unwrap();
                sku_mut.new_buffer = min_buf;
                sku_mut.new_fill_rate = min_fr;
            }

            // Commit to groups (as init — marginal value = 0 is acceptable here)
            self.commit_result(
                &MarginalValueResult {
                    key,
                    new_buffer: min_buf,
                    new_fill_rate: min_fr,
                    marginal_value: 0.0,
                    dependant_changes: vec![],
                    exhausted: false,
                },
                skus,
                targets,
                true, // is_init
            );
        }

        Ok(())
    }

    // ── Phase 3: Parallel greedy loop ─────────────────────────────────────

    /// The core parallel greedy loop.
    ///
    /// Each iteration:
    /// 1. Selects the top N candidates by marginal value.
    /// 2. Evaluates all N in parallel (Rayon).
    /// 3. Commits all N serially.
    /// 4. Recomputes marginal values for affected SKUs.
    fn greedy_loop(
        &self,
        skus: &mut SkuStore,
        targets: &mut TargetDictionary,
    ) -> Result<u64, String> {
        let n_workers = self.cfg.effective_workers();
        let mut iterations = 0_u64;

        loop {
            if targets.all_completed() {
                break;
            }

            // 1. Select top N candidates (filter out exhausted / zero MV)
            let candidates = self.top_n_candidates(skus, n_workers);
            if candidates.is_empty() {
                // No more candidates — goals may not be fully achieved
                break;
            }

            // 2. Evaluate all candidates in parallel
            //    We take a read-only snapshot (the HashMap) for the parallel phase.
            let skus_ref: &SkuStore = skus;
            let targets_ref: &TargetDictionary = targets;
            let cfg_ref: &MeioConfig = &self.cfg;

            let results: Vec<Option<MarginalValueResult>> = candidates
                .par_iter()
                .map(|key| compute_marginal_value(key, skus_ref, targets_ref, cfg_ref))
                .collect();

            // 3. Commit results serially
            let mut any_progress = false;
            for maybe_result in results {
                let result = match maybe_result {
                    Some(r) => r,
                    None => continue,
                };

                if result.marginal_value <= 0.0 && !result.exhausted {
                    // No improvement possible from this SKU
                    if let Some(sku) = skus.get_mut(&result.key) {
                        sku.marginal_value = -LARGE_QUANTITY;
                    }
                    continue;
                }

                if targets.all_completed() {
                    break;
                }

                let all_done = self.commit_result(&result, skus, targets, false);
                any_progress = true;

                if all_done {
                    return Ok(iterations + 1);
                }
            }

            iterations += 1;

            if !any_progress {
                // All candidates were exhausted — terminate
                break;
            }

            // 4. Recompute marginal values for all affected SKUs
            //    (those that were committed plus their dependants)
            // Collect keys that need recomputing: committed SKUs + their dependant keys
            let affected_keys: Vec<SkuKey> = candidates.clone();
            for key in &affected_keys {
                // Collect dependant keys from a read-only borrow, then drop it
                let deps: Vec<SkuKey> = skus
                    .get(key)
                    .map(|sku| sku.dependant_changes.iter().map(|d| d.key).collect())
                    .unwrap_or_default();

                for dep_key in deps {
                    let result = compute_marginal_value(&dep_key, skus, targets, &self.cfg);
                    if let Some(r) = result {
                        if let Some(s) = skus.get_mut(&dep_key) {
                            s.marginal_value = r.marginal_value;
                            s.new_buffer = r.new_buffer;
                            s.new_fill_rate = r.new_fill_rate;
                            s.dependant_changes = r.dependant_changes;
                        }
                    }
                }

                let result = compute_marginal_value(key, skus, targets, &self.cfg);
                if let Some(r) = result {
                    if let Some(s) = skus.get_mut(key) {
                        s.marginal_value = r.marginal_value;
                        s.new_buffer = r.new_buffer;
                        s.new_fill_rate = r.new_fill_rate;
                        s.dependant_changes = r.dependant_changes;
                    }
                }
            }
        }

        Ok(iterations)
    }

    // ── Commit one result ─────────────────────────────────────────────────

    /// Apply a `MarginalValueResult` to the SKU store and target dictionary.
    ///
    /// Returns `true` when all groups are complete.
    ///
    /// Mirrors Python `MEIO.commit_logged_value`.
    fn commit_result(
        &self,
        result: &MarginalValueResult,
        skus: &mut SkuStore,
        targets: &mut TargetDictionary,
        is_init: bool,
    ) -> bool {
        // Short-circuit: if marginal_value is zero and this is not an init commit,
        // there is nothing useful to do.
        if result.marginal_value == 0.0 && !is_init {
            return targets.all_completed();
        }

        // 1. Update the main SKU
        let (old_fr, new_fr, old_buf, new_buf, j_tgt_groups, direct_dr, unit_cost) = {
            let sku = match skus.get_mut(&result.key) {
                Some(s) => s,
                None => return targets.all_completed(),
            };

            let old_fr = sku.current_fill_rate;
            let new_fr = result.new_fill_rate;
            let old_buf = sku.committed_buffer;
            let new_buf = result.new_buffer;

            sku.committed_buffer = new_buf;
            sku.current_fill_rate = new_fr;
            sku.marginal_value = result.marginal_value;
            sku.new_buffer = new_buf;
            sku.new_fill_rate = new_fr;
            sku.dependant_changes = result.dependant_changes.clone();

            (
                old_fr,
                new_fr,
                old_buf,
                new_buf,
                sku.j_target_groups.clone(),
                sku.direct_demand_rate,
                sku.unit_cost,
            )
        };

        // 2. Commit to target groups for main SKU
        let (sku_done, all_done) = targets.commit_groups(
            &j_tgt_groups,
            new_fr,
            old_fr,
            direct_dr,
            new_buf,
            old_buf,
            unit_cost,
        );

        if sku_done && !is_init {
            if let Some(sku) = skus.get_mut(&result.key) {
                sku.marginal_value = -LARGE_QUANTITY;
            }
        }

        if all_done {
            return true;
        }

        // 3. Commit dependant changes
        for dep_change in &result.dependant_changes {
            let dep_key = dep_change.key;
            let (dep_old_fr, dep_new_buf, dep_committed_buf, dep_j_groups, dep_dr) = {
                let dep_sku = match skus.get_mut(&dep_key) {
                    Some(s) => s,
                    None => continue,
                };
                let old = dep_sku.current_fill_rate;
                let nb = dep_sku.committed_buffer; // buffer unchanged for dependants
                let cb = dep_sku.committed_buffer;
                dep_sku.current_fill_rate = dep_change.new_fill_rate;
                dep_sku.current_wait_time = dep_change.new_wait_time;
                (old, nb, cb, dep_sku.j_target_groups.clone(), dep_sku.direct_demand_rate)
            };

            let (dep_sku_done, dep_all_done) = targets.commit_groups(
                &dep_j_groups,
                dep_change.new_fill_rate,
                dep_old_fr,
                dep_dr,
                dep_new_buf,
                dep_committed_buf,
                0.0, // no cost for dependant fill-rate changes
            );

            if dep_sku_done {
                if let Some(sku) = skus.get_mut(&dep_key) {
                    sku.marginal_value = 0.0;
                }
            }

            if dep_all_done {
                return true;
            }
        }

        targets.all_completed()
    }

    // ── Candidate selection ───────────────────────────────────────────────

    /// Select the top N SKUs by marginal value.
    ///
    /// Excludes SKUs with marginal_value ≤ -LARGE_QUANTITY (exhausted).
    fn top_n_candidates(&self, skus: &SkuStore, n: usize) -> Vec<SkuKey> {
        // Collect all viable candidates
        let mut candidates: Vec<(SkuKey, f64)> = skus
            .iter()
            .filter(|(_, s)| s.marginal_value > -LARGE_QUANTITY / 2.0)
            .map(|(k, s)| (*k, s.marginal_value))
            .collect();

        if candidates.is_empty() {
            return vec![];
        }

        // Partial sort: bring top N to the front (O(m) average for k << m)
        let take = n.min(candidates.len());
        candidates.select_nth_unstable_by(take.saturating_sub(1), |a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates[..take]
            .iter()
            .map(|(k, _)| *k)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{GroupTarget, MeioConfig, OptimizationParameters, OptimizationScope};
    use crate::sku::{SkuRecord, TargetGroupRef};
    use std::collections::HashMap;

    fn make_config(workers: usize) -> MeioConfig {
        MeioConfig {
            scopes: vec![],
            optimization_params: vec![],
            parallel_workers: workers,
            distribution_threshold: 25,
            consider_eoq: true,
            line_fill_rate: true,
            precision_jump: 0.0,
            big_jump_threshold: 0.95,
        }
    }

    fn make_sku(item_id: i64, site_id: i64, demand: f64) -> SkuRecord {
        SkuRecord {
            item_id,
            site_id,
            total_demand_rate: demand,
            direct_demand_rate: demand,
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
            total_fcst_monthly: demand * 30.0,
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
            scenario_id: Some(42),
        }
    }

    #[test]
    fn optimizer_runs_without_panic() {
        let cfg = make_config(2);
        let targets = vec![GroupTarget {
            group_name: "group_A".to_string(),
            fill_rate_target: 0.90,
            max_budget: f64::INFINITY,
        }];

        let mut skus: SkuStore = HashMap::new();
        skus.insert((1, 10), make_sku(1, 10, 5.0));
        skus.insert((2, 10), make_sku(2, 10, 3.0));

        let optimizer = Optimizer::new(cfg, targets);
        let result = optimizer.run(skus).expect("optimizer should not error");

        assert_eq!(result.sku_results.len(), 2);
        assert_eq!(result.group_results.len(), 1);
    }

    #[test]
    fn optimizer_achieves_fill_rate_target() {
        let cfg = make_config(1);
        let targets = vec![GroupTarget {
            group_name: "group_A".to_string(),
            fill_rate_target: 0.80,
            max_budget: f64::INFINITY,
        }];

        let mut skus: SkuStore = HashMap::new();
        skus.insert((1, 10), make_sku(1, 10, 5.0));

        let optimizer = Optimizer::new(cfg, targets);
        let result = optimizer.run(skus).expect("optimizer should not error");

        let group = &result.group_results[0];
        assert!(
            group.achieved_fill_rate >= 0.79,
            "Expected fill rate ≥ 0.79, got {}",
            group.achieved_fill_rate
        );
    }

    #[test]
    fn top_n_candidates_returns_highest_mv() {
        let cfg = make_config(2);
        let optimizer = Optimizer::new(cfg, vec![]);

        let mut skus: SkuStore = HashMap::new();
        let mut s1 = make_sku(1, 10, 5.0);
        s1.marginal_value = 10.0;
        let mut s2 = make_sku(2, 10, 3.0);
        s2.marginal_value = 5.0;
        let mut s3 = make_sku(3, 10, 1.0);
        s3.marginal_value = 1.0;
        skus.insert(s1.key(), s1);
        skus.insert(s2.key(), s2);
        skus.insert(s3.key(), s3);

        let top2 = optimizer.top_n_candidates(&skus, 2);
        assert_eq!(top2.len(), 2);
        // Both top-2 should be either (1,10) or (2,10)
        for k in &top2 {
            assert!(k.0 == 1 || k.0 == 2, "Expected item_id 1 or 2, got {}", k.0);
        }
    }
}
