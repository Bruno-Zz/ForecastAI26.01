//! Target dictionary: tracks service-level and budget progress per group.
//!
//! The `TargetDictionary` is mutated as the greedy algorithm commits SKUs.
//! It answers two questions:
//!
//! 1. **Is a group done?**  (`group_completed`)
//!    A group is done when *both* the fill-rate target AND the budget cap are
//!    satisfied (or exceeded).
//!
//! 2. **Are all groups done?**  (`all_completed`)
//!    When every group is done the optimizer terminates.

use std::collections::HashMap;

use crate::config::GroupTarget;
use crate::sku::{GroupResult, SkuStore, TargetGroupRef};

// ── GroupState ────────────────────────────────────────────────────────────

/// Mutable state for one target group, tracked across the optimization run.
#[derive(Debug, Clone)]
pub struct GroupState {
    /// Group identifier (matches `GroupTarget::group_name`).
    pub group_name: String,
    /// Target fill rate for this group.
    pub fill_rate_target: f64,
    /// Budget cap for this group.  `f64::INFINITY` = no cap.
    pub max_budget: f64,
    /// Weighted sum of current fill rates across all member SKUs.
    /// `achieved_fill_rate = weighted_fill_sum / total_direct_demand_rate`
    pub weighted_fill_sum: f64,
    /// Total direct demand rate of all member SKUs (the denominator).
    pub total_direct_demand_rate: f64,
    /// Total investment committed so far (cumulative unit_cost × Δbuffer).
    pub achieved_budget: f64,
    /// True once both fill_rate_target and max_budget constraints are met.
    pub completed: bool,
}

impl GroupState {
    /// Current achieved fill rate (weighted average).
    #[inline]
    pub fn achieved_fill_rate(&self) -> f64 {
        if self.total_direct_demand_rate > 0.0 {
            (self.weighted_fill_sum / self.total_direct_demand_rate).clamp(0.0, 1.0)
        } else {
            0.0
        }
    }

    /// Re-evaluate `completed` flag based on current state.
    fn update_completed(&mut self) {
        let fill_ok = self.achieved_fill_rate() >= self.fill_rate_target;
        let budget_ok = self.achieved_budget >= self.max_budget;
        self.completed = fill_ok || budget_ok;
    }
}

// ── TargetDictionary ──────────────────────────────────────────────────────

/// Manages service-level and budget tracking for all target groups.
#[derive(Debug)]
pub struct TargetDictionary {
    groups: HashMap<String, GroupState>,
}

impl TargetDictionary {
    // ── Constructor ───────────────────────────────────────────────────────

    /// Build the target dictionary from a list of `GroupTarget` definitions
    /// and the full SKU store (needed to compute initial demand totals).
    pub fn new(targets: &[GroupTarget], skus: &SkuStore) -> Self {
        let mut groups: HashMap<String, GroupState> = targets
            .iter()
            .map(|t| {
                (
                    t.group_name.clone(),
                    GroupState {
                        group_name: t.group_name.clone(),
                        fill_rate_target: t.fill_rate_target,
                        max_budget: t.max_budget,
                        weighted_fill_sum: 0.0,
                        total_direct_demand_rate: 0.0,
                        achieved_budget: 0.0,
                        completed: false,
                    },
                )
            })
            .collect();

        // Accumulate each SKU's direct demand rate into the groups it belongs to.
        for sku in skus.values() {
            for tgr in &sku.j_target_groups {
                if let Some(g) = groups.get_mut(&tgr.io_tgt_group) {
                    g.total_direct_demand_rate += sku.direct_demand_rate;
                }
            }
        }

        TargetDictionary { groups }
    }

    // ── Readers ───────────────────────────────────────────────────────────

    /// Current state of a group.  Returns `None` if the group is unknown.
    pub fn current_group_values(
        &self,
        group_name: &str,
    ) -> Option<(bool, f64, f64, f64, f64, f64, f64)> {
        self.groups.get(group_name).map(|g| {
            (
                g.completed,
                g.fill_rate_target,
                g.achieved_fill_rate(),
                g.total_direct_demand_rate,   // total demand (denominator)
                g.total_direct_demand_rate,   // direct demand (same in this model)
                g.max_budget,
                g.achieved_budget,
            )
        })
    }

    /// True when every registered group has reached its target.
    pub fn all_completed(&self) -> bool {
        self.groups.values().all(|g| g.completed)
    }

    // ── Commit ────────────────────────────────────────────────────────────

    /// Commit a SKU's fill-rate change to all groups it belongs to.
    ///
    /// Returns `(sku_completed, all_completed)`:
    /// - `sku_completed` — true when all groups this SKU belongs to are done.
    /// - `all_completed` — true when all groups in the dictionary are done.
    ///
    /// Mirrors Python `targetDictionary.commit_groups`.
    pub fn commit_groups(
        &mut self,
        target_group_refs: &[TargetGroupRef],
        new_fill_rate: f64,
        old_fill_rate: f64,
        direct_demand_rate: f64,
        new_buffer: f64,
        committed_buffer: f64,
        unit_cost: f64,
    ) -> (bool, bool) {
        let fill_rate_delta = new_fill_rate - old_fill_rate;
        let budget_delta = (new_buffer - committed_buffer) * unit_cost;

        let mut sku_all_groups_done = true;

        for tgr in target_group_refs {
            if let Some(g) = self.groups.get_mut(&tgr.io_tgt_group) {
                if !g.completed {
                    // Update weighted fill sum
                    g.weighted_fill_sum += direct_demand_rate * fill_rate_delta;
                    g.achieved_budget += budget_delta;
                    g.update_completed();
                }
                if !g.completed {
                    sku_all_groups_done = false;
                }
            }
        }

        let all_done = self.all_completed();
        (sku_all_groups_done, all_done)
    }

    // ── Marginal gain helper ──────────────────────────────────────────────

    /// Compute the marginal group gain for a SKU, given a proposed fill-rate
    /// increase.
    ///
    /// This is the sum across all non-completed groups of the increase in the
    /// group fill rate weighted by the group's demand (minus the SKU's own
    /// contribution, scaled by participation).
    ///
    /// Mirrors Python `group_completion_for_sku`.
    pub fn marginal_group_gain(
        &self,
        target_group_refs: &[TargetGroupRef],
        direct_demand_rate: f64,
        new_fill_rate: f64,
        current_fill_rate: f64,
        sku_group_participation: f64,
        sku_max_fill_rate: f64,
    ) -> f64 {
        // If the SKU would exceed its own max fill rate, gain is zero.
        if new_fill_rate > sku_max_fill_rate {
            return 0.0;
        }

        let fill_rate_increase = new_fill_rate - current_fill_rate;
        let mut total_gain = 0.0;
        let mut any_group_incomplete = false;

        for tgr in target_group_refs {
            if let Some(g) = self.groups.get(&tgr.io_tgt_group) {
                if g.completed {
                    continue;
                }
                any_group_incomplete = true;

                let group_dr = g.total_direct_demand_rate;
                if group_dr > 0.0 {
                    // New group fill rate = (old weighted sum + sku_increase) / group_dr
                    let new_group_fr =
                        (g.weighted_fill_sum + direct_demand_rate * fill_rate_increase) / group_dr;
                    let group_fr_increase = new_group_fr - g.achieved_fill_rate();

                    // Marginal gain = change in group fill rate × "other" demand
                    // (accounts for multi-group participation scaling)
                    let other_demand = group_dr
                        - direct_demand_rate
                            * (sku_group_participation - 1.0)
                            / sku_group_participation.max(1e-9);
                    total_gain += group_fr_increase * other_demand;
                }
            }
        }

        if !any_group_incomplete {
            0.0
        } else {
            total_gain
        }
    }

    // ── Output ────────────────────────────────────────────────────────────

    /// Convert all group states to `GroupResult` records for output.
    pub fn to_results(&self) -> Vec<GroupResult> {
        self.groups
            .values()
            .map(|g| GroupResult {
                group_name: g.group_name.clone(),
                achieved_fill_rate: g.achieved_fill_rate(),
                fill_rate_target: g.fill_rate_target,
                achieved_budget: g.achieved_budget,
                max_budget: g.max_budget,
                completed: g.completed,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GroupTarget;
    use std::collections::HashMap;

    fn make_targets() -> Vec<GroupTarget> {
        vec![GroupTarget {
            group_name: "group_A".to_string(),
            fill_rate_target: 0.95,
            max_budget: f64::INFINITY,
        }]
    }

    #[test]
    fn commit_increases_achieved_fill_rate() {
        let skus: SkuStore = HashMap::new();
        let mut td = TargetDictionary::new(&make_targets(), &skus);
        // Seed demand total manually
        td.groups.get_mut("group_A").unwrap().total_direct_demand_rate = 100.0;

        let refs = vec![TargetGroupRef {
            io_tgt_group: "group_A".to_string(),
            group_participation: 1.0,
        }];
        let (sku_done, all_done) = td.commit_groups(&refs, 0.5, 0.0, 100.0, 50.0, 0.0, 10.0);
        assert!(!sku_done);
        assert!(!all_done);
        assert!((td.groups["group_A"].achieved_fill_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn group_completes_when_target_reached() {
        let skus: SkuStore = HashMap::new();
        let mut td = TargetDictionary::new(&make_targets(), &skus);
        td.groups.get_mut("group_A").unwrap().total_direct_demand_rate = 100.0;

        let refs = vec![TargetGroupRef {
            io_tgt_group: "group_A".to_string(),
            group_participation: 1.0,
        }];
        let (sku_done, all_done) = td.commit_groups(&refs, 0.96, 0.0, 100.0, 96.0, 0.0, 10.0);
        assert!(sku_done);
        assert!(all_done);
    }
}
