"""
Causal scenario batch runner.

Workflow
--------
1. Load base fleet plan + BOM + effectivity + MDFH (once)
2. For each scenario: shallow-patch fleet plan -> compute demand
3. Save causal_results (all scenarios in one bulk insert)
4. Optionally: patch SkuRecord demand rates and feed meio_runner.run_scenarios()
"""
import logging

import pandas as pd

from db.db import get_conn, get_schema, bulk_insert
from causal.bom import load_bom, load_effectivity, build_effective_bom
from causal.fleet import load_fleet_plan, load_scenarios, apply_fleet_overrides
from causal.maintenance import (load_task_cards, load_maintenance_calendar,
                                compute_scheduled_demand)
from causal.demand_generator import generate_demand, aggregate_to_meio_rate

logger = logging.getLogger(__name__)


def run_causal_scenarios(scenario_ids: list[int],
                          horizon_periods: int = 24,
                          feed_meio: bool = True) -> dict:
    """
    Run causal demand generation for the given scenario IDs.

    Parameters
    ----------
    scenario_ids    : IDs of causal_scenarios rows to process
    horizon_periods : number of periods for MEIO rate aggregation
    feed_meio       : if True, push causal rates to meio_runner

    Returns
    -------
    dict with keys: scenarios_run, demand_rows
    """
    schema = get_schema()

    # -- 1. Load base data ONCE -------------------------------------------
    base_fleet = load_fleet_plan(scenario_id=0)
    bom_df = load_bom()
    eff_df = load_effectivity()
    task_cards = load_task_cards()
    scenarios = load_scenarios(scenario_ids)

    if not scenarios:
        logger.warning("run_causal_scenarios: no scenarios found for ids %s", scenario_ids)
        return {"scenarios_run": 0, "demand_rows": 0}

    # -- 2. Process each scenario -----------------------------------------
    all_demand_rows: list[dict] = []
    meio_rate_patches: list[pd.DataFrame] = []

    for sc in scenarios:
        sid = sc["scenario_id"]
        logger.info("Processing causal scenario %d: %s", sid, sc.get("name", ""))
        patched_fleet = apply_fleet_overrides(base_fleet, sc.get("fleet_overrides", {}))
        effective_bom = build_effective_bom(patched_fleet, bom_df, eff_df)
        calendar = load_maintenance_calendar(scenario_id=sid)
        sched_demand = compute_scheduled_demand(calendar, task_cards, periods=[])

        demand_df = generate_demand(effective_bom, patched_fleet, sched_demand, sid)
        all_demand_rows.extend(demand_df.to_dict(orient="records"))

        if feed_meio and not demand_df.empty:
            rate_df = aggregate_to_meio_rate(demand_df, horizon_periods)
            meio_rate_patches.append(rate_df)

    # -- 3. Bulk-save causal_results --------------------------------------
    if all_demand_rows:
        cols = ["scenario_id", "item_id", "site_id", "period_start",
                "demand_mean", "demand_stddev", "scheduled_demand",
                "unscheduled_demand", "removal_driver"]
        bulk_insert(schema, "causal_results", all_demand_rows, cols, page_size=5000)
    logger.info("Saved %d causal demand rows for %d scenarios",
                len(all_demand_rows), len(scenarios))

    # -- 4. Feed MEIO (optional) ------------------------------------------
    if feed_meio and meio_rate_patches:
        from meio_runner import run_scenarios as meio_run  # import inside guard
        all_rates = pd.concat(meio_rate_patches, ignore_index=True)
        linked_meio_ids = [sc["linked_meio_scenario_id"] for sc in scenarios
                           if sc.get("linked_meio_scenario_id")]
        if linked_meio_ids:
            meio_run(linked_meio_ids, causal_rate_overrides=all_rates)

    return {
        "scenarios_run": len(scenarios),
        "demand_rows": len(all_demand_rows),
    }
