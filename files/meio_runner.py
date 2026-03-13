"""
meio_runner.py — MEIO scenario batch optimizer
===============================================

Loads SKU data from PostgreSQL once, applies per-scenario parameter overrides,
then calls the Rust `meio_optimizer.run_optimization_batch()` in a single call.
Rayon handles parallelism inside Rust; no Dask overhead for the hot loop.

Usage
-----
    from meio_runner import run_scenarios
    summary = run_scenarios(scenario_ids=[1, 2, 3])

    # Or from the CLI (pipeline integration):
    python meio_runner.py --scenario-ids 1 2 3
    python meio_runner.py --base-only          # run only the is_base=true scenario
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2.extras

# ── Path setup ───────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from db.db import bulk_insert, get_conn, get_schema, load_config_from_db

logger = logging.getLogger(__name__)

# ── Rust extension ────────────────────────────────────────────────────────────
try:
    import meio_optimizer  # type: ignore  # Rust PyO3 extension
    _OPTIMIZER_AVAILABLE = True
except ImportError:
    logger.warning("meio_optimizer Rust extension not found — optimization will be skipped")
    _OPTIMIZER_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def run_scenarios(
    scenario_ids: list[int] | None = None,
    base_only: bool = False,
    config_path: str | None = None,
) -> dict:
    """
    Run MEIO optimization for the requested scenarios.

    Parameters
    ----------
    scenario_ids : list of int, optional
        Explicit scenario IDs to run.  If None and base_only=False, runs all
        enabled scenarios (all rows in meio_scenarios).
    base_only : bool
        If True, run only the scenario with is_base=True.
    config_path : str, optional
        Path to config.yaml.  Falls back to DB config if None.

    Returns
    -------
    dict
        Summary: {scenario_id: {total_inventory_value, weighted_fill_rate,
                                 sku_count, group_count}}
    """
    if not _OPTIMIZER_AVAILABLE:
        logger.error("meio_optimizer not available — aborting")
        return {}

    schema = get_schema()

    # ── 1. Load scenarios ────────────────────────────────────────────────────
    scenarios = _load_scenarios(schema, scenario_ids, base_only)
    if not scenarios:
        logger.warning("No MEIO scenarios found to run")
        return {}
    logger.info("Running %d scenario(s): %s",
                len(scenarios), [s["scenario_id"] for s in scenarios])

    # ── 2. Load base SKU data ONCE ───────────────────────────────────────────
    logger.info("Loading SKU records …")
    base_skus, unit_cost_map = _load_sku_records(schema)
    if not base_skus:
        logger.warning("No SKU records found — aborting")
        return {}
    logger.info("Loaded %d SKU records", len(base_skus))

    # ── 3. Load base MEIO config + group targets ─────────────────────────────
    base_config, base_group_targets = _load_meio_config(schema)

    # ── 4. Attach segment-based j_target_groups ──────────────────────────────
    logger.info("Loading segment membership for j_target_groups …")
    tg_map = _load_target_groups_by_sku(schema)
    _attach_target_groups(base_skus, tg_map)
    logger.info("Attached target groups from segments to %d (item,site) pairs", len(tg_map))

    # ── 5. Load repair flows and attach to SKU records ──────────────────────
    repair_flows = _load_repair_flows(schema)
    _attach_repair_flows(base_skus, repair_flows)
    logger.info("Attached repair flows to %d SKUs", len(repair_flows))

    # ── 6. Build one batch entry per scenario ────────────────────────────────
    batches: list[dict] = []
    for s in scenarios:
        overrides = s.get("param_overrides") or {}
        patched_skus    = _apply_sku_overrides(base_skus, overrides)
        patched_config  = _apply_config_overrides(base_config, overrides)
        patched_targets = _apply_target_overrides(base_group_targets, overrides)
        batches.append({
            # run_optimization_batch expects pre-serialised JSON strings
            "skus_json":          json.dumps(patched_skus),
            "config_json":        json.dumps(patched_config),
            "group_targets_json": json.dumps(patched_targets),
            # carry scenario_id through for result tagging (not consumed by Rust)
            "_scenario_id":       s["scenario_id"],
        })

    # ── 7. Single Rust batch call (rayon-parallel inside) ────────────────────
    logger.info("Submitting %d batch(es) to meio_optimizer.run_optimization_batch …",
                len(batches))
    t0 = datetime.now(timezone.utc)

    # Strip the _scenario_id sentinel before passing to Rust
    rust_batches = [
        {k: v for k, v in b.items() if not k.startswith("_")}
        for b in batches
    ]
    raw_results: list[dict] = json.loads(
        meio_optimizer.run_optimization_batch(json.dumps(rust_batches))
    )

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    logger.info("Rust optimizer finished in %.2fs", elapsed)

    # ── 8. Tag results with scenario_id ─────────────────────────────────────
    all_sku_results:   list[dict] = []
    all_group_results: list[dict] = []
    now_ts = datetime.now(timezone.utc).isoformat()

    for batch, result in zip(batches, raw_results):
        sid = batch["_scenario_id"]
        for r in result.get("sku_results", []):
            r["scenario_id"] = sid
            r["run_at"]      = now_ts
            # inventory_value comes from Rust (committed_buffer × unit_cost)
            all_sku_results.append(r)
        for g in result.get("group_results", []):
            g["scenario_id"] = sid
            g["run_at"]      = now_ts
            all_group_results.append(g)

    # ── 9. Persist to DB ─────────────────────────────────────────────────────
    _save_sku_results(schema, all_sku_results)
    _save_group_results(schema, all_group_results)
    logger.info("Saved %d SKU results and %d group results",
                len(all_sku_results), len(all_group_results))

    # ── 10. Build summary ────────────────────────────────────────────────────
    return _build_summary(scenarios, raw_results)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_scenarios(schema: str, scenario_ids: list[int] | None,
                    base_only: bool) -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if base_only:
            cur.execute(f"SELECT * FROM {schema}.meio_scenarios WHERE is_base = TRUE")
        elif scenario_ids:
            cur.execute(
                f"SELECT * FROM {schema}.meio_scenarios WHERE scenario_id = ANY(%s)",
                (scenario_ids,),
            )
        else:
            cur.execute(f"SELECT * FROM {schema}.meio_scenarios ORDER BY scenario_id")
        return [dict(r) for r in cur.fetchall()]


def _load_sku_records(schema: str) -> tuple[list[dict], dict]:
    """
    Load SKU records from the DB.  Returns (sku_list, unit_cost_map).

    The query joins item, site, demand statistics, forecast, and target tables
    to produce the full SkuRecord JSON format expected by the Rust optimizer.
    Fields map 1-to-1 with src/sku.rs::SkuRecord.
    """
    sql = f"""
        SELECT
            i.item_id,
            i.site_id,
            COALESCE(i.demand_rate, 0.0)          AS total_demand_rate,
            COALESCE(i.direct_demand_rate, 0.0)   AS direct_demand_rate,
            COALESCE(i.indirect_demand_rate, 0.0) AS indirect_demand_rate,
            COALESCE(i.avg_size, 1.0)             AS avg_size,
            COALESCE(i.eoq, 0.0)                  AS eoq,
            COALESCE(i.leg_lead_time, 0.0)        AS leg_lead_time,
            COALESCE(i.total_lead_time, 0.0)      AS total_lead_time,
            COALESCE(i.wait_time, 0.0)            AS wait_time,
            COALESCE(i.current_wait_time, 0.0)    AS current_wait_time,
            COALESCE(i.dmd_stddev, 0.0)           AS dmd_stddev,
            COALESCE(i.lt_stddev, 0.0)            AS lt_stddev,
            COALESCE(i.mad, 0.0)                  AS mad,
            COALESCE(i.dmd_coeff_variation, 1.0)  AS dmd_coefficient_of_variation,
            COALESCE(i.varcoeff_max, 2.0)         AS varcoeff_max,
            COALESCE(i.unit_cost, 0.0)            AS unit_cost,
            COALESCE(i.sku_count, 1)              AS sku_count,
            COALESCE(i.fcst_monthly, 0.0)         AS total_fcst_monthly,
            COALESCE(i.on_hand, 0.0)              AS on_hand,
            COALESCE(i.max_fill_rate, 1.0)        AS sku_max_fill_rate,
            COALESCE(i.min_fill_rate, 0.0)        AS sku_min_fill_rate,
            COALESCE(i.max_sl_qty, 99999999.0)    AS sku_max_sl_qty,
            COALESCE(i.min_sl_qty, 0.0)           AS sku_min_sl_qty,
            COALESCE(i.max_sl_slices, 99999999.0) AS sku_max_sl_slices,
            COALESCE(i.min_sl_slices, 0.0)        AS sku_min_sl_slices,
            COALESCE(i.use_existing_inventory, false) AS use_existing_inventory,
            LEAST(COALESCE(i.max_fill_rate, 1.0),
                  COALESCE(i.min_fill_rate, 0.0)) AS sku_tgt_fillrate,
            '[]'::jsonb                              AS j_target_groups,
            COALESCE(i.group_participation, 1.0)    AS group_participation,
            COALESCE(i.repl_site_ids, '[]'::jsonb)  AS repl_site_ids,
            COALESCE(i.kits, '[]'::jsonb)            AS kits,
            COALESCE(i.components, '[]'::jsonb)      AS components,
            COALESCE(i.parent_ids, '[]'::jsonb)      AS parent_ids,
            COALESCE(i.asset_mode, false)            AS asset_mode,
            COALESCE(i.criticality, 1.0)             AS criticality,
            NULL                                     AS scenario_id,
            0.0                                      AS committed_buffer,
            -1.0                                     AS new_buffer,
            0.0                                      AS current_fill_rate,
            -1.0                                     AS new_fill_rate,
            0.0                                      AS marginal_value,
            false                                    AS sku_init_set
        FROM {schema}.item i
        WHERE COALESCE(i.demand_rate, 0.0) > 0
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()

    skus = []
    unit_cost_map = {}
    for row in rows:
        d = dict(row)
        # JSON array columns — already parsed by psycopg2 JSONB
        for arr_col in ("j_target_groups", "repl_site_ids", "kits", "components", "parent_ids"):
            if isinstance(d[arr_col], str):
                d[arr_col] = json.loads(d[arr_col])
        d["dependant_changes"] = []
        d["sku_init_set"] = bool(d["sku_init_set"])
        key = (d["item_id"], d["site_id"])
        unit_cost_map[key] = d.get("unit_cost", 0.0)
        skus.append(d)

    return skus, unit_cost_map


def _load_meio_config(schema: str) -> tuple[dict, list[dict]]:
    """
    Load the default MEIO config and group targets.

    Config scalars (parallel_workers, precision_jump, etc.) come from the DB /
    YAML config as before.

    Group targets are derived from the *segment* table so that MEIO groups are
    always in sync with the application's segment definitions.  Optional
    fill_rate_target / max_budget overrides are stored as a ``parameters`` row
    with ``parameter_type = 'meio_target'`` linked to the segment via
    ``parameter_segment``.
    """
    cfg = load_config_from_db()
    meio_cfg = cfg.get("meio", {})

    config = {
        "scopes":               meio_cfg.get("scopes", []),
        "optimization_params":  meio_cfg.get("optimization_params", []),
        "parallel_workers":     meio_cfg.get("parallel_workers", 0),
        "distribution_threshold": meio_cfg.get("distribution_threshold", 25),
        "consider_eoq":         meio_cfg.get("consider_eoq", True),
        "line_fill_rate":       meio_cfg.get("line_fill_rate", True),
        "precision_jump":       meio_cfg.get("precision_jump", 0.0),
        "big_jump_threshold":   meio_cfg.get("big_jump_threshold", 0.95),
        "asset_targets":        meio_cfg.get("asset_targets", []),
    }

    # Default fill-rate target and max budget when no per-segment override exists.
    default_fr     = float(meio_cfg.get("default_fill_rate_target", 0.95))
    default_budget = float(meio_cfg.get("default_max_budget", 1e12))

    # Load segments + any meio_target parameter overrides from the DB.
    group_targets = _load_group_targets_from_segments(
        schema, default_fr=default_fr, default_budget=default_budget
    )

    # Backward-compat: if no segments are defined yet, fall back to the YAML
    # optimization_params[].group_targets list (legacy config path).
    if not group_targets:
        for op in config["optimization_params"]:
            group_targets.extend(op.get("group_targets", []))

    return config, group_targets


def _load_group_targets_from_segments(
    schema: str,
    *,
    default_fr: float = 0.95,
    default_budget: float = 1e12,
) -> list[dict]:
    """
    Build a list of GroupTarget dicts from the segment table.

    Each segment becomes one ``{group_name, fill_rate_target, max_budget}``
    entry consumed by the Rust optimizer.  If a ``parameters`` row with
    ``parameter_type = 'meio_target'`` is linked to the segment via
    ``parameter_segment``, its ``parameters_set`` values override the defaults.

    Segments that are flagged ``is_default = TRUE`` and cover every SKU (e.g.
    the built-in "All" segment) are included so the optimizer always has at
    least one group constraint.
    """
    sql = f"""
        SELECT
            s.id          AS segment_id,
            s.name        AS group_name,
            COALESCE(
                (p.parameters_set->>'fill_rate_target')::float,
                %(default_fr)s
            )             AS fill_rate_target,
            COALESCE(
                (p.parameters_set->>'max_budget')::float,
                %(default_budget)s
            )             AS max_budget
        FROM {schema}.segment s
        LEFT JOIN {schema}.parameter_segment ps
               ON ps.segment_id = s.id
        LEFT JOIN {schema}.parameters p
               ON p.id = ps.parameter_id
              AND p.parameter_type = 'meio_target'
        ORDER BY s.id
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, {"default_fr": default_fr, "default_budget": default_budget})
        rows = cur.fetchall()

    return [
        {
            "group_name":       r["group_name"],
            "fill_rate_target": float(r["fill_rate_target"]),
            "max_budget":       float(r["max_budget"]),
        }
        for r in rows
    ]


def _load_repair_flows(schema: str) -> dict[tuple[int, int], dict]:
    """Load repair flow parameters keyed by (item_id, site_id)."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SELECT * FROM {schema}.meio_repair_flows")
        return {
            (r["item_id"], r["site_id"]): {
                "return_rate":     float(r["return_rate"]),
                "repair_yield":    float(r["repair_yield"]),
                "repair_tat_mean": float(r["repair_tat_mean"]),
                "repair_tat_cv":   float(r["repair_tat_cv"]),
                "wip_qty":         float(r["wip_qty"]),
            }
            for r in cur.fetchall()
        }


def _load_target_groups_by_sku(schema: str) -> dict[tuple[int, int], list[dict]]:
    """
    Return a mapping of (item_id, site_id) → j_target_groups list.

    Built from ``segment_membership`` joined to ``segment``.  Each SKU
    gets one entry per segment it belongs to.  The ``group_participation``
    weight is 1.0 (equal weight within a group); per-SKU overrides are not
    stored at segment-membership level.

    SKUs not present in ``segment_membership`` will receive an empty list
    (no group constraints — optimizer treats them as unconstrained).
    """
    sql = f"""
        SELECT
            sm.item_id,
            sm.site_id,
            s.name AS io_tgt_group
        FROM {schema}.segment_membership sm
        JOIN {schema}.segment s ON s.id = sm.segment_id
        WHERE sm.item_id IS NOT NULL
          AND sm.site_id  IS NOT NULL
        ORDER BY sm.item_id, sm.site_id, s.id
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()

    result: dict[tuple[int, int], list[dict]] = {}
    for r in rows:
        key = (int(r["item_id"]), int(r["site_id"]))
        result.setdefault(key, []).append(
            {"io_tgt_group": r["io_tgt_group"], "group_participation": 1.0}
        )
    return result


def _attach_target_groups(skus: list[dict],
                           tg_map: dict[tuple[int, int], list[dict]]) -> None:
    """
    Mutate SKU records in-place to inject j_target_groups from segment_membership.

    SKUs with no segment assignment keep an empty list, which means the
    optimizer optimises them without a group fill-rate constraint.
    """
    for sku in skus:
        key = (sku["item_id"], sku["site_id"])
        sku["j_target_groups"] = tg_map.get(key, [])


def _attach_repair_flows(skus: list[dict],
                         repair_flows: dict[tuple[int, int], dict]) -> None:
    """Mutate SKU records in-place to inject repair_flow where available."""
    for sku in skus:
        key = (sku["item_id"], sku["site_id"])
        rf = repair_flows.get(key)
        sku["repair_flow"] = rf  # None if not found (Rust: Option<RepairFlow>)


# ── Scenario override application ────────────────────────────────────────────

def _apply_sku_overrides(skus: list[dict], overrides: dict) -> list[dict]:
    """
    Apply sparse overrides to every SKU record.

    Supported override keys (under "sku_overrides"):
      demand_multiplier    — scale total/direct/indirect demand rates
      lead_time_multiplier — scale leg/total lead times
      lt_stddev_multiplier — scale lead-time standard deviation
      fill_rate_target     — override sku_tgt_fillrate for all SKUs
      service_level        — alias for fill_rate_target
    """
    sku_ov = overrides.get("sku_overrides", {})
    if not sku_ov:
        return skus  # no copy needed — return original list

    dm  = float(sku_ov.get("demand_multiplier",    1.0))
    ltm = float(sku_ov.get("lead_time_multiplier", 1.0))
    lsm = float(sku_ov.get("lt_stddev_multiplier", 1.0))
    frt = sku_ov.get("fill_rate_target") or sku_ov.get("service_level")

    result = []
    for sku in skus:
        s = copy.copy(sku)   # shallow copy — fast, avoids deep-copying arrays
        if dm != 1.0:
            s["total_demand_rate"]    = s["total_demand_rate"]    * dm
            s["direct_demand_rate"]   = s["direct_demand_rate"]   * dm
            s["indirect_demand_rate"] = s["indirect_demand_rate"] * dm
        if ltm != 1.0:
            s["leg_lead_time"]   = s["leg_lead_time"]   * ltm
            s["total_lead_time"] = s["total_lead_time"] * ltm
        if lsm != 1.0:
            s["lt_stddev"] = s.get("lt_stddev", 0.0) * lsm
        if frt is not None:
            s["sku_tgt_fillrate"]   = float(frt)
            s["sku_min_fill_rate"]  = float(frt)
        result.append(s)

    return result


def _apply_config_overrides(config: dict, overrides: dict) -> dict:
    """Apply sparse overrides to the MeioConfig dict."""
    cfg_ov = overrides.get("config_overrides", {})
    if not cfg_ov:
        return config
    result = copy.copy(config)
    result.update(cfg_ov)
    return result


def _apply_target_overrides(group_targets: list[dict], overrides: dict) -> list[dict]:
    """Apply sparse overrides to group targets (e.g. change fill_rate_target)."""
    tgt_ov = overrides.get("group_target_overrides", {})
    if not tgt_ov:
        return group_targets
    result = []
    for gt in group_targets:
        g = copy.copy(gt)
        if g["group_name"] in tgt_ov:
            g.update(tgt_ov[g["group_name"]])
        result.append(g)
    return result


# ── DB persistence ────────────────────────────────────────────────────────────

def _save_sku_results(schema: str, results: list[dict]) -> None:
    if not results:
        return
    cols  = ["scenario_id", "item_id", "site_id",
             "committed_buffer", "fill_rate", "marginal_value", "inventory_value"]
    # Delete previous results for these scenario_ids first
    sids = list({r["scenario_id"] for r in results})
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {schema}.meio_results WHERE scenario_id = ANY(%s)", (sids,)
        )
        conn.commit()

    rows = [
        (r["scenario_id"], r["item_id"], r["site_id"],
         r.get("committed_buffer", 0.0), r.get("current_fill_rate", 0.0),
         r.get("marginal_value", 0.0),   r.get("inventory_value", 0.0))
        for r in results
    ]
    bulk_insert(None, f"{schema}.meio_results", cols, rows,
                truncate=False)  # we already deleted above


def _load_segment_name_map(schema: str) -> dict[str, int]:
    """Return {segment_name: segment_id} for all segments in the DB."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT id, name FROM {schema}.segment")
        return {name: sid for sid, name in cur.fetchall()}


def _save_group_results(schema: str, results: list[dict]) -> None:
    if not results:
        return

    # Build segment name→id lookup so we can store segment_id alongside group_name.
    seg_map = _load_segment_name_map(schema)

    cols = ["scenario_id", "group_name", "segment_id", "achieved_fill_rate",
            "fill_rate_target", "achieved_budget", "max_budget", "completed"]
    sids = list({r["scenario_id"] for r in results})
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {schema}.meio_group_results WHERE scenario_id = ANY(%s)", (sids,)
        )
        conn.commit()

    rows = [
        (r["scenario_id"], r["group_name"],
         seg_map.get(r["group_name"]),          # None if no matching segment
         r.get("achieved_fill_rate", 0.0), r.get("fill_rate_target", 0.0),
         r.get("achieved_budget", 0.0),    r.get("max_budget", 0.0),
         bool(r.get("completed", False)))
        for r in results
    ]
    bulk_insert(None, f"{schema}.meio_group_results", cols, rows, truncate=False)


def _build_summary(scenarios: list[dict], raw_results: list[dict]) -> dict:
    summary = {}
    for s, result in zip(scenarios, raw_results):
        sid  = s["scenario_id"]
        skus = result.get("sku_results", [])
        grps = result.get("group_results", [])

        total_inv = sum(r.get("inventory_value", 0.0) for r in skus)
        # Weighted average fill rate (equal weight per SKU)
        fill_rates = [r.get("current_fill_rate", 0.0) for r in skus]
        avg_fr = (sum(fill_rates) / len(fill_rates)) if fill_rates else 0.0

        summary[sid] = {
            "scenario_name":        s.get("name", ""),
            "sku_count":            len(skus),
            "group_count":          len(grps),
            "total_inventory_value": round(total_inv, 2),
            "weighted_fill_rate":   round(avg_fr, 4),
            "iterations":           result.get("iterations", 0),
        }
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def _main():
    parser = argparse.ArgumentParser(
        description="Run MEIO inventory optimization for one or more scenarios"
    )
    parser.add_argument(
        "--scenario-ids", nargs="*", type=int, default=None,
        help="Specific scenario IDs to run (default: all)"
    )
    parser.add_argument(
        "--base-only", action="store_true",
        help="Run only the base scenario (is_base=TRUE)"
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    summary = run_scenarios(
        scenario_ids=args.scenario_ids,
        base_only=args.base_only,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    _main()
