"""Fleet plan loading and scenario patching."""
import pandas as pd
from db.db import get_conn, get_schema


def load_fleet_plan(scenario_id: int = 0,
                    site_ids: list[int] | None = None) -> pd.DataFrame:
    """Return causal_fleet_plan for the given scenario.

    Columns: asset_id, asset_type_id, site_id, period_start, period_end,
             util_hours, util_cycles, util_landings, util_calendar_days
    """
    schema = get_schema()
    conn = get_conn()
    try:
        where_clauses = ["scenario_id = %s", "is_active = TRUE"]
        params: list = [scenario_id]
        if site_ids:
            ph = ",".join(["%s"] * len(site_ids))
            where_clauses.append(f"site_id IN ({ph})")
            params.extend(site_ids)
        where = " AND ".join(where_clauses)
        return pd.read_sql(f"""
            SELECT fleet_plan_id, scenario_id, asset_id, asset_type_id, site_id,
                   period_start, period_end,
                   util_hours, util_cycles, util_landings, util_calendar_days,
                   is_active
            FROM {schema}.causal_fleet_plan
            WHERE {where}
            ORDER BY asset_id, period_start
        """, conn, params=params)
    finally:
        conn.close()


def load_scenarios(scenario_ids: list[int]) -> list[dict]:
    """Return causal_scenarios rows including fleet_overrides and mdfh_overrides."""
    if not scenario_ids:
        return []
    schema = get_schema()
    conn = get_conn()
    try:
        ph = ",".join(["%s"] * len(scenario_ids))
        df = pd.read_sql(f"""
            SELECT scenario_id, name, description, is_base,
                   fleet_overrides, mdfh_overrides, linked_meio_scenario_id
            FROM {schema}.causal_scenarios
            WHERE scenario_id IN ({ph})
            ORDER BY scenario_id
        """, conn, params=list(scenario_ids))
        records = df.to_dict(orient="records")
        # Ensure fleet_overrides and mdfh_overrides are dicts
        import json
        for r in records:
            for key in ("fleet_overrides", "mdfh_overrides"):
                if isinstance(r[key], str):
                    r[key] = json.loads(r[key])
                elif r[key] is None:
                    r[key] = {}
        return records
    finally:
        conn.close()


def apply_fleet_overrides(base_fleet: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    """
    Sparse patch to fleet plan.  Supported override keys:
      utilization_multiplier: float          — scale all utilisation metrics
      site_overrides: {site_id: {util_hours_multiplier: float, ...}}
      asset_type_overrides: {asset_type_id: {active: bool, util_hours_multiplier: float}}
      new_assets: [{asset_id, asset_type_id, site_id, period_start, ...}]
      retired_assets: [asset_id, ...]        — set is_active = False

    This is a shallow copy + apply — base_fleet is never mutated.
    """
    if not overrides:
        return base_fleet.copy()

    fleet = base_fleet.copy()

    # --- global utilisation multiplier ---
    util_cols = ["util_hours", "util_cycles", "util_landings", "util_calendar_days"]
    mul = float(overrides.get("utilization_multiplier", 1.0))
    if mul != 1.0:
        for col in util_cols:
            if col in fleet.columns:
                fleet[col] = fleet[col] * mul

    # --- per-site multipliers ---
    site_overrides = overrides.get("site_overrides", {})
    for site_id_str, site_patch in site_overrides.items():
        site_id = int(site_id_str)
        mask = fleet["site_id"] == site_id
        for metric, metric_mul in site_patch.items():
            col = f"util_{metric}" if not metric.startswith("util_") else metric
            col = col.replace("_multiplier", "")
            # Handle shorthand like util_hours_multiplier -> util_hours
            if col.endswith("_multiplier"):
                col = col[: -len("_multiplier")]
            if col in fleet.columns:
                fleet.loc[mask, col] = fleet.loc[mask, col] * float(metric_mul)

    # --- per-asset-type overrides ---
    at_overrides = overrides.get("asset_type_overrides", {})
    for at_id_str, at_patch in at_overrides.items():
        at_id = int(at_id_str)
        mask = fleet["asset_type_id"] == at_id
        if "active" in at_patch and not at_patch["active"]:
            fleet.loc[mask, "is_active"] = False
        for metric, metric_mul in at_patch.items():
            if metric == "active":
                continue
            col = f"util_{metric}" if not metric.startswith("util_") else metric
            if col.endswith("_multiplier"):
                col = col[: -len("_multiplier")]
            if col in fleet.columns:
                fleet.loc[mask, col] = fleet.loc[mask, col] * float(metric_mul)

    # --- retired assets ---
    retired = overrides.get("retired_assets", [])
    if retired:
        fleet.loc[fleet["asset_id"].isin(retired), "is_active"] = False

    # --- new assets ---
    new_assets = overrides.get("new_assets", [])
    if new_assets:
        new_rows = pd.DataFrame(new_assets)
        # Fill missing util columns with 0
        for col in util_cols:
            if col not in new_rows.columns:
                new_rows[col] = 0.0
        if "is_active" not in new_rows.columns:
            new_rows["is_active"] = True
        fleet = pd.concat([fleet, new_rows], ignore_index=True)

    # Only return active rows
    if "is_active" in fleet.columns:
        fleet = fleet[fleet["is_active"]].copy()

    return fleet
