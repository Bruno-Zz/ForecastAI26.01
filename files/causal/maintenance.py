"""
Scheduled maintenance demand from task card library x maintenance calendar.

Scheduled demand is deterministic (date-certain) and summed by (item, site, period).
It is kept separate from unscheduled demand so planners can validate each component.
"""
import pandas as pd
from db.db import get_conn, get_schema


def load_task_cards(asset_type_ids: list[int] | None = None) -> pd.DataFrame:
    """Return causal_task_cards -> columns: check_type, asset_type_id, item_id, qty_per_event."""
    schema = get_schema()
    conn = get_conn()
    try:
        where = ""
        params: list = []
        if asset_type_ids:
            ph = ",".join(["%s"] * len(asset_type_ids))
            where = f"WHERE asset_type_id IN ({ph})"
            params = list(asset_type_ids)
        return pd.read_sql(f"""
            SELECT task_card_id, check_type, asset_type_id, item_id,
                   qty_per_event, is_mandatory
            FROM {schema}.causal_task_cards
            {where}
            ORDER BY asset_type_id, check_type
        """, conn, params=params or None)
    finally:
        conn.close()


def load_maintenance_calendar(scenario_id: int = 0,
                               horizon_start: str | None = None,
                               horizon_end: str | None = None) -> pd.DataFrame:
    """Return causal_maintenance_calendar filtered to horizon and scenario."""
    schema = get_schema()
    conn = get_conn()
    try:
        where_clauses = ["scenario_id = %s"]
        params: list = [scenario_id]
        if horizon_start:
            where_clauses.append("planned_date >= %s")
            params.append(horizon_start)
        if horizon_end:
            where_clauses.append("planned_date <= %s")
            params.append(horizon_end)
        where = " AND ".join(where_clauses)
        return pd.read_sql(f"""
            SELECT event_id, asset_id, asset_type_id, site_id,
                   check_type, planned_date, duration_days, scenario_id
            FROM {schema}.causal_maintenance_calendar
            WHERE {where}
            ORDER BY planned_date
        """, conn, params=params)
    finally:
        conn.close()


def compute_scheduled_demand(calendar_df: pd.DataFrame,
                             task_cards_df: pd.DataFrame,
                             periods: list[str]) -> pd.DataFrame:
    """
    Join calendar events to task cards; aggregate qty by (item_id, site_id, period).

    period = ISO week start that contains planned_date.
    Returns: DataFrame[item_id, site_id, period_start, scheduled_demand]
    """
    if calendar_df.empty or task_cards_df.empty:
        return pd.DataFrame(columns=["item_id", "site_id", "period_start", "scheduled_demand"])

    merged = calendar_df.merge(
        task_cards_df, on=["check_type", "asset_type_id"], how="inner"
    )
    if merged.empty:
        return pd.DataFrame(columns=["item_id", "site_id", "period_start", "scheduled_demand"])

    # Map planned_date to period_start (weekly buckets)
    merged["period_start"] = pd.to_datetime(merged["planned_date"]).dt.to_period("W").dt.start_time
    return (
        merged.groupby(["item_id", "site_id", "period_start"])["qty_per_event"]
        .sum()
        .reset_index()
        .rename(columns={"qty_per_event": "scheduled_demand"})
    )
