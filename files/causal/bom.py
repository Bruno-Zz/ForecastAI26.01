"""BOM loading, LRU->SRU tree traversal, and effectivity explosion."""
import pandas as pd
from db.db import get_conn, get_schema


def load_bom(asset_type_ids: list[int] | None = None) -> pd.DataFrame:
    """Return causal_bom joined to causal_mdfh for the given asset types.

    Result columns: bom_id, asset_type_id, item_id, qty_per_asset,
                    removal_driver, mdfh_mean, mdfh_stddev, is_lru, parent_bom_id
    """
    schema = get_schema()
    conn = get_conn()
    try:
        where = ""
        params: list = []
        if asset_type_ids:
            ph = ",".join(["%s"] * len(asset_type_ids))
            where = f"WHERE b.asset_type_id IN ({ph})"
            params = list(asset_type_ids)
        return pd.read_sql(f"""
            SELECT b.bom_id, b.asset_type_id, b.item_id, b.qty_per_asset,
                   b.removal_driver, b.is_lru, b.parent_bom_id,
                   COALESCE(b.mdfh_override, m.mdfh_mean, 0.0) AS mdfh_mean,
                   COALESCE(m.mdfh_stddev, 0.0)                 AS mdfh_stddev
            FROM {schema}.causal_bom b
            LEFT JOIN {schema}.causal_mdfh m
                   ON m.item_id = b.item_id
                  AND m.asset_type_id = b.asset_type_id
                  AND m.removal_driver = b.removal_driver
            {where}
        """, conn, params=params or None)
    finally:
        conn.close()


def load_effectivity() -> pd.DataFrame:
    """Return causal_effectivity rows (asset_id, item_id, effective, qty_override)."""
    schema = get_schema()
    conn = get_conn()
    try:
        return pd.read_sql(f"""
            SELECT effectivity_id, asset_id, asset_type_id, item_id,
                   effective, qty_override, effective_from, effective_to, sb_reference
            FROM {schema}.causal_effectivity
            ORDER BY asset_id, item_id
        """, conn)
    finally:
        conn.close()


def build_effective_bom(fleet_df: pd.DataFrame, bom_df: pd.DataFrame,
                        eff_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge fleet plan with BOM, apply per-tail effectivity overrides.

    Returns: DataFrame[asset_id, asset_type_id, item_id, removal_driver,
                        effective_qty, mdfh_mean, mdfh_stddev]

    effective_qty = bom.qty_per_asset * effectivity.qty_override (or 1.0 if None)
                    x 0 if effectivity.effective == False
    """
    # 1. Cross-join fleet x bom on asset_type_id
    merged = fleet_df[["asset_id", "asset_type_id"]].drop_duplicates().merge(
        bom_df, on="asset_type_id", how="left"
    )
    # 2. Apply effectivity overrides
    merged = merged.merge(
        eff_df[["asset_id", "item_id", "effective", "qty_override"]],
        on=["asset_id", "item_id"], how="left"
    )
    merged["effective"] = merged["effective"].fillna(True)
    merged["qty_override"] = merged["qty_override"].fillna(1.0)
    merged["effective_qty"] = (
        merged["qty_per_asset"] * merged["qty_override"] * merged["effective"].astype(float)
    )
    return merged[merged["effective_qty"] > 0].copy()


def explode_lru_to_sru(bom_df: pd.DataFrame) -> pd.DataFrame:
    """
    Walk the parent_bom_id tree to compute SRU demand from LRU removals.
    Returns augmented bom_df with SRU rows added (removal_driver inherited
    from LRU parent, qty multiplied down the tree).

    Uses iterative BFS to avoid recursion depth limits on deep BOMs.
    """
    if bom_df.empty:
        return bom_df.copy()

    # Index by bom_id for fast lookup
    bom_indexed = bom_df.set_index("bom_id")

    # Find all non-LRU rows (SRUs)
    sru_rows = bom_df[~bom_df["is_lru"]].copy()
    if sru_rows.empty:
        return bom_df.copy()

    result_rows = []

    for _, sru in sru_rows.iterrows():
        # Walk up the parent chain accumulating qty multiplier
        cumulative_qty = sru["qty_per_asset"]
        current_parent_id = sru.get("parent_bom_id")
        removal_driver = sru["removal_driver"]
        mdfh_mean = sru["mdfh_mean"]
        mdfh_stddev = sru["mdfh_stddev"]

        # BFS up to LRU ancestor
        visited = set()
        while current_parent_id is not None and current_parent_id not in visited:
            visited.add(current_parent_id)
            if current_parent_id not in bom_indexed.index:
                break
            parent = bom_indexed.loc[current_parent_id]
            cumulative_qty *= parent["qty_per_asset"]
            # Inherit removal driver from LRU root
            removal_driver = parent["removal_driver"]
            if parent.get("mdfh_mean", 0.0) > 0:
                mdfh_mean = parent["mdfh_mean"]
                mdfh_stddev = parent["mdfh_stddev"]
            if parent["is_lru"]:
                break
            current_parent_id = parent.get("parent_bom_id")

        # Emit a synthetic row representing SRU demand driven by LRU utilisation
        new_row = sru.to_dict()
        new_row["qty_per_asset"] = cumulative_qty
        new_row["removal_driver"] = removal_driver
        new_row["mdfh_mean"] = mdfh_mean
        new_row["mdfh_stddev"] = mdfh_stddev
        result_rows.append(new_row)

    if result_rows:
        sru_exploded = pd.DataFrame(result_rows)
        return pd.concat([bom_df, sru_exploded], ignore_index=True)
    return bom_df.copy()
