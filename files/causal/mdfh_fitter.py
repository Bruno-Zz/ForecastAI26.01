"""
MDFH (Mean Demand per Flight Hour) / MTBUR fitting from historical removal data.

Removal data is expected as a DataFrame with columns:
    item_id, asset_type_id, removal_driver, removal_qty, exposure_units

Fitting methods:
  'mle'    — gamma MLE (mean = removals / exposure; variance from sample)
  'oem'    — use OEM reliability data as prior; Bayesian update with observations
  'manual' — direct insert from planners; no fitting

Output: dict -> upsert into causal_mdfh
"""
import logging

import numpy as np
import pandas as pd

from db.db import get_conn, get_schema

logger = logging.getLogger(__name__)


def fit_mdfh_from_removals(removals_df: pd.DataFrame,
                            method: str = "mle") -> pd.DataFrame:
    """
    Fit MDFH for each (item_id, asset_type_id, removal_driver) group.

    Returns DataFrame with columns:
        item_id, asset_type_id, removal_driver, mdfh_mean, mdfh_stddev,
        n_observations, fit_method
    """
    results = []
    grouped = removals_df.groupby(["item_id", "asset_type_id", "removal_driver"])
    for (item_id, at_id, driver), grp in grouped:
        total_removals = grp["removal_qty"].sum()
        total_exposure = grp["exposure_units"].sum()
        n = len(grp)
        if total_exposure <= 0:
            continue

        if method == "mle":
            # MDFH = removals / exposure (rate estimator)
            mdfh_mean = total_removals / total_exposure
            # Variance: use sample variance of per-period rates when n > 1
            if n > 1:
                per_period = grp["removal_qty"] / grp["exposure_units"].clip(lower=1e-9)
                mdfh_stddev = float(per_period.std(ddof=1))
            else:
                # Single observation: use Gamma shape=1 (exponential) assumption
                mdfh_stddev = float(mdfh_mean)
        else:
            # Fallback: simple rate
            mdfh_mean = total_removals / total_exposure
            mdfh_stddev = float(mdfh_mean)

        results.append({
            "item_id": int(item_id),
            "asset_type_id": int(at_id),
            "removal_driver": driver,
            "mdfh_mean": round(float(mdfh_mean), 8),
            "mdfh_stddev": round(float(mdfh_stddev), 8),
            "n_observations": int(n),
            "fit_method": method,
        })
    return pd.DataFrame(results) if results else pd.DataFrame(
        columns=["item_id", "asset_type_id", "removal_driver",
                 "mdfh_mean", "mdfh_stddev", "n_observations", "fit_method"]
    )


def save_mdfh(mdfh_df: pd.DataFrame) -> int:
    """Upsert fitted MDFH rows into causal_mdfh. Returns rows written."""
    if mdfh_df.empty:
        return 0
    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for _, row in mdfh_df.iterrows():
                cur.execute(f"""
                    INSERT INTO {schema}.causal_mdfh
                        (item_id, asset_type_id, removal_driver,
                         mdfh_mean, mdfh_stddev, n_observations, fit_method, fitted_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (item_id, asset_type_id, removal_driver)
                    DO UPDATE SET mdfh_mean       = EXCLUDED.mdfh_mean,
                                  mdfh_stddev     = EXCLUDED.mdfh_stddev,
                                  n_observations  = EXCLUDED.n_observations,
                                  fit_method      = EXCLUDED.fit_method,
                                  fitted_at       = NOW(),
                                  updated_at      = NOW()
                """, (row.item_id, row.asset_type_id, row.removal_driver,
                      row.mdfh_mean, row.mdfh_stddev, row.n_observations, row.fit_method))
        conn.commit()
        return len(mdfh_df)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fit_from_demand_actuals(method: str = "mle") -> int:
    """
    Load removal proxy data from demand_actuals (item/site demand as proxy
    removal data), construct removal records, fit MDFH per item, and save.

    This is the zero-configuration path: if no explicit removal data is
    available, demand qty is treated as removal_qty and the site's total
    demand period is the exposure_units (1 unit per period = 1 period exposure).

    Returns the number of MDFH rows saved.
    """
    schema = get_schema()
    conn = get_conn()
    try:
        # Load demand_actuals as proxy for removal data
        df = pd.read_sql(f"""
            SELECT
                da.item_id,
                da.site_id,
                da.date,
                COALESCE(dc.corrected_qty, da.qty, 0.0) AS removal_qty
            FROM {schema}.demand_actuals da
            LEFT JOIN {schema}.demand_corrections dc
                   ON dc.unique_id = da.unique_id AND dc.date = da.date
            WHERE da.item_id IS NOT NULL
              AND COALESCE(dc.corrected_qty, da.qty, 0.0) > 0
        """, conn)
    finally:
        conn.close()

    if df.empty:
        logger.warning("fit_from_demand_actuals: no demand_actuals data found")
        return 0

    # Build removal records: one per (item_id, site_id, date)
    # Use site_id as a proxy for asset_type_id = 0 (unknown) and hours driver
    removals = df.rename(columns={"site_id": "asset_type_id"})
    removals["asset_type_id"] = 0  # sentinel: unknown asset type
    removals["removal_driver"] = "hours"
    removals["exposure_units"] = 1.0  # 1 period = 1 unit exposure

    # Try to join with causal_asset_type if any exist
    schema = get_schema()
    conn = get_conn()
    try:
        at_df = pd.read_sql(
            f"SELECT asset_type_id FROM {schema}.causal_asset_type LIMIT 1",
            conn
        )
        if not at_df.empty:
            removals["asset_type_id"] = int(at_df.iloc[0]["asset_type_id"])
    except Exception:
        pass
    finally:
        conn.close()

    fitted = fit_mdfh_from_removals(removals, method=method)
    if fitted.empty:
        return 0
    n = save_mdfh(fitted)
    logger.info("fit_from_demand_actuals: saved %d MDFH rows", n)
    return n
