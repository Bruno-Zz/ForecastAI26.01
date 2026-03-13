"""
Causal demand generator.

D(item, site, period) = Sum_assets [
      effective_qty(asset, item)
    x mdfh_mean(item, asset_type, driver)
    x utilisation(asset, site, period, driver)
]

Variance (analytical — no MC needed):
  Var[D] = Sum_assets [
      effective_qty^2 x (mu_u^2 x sigma_m^2 + mu_m^2 x sigma_u^2 + sigma_m^2 x sigma_u^2)
  ]
  where mu_u / sigma_u = utilisation mean / std per period (from fleet plan variance)
        mu_m / sigma_m = MDFH mean / std

Returns per-(item, site, scenario, period) records ready for causal_results.
"""
import numpy as np
import pandas as pd


def generate_demand(effective_bom: pd.DataFrame,
                    fleet_plan: pd.DataFrame,
                    scheduled_demand: pd.DataFrame,
                    scenario_id: int) -> pd.DataFrame:
    """
    Parameters
    ----------
    effective_bom : DataFrame[asset_id, asset_type_id, item_id, removal_driver,
                               effective_qty, mdfh_mean, mdfh_stddev]
    fleet_plan    : DataFrame[asset_id, site_id, period_start, period_end,
                               util_hours, util_cycles, util_landings, util_calendar_days]
    scheduled_demand: DataFrame[item_id, site_id, period_start, scheduled_demand]
    scenario_id   : int

    Returns
    -------
    DataFrame[scenario_id, item_id, site_id, period_start,
              demand_mean, demand_stddev, scheduled_demand, unscheduled_demand,
              removal_driver]
    """
    if effective_bom.empty or fleet_plan.empty:
        return pd.DataFrame(columns=[
            "scenario_id", "item_id", "site_id", "period_start",
            "demand_mean", "demand_stddev", "scheduled_demand",
            "unscheduled_demand", "removal_driver"
        ])

    # Melt fleet plan to long-form per driver
    driver_map = {
        "hours": "util_hours", "cycles": "util_cycles",
        "landings": "util_landings", "calendar_days": "util_calendar_days"
    }
    available_util_cols = [c for c in driver_map.values() if c in fleet_plan.columns]
    fleet_long = fleet_plan.melt(
        id_vars=["asset_id", "site_id", "period_start"],
        value_vars=available_util_cols,
        var_name="util_col", value_name="utilisation"
    )
    # Reverse map util_col -> removal_driver
    rev = {v: k for k, v in driver_map.items()}
    fleet_long["removal_driver"] = fleet_long["util_col"].map(rev)

    # Join effective_bom to fleet_long on (asset_id, removal_driver)
    joined = effective_bom.merge(
        fleet_long, on=["asset_id", "removal_driver"], how="inner"
    )

    if joined.empty:
        return pd.DataFrame(columns=[
            "scenario_id", "item_id", "site_id", "period_start",
            "demand_mean", "demand_stddev", "scheduled_demand",
            "unscheduled_demand", "removal_driver"
        ])

    # Unscheduled demand per asset per period
    joined["asset_unscheduled"] = (
        joined["effective_qty"] * joined["mdfh_mean"] * joined["utilisation"]
    )

    # Variance per asset per period (analytical formula)
    # sigma_u = 0 for deterministic fleet plan (no utilisation uncertainty)
    sigma_u = pd.Series(0.0, index=joined.index)
    mu_u = joined["utilisation"]
    mu_m = joined["mdfh_mean"]
    sigma_m = joined["mdfh_stddev"]
    joined["asset_variance"] = joined["effective_qty"] ** 2 * (
        mu_u ** 2 * sigma_m ** 2
        + mu_m ** 2 * sigma_u ** 2
        + sigma_m ** 2 * sigma_u ** 2
    )

    # Aggregate to (item, site, period)
    agg = (
        joined.groupby(["item_id", "site_id", "period_start", "removal_driver"])
        .agg(unscheduled_demand=("asset_unscheduled", "sum"),
             demand_variance=("asset_variance", "sum"))
        .reset_index()
    )
    agg["demand_stddev"] = np.sqrt(agg["demand_variance"].clip(lower=0))

    # Merge scheduled demand
    if not scheduled_demand.empty:
        agg = agg.merge(
            scheduled_demand[["item_id", "site_id", "period_start", "scheduled_demand"]],
            on=["item_id", "site_id", "period_start"], how="left"
        )
    else:
        agg["scheduled_demand"] = 0.0

    agg["scheduled_demand"] = agg["scheduled_demand"].fillna(0.0)
    agg["demand_mean"] = agg["unscheduled_demand"] + agg["scheduled_demand"]

    agg["scenario_id"] = scenario_id
    return agg[["scenario_id", "item_id", "site_id", "period_start",
                "demand_mean", "demand_stddev", "scheduled_demand",
                "unscheduled_demand", "removal_driver"]]


def aggregate_to_meio_rate(demand_df: pd.DataFrame,
                            horizon_periods: int) -> pd.DataFrame:
    """
    Reduce the time-series demand into a scalar rate for MEIO.

    MEIO expects a steady-state rate (units/day).  Use the mean over the
    planning horizon; variance is averaged (independent periods assumption).

    Returns: DataFrame[item_id, site_id, scenario_id,
                        total_demand_rate, dmd_stddev]
    """
    if demand_df.empty:
        return pd.DataFrame(columns=["item_id", "site_id", "scenario_id",
                                     "total_demand_rate", "dmd_stddev"])

    grp = demand_df.groupby(["item_id", "site_id", "scenario_id"])
    rate = grp["demand_mean"].mean().rename("total_demand_rate")
    # Pooled stddev: sqrt(mean of variances) — conservative vs. total-period variance
    var = grp.apply(lambda g: (g["demand_stddev"] ** 2).mean() ** 0.5).rename("dmd_stddev")
    return pd.concat([rate, var], axis=1).reset_index()
