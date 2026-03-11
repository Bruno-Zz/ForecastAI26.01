"""
One-time migration: load existing parquet output files into PostgreSQL tables.

Run from the files/ directory:
    python migrate_parquet_to_db.py

This populates the zcube schema with data from the output/ parquet files
that were produced by previous pipeline runs.
"""

import sys
import json
import logging
from pathlib import Path

import pandas as pd
import numpy as np

# Ensure files/ is on the path
_files_dir = Path(__file__).resolve().parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

from db.db import get_conn, init_schema, get_schema, bulk_insert, jsonb_serialize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = str(_files_dir / "config" / "config.yaml")


def _safe_json(val):
    """Convert a value to a JSON-safe Python object for JSONB columns."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return jsonb_serialize(val)


def migrate_characteristics():
    """Load time_series_characteristics.parquet -> zcube.time_series_characteristics."""
    pq = _files_dir / "output" / "time_series_characteristics.parquet"
    if not pq.exists():
        logger.warning(f"Skipping characteristics — {pq} not found")
        return
    df = pd.read_parquet(pq)
    if df.empty:
        logger.info("characteristics parquet is empty, skipping")
        return

    schema = get_schema(CONFIG_PATH)
    table = f"{schema}.time_series_characteristics"

    # JSONB columns
    jsonb_cols = {'seasonal_periods', 'recommended_methods'}
    cols = list(df.columns)
    rows = []
    for row in df.itertuples(index=False, name=None):
        rows.append(tuple(
            _safe_json(v) if c in jsonb_cols else (None if isinstance(v, float) and np.isnan(v) else v)
            for c, v in zip(cols, row)
        ))
    n = bulk_insert(CONFIG_PATH, table, cols, rows)
    logger.info(f"Loaded {n} rows into {table}")


def migrate_forecasts():
    """Load forecasts_all_methods.parquet -> zcube.forecast_results."""
    pq = _files_dir / "output" / "forecasts_all_methods.parquet"
    if not pq.exists():
        logger.warning(f"Skipping forecasts — {pq} not found")
        return
    df = pd.read_parquet(pq)
    if df.empty:
        logger.info("forecasts parquet is empty, skipping")
        return

    schema = get_schema(CONFIG_PATH)
    table = f"{schema}.forecast_results"

    # JSONB columns: point_forecast, quantiles, hyperparameters
    jsonb_cols = {'point_forecast', 'quantiles', 'hyperparameters'}
    cols = list(df.columns)
    rows = []
    for row in df.itertuples(index=False, name=None):
        rows.append(tuple(
            _safe_json(v) if c in jsonb_cols else (None if isinstance(v, float) and np.isnan(v) else v)
            for c, v in zip(cols, row)
        ))
    n = bulk_insert(CONFIG_PATH, table, cols, rows)
    logger.info(f"Loaded {n} rows into {table}")


def migrate_backtest_metrics():
    """Load backtest_metrics.parquet -> zcube.backtest_metrics."""
    pq = _files_dir / "output" / "backtest_metrics.parquet"
    if not pq.exists():
        logger.warning(f"Skipping backtest_metrics — {pq} not found")
        return
    df = pd.read_parquet(pq)
    if df.empty:
        logger.info("backtest_metrics parquet is empty, skipping")
        return

    schema = get_schema(CONFIG_PATH)
    table = f"{schema}.backtest_metrics"

    # Convert problematic object-type columns to float
    for col in ['coverage_80', 'aic', 'bic', 'aicc']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    cols = list(df.columns)
    rows = []
    for row in df.itertuples(index=False, name=None):
        rows.append(tuple(
            None if (isinstance(v, float) and np.isnan(v)) else v
            for v in row
        ))
    n = bulk_insert(CONFIG_PATH, table, cols, rows)
    logger.info(f"Loaded {n} rows into {table}")


def migrate_best_methods():
    """Load best_method_per_series.parquet -> zcube.best_method_per_series."""
    pq = _files_dir / "output" / "best_method_per_series.parquet"
    if not pq.exists():
        logger.warning(f"Skipping best_methods — {pq} not found")
        return
    df = pd.read_parquet(pq)
    if df.empty:
        logger.info("best_methods parquet is empty, skipping")
        return

    schema = get_schema(CONFIG_PATH)
    table = f"{schema}.best_method_per_series"

    # all_rankings is JSONB
    jsonb_cols = {'all_rankings'}
    cols = list(df.columns)
    rows = []
    for row in df.itertuples(index=False, name=None):
        rows.append(tuple(
            _safe_json(v) if c in jsonb_cols else (None if isinstance(v, float) and np.isnan(v) else v)
            for c, v in zip(cols, row)
        ))
    n = bulk_insert(CONFIG_PATH, table, cols, rows)
    logger.info(f"Loaded {n} rows into {table}")


def migrate_outliers():
    """Load detected_outliers.parquet -> zcube.detected_outliers."""
    pq = _files_dir / "output" / "detected_outliers.parquet"
    if not pq.exists():
        logger.warning(f"Skipping outliers — {pq} not found")
        return
    df = pd.read_parquet(pq)
    if df.empty:
        logger.info("outliers parquet is empty, skipping")
        return

    schema = get_schema(CONFIG_PATH)
    table = f"{schema}.detected_outliers"

    cols = list(df.columns)
    rows = []
    for row in df.itertuples(index=False, name=None):
        rows.append(tuple(
            None if (isinstance(v, float) and np.isnan(v)) else v
            for v in row
        ))
    n = bulk_insert(CONFIG_PATH, table, cols, rows)
    logger.info(f"Loaded {n} rows into {table}")


def migrate_forecasts_by_origin():
    """Load forecasts_by_origin.parquet -> zcube.forecasts_by_origin."""
    pq = _files_dir / "output" / "forecasts_by_origin.parquet"
    if not pq.exists():
        logger.warning(f"Skipping forecasts_by_origin — {pq} not found")
        return
    df = pd.read_parquet(pq)
    if df.empty:
        logger.info("forecasts_by_origin parquet is empty, skipping")
        return

    schema = get_schema(CONFIG_PATH)
    table = f"{schema}.forecasts_by_origin"

    cols = list(df.columns)
    rows = []
    for row in df.itertuples(index=False, name=None):
        rows.append(tuple(
            None if (isinstance(v, float) and np.isnan(v)) else v
            for v in row
        ))
    n = bulk_insert(CONFIG_PATH, table, cols, rows, page_size=10000)
    logger.info(f"Loaded {n} rows into {table}")


def reconstruct_demand_actuals():
    """
    Reconstruct demand_actuals from forecasts_by_origin actual values.

    The forecasts_by_origin parquet contains (unique_id, forecast_origin,
    horizon_step, actual_value) which gives us actual observations at various
    dates. We extract unique (unique_id, date, value) triples from these.
    """
    pq = _files_dir / "output" / "forecasts_by_origin.parquet"
    if not pq.exists():
        logger.warning("Cannot reconstruct demand_actuals — forecasts_by_origin not found")
        return

    df = pd.read_parquet(pq)
    if df.empty:
        logger.info("forecasts_by_origin is empty, cannot reconstruct demand_actuals")
        return

    schema = get_schema(CONFIG_PATH)

    # The forecast_origin column is a date string.
    # Each origin + horizon_step gives a target date.
    # We need to figure out the frequency (monthly) and compute target dates.
    # From the config, frequency is 'M' (monthly).
    logger.info("Reconstructing demand_actuals from forecasts_by_origin actual values...")

    # Parse forecast_origin to datetime
    df['forecast_origin'] = pd.to_datetime(df['forecast_origin'])

    # Each forecast_origin + horizon_step months = target date
    # Filter to rows that have actual_value (non-null)
    actuals = df[df['actual_value'].notna()].copy()
    if actuals.empty:
        logger.warning("No actual_value entries in forecasts_by_origin, trying outliers fallback")
        _fallback_demand_from_outliers()
        return

    # Compute target date: forecast_origin + horizon_step months
    actuals['date'] = actuals.apply(
        lambda r: r['forecast_origin'] + pd.DateOffset(months=int(r['horizon_step'])),
        axis=1,
    )
    actuals['qty'] = actuals['actual_value']

    # Deduplicate: same (unique_id, date) might appear from multiple origins
    actuals_dedup = actuals.groupby(['unique_id', 'date'])['qty'].mean().reset_index()
    actuals_dedup = actuals_dedup.sort_values(['unique_id', 'date'])

    logger.info(
        f"Reconstructed {len(actuals_dedup)} demand_actuals rows "
        f"for {actuals_dedup['unique_id'].nunique()} series"
    )

    # Now also add non-holdout history if we can infer it from characteristics
    # For now, just insert what we have
    table = f"{schema}.demand_actuals"
    cols = ['unique_id', 'date', 'qty']
    rows = [
        (row.unique_id, row.date, float(row.qty))
        for row in actuals_dedup.itertuples(index=False)
    ]
    n = bulk_insert(CONFIG_PATH, table, cols, rows)
    logger.info(f"Loaded {n} rows into {table}")


def _fallback_demand_from_outliers():
    """If forecasts_by_origin has no actuals, try outliers for at least some data."""
    pq = _files_dir / "output" / "detected_outliers.parquet"
    if not pq.exists():
        return
    df = pd.read_parquet(pq)
    if df.empty:
        return

    schema = get_schema(CONFIG_PATH)
    table = f"{schema}.demand_actuals"

    # detected_outliers has: unique_id, date, original_value, corrected_value
    records = df[['unique_id', 'date', 'original_value', 'corrected_value']].copy()
    records = records.rename(columns={'original_value': 'qty', 'corrected_value': 'corrected_qty'})
    records = records.drop_duplicates(subset=['unique_id', 'date'])

    cols = ['unique_id', 'date', 'qty', 'corrected_qty']
    rows = [
        (r.unique_id, r.date, float(r.qty), float(r.corrected_qty))
        for r in records.itertuples(index=False)
    ]
    n = bulk_insert(CONFIG_PATH, table, cols, rows)
    logger.info(f"Loaded {n} rows into {table} (from outliers fallback)")


def main():
    logger.info("=" * 60)
    logger.info("Starting parquet -> PostgreSQL migration")
    logger.info("=" * 60)

    # Ensure schema exists
    init_schema(CONFIG_PATH)

    # Migrate each output table
    migrate_characteristics()
    migrate_forecasts()
    migrate_backtest_metrics()
    migrate_best_methods()
    migrate_outliers()
    migrate_forecasts_by_origin()

    # Try to reconstruct demand_actuals from available data
    reconstruct_demand_actuals()

    # Final summary
    conn = get_conn(CONFIG_PATH)
    schema = get_schema(CONFIG_PATH)
    cur = conn.cursor()
    logger.info("=" * 60)
    logger.info("Migration complete. Table row counts:")
    for t in [
        'demand_actuals', 'time_series_characteristics',
        'forecast_results', 'backtest_metrics',
        'best_method_per_series', 'detected_outliers',
        'forecasts_by_origin',
    ]:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{t}")
        count = cur.fetchone()[0]
        logger.info(f"  {t}: {count:,} rows")
    conn.close()
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
