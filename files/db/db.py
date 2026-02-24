"""
Database helpers for the ForecastAI PostgreSQL backend (zcube schema).

Provides:
    get_conn(config_path)    – returns a psycopg2 connection
    init_schema(config_path) – creates the zcube schema and all required tables
    bulk_insert(...)         – generic TRUNCATE + execute_values helper
    jsonb_serialize(obj)     – converts numpy/pandas objects to JSON-safe Python
"""

import json
import logging
from pathlib import Path
from typing import List, Optional, Sequence, Tuple, Union

import psycopg2
import psycopg2.extras
import yaml

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Config & connection
# ═══════════════════════════════════════════════════════════════════════════

def _load_pg_config(config_path: Union[str, Path]) -> dict:
    """Read the postgres block from config.yaml."""
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg["data_source"]["postgres"]


def get_conn(config_path: Union[str, Path]) -> psycopg2.extensions.connection:
    """
    Create and return a new psycopg2 connection using config.yaml settings.

    The caller is responsible for calling conn.close() when done.
    """
    pg = _load_pg_config(config_path)
    conn = psycopg2.connect(
        host=pg.get("host", "localhost"),
        port=pg.get("port", 5432),
        dbname=pg.get("database", "postgres"),
        user=pg.get("user", "postgres"),
        password=pg.get("password", ""),
        options=f"-c search_path={pg.get('schema', 'zcube')},public",
    )
    conn.autocommit = False
    return conn


def get_schema(config_path: Union[str, Path]) -> str:
    """Return the schema name from config.yaml (default 'zcube')."""
    pg = _load_pg_config(config_path)
    return pg.get("schema", "zcube")


# ═══════════════════════════════════════════════════════════════════════════
# JSON / JSONB serialisation helpers
# ═══════════════════════════════════════════════════════════════════════════

def jsonb_serialize(obj):
    """
    Recursively convert numpy/pandas objects to JSON-safe Python types.

    Use this before passing values to psycopg2 for JSONB columns.
    Returns a ``json.dumps()``-ready string if *obj* is a dict or list,
    otherwise returns the scalar value.
    """
    import numpy as np

    def _convert(o):
        if o is None:
            return None
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, (np.floating,)):
            v = float(o)
            if np.isnan(v) or np.isinf(v):
                return None
            return v
        if isinstance(o, np.bool_):
            return bool(o)
        if isinstance(o, np.ndarray):
            return [_convert(x) for x in o.tolist()]
        if isinstance(o, dict):
            return {str(k): _convert(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [_convert(x) for x in o]
        if isinstance(o, float):
            import math
            if math.isnan(o) or math.isinf(o):
                return None
            return o
        return o

    converted = _convert(obj)
    if isinstance(converted, (dict, list)):
        return json.dumps(converted)
    return converted


# ═══════════════════════════════════════════════════════════════════════════
# Schema initialisation
# ═══════════════════════════════════════════════════════════════════════════

def init_schema(config_path: Union[str, Path]) -> None:
    """
    Ensure the zcube schema and all required tables exist.

    Safe to call repeatedly — uses IF NOT EXISTS everywhere.
    """
    pg = _load_pg_config(config_path)
    schema = pg.get("schema", "zcube")

    ddl = f"""
    -- Schema
    CREATE SCHEMA IF NOT EXISTS {schema};

    -- ─── Master tables ───────────────────────────────────────────────

    -- Items
    CREATE TABLE IF NOT EXISTS {schema}.item (
        id          INTEGER PRIMARY KEY,
        name        TEXT
    );

    -- Sites
    CREATE TABLE IF NOT EXISTS {schema}.site (
        id          INTEGER PRIMARY KEY,
        name        TEXT
    );

    -- ─── Demand actuals ──────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.demand_actuals (
        id              SERIAL PRIMARY KEY,
        item_id         INTEGER,
        site_id         INTEGER,
        channel         TEXT,
        date            DATE NOT NULL,
        qty             DOUBLE PRECISION,
        corrected_qty   DOUBLE PRECISION,
        item_name       TEXT,
        site_name       TEXT,
        unique_id       TEXT,
        UNIQUE (item_id, site_id, channel, date)
    );
    CREATE INDEX IF NOT EXISTS idx_demand_unique_id
        ON {schema}.demand_actuals (unique_id);

    -- ─── Detected outliers ───────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.detected_outliers (
        id                SERIAL PRIMARY KEY,
        unique_id         TEXT NOT NULL,
        date              DATE NOT NULL,
        original_value    DOUBLE PRECISION,
        corrected_value   DOUBLE PRECISION,
        detection_method  TEXT,
        correction_method TEXT,
        z_score           DOUBLE PRECISION,
        lower_bound       DOUBLE PRECISION,
        upper_bound       DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS idx_outliers_unique_id
        ON {schema}.detected_outliers (unique_id);

    -- ─── Time-series characteristics ─────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.time_series_characteristics (
        id                           SERIAL PRIMARY KEY,
        unique_id                    TEXT NOT NULL UNIQUE,
        n_observations               INTEGER,
        date_range_start             TEXT,
        date_range_end               TEXT,
        mean                         DOUBLE PRECISION,
        std                          DOUBLE PRECISION,
        has_seasonality              BOOLEAN,
        seasonal_periods             JSONB DEFAULT '[]',
        seasonal_strength            DOUBLE PRECISION,
        has_trend                    BOOLEAN,
        trend_direction              TEXT,
        trend_strength               DOUBLE PRECISION,
        is_intermittent              BOOLEAN,
        zero_ratio                   DOUBLE PRECISION,
        adi                          DOUBLE PRECISION,
        cov                          DOUBLE PRECISION,
        is_stationary                BOOLEAN,
        adf_pvalue                   DOUBLE PRECISION,
        complexity_score             DOUBLE PRECISION,
        complexity_level             TEXT,
        sufficient_for_ml            BOOLEAN,
        sufficient_for_deep_learning BOOLEAN,
        recommended_methods          JSONB DEFAULT '[]'
    );
    CREATE INDEX IF NOT EXISTS idx_chars_unique_id
        ON {schema}.time_series_characteristics (unique_id);

    -- ─── Forecast results (all methods) ──────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.forecast_results (
        id                SERIAL PRIMARY KEY,
        unique_id         TEXT NOT NULL,
        method            TEXT NOT NULL,
        point_forecast    JSONB,
        quantiles         JSONB,
        hyperparameters   JSONB,
        training_time     DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS idx_forecasts_uid_method
        ON {schema}.forecast_results (unique_id, method);

    -- ─── Backtest metrics ────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.backtest_metrics (
        id                SERIAL PRIMARY KEY,
        unique_id         TEXT NOT NULL,
        method            TEXT NOT NULL,
        forecast_origin   DATE,
        horizon           INTEGER,
        mae               DOUBLE PRECISION,
        rmse              DOUBLE PRECISION,
        mape              DOUBLE PRECISION,
        smape             DOUBLE PRECISION,
        mase              DOUBLE PRECISION,
        bias              DOUBLE PRECISION,
        crps              DOUBLE PRECISION,
        winkler_score     DOUBLE PRECISION,
        coverage_50       DOUBLE PRECISION,
        coverage_80       DOUBLE PRECISION,
        coverage_90       DOUBLE PRECISION,
        coverage_95       DOUBLE PRECISION,
        quantile_loss     DOUBLE PRECISION,
        aic               DOUBLE PRECISION,
        bic               DOUBLE PRECISION,
        aicc              DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS idx_metrics_uid
        ON {schema}.backtest_metrics (unique_id);

    -- ─── Forecasts by origin (per-step backtest detail) ──────────────

    CREATE TABLE IF NOT EXISTS {schema}.forecasts_by_origin (
        id                SERIAL PRIMARY KEY,
        unique_id         TEXT NOT NULL,
        method            TEXT NOT NULL,
        forecast_origin   DATE,
        horizon_step      INTEGER,
        point_forecast    DOUBLE PRECISION,
        actual_value      DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS idx_fbo_uid
        ON {schema}.forecasts_by_origin (unique_id, method);

    -- ─── Best method per series ──────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.best_method_per_series (
        id                SERIAL PRIMARY KEY,
        unique_id         TEXT NOT NULL UNIQUE,
        best_method       TEXT,
        best_score        DOUBLE PRECISION,
        runner_up_method  TEXT,
        runner_up_score   DOUBLE PRECISION,
        all_rankings      JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_bestmethod_uid
        ON {schema}.best_method_per_series (unique_id);

    -- ─── Fitted distributions (MEIO) ─────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.fitted_distributions (
        id                       SERIAL PRIMARY KEY,
        unique_id                TEXT NOT NULL,
        method                   TEXT NOT NULL,
        forecast_horizon         INTEGER,
        distribution_type        TEXT,
        mean                     DOUBLE PRECISION,
        std                      DOUBLE PRECISION,
        params                   JSONB,
        ks_statistic             DOUBLE PRECISION,
        ks_pvalue                DOUBLE PRECISION,
        service_level_quantiles  JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_dist_uid
        ON {schema}.fitted_distributions (unique_id, method);

    -- ─── Forecast adjustments (overrides / deltas entered via the UI) ─

    CREATE TABLE IF NOT EXISTS {schema}.forecast_adjustments (
        id              SERIAL PRIMARY KEY,
        unique_id       TEXT NOT NULL,
        forecast_date   DATE NOT NULL,
        adjustment_type TEXT NOT NULL CHECK (adjustment_type IN ('adjustment', 'override')),
        value           DOUBLE PRECISION NOT NULL,
        note            TEXT,
        created_by      TEXT DEFAULT 'ui',
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (unique_id, forecast_date, adjustment_type)
    );

    -- ─── Hyperparameter overrides (user-edited params from the UI) ───

    CREATE TABLE IF NOT EXISTS {schema}.hyperparameter_overrides (
        id          SERIAL PRIMARY KEY,
        unique_id   TEXT NOT NULL,
        method      TEXT NOT NULL,
        overrides   JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        updated_at  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (unique_id, method)
    );
    CREATE INDEX IF NOT EXISTS idx_hypoverrides_uid
        ON {schema}.hyperparameter_overrides (unique_id);

    -- ─── Process log (pipeline run tracking) ─────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.process_log (
        id              SERIAL PRIMARY KEY,
        run_id          TEXT NOT NULL,
        step_name       TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'pending',
        started_at      TIMESTAMPTZ,
        finished_at     TIMESTAMPTZ,
        duration_sec    DOUBLE PRECISION,
        rows_processed  INTEGER,
        error_message   TEXT,
        log_tail        TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_process_log_run
        ON {schema}.process_log (run_id);
    """

    # Separate ALTER statements for adding columns to existing tables
    alter_stmts = [
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'demand_actuals'
                  AND column_name = 'corrected_qty'
            ) THEN
                ALTER TABLE {schema}.demand_actuals
                    ADD COLUMN corrected_qty DOUBLE PRECISION;
            END IF;
        END $$;
        """,
    ]

    conn = get_conn(config_path)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
            for stmt in alter_stmts:
                cur.execute(stmt)
        conn.commit()
        logger.info(f"Schema '{schema}' and tables initialised")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Bulk insert helper
# ═══════════════════════════════════════════════════════════════════════════

def bulk_insert(
    config_path: Union[str, Path],
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Tuple],
    *,
    truncate: bool = True,
    delete_where: Optional[str] = None,
    page_size: int = 5000,
) -> int:
    """
    Generic bulk insert using psycopg2 execute_values.

    Parameters
    ----------
    config_path : str | Path
        Path to config.yaml.
    table_name : str
        Fully-qualified table name, e.g. ``'zcube.forecast_results'``.
    columns : sequence of str
        Column names matching the tuple order in *rows*.
    rows : sequence of tuple
        Data rows to insert.
    truncate : bool
        If True (default), TRUNCATE the table before inserting.
        Ignored when *delete_where* is provided.
    delete_where : str | None
        Optional SQL WHERE clause (without the WHERE keyword) for a
        targeted DELETE instead of TRUNCATE.  Example:
        ``"unique_id IN ('A', 'B')"`` will run
        ``DELETE FROM table WHERE unique_id IN ('A', 'B')``.
        When provided, *truncate* is ignored.
    page_size : int
        Rows per INSERT batch (default 5000).

    Returns
    -------
    int
        Number of rows inserted.
    """
    if not rows:
        logger.warning(f"bulk_insert: no rows for {table_name}")
        return 0

    cols_sql = ", ".join(columns)
    insert_sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES %s"

    conn = get_conn(config_path)
    try:
        with conn.cursor() as cur:
            if delete_where:
                cur.execute(f"DELETE FROM {table_name} WHERE {delete_where}")
            elif truncate:
                cur.execute(f"TRUNCATE TABLE {table_name}")
            psycopg2.extras.execute_values(
                cur, insert_sql, rows, page_size=page_size,
            )
        conn.commit()
        logger.info(f"bulk_insert: {len(rows):,} rows → {table_name}")
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_table(
    config_path: Union[str, Path],
    table_name: str,
    columns: str = "*",
    where: str = "",
) -> "pd.DataFrame":
    """
    Read a table (or filtered subset) into a pandas DataFrame.

    Parameters
    ----------
    config_path : str | Path
        Path to config.yaml.
    table_name : str
        Fully-qualified table name, e.g. ``'zcube.backtest_metrics'``.
    columns : str
        Column list for SELECT (default ``'*'``).
    where : str
        Optional WHERE clause (without the keyword), e.g. ``"unique_id = '123'"``

    Returns
    -------
    pd.DataFrame
    """
    import pandas as pd

    query = f"SELECT {columns} FROM {table_name}"
    if where:
        query += f" WHERE {where}"

    conn = get_conn(config_path)
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()
