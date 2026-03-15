"""
Database helpers for the ForecastAI PostgreSQL backend (zcube schema).

Provides:
    get_conn()               – returns a psycopg2 connection
    get_schema()             – returns the target schema name
    load_config_from_db()    – load all default parameter sets as a config dict
    init_schema()            – creates the zcube schema and all required tables
    bulk_insert(...)         – generic TRUNCATE + execute_values helper
    jsonb_serialize(obj)     – converts numpy/pandas objects to JSON-safe Python

Connection credentials are read from (in priority order):
  1. config/config.yaml  — minimal file with only database section
  2. DB_* environment variables (or .env file loaded via python-dotenv)

All other settings (auth, ETL source DB, logging, forecasting parameters,
etc.) are stored in the zcube.parameters table — see load_config_from_db().
"""

import json
import logging
import os
from contextvars import ContextVar
from pathlib import Path
from typing import List, Optional, Sequence, Tuple, Union

import psycopg2
import psycopg2.extras

# Load .env if present (python-dotenv is in requirements)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

logger = logging.getLogger(__name__)

# ─── resolve the canonical config.yaml location once at import time ──────────
_CONFIG_YAML = Path(__file__).resolve().parent.parent / "config" / "config.yaml"


# ═══════════════════════════════════════════════════════════════════════════
# Per-request account context (multi-tenancy)
# ═══════════════════════════════════════════════════════════════════════════

# Holds the DB config dict for the current request's tenant account.
# Set by JWTAuthMiddleware after decoding the JWT's account_id.
# When None, get_conn() / get_schema() fall back to config.yaml (single-tenant path).
_account_ctx: ContextVar[Optional[dict]] = ContextVar("_account_ctx", default=None)


def set_account_context(cfg: dict):
    """Set the per-request account DB config.

    Returns the ContextVar token — pass to reset_account_context() in a
    finally block to restore the previous value.

    cfg keys: host, port, database, user, password, schema, sslmode
    """
    return _account_ctx.set(cfg)


def reset_account_context(token) -> None:
    """Reset the per-request account context using the token from set_account_context()."""
    _account_ctx.reset(token)


# ═══════════════════════════════════════════════════════════════════════════
# Config & connection
# ═══════════════════════════════════════════════════════════════════════════


def _get_pg_config() -> dict:
    """
    Read PostgreSQL connection settings.

    Priority:
      1. config/config.yaml  ->  database:  section
      2. DB_* environment variables (populated from .env or the process environment)
    """
    yaml_cfg: dict = {}
    if _CONFIG_YAML.exists():
        try:
            import yaml
            with open(_CONFIG_YAML, "r") as fh:
                raw = yaml.safe_load(fh) or {}
            yaml_cfg = raw.get("database", {})
        except Exception as exc:
            logger.debug("Could not read config.yaml database section: %s", exc)

    def _get(yaml_key: str, env_key: str, default: str) -> str:
        # Environment variables take priority so that multi-tenant pipeline
        # subprocesses can override config.yaml by injecting DB_* env vars.
        return os.environ.get(env_key) or yaml_cfg.get(yaml_key) or default

    return {
        "host":     _get("host",     "DB_HOST",     "localhost"),
        "port":     int(_get("port",     "DB_PORT",     "5432")),
        "database": _get("name",     "DB_NAME",     "postgres"),
        "user":     _get("user",     "DB_USER",     "postgres"),
        "password": _get("password", "DB_PASSWORD", ""),
        "schema":   _get("schema",   "DB_SCHEMA",   "zcube"),
        "sslmode":  _get("sslmode",  "DB_SSLMODE",  "disable"),
    }


def get_conn(config_path=None) -> psycopg2.extensions.connection:
    """
    Create and return a new psycopg2 connection.

    In multi-tenant mode the per-request ContextVar (_account_ctx) is checked
    first; if set, those credentials are used.  Otherwise falls back to
    config/config.yaml (database section) or DB_* env vars (single-tenant path).

    The legacy *config_path* parameter is accepted but ignored.
    The caller is responsible for calling conn.close() when done.
    """
    pg = _account_ctx.get() or _get_pg_config()
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["user"],
        password=pg["password"],
        sslmode=pg.get("sslmode", "disable"),
        options=f"-c search_path={pg['schema']},public",
    )
    conn.autocommit = False
    return conn


def get_schema(config_path=None) -> str:
    """Return the target schema name.

    In multi-tenant mode returns the schema from the per-request ContextVar.
    Falls back to config/config.yaml (database.schema) or DB_SCHEMA env var.
    The legacy *config_path* parameter is accepted but ignored.
    """
    ctx = _account_ctx.get()
    return ctx["schema"] if ctx else _get_pg_config()["schema"]


def load_config_from_db() -> dict:
    """
    Load all *default* parameter sets from the DB and return them as a
    unified config dict keyed by parameter_type.

    This is the runtime replacement for reading config.yaml.  Components call
    this when they cannot find a config file on disk.

    Returns a dict such as:
        {
            'outlier_detection': {...},
            'characterization': {...},
            'forecasting': {...},
            ...
        }
    """
    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT parameter_type, parameters_set "
                f"FROM {schema}.parameters WHERE is_default = TRUE"
            )
            result: dict = {}
            for param_type, param_set in cur.fetchall():
                result[param_type] = (
                    param_set if isinstance(param_set, dict)
                    else json.loads(param_set or "{}")
                )
            return result
    except Exception as exc:
        logger.warning("load_config_from_db failed: %s — returning empty config", exc)
        return {}
    finally:
        conn.close()


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


def init_schema(config_path=None) -> None:
    """
    Ensure the zcube schema and all required tables exist.

    Safe to call repeatedly — uses IF NOT EXISTS everywhere.
    The legacy *config_path* parameter is accepted but ignored.
    """
    schema = get_schema()

    ddl = f"""
    -- Schema
    CREATE SCHEMA IF NOT EXISTS {schema};

    -- ─── Mooncake columnar extension (optional — skip gracefully if absent) ──
    DO $$
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_mooncake CASCADE;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg_mooncake not available (%): columnar storage disabled.', SQLERRM;
    END $$;

    -- ─── Lookup / type tables (must come before item/site) ───────────

    -- Item types (mirrors dp_plan.dp_item_type)
    CREATE TABLE IF NOT EXISTS {schema}.item_type (
        id          BIGINT PRIMARY KEY,
        xuid        TEXT,
        name        TEXT,
        description TEXT
    );

    -- Site types (mirrors dp_plan.dp_site_type)
    CREATE TABLE IF NOT EXISTS {schema}.site_type (
        id          BIGINT PRIMARY KEY,
        xuid        TEXT,
        name        TEXT,
        description TEXT
    );

    -- ─── Master tables ───────────────────────────────────────────────

    -- Items (mirrors dp_plan.dp_item)
    CREATE TABLE IF NOT EXISTS {schema}.item (
        id          BIGINT PRIMARY KEY,
        xuid        TEXT,
        name        TEXT,
        description TEXT,
        attributes  JSONB,
        type_id     BIGINT,
        image_url   TEXT
    );

    -- Sites (mirrors dp_plan.dp_site)
    CREATE TABLE IF NOT EXISTS {schema}.site (
        id          BIGINT PRIMARY KEY,
        xuid        TEXT,
        name        TEXT,
        description TEXT,
        attributes  JSONB,
        type_id     BIGINT,
        longitude   NUMERIC(10,7),
        latitude    NUMERIC(10,7)
    );

    -- ─── Demand actuals ──────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.demand_actuals (
        item_id         INTEGER,
        site_id         INTEGER,
        channel         TEXT,
        date            DATE NOT NULL,
        qty             DOUBLE PRECISION,
        item_name       TEXT,
        site_name       TEXT,
        unique_id       TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_demand_unique_id
        ON {schema}.demand_actuals (unique_id);
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.routines
            WHERE routine_schema = 'mooncake' AND routine_name = 'create_table'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'mooncake_demand_actuals'
        ) THEN
            EXECUTE 'CALL mooncake.create_table(''mooncake_demand_actuals'', ''demand_actuals'')';
        END IF;
    END $$;

    -- Outlier corrections stored separately (heap — supports UPDATE)
    CREATE TABLE IF NOT EXISTS {schema}.demand_corrections (
        unique_id       TEXT NOT NULL,
        date            DATE NOT NULL,
        corrected_qty   DOUBLE PRECISION,
        PRIMARY KEY (unique_id, date)
    );

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
        upper_bound       DOUBLE PRECISION,
        scenario_id       BIGINT NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_outliers_unique_id
        ON {schema}.detected_outliers (unique_id);

    -- ─── Time-series characteristics ─────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.time_series_characteristics (
        id                           SERIAL PRIMARY KEY,
        unique_id                    TEXT NOT NULL,
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
        recommended_methods          JSONB DEFAULT '[]',
        scenario_id                  BIGINT NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_chars_unique_id
        ON {schema}.time_series_characteristics (unique_id);

    -- ─── Forecast results (all methods) ──────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.forecast_results (
        unique_id         TEXT NOT NULL,
        method            TEXT NOT NULL,
        point_forecast    JSONB,
        quantiles         JSONB,
        hyperparameters   JSONB,
        training_time     DOUBLE PRECISION,
        scenario_id       BIGINT NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_forecasts_uid_method
        ON {schema}.forecast_results (unique_id, method);
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.routines
            WHERE routine_schema = 'mooncake' AND routine_name = 'create_table'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'mooncake_forecast_results'
        ) THEN
            EXECUTE 'CALL mooncake.create_table(''mooncake_forecast_results'', ''forecast_results'')';
        END IF;
    END $$;

    -- ─── Backtest metrics ────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.backtest_metrics (
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
        aicc              DOUBLE PRECISION,
        metric_source     TEXT DEFAULT 'rolling_window',
        scenario_id       BIGINT NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_metrics_uid
        ON {schema}.backtest_metrics (unique_id);
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.routines
            WHERE routine_schema = 'mooncake' AND routine_name = 'create_table'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'mooncake_backtest_metrics'
        ) THEN
            EXECUTE 'CALL mooncake.create_table(''mooncake_backtest_metrics'', ''backtest_metrics'')';
        END IF;
    END $$;

    -- ─── Forecasts by origin (per-step backtest detail) ──────────────

    CREATE TABLE IF NOT EXISTS {schema}.forecasts_by_origin (
        unique_id         TEXT NOT NULL,
        method            TEXT NOT NULL,
        forecast_origin   DATE,
        horizon_step      INTEGER,
        point_forecast    DOUBLE PRECISION,
        actual_value      DOUBLE PRECISION,
        scenario_id       BIGINT NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_fbo_uid
        ON {schema}.forecasts_by_origin (unique_id, method);
    CREATE INDEX IF NOT EXISTS idx_fbo_uid_origin
        ON {schema}.forecasts_by_origin (unique_id, forecast_origin);
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.routines
            WHERE routine_schema = 'mooncake' AND routine_name = 'create_table'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'mooncake_forecasts_by_origin'
        ) THEN
            EXECUTE 'CALL mooncake.create_table(''mooncake_forecasts_by_origin'', ''forecasts_by_origin'')';
        END IF;
    END $$;

    -- ─── Series hashes (incremental processing) ──────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.series_hashes (
        unique_id     TEXT NOT NULL PRIMARY KEY,
        data_hash     TEXT,          -- MD5 of sorted (date, qty) values at last ETL
        forecast_hash TEXT,          -- data_hash value when last forecast was run
        hashed_at     TIMESTAMPTZ DEFAULT NOW()
    );

    -- Composite indexes for query performance
    CREATE INDEX IF NOT EXISTS idx_demand_uid_date
        ON {schema}.demand_actuals (unique_id, date DESC);
    CREATE INDEX IF NOT EXISTS idx_metrics_uid_origin
        ON {schema}.backtest_metrics (unique_id, forecast_origin);

    -- ─── Best method per series ──────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.best_method_per_series (
        id                SERIAL PRIMARY KEY,
        unique_id         TEXT NOT NULL,
        best_method       TEXT,
        best_score        DOUBLE PRECISION,
        runner_up_method  TEXT,
        runner_up_score   DOUBLE PRECISION,
        all_rankings      JSONB,
        scenario_id       BIGINT NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_bestmethod_uid
        ON {schema}.best_method_per_series (unique_id);

    -- ─── Fitted distributions (MEIO) ─────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.fitted_distributions (
        unique_id                TEXT NOT NULL,
        method                   TEXT NOT NULL,
        forecast_horizon         INTEGER,
        distribution_type        TEXT,
        mean                     DOUBLE PRECISION,
        std                      DOUBLE PRECISION,
        params                   JSONB,
        ks_statistic             DOUBLE PRECISION,
        ks_pvalue                DOUBLE PRECISION,
        service_level_quantiles  JSONB,
        scenario_id              BIGINT NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_dist_uid
        ON {schema}.fitted_distributions (unique_id, method);
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.routines
            WHERE routine_schema = 'mooncake' AND routine_name = 'create_table'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'mooncake_fitted_distributions'
        ) THEN
            EXECUTE 'CALL mooncake.create_table(''mooncake_fitted_distributions'', ''fitted_distributions'')';
        END IF;
    END $$;

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

    -- ─── Segments ────────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.segment (
        id          SERIAL PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT,
        criteria    JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        is_default  BOOLEAN DEFAULT FALSE,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        updated_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {schema}.segment_membership (
        id          SERIAL PRIMARY KEY,
        segment_id  INTEGER NOT NULL REFERENCES {schema}.segment(id) ON DELETE CASCADE,
        unique_id   TEXT NOT NULL,
        item_id     BIGINT,
        site_id     BIGINT,
        assigned_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (segment_id, unique_id)
    );
    CREATE INDEX IF NOT EXISTS idx_seg_membership_seg
        ON {schema}.segment_membership (segment_id);
    CREATE INDEX IF NOT EXISTS idx_seg_membership_uid
        ON {schema}.segment_membership (unique_id);

    -- ─── Process log (pipeline run tracking) ─────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.process_log (
        id              SERIAL PRIMARY KEY,
        run_id          TEXT NOT NULL,
        step_name       TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'pending',
        started_at      TIMESTAMPTZ,
        ended_at        TIMESTAMPTZ,
        duration_s      DOUBLE PRECISION,
        rows_processed  INTEGER,
        error_message   TEXT,
        log_tail        TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_process_log_run
        ON {schema}.process_log (run_id);

    -- Authentication: users
    CREATE TABLE IF NOT EXISTS {schema}.users (
        id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        email           TEXT NOT NULL UNIQUE,
        display_name    TEXT NOT NULL,
        hashed_password TEXT,
        auth_provider   TEXT NOT NULL DEFAULT 'local'
                        CHECK (auth_provider IN ('local', 'microsoft', 'google')),
        role            TEXT NOT NULL DEFAULT 'user'
                        CHECK (role IN ('admin', 'user')),
        is_active       BOOLEAN NOT NULL DEFAULT TRUE,
        allowed_segments JSONB DEFAULT '[]'::jsonb,
        can_run_process BOOLEAN DEFAULT FALSE,
        can_create_override BOOLEAN DEFAULT FALSE,
        allowed_segments_edit JSONB DEFAULT '[]'::jsonb,
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_users_email
        ON {schema}.users (email);

    -- Authentication: revoked JWT tokens (for logout / forced invalidation)
    CREATE TABLE IF NOT EXISTS {schema}.revoked_tokens (
        jti         TEXT PRIMARY KEY,
        revoked_at  TIMESTAMPTZ DEFAULT NOW()
    );

    -- Parameter sets (pipeline config stored in DB, versioned)
    CREATE TABLE IF NOT EXISTS {schema}.parameters (
        id              SERIAL PRIMARY KEY,
        parameter_type  TEXT NOT NULL,
        name            TEXT NOT NULL DEFAULT 'Default',
        label           TEXT NOT NULL,
        parameters_set  JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        description     TEXT,
        is_default      BOOLEAN DEFAULT FALSE,
        sort_order      INTEGER DEFAULT 0,
        updated_at      TIMESTAMPTZ DEFAULT NOW(),
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(parameter_type, name)
    );

    -- Junction: parameter versions ↔ segments
    CREATE TABLE IF NOT EXISTS {schema}.parameter_segment (
        id              SERIAL PRIMARY KEY,
        parameter_id    INTEGER NOT NULL REFERENCES {schema}.parameters(id) ON DELETE CASCADE,
        segment_id      INTEGER NOT NULL REFERENCES {schema}.segment(id) ON DELETE CASCADE,
        UNIQUE(parameter_id, segment_id)
    );
    CREATE INDEX IF NOT EXISTS idx_param_seg_param
        ON {schema}.parameter_segment (parameter_id);
    CREATE INDEX IF NOT EXISTS idx_param_seg_seg
        ON {schema}.parameter_segment (segment_id);

    -- Resolution: each series -> assigned parameter version per business type
    CREATE TABLE IF NOT EXISTS {schema}.series_parameter_assignment (
        unique_id                       TEXT NOT NULL PRIMARY KEY,
        item_id                         BIGINT,
        site_id                         BIGINT,
        forecasting_parameter_id        INTEGER REFERENCES {schema}.parameters(id) ON DELETE SET NULL,
        outlier_detection_parameter_id  INTEGER REFERENCES {schema}.parameters(id) ON DELETE SET NULL,
        characterization_parameter_id   INTEGER REFERENCES {schema}.parameters(id) ON DELETE SET NULL,
        evaluation_parameter_id         INTEGER REFERENCES {schema}.parameters(id) ON DELETE SET NULL,
        best_method_parameter_id        INTEGER REFERENCES {schema}.parameters(id) ON DELETE SET NULL,
        updated_at                      TIMESTAMPTZ DEFAULT NOW()
    );

    -- ─── ABC Classification ──────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.abc_configuration (
        id              SERIAL PRIMARY KEY,
        name            TEXT NOT NULL UNIQUE,
        metric          TEXT NOT NULL CHECK (metric IN ('hits', 'demand', 'value')),
        lookback_months INTEGER NOT NULL DEFAULT 12,
        granularity     TEXT NOT NULL DEFAULT 'item_site'
                        CHECK (granularity IN ('item_site', 'item')),
        method          TEXT NOT NULL DEFAULT 'cumulative_pct'
                        CHECK (method IN ('cumulative_pct', 'rank_pct', 'rank_absolute')),
        class_labels    JSONB NOT NULL DEFAULT '["A","B","C"]'::jsonb,
        thresholds      JSONB NOT NULL DEFAULT '[80, 95]'::jsonb,
        segment_id      INTEGER REFERENCES {schema}.segment(id) ON DELETE SET NULL,
        is_active       BOOLEAN DEFAULT TRUE,
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {schema}.abc_results (
        id              SERIAL PRIMARY KEY,
        config_id       INTEGER NOT NULL REFERENCES {schema}.abc_configuration(id) ON DELETE CASCADE,
        unique_id       TEXT NOT NULL,
        class_label     TEXT NOT NULL,
        metric_value    DOUBLE PRECISION,
        rank            INTEGER,
        cumulative_pct  DOUBLE PRECISION,
        computed_at     TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (config_id, unique_id)
    );
    CREATE INDEX IF NOT EXISTS idx_abc_results_config
        ON {schema}.abc_results (config_id);
    CREATE INDEX IF NOT EXISTS idx_abc_results_uid
        ON {schema}.abc_results (unique_id);

    -- ─── Audit log ────────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.audit_log (
        id              SERIAL PRIMARY KEY,
        entity_type     TEXT NOT NULL,
        entity_id       INTEGER,
        action          TEXT NOT NULL,
        old_value       JSONB,
        new_value       JSONB,
        changed_by      TEXT DEFAULT 'system',
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_audit_entity
        ON {schema}.audit_log (entity_type, entity_id);
    CREATE INDEX IF NOT EXISTS idx_audit_created
        ON {schema}.audit_log (created_at DESC);

    -- ─── Forecast scenarios ──────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS {schema}.forecast_scenarios (
        scenario_id      BIGSERIAL PRIMARY KEY,
        name             TEXT NOT NULL,
        description      TEXT,
        is_base          BOOLEAN NOT NULL DEFAULT FALSE,
        status           TEXT NOT NULL DEFAULT 'pending',
        run_at           TIMESTAMPTZ,
        error_msg        TEXT,
        created_by       TEXT,
        created_at       TIMESTAMPTZ DEFAULT NOW(),
        param_overrides  JSONB NOT NULL DEFAULT '{{}}',
        demand_overrides JSONB NOT NULL DEFAULT '{{}}'
    );
    CREATE INDEX IF NOT EXISTS idx_fscen_status
        ON {schema}.forecast_scenarios (status);

    -- ─── Causal / Asset-Driven Demand tables ──────────────────────────

    -- 1. Asset type master
    CREATE TABLE IF NOT EXISTS {schema}.causal_asset_type (
        asset_type_id    BIGSERIAL PRIMARY KEY,
        code             TEXT NOT NULL UNIQUE,
        name             TEXT,
        removal_drivers  TEXT[] NOT NULL DEFAULT ARRAY['hours','cycles'],
        aog_cost_per_day DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        mean_aog_days    DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        updated_at       TIMESTAMPTZ DEFAULT NOW()
    );

    -- 2. BOM lines (per asset type; supports LRU->SRU parent chain)
    CREATE TABLE IF NOT EXISTS {schema}.causal_bom (
        bom_id           BIGSERIAL PRIMARY KEY,
        asset_type_id    BIGINT NOT NULL REFERENCES {schema}.causal_asset_type(asset_type_id),
        item_id          BIGINT NOT NULL,
        qty_per_asset    DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        removal_driver   TEXT NOT NULL DEFAULT 'hours',
        mdfh_override    DOUBLE PRECISION,
        is_lru           BOOLEAN NOT NULL DEFAULT TRUE,
        repair_yield     DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        parent_bom_id    BIGINT REFERENCES {schema}.causal_bom(bom_id),
        updated_at       TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (asset_type_id, item_id)
    );

    -- 3. Per-asset-instance effectivity (tail/serial level BOM overrides)
    CREATE TABLE IF NOT EXISTS {schema}.causal_effectivity (
        effectivity_id   BIGSERIAL PRIMARY KEY,
        asset_id         TEXT NOT NULL,
        asset_type_id    BIGINT NOT NULL REFERENCES {schema}.causal_asset_type(asset_type_id),
        item_id          BIGINT NOT NULL,
        effective        BOOLEAN NOT NULL DEFAULT TRUE,
        qty_override     DOUBLE PRECISION,
        effective_from   DATE,
        effective_to     DATE,
        sb_reference     TEXT,
        updated_at       TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (asset_id, item_id)
    );

    -- 4. Fleet plan (per asset instance x site x period)
    CREATE TABLE IF NOT EXISTS {schema}.causal_fleet_plan (
        fleet_plan_id      BIGSERIAL PRIMARY KEY,
        scenario_id        BIGINT NOT NULL DEFAULT 0,
        asset_id           TEXT NOT NULL,
        asset_type_id      BIGINT NOT NULL REFERENCES {schema}.causal_asset_type(asset_type_id),
        site_id            BIGINT NOT NULL,
        period_start       DATE NOT NULL,
        period_end         DATE NOT NULL,
        util_hours         DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        util_cycles        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        util_landings      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        util_calendar_days DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        is_active          BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at         TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_causal_fleet ON {schema}.causal_fleet_plan
        (scenario_id, site_id, period_start);

    -- 5. Fitted MDFH / MTBUR per part x asset type x removal driver
    CREATE TABLE IF NOT EXISTS {schema}.causal_mdfh (
        mdfh_id          BIGSERIAL PRIMARY KEY,
        item_id          BIGINT NOT NULL,
        asset_type_id    BIGINT NOT NULL REFERENCES {schema}.causal_asset_type(asset_type_id),
        removal_driver   TEXT NOT NULL DEFAULT 'hours',
        mdfh_mean        DOUBLE PRECISION NOT NULL,
        mdfh_stddev      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        n_observations   INT NOT NULL DEFAULT 0,
        fit_method       TEXT NOT NULL DEFAULT 'mle',
        fitted_at        TIMESTAMPTZ DEFAULT NOW(),
        updated_at       TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (item_id, asset_type_id, removal_driver)
    );

    -- 6. Scheduled maintenance task card library
    CREATE TABLE IF NOT EXISTS {schema}.causal_task_cards (
        task_card_id     BIGSERIAL PRIMARY KEY,
        check_type       TEXT NOT NULL,
        asset_type_id    BIGINT REFERENCES {schema}.causal_asset_type(asset_type_id),
        item_id          BIGINT NOT NULL,
        qty_per_event    DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        is_mandatory     BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at       TIMESTAMPTZ DEFAULT NOW()
    );

    -- 7. Planned maintenance calendar (per asset instance)
    CREATE TABLE IF NOT EXISTS {schema}.causal_maintenance_calendar (
        event_id         BIGSERIAL PRIMARY KEY,
        asset_id         TEXT NOT NULL,
        asset_type_id    BIGINT NOT NULL REFERENCES {schema}.causal_asset_type(asset_type_id),
        site_id          BIGINT NOT NULL,
        check_type       TEXT NOT NULL,
        planned_date     DATE NOT NULL,
        duration_days    INT NOT NULL DEFAULT 1,
        scenario_id      BIGINT NOT NULL DEFAULT 0,
        updated_at       TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_causal_maint ON {schema}.causal_maintenance_calendar
        (scenario_id, site_id, planned_date);

    -- 8. Causal scenario definitions
    CREATE TABLE IF NOT EXISTS {schema}.causal_scenarios (
        scenario_id             BIGSERIAL PRIMARY KEY,
        name                    TEXT NOT NULL,
        description             TEXT,
        is_base                 BOOLEAN NOT NULL DEFAULT FALSE,
        created_by              TEXT,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        fleet_overrides         JSONB NOT NULL DEFAULT '{{}}',
        mdfh_overrides          JSONB NOT NULL DEFAULT '{{}}',
        linked_meio_scenario_id BIGINT
    );

    -- 9. Causal demand output (feeds MEIO; columnar for fast period-range reads)
    CREATE TABLE IF NOT EXISTS {schema}.causal_results (
        scenario_id        BIGINT NOT NULL,
        item_id            BIGINT NOT NULL,
        site_id            BIGINT NOT NULL,
        period_start       DATE NOT NULL,
        demand_mean        DOUBLE PRECISION NOT NULL,
        demand_stddev      DOUBLE PRECISION NOT NULL,
        scheduled_demand   DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        unscheduled_demand DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        removal_driver     TEXT,
        run_at             TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_causal_res ON {schema}.causal_results
        (scenario_id, item_id, site_id, period_start);
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.routines
            WHERE routine_schema = 'mooncake' AND routine_name = 'create_table'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'mooncake_causal_results'
        ) THEN
            EXECUTE 'CALL mooncake.create_table(''mooncake_causal_results'', ''causal_results'')';
        END IF;
    END $$;

    -- 10. Multi-site pooling recommendations (Phase 2)
    CREATE TABLE IF NOT EXISTS {schema}.causal_pooling_recommendations (
        scenario_id          BIGINT NOT NULL,
        item_id              BIGINT NOT NULL,
        hub_site_id          BIGINT NOT NULL,
        n_sites              INT NOT NULL,
        local_investment     DOUBLE PRECISION NOT NULL,
        pooled_investment    DOUBLE PRECISION NOT NULL,
        savings_pct          DOUBLE PRECISION NOT NULL,
        recommended_strategy TEXT NOT NULL,
        run_at               TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (scenario_id, item_id)
    );

    -- 11. Historical AOG events (Phase 3)
    CREATE TABLE IF NOT EXISTS {schema}.causal_aog_events (
        event_id        BIGSERIAL PRIMARY KEY,
        asset_id        TEXT NOT NULL,
        asset_type_id   BIGINT NOT NULL,
        item_id         BIGINT NOT NULL,
        event_date      DATE NOT NULL,
        duration_days   DOUBLE PRECISION NOT NULL,
        cost_actual     DOUBLE PRECISION,
        notes           TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );

    -- ─── MEIO supply chain tables ─────────────────────────────────────────

    -- Route types (BUY / MAKE / TRANSFER planning policies)
    CREATE TABLE IF NOT EXISTS {schema}.route_type (
        id                      BIGINT PRIMARY KEY,
        xuid                    TEXT NOT NULL,
        name                    TEXT NOT NULL,
        description             TEXT,
        planning_type           TEXT NOT NULL DEFAULT 'BUY',
        calc_supply_type_id     BIGINT,
        calc_dep_demand_type_id BIGINT
    );

    -- Item master for supply chain (extended vs demand item)
    CREATE TABLE IF NOT EXISTS {schema}.meio_item (
        id          BIGINT PRIMARY KEY,
        xuid        TEXT NOT NULL,
        name        TEXT NOT NULL,
        description TEXT,
        attributes  JSONB,
        type_id     BIGINT,
        group_id    BIGINT
    );

    -- Supply routes (item x site with lead times, costs, sourcing policy)
    CREATE TABLE IF NOT EXISTS {schema}.route (
        id                           BIGINT PRIMARY KEY,
        item_id                      BIGINT NOT NULL,
        site_id                      BIGINT NOT NULL,
        supplier_id                  BIGINT,
        type_id                      BIGINT NOT NULL REFERENCES {schema}.route_type(id),
        tag                          TEXT,
        source_item_id               BIGINT,
        source_site_id               BIGINT,
        bom_alternate                TEXT,
        min_qty                      DOUBLE PRECISION,
        mult_qty                     DOUBLE PRECISION,
        max_qty                      DOUBLE PRECISION,
        quota                        DOUBLE PRECISION,
        priority                     SMALLINT,
        lead_time                    SMALLINT,
        pick_pack_time               SMALLINT,
        transit_time                 SMALLINT,
        inspection_time              SMALLINT,
        safety_lead_time             SMALLINT,
        ptf                          SMALLINT,
        lead_time_calendar_id        BIGINT,
        pick_pack_time_calendar_id   BIGINT,
        ship_calendar_id             BIGINT,
        transit_time_calendar_id     BIGINT,
        dock_calendar_id             BIGINT,
        inspection_time_calendar_id  BIGINT,
        safety_lead_time_calendar_id BIGINT,
        ptf_calendar_id              BIGINT,
        yield                        DOUBLE PRECISION,
        unit_cost                    DOUBLE PRECISION,
        order_cost                   DOUBLE PRECISION,
        unit_cost_currency_id        BIGINT,
        order_cost_currency_id       BIGINT,
        uom_id                       BIGINT,
        end_date                     DATE
    );
    CREATE INDEX IF NOT EXISTS idx_route_item_site
        ON {schema}.route (item_id, site_id);

    -- Bill of materials (parent → child component relationships)
    CREATE TABLE IF NOT EXISTS {schema}.bill_of_material (
        id              BIGINT PRIMARY KEY,
        item_id         BIGINT NOT NULL,
        site_id         BIGINT NOT NULL,
        child_item_id   BIGINT NOT NULL,
        child_site_id   BIGINT NOT NULL,
        tag             TEXT,
        type_id         BIGINT,
        alternate       TEXT,
        item_qty        DOUBLE PRECISION NOT NULL DEFAULT 1,
        child_qty       DOUBLE PRECISION NOT NULL DEFAULT 1,
        attach_rate     DOUBLE PRECISION NOT NULL DEFAULT 1,
        start_date      DATE,
        end_date        DATE,
        "offset"        SMALLINT,
        child_uom_id    BIGINT,
        fixed_child_qty DOUBLE PRECISION,
        scrap           DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS idx_bom_item_site
        ON {schema}.bill_of_material (item_id, site_id);
    CREATE INDEX IF NOT EXISTS idx_bom_child
        ON {schema}.bill_of_material (child_item_id, child_site_id);

    -- Item chains (supersession / substitution chains)
    CREATE TABLE IF NOT EXISTS {schema}.item_chain (
        id            BIGINT PRIMARY KEY,
        description   TEXT,
        item_id       BIGINT NOT NULL,
        site_id       BIGINT NOT NULL,
        child_item_id BIGINT NOT NULL,
        child_site_id BIGINT NOT NULL,
        policy_id     BIGINT
    );
    CREATE INDEX IF NOT EXISTS idx_item_chain_item
        ON {schema}.item_chain (item_id, site_id);

    -- On-hand inventory types (usable / quarantine / etc.)
    CREATE TABLE IF NOT EXISTS {schema}.on_hand_type (
        id                  BIGINT PRIMARY KEY,
        xuid                TEXT NOT NULL,
        name                TEXT NOT NULL,
        description         TEXT,
        planning_type       TEXT,
        allocation_sequence SMALLINT
    );

    -- On-hand inventory balances
    CREATE TABLE IF NOT EXISTS {schema}.on_hand (
        id                    BIGINT PRIMARY KEY,
        item_id               BIGINT NOT NULL,
        site_id               BIGINT NOT NULL,
        type_id               BIGINT REFERENCES {schema}.on_hand_type(id),
        tag                   TEXT,
        qty                   DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        unit_cost             DOUBLE PRECISION,
        unit_cost_currency_id BIGINT,
        expiry_date           DATE,
        attributes            JSONB,
        feature_id            BIGINT
    );
    CREATE INDEX IF NOT EXISTS idx_on_hand_item_site
        ON {schema}.on_hand (item_id, site_id);

    -- Item-site cost parameters (order cost, holding rate, cost price)
    CREATE TABLE IF NOT EXISTS {schema}.item_site (
        item_id      BIGINT NOT NULL,
        site_id      BIGINT NOT NULL,
        order_cost   DOUBLE PRECISION,
        holding_rate DOUBLE PRECISION,
        cost_price   DOUBLE PRECISION,
        PRIMARY KEY (item_id, site_id)
    );
    CREATE INDEX IF NOT EXISTS idx_item_site_item
        ON {schema}.item_site (item_id);
    CREATE INDEX IF NOT EXISTS idx_item_site_site
        ON {schema}.item_site (site_id);
    """

    # Separate ALTER statements for adding columns to existing tables
    alter_stmts = [
        # Migration: rename config_sections -> parameters (if old table exists)
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{schema}'
                  AND table_name = 'config_sections'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{schema}'
                  AND table_name = 'parameters'
            ) THEN
                ALTER TABLE {schema}.config_sections RENAME TO parameters;
            END IF;
        END $$;
        """,
        # Migration: rename config_sections columns -> parameter_type, parameters_set
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'parameters'
                  AND column_name = 'section'
            ) THEN
                ALTER TABLE {schema}.parameters RENAME COLUMN section TO parameter_type;
            END IF;
        END $$;
        """,
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'parameters'
                  AND column_name = 'config'
            ) THEN
                ALTER TABLE {schema}.parameters RENAME COLUMN config TO parameters_set;
            END IF;
        END $$;
        """,
        # Migration: add 'name' column to parameters (versioning)
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'parameters'
                  AND column_name = 'name'
            ) THEN
                ALTER TABLE {schema}.parameters ADD COLUMN name TEXT NOT NULL DEFAULT 'Default';
            END IF;
        END $$;
        """,
        # Migration: add 'is_default' column to parameters
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'parameters'
                  AND column_name = 'is_default'
            ) THEN
                ALTER TABLE {schema}.parameters ADD COLUMN is_default BOOLEAN DEFAULT FALSE;
                UPDATE {schema}.parameters SET is_default = TRUE;
            END IF;
        END $$;
        """,
        # Migration: drop old UNIQUE(parameter_type), add UNIQUE(parameter_type, name)
        f"""
        DO $$
        DECLARE
            _cname TEXT;
        BEGIN
            -- Find and drop any UNIQUE constraint on parameter_type alone
            SELECT tc.constraint_name INTO _cname
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
             AND tc.table_schema = ccu.table_schema
            WHERE tc.table_schema = '{schema}'
              AND tc.table_name = 'parameters'
              AND tc.constraint_type = 'UNIQUE'
              AND ccu.column_name = 'parameter_type'
              AND NOT EXISTS (
                  SELECT 1 FROM information_schema.constraint_column_usage ccu2
                  WHERE ccu2.constraint_name = tc.constraint_name
                    AND ccu2.table_schema = tc.table_schema
                    AND ccu2.column_name = 'name'
              )
            LIMIT 1;

            IF _cname IS NOT NULL THEN
                EXECUTE format('ALTER TABLE {schema}.parameters DROP CONSTRAINT %I', _cname);
            END IF;

            -- Add composite unique if it doesn't exist yet
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
                WHERE n.nspname = '{schema}'
                  AND c.conrelid = '{schema}.parameters'::regclass
                  AND c.contype = 'u'
                  AND array_length(c.conkey, 1) = 2
            ) THEN
                ALTER TABLE {schema}.parameters
                    ADD CONSTRAINT parameters_parameter_type_name_key
                    UNIQUE (parameter_type, name);
            END IF;
        END $$;
        """,
        # Migration: add sort_order to parameters (priority ordering)
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'parameters'
                  AND column_name = 'sort_order'
            ) THEN
                ALTER TABLE {schema}.parameters ADD COLUMN sort_order INTEGER DEFAULT 0;
                UPDATE {schema}.parameters SET sort_order = 9999 WHERE is_default = TRUE;
                UPDATE {schema}.parameters SET sort_order = id WHERE is_default = FALSE;
            END IF;
        END $$;
        """,
        # demand_actuals — corrected_qty (legacy add)
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
        # item — xuid
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'item'
                  AND column_name = 'xuid'
            ) THEN
                ALTER TABLE {schema}.item ADD COLUMN xuid TEXT;
            END IF;
        END $$;
        """,
        # item — description
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'item'
                  AND column_name = 'description'
            ) THEN
                ALTER TABLE {schema}.item ADD COLUMN description TEXT;
            END IF;
        END $$;
        """,
        # item — attributes
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'item'
                  AND column_name = 'attributes'
            ) THEN
                ALTER TABLE {schema}.item ADD COLUMN attributes JSONB;
            END IF;
        END $$;
        """,
        # item — type_id
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'item'
                  AND column_name = 'type_id'
            ) THEN
                ALTER TABLE {schema}.item ADD COLUMN type_id BIGINT;
            END IF;
        END $$;
        """,
        # item — image_url
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'item'
                  AND column_name = 'image_url'
            ) THEN
                ALTER TABLE {schema}.item ADD COLUMN image_url TEXT;
            END IF;
        END $$;
        """,
        # site — xuid
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'site'
                  AND column_name = 'xuid'
            ) THEN
                ALTER TABLE {schema}.site ADD COLUMN xuid TEXT;
            END IF;
        END $$;
        """,
        # site — description
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'site'
                  AND column_name = 'description'
            ) THEN
                ALTER TABLE {schema}.site ADD COLUMN description TEXT;
            END IF;
        END $$;
        """,
        # site — attributes
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'site'
                  AND column_name = 'attributes'
            ) THEN
                ALTER TABLE {schema}.site ADD COLUMN attributes JSONB;
            END IF;
        END $$;
        """,
        # site — type_id
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'site'
                  AND column_name = 'type_id'
            ) THEN
                ALTER TABLE {schema}.site ADD COLUMN type_id BIGINT;
            END IF;
        END $$;
        """,
        # time_series_characteristics — abc_class
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'time_series_characteristics'
                  AND column_name = 'abc_class'
            ) THEN
                ALTER TABLE {schema}.time_series_characteristics
                    ADD COLUMN abc_class TEXT;
            END IF;
        END $$;
        """,
        # users — update auth_provider CHECK to include 'google'
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = 'users'
            ) THEN
                ALTER TABLE {schema}.users DROP CONSTRAINT IF EXISTS users_auth_provider_check;
                ALTER TABLE {schema}.users ADD CONSTRAINT users_auth_provider_check
                    CHECK (auth_provider IN ('local', 'microsoft', 'google'));
            END IF;
        END $$;
        """,
        # backtest_metrics — metric_source column
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'backtest_metrics'
                  AND column_name = 'metric_source'
            ) THEN
                ALTER TABLE {schema}.backtest_metrics
                    ADD COLUMN metric_source TEXT DEFAULT 'rolling_window';
            END IF;
        END $$;
        """,
        # site — longitude
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'site'
                  AND column_name = 'longitude'
            ) THEN
                ALTER TABLE {schema}.site ADD COLUMN longitude NUMERIC(10,7);
            END IF;
        END $$;
        """,
        # site — latitude
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'site'
                  AND column_name = 'latitude'
            ) THEN
                ALTER TABLE {schema}.site ADD COLUMN latitude NUMERIC(10,7);
            END IF;
        END $$;
        """,
        # users — allowed_segments
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'users'
                  AND column_name = 'allowed_segments'
            ) THEN
                ALTER TABLE {schema}.users ADD COLUMN allowed_segments JSONB DEFAULT '[]'::jsonb;
            END IF;
        END $$;
        """,
        # users — can_run_process
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'users'
                  AND column_name = 'can_run_process'
            ) THEN
                ALTER TABLE {schema}.users ADD COLUMN can_run_process BOOLEAN DEFAULT FALSE;
            END IF;
        END $$;
        """,
        # users — can_create_override
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'users'
                  AND column_name = 'can_create_override'
            ) THEN
                ALTER TABLE {schema}.users ADD COLUMN can_create_override BOOLEAN DEFAULT FALSE;
            END IF;
        END $$;
        """,
        # users — allowed_segments_edit
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'users'
                  AND column_name = 'allowed_segments_edit'
            ) THEN
                ALTER TABLE {schema}.users ADD COLUMN allowed_segments_edit JSONB DEFAULT '[]'::jsonb;
            END IF;
        END $$;
        """,
        # scenario_id — detected_outliers
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'detected_outliers'
                  AND column_name = 'scenario_id'
            ) THEN
                ALTER TABLE {schema}.detected_outliers
                    ADD COLUMN scenario_id BIGINT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # scenario_id — time_series_characteristics
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'time_series_characteristics'
                  AND column_name = 'scenario_id'
            ) THEN
                ALTER TABLE {schema}.time_series_characteristics
                    ADD COLUMN scenario_id BIGINT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # scenario_id — forecast_results
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'forecast_results'
                  AND column_name = 'scenario_id'
            ) THEN
                ALTER TABLE {schema}.forecast_results
                    ADD COLUMN scenario_id BIGINT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # scenario_id — backtest_metrics
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'backtest_metrics'
                  AND column_name = 'scenario_id'
            ) THEN
                ALTER TABLE {schema}.backtest_metrics
                    ADD COLUMN scenario_id BIGINT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # scenario_id — forecasts_by_origin
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'forecasts_by_origin'
                  AND column_name = 'scenario_id'
            ) THEN
                ALTER TABLE {schema}.forecasts_by_origin
                    ADD COLUMN scenario_id BIGINT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # scenario_id — best_method_per_series
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'best_method_per_series'
                  AND column_name = 'scenario_id'
            ) THEN
                ALTER TABLE {schema}.best_method_per_series
                    ADD COLUMN scenario_id BIGINT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # scenario_id — fitted_distributions
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = 'fitted_distributions'
                  AND column_name = 'scenario_id'
            ) THEN
                ALTER TABLE {schema}.fitted_distributions
                    ADD COLUMN scenario_id BIGINT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # forecast_scenarios table creation
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.forecast_scenarios (
            scenario_id      BIGSERIAL PRIMARY KEY,
            name             TEXT NOT NULL,
            description      TEXT,
            is_base          BOOLEAN NOT NULL DEFAULT FALSE,
            status           TEXT NOT NULL DEFAULT 'pending',
            run_at           TIMESTAMPTZ,
            error_msg        TEXT,
            created_by       TEXT,
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            param_overrides  JSONB NOT NULL DEFAULT '{{}}',
            demand_overrides JSONB NOT NULL DEFAULT '{{}}'
        );
        """,
        f"""
        INSERT INTO {schema}.forecast_scenarios (scenario_id, name, description, is_base, status)
        VALUES (1, 'Base', 'Default scenario — global configuration', TRUE, 'complete')
        ON CONFLICT (scenario_id) DO NOTHING;
        """,
        f"""
        SELECT setval(
            pg_get_serial_sequence('{schema}.forecast_scenarios', 'scenario_id'),
            GREATEST((SELECT MAX(scenario_id) FROM {schema}.forecast_scenarios), 1)
        );
        """,
        # Drop old unique constraint on time_series_characteristics.unique_id and add composite
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname = 'time_series_characteristics_unique_id_key'
                  AND n.nspname = '{schema}'
            ) THEN
                ALTER TABLE {schema}.time_series_characteristics
                    DROP CONSTRAINT time_series_characteristics_unique_id_key;
            END IF;
        END $$;
        """,
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_tsc_uid_scen
            ON {schema}.time_series_characteristics (unique_id, scenario_id);
        """,
        # Drop old unique constraint on best_method_per_series.unique_id and add composite
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname = 'best_method_per_series_unique_id_key'
                  AND n.nspname = '{schema}'
            ) THEN
                ALTER TABLE {schema}.best_method_per_series
                    DROP CONSTRAINT best_method_per_series_unique_id_key;
            END IF;
        END $$;
        """,
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bmp_uid_scen
            ON {schema}.best_method_per_series (unique_id, scenario_id);
        """,
    ]

    conn = get_conn()
    try:
        # Phase 1: CREATE TABLE statements + seed data — all or nothing
        with conn.cursor() as cur:
            cur.execute(ddl)
            # Seed the default "All" segment (matches every series)
            cur.execute(f"""
                INSERT INTO {schema}.segment (name, description, criteria, is_default)
                VALUES ('All', 'All item/site combinations', '{{}}'::jsonb, TRUE)
                ON CONFLICT (name) DO NOTHING
            """)
            # Seed default performance parameters
            cur.execute(f"""
                INSERT INTO {schema}.parameters
                    (parameter_type, name, label, parameters_set, is_default, sort_order)
                VALUES (
                    'performance', 'Default', 'Default',
                    '{{"n_jobs": 1, "incremental_processing": false}}'::jsonb,
                    TRUE, 9999
                )
                ON CONFLICT (parameter_type, name) DO NOTHING
            """)
            # Seed base forecast scenario
            cur.execute(f"""
                INSERT INTO {schema}.forecast_scenarios (scenario_id, name, description, is_base, status)
                VALUES (1, 'Base', 'Default scenario — global configuration', TRUE, 'complete')
                ON CONFLICT (scenario_id) DO NOTHING
            """)
            # Advance sequence past manually-inserted id=1 so next INSERT gets id=2+
            cur.execute(f"""
                SELECT setval(
                    pg_get_serial_sequence('{schema}.forecast_scenarios', 'scenario_id'),
                    GREATEST((SELECT MAX(scenario_id) FROM {schema}.forecast_scenarios), 1)
                )
            """)
        conn.commit()
        logger.info(f"Schema '{schema}' tables created/verified")

        # Phase 2: ALTER statements run independently so one failure doesn't
        # roll back all the others (e.g. adding abc_class to a table that may
        # not exist yet if characterisation has never been run).
        for stmt in alter_stmts:
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
                conn.commit()
            except Exception as alter_err:
                conn.rollback()
                logger.warning(f"ALTER skipped (will retry next startup): {alter_err}")

        logger.info(f"Schema '{schema}' fully initialised")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Bulk insert helper
# ═══════════════════════════════════════════════════════════════════════════


def bulk_insert(
    config_path=None,
    table_name: str = "",
    columns: Sequence[str] = (),
    rows: Sequence[Tuple] = (),
    *,
    truncate: bool = True,
    delete_where: Optional[str] = None,
    page_size: int = 5000,
) -> int:
    """
    Generic bulk insert using psycopg2 execute_values.

    Parameters
    ----------
    config_path : ignored
        Accepted for backward compatibility; credentials come from env vars.
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

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if delete_where:
                cur.execute(f"DELETE FROM {table_name} WHERE {delete_where}")
            elif truncate:
                cur.execute(f"TRUNCATE TABLE {table_name}")
            psycopg2.extras.execute_values(
                cur,
                insert_sql,
                rows,
                page_size=page_size,
            )
        conn.commit()
        logger.info(f"bulk_insert: {len(rows):,} rows -> {table_name}")
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_table(
    config_path=None,
    table_name: str = "",
    columns: str = "*",
    where: str = "",
) -> "pd.DataFrame":
    """
    Read a table (or filtered subset) into a pandas DataFrame.

    Parameters
    ----------
    config_path : ignored
        Accepted for backward compatibility; credentials come from env vars.
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

    conn = get_conn()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def get_demand_for_scenario(schema: str, scenario_id: int, conn) -> "pd.DataFrame":
    """
    Read demand_actuals applying a forecast scenario's demand_overrides.

    demand_overrides keys (all optional):
      demand_multiplier  float  — scale all y values (default 1.0)
      date_from          str    — ISO date, restrict training window start
      date_to            str    — ISO date, restrict training window end
      segment_id         int    — include only series belonging to this segment
      series_filter      list   — explicit list of unique_ids to include
    """
    import pandas as pd
    import psycopg2.extras as _pge

    cur = conn.cursor(cursor_factory=_pge.RealDictCursor)
    cur.execute(
        f"SELECT demand_overrides FROM {schema}.forecast_scenarios WHERE scenario_id = %s",
        (scenario_id,),
    )
    row = cur.fetchone()
    ov: dict = dict(row["demand_overrides"] or {}) if row else {}

    date_from:     str | None       = ov.get("date_from")
    date_to:       str | None       = ov.get("date_to")
    series_filter: list | None      = ov.get("series_filter")
    seg_id:        int | None       = ov.get("segment_id")
    multiplier:    float            = float(ov.get("demand_multiplier", 1.0))

    wheres: list[str] = []
    params: list      = []

    if date_from:
        wheres.append("date >= %s")
        params.append(date_from)
    if date_to:
        wheres.append("date <= %s")
        params.append(date_to)
    if series_filter:
        wheres.append("unique_id = ANY(%s)")
        params.append(series_filter)
    if seg_id:
        wheres.append(
            f"unique_id IN (SELECT unique_id FROM {schema}.segment_membership"
            f" WHERE segment_id = %s)"
        )
        params.append(seg_id)

    where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""
    cur.execute(
        f"SELECT unique_id, date, y FROM {schema}.demand_actuals "
        f"{where_clause} ORDER BY unique_id, date",
        params,
    )
    df = pd.DataFrame(cur.fetchall(), columns=["unique_id", "date", "y"])
    if multiplier != 1.0 and not df.empty:
        df["y"] = (df["y"] * multiplier).round(6)
    return df


def load_config_for_scenario(schema: str, scenario_id: int) -> dict:
    """
    Return the effective runtime config for a scenario.

    Starts from the global DB config (load_config_from_db), then deep-merges
    the scenario's param_overrides on top.  Scalar overrides replace values
    directly; dict overrides are merged key-by-key.

    param_overrides can contain: horizon, backtest_windows, methods,
    outlier_method, outlier_sensitivity, distribution_horizon, and any other
    top-level key from the global config.
    """
    cfg = load_config_from_db()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            f"SELECT param_overrides FROM {schema}.forecast_scenarios"
            f" WHERE scenario_id = %s",
            (scenario_id,),
        )
        row = cur.fetchone()
    ov: dict = dict(row["param_overrides"] or {}) if row else {}
    for k, v in ov.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k] = {**cfg[k], **v}
        else:
            cfg[k] = v
    return cfg
