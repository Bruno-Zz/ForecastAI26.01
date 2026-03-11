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
# Config & connection
# ═══════════════════════════════════════════════════════════════════════════


def _get_pg_config() -> dict:
    """
    Read PostgreSQL connection settings.

    Priority:
      1. config/config.yaml  →  database:  section
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
        return yaml_cfg.get(yaml_key) or os.environ.get(env_key, default)

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

    Credentials come from config/config.yaml (database section) or DB_* env vars.
    The legacy *config_path* parameter is accepted but ignored.
    The caller is responsible for calling conn.close() when done.
    """
    pg = _get_pg_config()
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["user"],
        password=pg["password"],
        sslmode=pg["sslmode"],
        options=f"-c search_path={pg['schema']},public",
    )
    conn.autocommit = False
    return conn


def get_schema(config_path=None) -> str:
    """Return the target schema name.

    Reads from config/config.yaml (database.schema) or DB_SCHEMA env var.
    The legacy *config_path* parameter is accepted but ignored.
    """
    return _get_pg_config()["schema"]


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

    -- ─── Mooncake columnar extension ──────────────────────────────────
    CREATE EXTENSION IF NOT EXISTS pg_mooncake;

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
        type_id     BIGINT
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
    ) USING columnstore;
    CREATE INDEX IF NOT EXISTS idx_demand_unique_id
        ON {schema}.demand_actuals (unique_id);

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
        unique_id         TEXT NOT NULL,
        method            TEXT NOT NULL,
        point_forecast    JSONB,
        quantiles         JSONB,
        hyperparameters   JSONB,
        training_time     DOUBLE PRECISION
    ) USING columnstore;
    CREATE INDEX IF NOT EXISTS idx_forecasts_uid_method
        ON {schema}.forecast_results (unique_id, method);

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
        metric_source     TEXT DEFAULT 'rolling_window'
    ) USING columnstore;
    CREATE INDEX IF NOT EXISTS idx_metrics_uid
        ON {schema}.backtest_metrics (unique_id);

    -- ─── Forecasts by origin (per-step backtest detail) ──────────────

    CREATE TABLE IF NOT EXISTS {schema}.forecasts_by_origin (
        unique_id         TEXT NOT NULL,
        method            TEXT NOT NULL,
        forecast_origin   DATE,
        horizon_step      INTEGER,
        point_forecast    DOUBLE PRECISION,
        actual_value      DOUBLE PRECISION
    ) USING columnstore;
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
    ) USING columnstore;
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

    -- Resolution: each series → assigned parameter version per business type
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
    """

    # Separate ALTER statements for adding columns to existing tables
    alter_stmts = [
        # Migration: rename config_sections → parameters (if old table exists)
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
        # Migration: rename config_sections columns → parameter_type, parameters_set
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
