-- ForecastAI Database Schema DDL
-- This file contains the complete database schema definition
-- Run this script to create the database schema from scratch
-- Replace {schema} with your desired schema name (e.g., zcube)

-- Schema
CREATE SCHEMA IF NOT EXISTS {schema};

-- Mooncake columnar extension
CREATE EXTENSION IF NOT EXISTS pg_mooncake;

-- ─── Lookup / type tables ───────────────────────────────────────────────

-- Item types
CREATE TABLE IF NOT EXISTS {schema}.item_type (
    id          BIGINT PRIMARY KEY,
    xuid        TEXT,
    name        TEXT,
    description TEXT
);

-- Site types
CREATE TABLE IF NOT EXISTS {schema}.site_type (
    id          BIGINT PRIMARY KEY,
    xuid        TEXT,
    name        TEXT,
    description TEXT
);

-- ─── Master tables ───────────────────────────────────────────────────

-- Items
CREATE TABLE IF NOT EXISTS {schema}.item (
    id          BIGINT PRIMARY KEY,
    xuid        TEXT,
    name        TEXT,
    description TEXT,
    attributes  JSONB,
    type_id     BIGINT
);

-- Sites
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

-- ─── Demand actuals ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {schema}.demand_actuals (
    item_id         INTEGER,
    site_id         INTEGER,
    channel         TEXT,
    date            DATE NOT NULL,
    qty             DOUBLE PRECISION,
    corrected_qty   DOUBLE PRECISION,
    item_name       TEXT,
    site_name       TEXT,
    unique_id       TEXT
) USING columnstore;
CREATE INDEX IF NOT EXISTS idx_demand_unique_id ON {schema}.demand_actuals (unique_id);

-- Outlier corrections
CREATE TABLE IF NOT EXISTS {schema}.demand_corrections (
    unique_id       TEXT NOT NULL,
    date            DATE NOT NULL,
    corrected_qty   DOUBLE PRECISION,
    PRIMARY KEY (unique_id, date)
);

-- ─── Detected outliers ───────────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_outliers_unique_id ON {schema}.detected_outliers (unique_id);

-- ─── Time-series characteristics ───────────────────────────────────────

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
    abc_class                    TEXT,
    scenario_id                  BIGINT NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_chars_unique_id ON {schema}.time_series_characteristics (unique_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tsc_uid_scen
    ON {schema}.time_series_characteristics (unique_id, scenario_id);

-- ─── Forecast results ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {schema}.forecast_results (
    unique_id         TEXT NOT NULL,
    method            TEXT NOT NULL,
    point_forecast    JSONB,
    quantiles         JSONB,
    hyperparameters   JSONB,
    training_time     DOUBLE PRECISION,
    scenario_id       BIGINT NOT NULL DEFAULT 1
) USING columnstore;
CREATE INDEX IF NOT EXISTS idx_forecasts_uid_method ON {schema}.forecast_results (unique_id, method);

-- ─── Backtest metrics ─────────────────────────────────────────────────

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
) USING columnstore;
CREATE INDEX IF NOT EXISTS idx_metrics_uid ON {schema}.backtest_metrics (unique_id);

-- ─── Forecasts by origin ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {schema}.forecasts_by_origin (
    unique_id         TEXT NOT NULL,
    method            TEXT NOT NULL,
    forecast_origin   DATE,
    horizon_step      INTEGER,
    point_forecast    DOUBLE PRECISION,
    actual_value      DOUBLE PRECISION,
    scenario_id       BIGINT NOT NULL DEFAULT 1
) USING columnstore;
CREATE INDEX IF NOT EXISTS idx_fbo_uid ON {schema}.forecasts_by_origin (unique_id, method);

-- ─── Best method per series ─────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_bestmethod_uid ON {schema}.best_method_per_series (unique_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bmp_uid_scen
    ON {schema}.best_method_per_series (unique_id, scenario_id);

-- ─── Fitted distributions (MEIO) ─────────────────────────────────────

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
) USING columnstore;
CREATE INDEX IF NOT EXISTS idx_dist_uid ON {schema}.fitted_distributions (unique_id, method);

-- ─── Forecast adjustments ────────────────────────────────────────────

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

-- ─── Hyperparameter overrides ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {schema}.hyperparameter_overrides (
    id          SERIAL PRIMARY KEY,
    unique_id   TEXT NOT NULL,
    method      TEXT NOT NULL,
    overrides   JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (unique_id, method)
);
CREATE INDEX IF NOT EXISTS idx_hypoverrides_uid ON {schema}.hyperparameter_overrides (unique_id);

-- ─── ABC Classification ─────────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_abc_results_config ON {schema}.abc_results (config_id);
CREATE INDEX IF NOT EXISTS idx_abc_results_uid ON {schema}.abc_results (unique_id);

-- ─── Segments ───────────────────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_seg_membership_seg ON {schema}.segment_membership (segment_id);
CREATE INDEX IF NOT EXISTS idx_seg_membership_uid ON {schema}.segment_membership (unique_id);

-- ─── Process log ───────────────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_process_log_run ON {schema}.process_log (run_id);

-- ─── Users (Authentication) ─────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_users_email ON {schema}.users (email);

-- ─── Revoked tokens (Authentication) ────────────────────────────────

CREATE TABLE IF NOT EXISTS {schema}.revoked_tokens (
    jti         TEXT PRIMARY KEY,
    revoked_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Parameters ─────────────────────────────────────────────────────

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

-- ─── Parameter segments ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {schema}.parameter_segment (
    id              SERIAL PRIMARY KEY,
    parameter_id    INTEGER NOT NULL REFERENCES {schema}.parameters(id) ON DELETE CASCADE,
    segment_id      INTEGER NOT NULL REFERENCES {schema}.segment(id) ON DELETE CASCADE,
    UNIQUE(parameter_id, segment_id)
);
CREATE INDEX IF NOT EXISTS idx_param_seg_param ON {schema}.parameter_segment (parameter_id);
CREATE INDEX IF NOT EXISTS idx_param_seg_seg ON {schema}.parameter_segment (segment_id);

-- ─── Series parameter assignment ──────────────────────────────────

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

-- ─── Audit log ─────────────────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_audit_entity ON {schema}.audit_log (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON {schema}.audit_log (created_at DESC);

-- ─── Forecast scenarios ──────────────────────────────────────────────────────

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
    param_overrides  JSONB NOT NULL DEFAULT '{}',
    demand_overrides JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_fscen_status
    ON {schema}.forecast_scenarios (status);

-- Seed base forecast scenario (id=1)
INSERT INTO {schema}.forecast_scenarios (scenario_id, name, description, is_base, status)
VALUES (1, 'Base', 'Default scenario — global configuration', TRUE, 'complete')
ON CONFLICT (scenario_id) DO NOTHING;

-- Seed default "All" segment
INSERT INTO {schema}.segment (name, description, criteria, is_default)
VALUES ('All', 'All item/site combinations', '{}'::jsonb, TRUE)
ON CONFLICT (name) DO NOTHING;
