-- ============================================================================
-- ForecastAI Master Database Schema
-- Run once against the 'forecastai_master' database.
--
-- Setup:
--   CREATE DATABASE forecastai_master;
--   \c forecastai_master
--   \i master_schema.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS master;

-- ─── Accounts ────────────────────────────────────────────────────────────────
-- Each row = one tenant database on the same PostgreSQL server.
-- db_name  : the PostgreSQL database name  (e.g. "forecastai_acme")
-- schema_name : schema inside that database (default "zcube")
-- connection_params : optional JSONB overrides for host/port/user/password
--   when NULL → use same server credentials as this master DB
--
CREATE TABLE IF NOT EXISTS master.accounts (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name      TEXT        NOT NULL UNIQUE,
    db_name           TEXT        NOT NULL UNIQUE,
    schema_name       TEXT        NOT NULL DEFAULT 'zcube',
    connection_params JSONB       DEFAULT NULL,
    is_active         BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounts_db_name  ON master.accounts (db_name);
CREATE INDEX IF NOT EXISTS idx_accounts_active   ON master.accounts (is_active);

-- ─── Superadmins ─────────────────────────────────────────────────────────────
-- SuperAdmin users are stored ONLY here — never in any tenant DB.
-- auth_provider 'local' uses bcrypt hashed_password.
-- 'microsoft' / 'google' OAuth are validated against the provider; hashed_password is NULL.
--
CREATE TABLE IF NOT EXISTS master.superadmins (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT        NOT NULL UNIQUE,
    display_name    TEXT        NOT NULL,
    hashed_password TEXT,
    auth_provider   TEXT        NOT NULL DEFAULT 'local'
                                CHECK (auth_provider IN ('local', 'microsoft', 'google')),
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_superadmins_email ON master.superadmins (email);

-- ─── Parameters ──────────────────────────────────────────────────────────────
-- Stores the shared JWT secret (and any future global settings).
-- Mirrors the structure of {schema}.parameters in tenant DBs.
--
CREATE TABLE IF NOT EXISTS master.parameters (
    id              SERIAL      PRIMARY KEY,
    parameter_type  TEXT        NOT NULL,
    name            TEXT        DEFAULT 'Default',
    parameters_set  JSONB,
    is_default      BOOLEAN     DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (parameter_type, name)
);

-- Seed: default auth parameter set with JWT secret placeholder.
-- Change the jwt_secret value before going to production.
INSERT INTO master.parameters (parameter_type, name, parameters_set, is_default)
VALUES (
    'auth',
    'Default',
    '{"jwt_secret": "CHANGE-ME-IN-PRODUCTION", "token_expiry_minutes": 480}',
    TRUE
)
ON CONFLICT (parameter_type, name) DO NOTHING;

-- ─── User-Account Assignments ──────────────────────────────────────────────
-- Tracks which accounts each user (by email) has access to.
-- One user can belong to multiple accounts — both login and admin UIs use this.
-- Populated automatically when a user is created via POST /auth/users,
-- and managed via the admin panel.
--
CREATE TABLE IF NOT EXISTS master.user_accounts (
    email       TEXT        NOT NULL,
    account_id  UUID        NOT NULL REFERENCES master.accounts(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (email, account_id)
);

CREATE INDEX IF NOT EXISTS idx_user_accounts_email   ON master.user_accounts (email);
CREATE INDEX IF NOT EXISTS idx_user_accounts_account ON master.user_accounts (account_id);
