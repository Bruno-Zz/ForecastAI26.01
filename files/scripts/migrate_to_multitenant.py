#!/usr/bin/env python3
"""
One-time migration script: register the existing single-tenant account
in the master database and create the first superAdmin user.

Run AFTER:
  1. CREATE DATABASE forecastai_master;
  2. psql -d forecastai_master -f files/DDL/master_schema.sql

Usage:
  cd files
  python scripts/migrate_to_multitenant.py

The script will prompt for:
  - SuperAdmin email
  - SuperAdmin display name
  - SuperAdmin password
  - The display name for the existing account (default: "Default")

It is safe to run multiple times — INSERT ON CONFLICT DO NOTHING protects
against duplicates.
"""

import getpass
import json
import sys
from pathlib import Path

# Ensure files/ is on sys.path
_files_dir = Path(__file__).resolve().parent.parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

try:
    import yaml
    import psycopg2
    from passlib.context import CryptContext
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install psycopg2-binary pyyaml passlib[bcrypt]")
    sys.exit(1)

_CONFIG_YAML = _files_dir / "config" / "config.yaml"
_MASTER_SCHEMA_SQL = _files_dir / "DDL" / "master_schema.sql"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def load_yaml_config() -> dict:
    if not _CONFIG_YAML.exists():
        print(f"config.yaml not found at {_CONFIG_YAML}")
        sys.exit(1)
    with open(_CONFIG_YAML) as fh:
        return yaml.safe_load(fh) or {}


def get_pg_config(section: str, cfg: dict) -> dict:
    db = cfg.get(section, {})
    return {
        "host":     db.get("host", "localhost"),
        "port":     int(db.get("port", 5432)),
        "database": db.get("name", section),
        "user":     db.get("user", "postgres"),
        "password": db.get("password", ""),
        "schema":   db.get("schema", "master"),
        "sslmode":  db.get("sslmode", "disable"),
    }


def connect(pg: dict):
    return psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["user"], password=pg["password"], sslmode=pg["sslmode"],
    )


def main():
    cfg = load_yaml_config()
    master_pg = get_pg_config("master_database", cfg)
    account_pg = get_pg_config("database", cfg)

    print("=" * 60)
    print("ForecastAI Multi-Tenancy Migration")
    print("=" * 60)
    print(f"\nMaster DB  : {master_pg['database']} @ {master_pg['host']}:{master_pg['port']}")
    print(f"Account DB : {account_pg['database']}  schema={account_pg['schema']} "
          f"@ {account_pg['host']}:{account_pg['port']}")
    print()

    # ── Step 1: initialise master schema ────────────────────────────────────
    print("[1/4] Initialising master schema...")
    if not _MASTER_SCHEMA_SQL.exists():
        print(f"  ⚠  master_schema.sql not found at {_MASTER_SCHEMA_SQL}")
        sys.exit(1)
    try:
        conn = connect(master_pg)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(_MASTER_SCHEMA_SQL.read_text())
        conn.close()
        print("  ✓ Master schema OK")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        sys.exit(1)

    # ── Step 2: register existing account ───────────────────────────────────
    print("\n[2/4] Registering existing account...")
    default_display = input("  Account display name [Default]: ").strip() or "Default"
    try:
        conn = connect(master_pg)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO master.accounts "
                "(display_name, db_name, schema_name) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (db_name) DO UPDATE SET display_name = EXCLUDED.display_name "
                "RETURNING id",
                (default_display, account_pg["database"], account_pg["schema"]),
            )
            account_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        print(f"  ✓ Account registered (id={account_id})")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        sys.exit(1)

    # ── Step 3: migrate JWT secret ───────────────────────────────────────────
    print("\n[3/4] Migrating JWT secret...")
    jwt_secret = None
    try:
        # Read existing secret from the account DB
        conn = connect(account_pg)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT parameters_set FROM {account_pg['schema']}.parameters "
                "WHERE parameter_type = 'auth' AND is_default = TRUE LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                ps = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
                jwt_secret = ps.get("jwt_secret")
        conn.close()
    except Exception as e:
        print(f"  ⚠  Could not read from account DB: {e}")

    if jwt_secret and jwt_secret not in ("CHANGE-ME", ""):
        try:
            conn = connect(master_pg)
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE master.parameters "
                    "SET parameters_set = jsonb_set(parameters_set, '{jwt_secret}', %s) "
                    "WHERE parameter_type = 'auth' AND is_default = TRUE",
                    (json.dumps(jwt_secret),),
                )
            conn.commit()
            conn.close()
            print(f"  ✓ JWT secret copied from account DB to master DB")
        except Exception as e:
            print(f"  ⚠  Could not write JWT secret to master: {e}")
    else:
        print("  ⚠  JWT secret not found or is placeholder — update master.parameters manually")

    # ── Step 4: create first superAdmin ──────────────────────────────────────
    print("\n[4/4] Creating first superAdmin...")
    sa_email = input("  SuperAdmin email: ").strip()
    if not sa_email:
        print("  ⚠  No email entered — skipping superAdmin creation")
    else:
        sa_name = input("  SuperAdmin display name: ").strip() or sa_email.split("@")[0]
        sa_password = getpass.getpass("  SuperAdmin password: ")
        sa_password2 = getpass.getpass("  Confirm password: ")
        if sa_password != sa_password2:
            print("  ✗ Passwords do not match — skipping")
        else:
            hashed = pwd_context.hash(sa_password)
            try:
                conn = connect(master_pg)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO master.superadmins "
                        "(email, display_name, hashed_password, auth_provider) "
                        "VALUES (%s, %s, %s, 'local') "
                        "ON CONFLICT (email) DO NOTHING",
                        (sa_email.lower(), sa_name, hashed),
                    )
                conn.commit()
                conn.close()
                print(f"  ✓ SuperAdmin created: {sa_email}")
            except Exception as e:
                print(f"  ✗ Failed: {e}")

    print("\n" + "=" * 60)
    print("Migration complete.")
    print("Restart the API server to load the new account cache.")
    print("=" * 60)


if __name__ == "__main__":
    main()
