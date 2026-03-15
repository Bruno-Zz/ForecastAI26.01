#!/usr/bin/env python3
"""
Utility: add (or update) a superAdmin in master.superadmins.

Usage:
  cd files
  python scripts/add_superadmin.py
  python scripts/add_superadmin.py --email user@example.com --name "Full Name"

Prompts for password interactively. Safe to re-run — uses ON CONFLICT DO UPDATE.
"""

import argparse
import getpass
import sys
from pathlib import Path

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
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def load_master_pg() -> dict:
    if not _CONFIG_YAML.exists():
        print(f"config.yaml not found at {_CONFIG_YAML}")
        sys.exit(1)
    with open(_CONFIG_YAML) as fh:
        cfg = yaml.safe_load(fh) or {}
    section = cfg.get("master_database") or cfg.get("database", {})
    return {
        "host":     section.get("host", "localhost"),
        "port":     int(section.get("port", 5432)),
        "database": section.get("name", "forecastai_master"),
        "user":     section.get("user", "postgres"),
        "password": section.get("password", ""),
        "sslmode":  section.get("sslmode", "disable"),
    }


def main():
    parser = argparse.ArgumentParser(description="Add or update a ForecastAI superAdmin")
    parser.add_argument("--email", default=None, help="SuperAdmin email address")
    parser.add_argument("--name",  default=None, help="Display name")
    args = parser.parse_args()

    pg = load_master_pg()

    # ── Prompt for missing fields ────────────────────────────────────────────
    email = args.email or input("  Email: ").strip()
    if not email:
        print("  Email cannot be empty.")
        sys.exit(1)

    display_name = args.name or input(f"  Display name [{email.split('@')[0]}]: ").strip()
    if not display_name:
        display_name = email.split("@")[0]

    print(f"\nAdding superAdmin: {email}")
    password = getpass.getpass("  Password: ")
    if not password:
        print("  Password cannot be empty.")
        sys.exit(1)
    confirm = getpass.getpass("  Confirm password: ")
    if password != confirm:
        print("  Passwords do not match.")
        sys.exit(1)

    hashed = pwd_context.hash(password)

    try:
        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["user"], password=pg["password"], sslmode=pg["sslmode"],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO master.superadmins
                    (email, display_name, hashed_password, auth_provider, is_active)
                VALUES (%s, %s, %s, 'local', TRUE)
                ON CONFLICT (email) DO UPDATE
                    SET display_name    = EXCLUDED.display_name,
                        hashed_password = EXCLUDED.hashed_password,
                        auth_provider   = 'local',
                        is_active       = TRUE,
                        updated_at      = NOW()
                RETURNING id
                """,
                (email.lower(), display_name, hashed),
            )
            sa_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        print(f"\n  SuperAdmin ready  (id={sa_id})")
        print(f"    email        : {email}")
        print(f"    display_name : {display_name}")
        print(f"    auth_provider: local")
        print("\n  You can now log in at the ForecastAI login screen.")
        print("  On email blur the account list will appear automatically.")
    except psycopg2.OperationalError as e:
        print(f"\n  Could not connect to master DB ({pg['database']} @ "
              f"{pg['host']}:{pg['port']}): {e}")
        print("  Make sure forecastai_master exists and the migration has been run.")
        sys.exit(1)
    except Exception as e:
        print(f"\n  Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
