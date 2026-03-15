"""
Master database helpers for ForecastAI multi-tenancy.

The 'forecastai_master' database stores:
  - master.accounts      — tenant registry
  - master.superadmins   — superAdmin users (not in any tenant DB)
  - master.parameters    — shared JWT secret

Connection config is read from config/config.yaml → master_database: section.
Falls back to the same credentials as the main database: section if
master_database: is absent (useful during migration).
"""

import json
import logging
import os
from pathlib import Path
from typing import List, Optional

import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

logger = logging.getLogger(__name__)

_CONFIG_YAML = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
_MASTER_SCHEMA = "master"

# ─── bcrypt ──────────────────────────────────────────────────────────────────
try:
    from passlib.context import CryptContext
    _pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
except ImportError:
    _pwd_context = None


def _hash_password(password: str) -> str:
    if _pwd_context is None:
        raise RuntimeError("passlib not installed")
    return _pwd_context.hash(password)


def _verify_password(plain: str, hashed: str) -> bool:
    if _pwd_context is None:
        return False
    try:
        return _pwd_context.verify(plain, hashed)
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Config helpers
# ═══════════════════════════════════════════════════════════════════════════════


def _get_master_pg_config() -> dict:
    """Read master_database: section from config.yaml.

    Falls back to database: section (single-tenant compat) if absent.
    """
    yaml_cfg: dict = {}
    fallback: dict = {}
    if _CONFIG_YAML.exists():
        try:
            import yaml
            with open(_CONFIG_YAML, "r") as fh:
                raw = yaml.safe_load(fh) or {}
            yaml_cfg = raw.get("master_database", {})
            fallback = raw.get("database", {})
        except Exception as exc:
            logger.debug("Could not read config.yaml master_database section: %s", exc)

    def _get(yaml_key: str, env_key: str, default: str) -> str:
        return (
            yaml_cfg.get(yaml_key)
            or os.environ.get(env_key)
            or fallback.get(yaml_key)
            or default
        )

    return {
        "host":     _get("host",     "MASTER_DB_HOST",     "localhost"),
        "port":     int(_get("port", "MASTER_DB_PORT",     "5432")),
        "database": _get("name",     "MASTER_DB_NAME",     "forecastai_master"),
        "user":     _get("user",     "MASTER_DB_USER",     "postgres"),
        "password": _get("password", "MASTER_DB_PASSWORD", ""),
        "schema":   _get("schema",   "MASTER_DB_SCHEMA",   _MASTER_SCHEMA),
        "sslmode":  _get("sslmode",  "MASTER_DB_SSLMODE",  "disable"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Connection
# ═══════════════════════════════════════════════════════════════════════════════


def get_master_conn() -> psycopg2.extensions.connection:
    """Return a new psycopg2 connection to the master database.

    The caller is responsible for calling conn.close() when done.
    """
    pg = _get_master_pg_config()
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


def get_master_pg_config() -> dict:
    """Public accessor for the master PG config (used by admin.py provisioning)."""
    return _get_master_pg_config()


# ═══════════════════════════════════════════════════════════════════════════════
# JWT secret
# ═══════════════════════════════════════════════════════════════════════════════


def get_master_jwt_secret() -> str:
    """Return the JWT secret stored in master.parameters (auth row)."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT parameters_set FROM master.parameters "
                    "WHERE parameter_type = 'auth' AND is_default = TRUE LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    ps = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
                    secret = ps.get("jwt_secret", "")
                    if secret and secret != "CHANGE-ME-IN-PRODUCTION":
                        return secret
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Could not read master JWT secret: %s", exc)
    return "CHANGE-ME-IN-PRODUCTION"


def get_master_token_expiry_minutes() -> int:
    """Return token expiry minutes from master.parameters."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT parameters_set FROM master.parameters "
                    "WHERE parameter_type = 'auth' AND is_default = TRUE LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    ps = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
                    return int(ps.get("token_expiry_minutes", 480))
        finally:
            conn.close()
    except Exception:
        pass
    return 480


# ═══════════════════════════════════════════════════════════════════════════════
# Account CRUD
# ═══════════════════════════════════════════════════════════════════════════════


def get_all_accounts() -> List[dict]:
    """Return all active accounts from master.accounts."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id::text, display_name, db_name, schema_name, "
                    "       connection_params, is_active, created_at, updated_at "
                    "FROM master.accounts WHERE is_active = TRUE ORDER BY display_name"
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("get_all_accounts failed: %s", exc)
        return []


def get_all_accounts_including_inactive() -> List[dict]:
    """Return all accounts (including inactive) — for admin screens."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id::text, display_name, db_name, schema_name, "
                    "       connection_params, is_active, created_at, updated_at "
                    "FROM master.accounts ORDER BY created_at"
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("get_all_accounts_including_inactive failed: %s", exc)
        return []


def get_account_by_id(account_id: str) -> Optional[dict]:
    """Return a single account row or None."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id::text, display_name, db_name, schema_name, "
                    "       connection_params, is_active, created_at, updated_at "
                    "FROM master.accounts WHERE id = %s",
                    (account_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
        finally:
            conn.close()
    except Exception as exc:
        logger.error("get_account_by_id(%s) failed: %s", account_id, exc)
        return None


def create_account_record(
    display_name: str,
    db_name: str,
    schema_name: str = "zcube",
    connection_params: Optional[dict] = None,
) -> dict:
    """Insert a new account record and return it."""
    conn = get_master_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO master.accounts "
                "(display_name, db_name, schema_name, connection_params) "
                "VALUES (%s, %s, %s, %s) "
                "RETURNING id::text, display_name, db_name, schema_name, "
                "          connection_params, is_active, created_at",
                (
                    display_name,
                    db_name,
                    schema_name,
                    json.dumps(connection_params) if connection_params else None,
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return dict(row)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def deactivate_account(account_id: str) -> bool:
    """Soft-delete: set is_active=FALSE. Returns True if a row was updated."""
    conn = get_master_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE master.accounts SET is_active = FALSE, updated_at = NOW() "
                "WHERE id = %s AND is_active = TRUE",
                (account_id,),
            )
            updated = cur.rowcount
        conn.commit()
        return updated > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# SuperAdmin CRUD
# ═══════════════════════════════════════════════════════════════════════════════


def get_superadmin_by_email(email: str) -> Optional[dict]:
    """Return superadmin row by email, or None."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id::text, email, display_name, hashed_password, "
                    "       auth_provider, is_active "
                    "FROM master.superadmins WHERE email = %s AND is_active = TRUE",
                    (email.lower().strip(),),
                )
                row = cur.fetchone()
                return dict(row) if row else None
        finally:
            conn.close()
    except Exception as exc:
        logger.error("get_superadmin_by_email(%s) failed: %s", email, exc)
        return None


def verify_superadmin(email: str, password: str) -> Optional[dict]:
    """Return superadmin dict if credentials are valid, else None."""
    sa = get_superadmin_by_email(email)
    if not sa:
        return None
    if sa.get("auth_provider") != "local":
        # OAuth superadmins authenticate differently (token passed by OAuth provider)
        return None
    if not sa.get("hashed_password"):
        return None
    if _verify_password(password, sa["hashed_password"]):
        return sa
    return None


def create_superadmin(
    email: str,
    display_name: str,
    password: str,
    auth_provider: str = "local",
) -> dict:
    """Create a new superAdmin. Returns the inserted row."""
    hashed = _hash_password(password) if auth_provider == "local" and password else None
    conn = get_master_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO master.superadmins "
                "(email, display_name, hashed_password, auth_provider) "
                "VALUES (%s, %s, %s, %s) "
                "RETURNING id::text, email, display_name, auth_provider, is_active, created_at",
                (email.lower().strip(), display_name, hashed, auth_provider),
            )
            row = cur.fetchone()
        conn.commit()
        return dict(row)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_superadmins() -> List[dict]:
    """Return all active superadmins (no passwords)."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id::text, email, display_name, auth_provider, "
                    "       is_active, created_at "
                    "FROM master.superadmins WHERE is_active = TRUE ORDER BY email"
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("list_superadmins failed: %s", exc)
        return []


def count_superadmins() -> int:
    """Return total number of active superadmins."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM master.superadmins WHERE is_active = TRUE"
                )
                return cur.fetchone()[0]
        finally:
            conn.close()
    except Exception:
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# Master DB initialisation (idempotent)
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# User-Account Assignments
# ═══════════════════════════════════════════════════════════════════════════════


def get_accounts_for_user(email: str) -> List[dict]:
    """Return active accounts the user email is assigned to (via master.user_accounts)."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT a.id::text, a.display_name, a.db_name, a.schema_name, "
                    "       a.connection_params, a.is_active "
                    "FROM master.user_accounts ua "
                    "JOIN master.accounts a ON a.id = ua.account_id "
                    "WHERE ua.email = %s AND a.is_active = TRUE "
                    "ORDER BY a.display_name",
                    (email.lower().strip(),),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("get_accounts_for_user(%s) failed: %s", email, exc)
        return []


def assign_user_to_account(email: str, account_id: str) -> None:
    """Add a user-account assignment (idempotent — ON CONFLICT DO NOTHING)."""
    conn = get_master_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO master.user_accounts (email, account_id) "
                "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (email.lower().strip(), account_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def remove_user_from_account(email: str, account_id: str) -> bool:
    """Remove a user-account assignment. Returns True if a row was deleted."""
    conn = get_master_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM master.user_accounts WHERE email = %s AND account_id = %s",
                (email.lower().strip(), account_id),
            )
            deleted = cur.rowcount
        conn.commit()
        return deleted > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_user_account_assignments() -> dict:
    """Return {email: [{id, display_name}, ...]} mapping from master.user_accounts."""
    try:
        conn = get_master_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT ua.email, a.id::text AS id, a.display_name "
                    "FROM master.user_accounts ua "
                    "JOIN master.accounts a ON a.id = ua.account_id "
                    "ORDER BY ua.email, a.display_name"
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        result: dict = {}
        for r in rows:
            e = r["email"]
            if e not in result:
                result[e] = []
            result[e].append({"id": r["id"], "display_name": r["display_name"]})
        return result
    except Exception as exc:
        logger.error("get_user_account_assignments failed: %s", exc)
        return {}


def init_master_schema(sql_path: Optional[Path] = None) -> None:
    """Run master_schema.sql against the master database (idempotent).

    Called at startup if the master DB is reachable.
    """
    if sql_path is None:
        sql_path = Path(__file__).resolve().parent.parent / "DDL" / "master_schema.sql"
    if not sql_path.exists():
        logger.warning("master_schema.sql not found at %s — skipping init", sql_path)
        return

    conn = get_master_conn()
    try:
        with open(sql_path, "r") as fh:
            ddl = fh.read()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        logger.info("Master schema initialised successfully.")
    except Exception as exc:
        conn.rollback()
        logger.error("Master schema init failed: %s", exc)
        raise
    finally:
        conn.close()
