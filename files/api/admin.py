"""
SuperAdmin account management API.
All endpoints require role='superadmin'.

Routes (prefix /api/admin):
  GET    /accounts                        — list all accounts
  POST   /accounts                        — provision new account (or clone)
  DELETE /accounts/{id}                   — soft-delete (deactivate) account
  GET    /accounts/{id}/provision-status  — poll background provisioning job
  POST   /accounts/{id}/clone             — clone account into new DB

  GET    /superadmins                     — list superAdmins
  POST   /superadmins                     — create superAdmin

  POST   /cache/refresh                   — reload account cache from master DB
"""

import logging
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])

# ─── Reference to main.py's _account_cache (set at startup) ─────────────────
_account_cache_ref: dict = {}


def set_account_cache_ref(cache: dict) -> None:
    """Called from main.py at startup to share the mutable cache dict."""
    global _account_cache_ref
    _account_cache_ref.clear()
    _account_cache_ref.update(cache)
    # Keep a reference to the same dict object so mutations propagate
    # (main.py calls _account_cache.clear() + _account_cache.update() on refresh)


# In-memory provision job status: {job_id: {"status": str, "message": str, ...}}
_provision_jobs: dict = {}


# ════════════════════════════════════════════════════════════════════════════
# Pydantic models
# ════════════════════════════════════════════════════════════════════════════


class AccountCreateRequest(BaseModel):
    display_name: str
    db_name: str
    schema_name: str = "zcube"
    clone_from_account_id: Optional[str] = None  # if set → clone instead of empty


class AccountCloneRequest(BaseModel):
    display_name: str
    db_name: str
    schema_name: str = "zcube"


class SuperAdminCreateRequest(BaseModel):
    email: str
    display_name: str
    password: str


# ════════════════════════════════════════════════════════════════════════════
# Auth guard
# ════════════════════════════════════════════════════════════════════════════


def _require_superadmin(request: Request) -> dict:
    user = getattr(request.state, "user", None)
    if not user or user.get("role") != "superadmin":
        raise HTTPException(status_code=403, detail="SuperAdmin access required")
    return user


# ════════════════════════════════════════════════════════════════════════════
# Account cache refresh helper
# ════════════════════════════════════════════════════════════════════════════


def _refresh_cache_from_master() -> None:
    """Reload account configs from master DB into the shared cache."""
    try:
        from db.master_db import get_all_accounts, get_master_pg_config
        master_pg = get_master_pg_config()
        new_entries: dict = {}
        for acc in get_all_accounts():
            cp = acc.get("connection_params") or {}
            new_entries[acc["id"]] = {
                "host":     cp.get("host", master_pg["host"]),
                "port":     cp.get("port", master_pg["port"]),
                "database": acc["db_name"],
                "user":     cp.get("user", master_pg["user"]),
                "password": cp.get("password", master_pg["password"]),
                "schema":   acc["schema_name"],
                "sslmode":  cp.get("sslmode", "disable"),
            }
        # Mutate the shared dict in-place so middleware sees the update
        _account_cache_ref.clear()
        _account_cache_ref.update(new_entries)
        logger.info("Account cache refreshed: %d accounts", len(new_entries))
    except Exception as exc:
        logger.error("Failed to refresh account cache: %s", exc)


# ════════════════════════════════════════════════════════════════════════════
# Provisioning helpers
# ════════════════════════════════════════════════════════════════════════════


def _get_postgres_admin_conn(master_pg: dict):
    """Connect to the 'postgres' template database to run CREATE DATABASE."""
    return psycopg2.connect(
        host=master_pg["host"],
        port=master_pg["port"],
        dbname="postgres",
        user=master_pg["user"],
        password=master_pg["password"],
        sslmode=master_pg.get("sslmode", "disable"),
    )


def _create_database(db_name: str, master_pg: dict) -> None:
    """CREATE DATABASE using autocommit (required for DDL outside transaction)."""
    conn = _get_postgres_admin_conn(master_pg)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # Sanitise db_name (only allow alphanumeric + underscore)
            safe = "".join(c for c in db_name if c.isalnum() or c == "_")
            if not safe or safe != db_name:
                raise ValueError(f"Invalid db_name: {db_name!r}")
            cur.execute(f'CREATE DATABASE "{safe}"')
        logger.info("Database created: %s", db_name)
    finally:
        conn.close()


def _drop_database(db_name: str, master_pg: dict) -> None:
    """DROP DATABASE if exists (used on provisioning failure rollback)."""
    conn = _get_postgres_admin_conn(master_pg)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            safe = "".join(c for c in db_name if c.isalnum() or c == "_")
            cur.execute(f'DROP DATABASE IF EXISTS "{safe}"')
        logger.info("Database dropped (rollback): %s", db_name)
    except Exception as exc:
        logger.warning("Could not drop database %s: %s", db_name, exc)
    finally:
        conn.close()


def _run_schema_on_new_db(db_name: str, schema_name: str, master_pg: dict) -> None:
    """Connect to the newly created DB and run init_schema() to create all tables."""
    from db.db import set_account_context, reset_account_context

    account_cfg = {
        "host":     master_pg["host"],
        "port":     master_pg["port"],
        "database": db_name,
        "user":     master_pg["user"],
        "password": master_pg["password"],
        "schema":   schema_name,
        "sslmode":  master_pg.get("sslmode", "disable"),
    }
    token = set_account_context(account_cfg)
    try:
        from db.db import init_schema
        init_schema()
        logger.info("Schema initialised in %s.%s", db_name, schema_name)
    finally:
        reset_account_context(token)


def _seed_new_db(db_name: str, schema_name: str, master_pg: dict) -> None:
    """Seed default parameters and admin user in the new tenant DB."""
    from db.db import set_account_context, reset_account_context

    account_cfg = {
        "host":     master_pg["host"],
        "port":     master_pg["port"],
        "database": db_name,
        "user":     master_pg["user"],
        "password": master_pg["password"],
        "schema":   schema_name,
        "sslmode":  master_pg.get("sslmode", "disable"),
    }
    token = set_account_context(account_cfg)
    try:
        try:
            from api.auth import seed_default_admin
            seed_default_admin()
        except Exception as exc:
            logger.warning("seed_default_admin skipped: %s", exc)
    finally:
        reset_account_context(token)


def _provision_empty(
    job_id: str,
    display_name: str,
    db_name: str,
    schema_name: str,
) -> None:
    """Background thread: provision a fresh empty account DB."""
    from db.master_db import create_account_record, get_master_pg_config

    master_pg = get_master_pg_config()
    try:
        _provision_jobs[job_id]["status"] = "creating_database"
        _create_database(db_name, master_pg)

        _provision_jobs[job_id]["status"] = "initialising_schema"
        _run_schema_on_new_db(db_name, schema_name, master_pg)

        _provision_jobs[job_id]["status"] = "seeding"
        _seed_new_db(db_name, schema_name, master_pg)

        _provision_jobs[job_id]["status"] = "registering"
        acc = create_account_record(display_name, db_name, schema_name)

        _provision_jobs[job_id]["status"] = "refreshing_cache"
        _refresh_cache_from_master()

        _provision_jobs[job_id].update({
            "status": "complete",
            "account_id": acc["id"],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("Account provisioned: %s (%s)", display_name, db_name)
    except Exception as exc:
        _provision_jobs[job_id].update({
            "status": "failed",
            "error": str(exc),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.error("Account provisioning failed for %s: %s", db_name, exc)
        # Best-effort rollback: drop the DB if it was created
        try:
            _drop_database(db_name, master_pg)
        except Exception:
            pass


def _provision_clone(
    job_id: str,
    display_name: str,
    db_name: str,
    schema_name: str,
    source_account_id: str,
) -> None:
    """Background thread: clone an existing account DB into a new one."""
    from db.master_db import (
        create_account_record,
        get_account_by_id,
        get_master_pg_config,
    )

    master_pg = get_master_pg_config()

    source = get_account_by_id(source_account_id)
    if not source:
        _provision_jobs[job_id].update({
            "status": "failed",
            "error": f"Source account {source_account_id} not found",
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        return

    source_db = source["db_name"]
    pg_user = master_pg["user"]
    pg_password = master_pg["password"]
    pg_host = master_pg["host"]
    pg_port = str(master_pg["port"])

    try:
        _provision_jobs[job_id]["status"] = "creating_database"
        _create_database(db_name, master_pg)

        _provision_jobs[job_id]["status"] = "cloning_data"
        # pg_dump → pg_restore pipeline
        env = {
            "PGPASSWORD": pg_password,
            "PATH": "/usr/bin:/usr/local/bin:/bin",  # include system paths
        }
        import os
        env.update(os.environ)  # inherit PATH so pg_dump is found on Windows too
        env["PGPASSWORD"] = pg_password

        dump_cmd = [
            "pg_dump",
            f"--host={pg_host}",
            f"--port={pg_port}",
            f"--username={pg_user}",
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            source_db,
        ]
        restore_cmd = [
            "pg_restore",
            f"--host={pg_host}",
            f"--port={pg_port}",
            f"--username={pg_user}",
            f"--dbname={db_name}",
            "--no-owner",
            "--no-acl",
        ]

        dump_proc = subprocess.Popen(
            dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        )
        restore_proc = subprocess.Popen(
            restore_cmd,
            stdin=dump_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        dump_proc.stdout.close()
        _, restore_err = restore_proc.communicate()
        dump_proc.wait()

        if restore_proc.returncode not in (0, 1):
            # pg_restore returns 1 for non-fatal warnings; 0 is clean
            raise RuntimeError(
                f"pg_restore failed (exit {restore_proc.returncode}): "
                f"{restore_err.decode()[:500]}"
            )
        logger.info("Cloned %s → %s", source_db, db_name)

        _provision_jobs[job_id]["status"] = "registering"
        acc = create_account_record(display_name, db_name, schema_name)

        _provision_jobs[job_id]["status"] = "refreshing_cache"
        _refresh_cache_from_master()

        _provision_jobs[job_id].update({
            "status": "complete",
            "account_id": acc["id"],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        _provision_jobs[job_id].update({
            "status": "failed",
            "error": str(exc),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.error("Clone failed for %s: %s", db_name, exc)
        try:
            _drop_database(db_name, master_pg)
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════════
# Account endpoints
# ════════════════════════════════════════════════════════════════════════════


@router.get("/accounts")
async def list_accounts(request: Request):
    """Return all accounts (including inactive) for the admin screen."""
    _require_superadmin(request)
    from db.master_db import get_all_accounts_including_inactive
    accounts = get_all_accounts_including_inactive()
    return {"accounts": accounts}


@router.post("/accounts")
async def create_account(body: AccountCreateRequest, request: Request):
    """Provision a new tenant account (empty or cloned).

    Returns immediately with a job_id; poll /accounts/{job_id}/provision-status.
    """
    _require_superadmin(request)

    # Validate db_name
    safe = "".join(c for c in body.db_name if c.isalnum() or c == "_")
    if not safe or safe != body.db_name:
        raise HTTPException(status_code=400, detail="db_name may only contain letters, digits, and underscores")

    job_id = str(uuid.uuid4())
    _provision_jobs[job_id] = {
        "status": "queued",
        "display_name": body.display_name,
        "db_name": body.db_name,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    if body.clone_from_account_id:
        t = threading.Thread(
            target=_provision_clone,
            args=(job_id, body.display_name, body.db_name, body.schema_name, body.clone_from_account_id),
            daemon=True,
        )
    else:
        t = threading.Thread(
            target=_provision_empty,
            args=(job_id, body.display_name, body.db_name, body.schema_name),
            daemon=True,
        )
    t.start()

    return {"job_id": job_id, "status": "queued"}


@router.get("/accounts/{job_id}/provision-status")
async def provision_status(job_id: str, request: Request):
    """Poll the status of a provisioning or clone job."""
    _require_superadmin(request)
    job = _provision_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.delete("/accounts/{account_id}")
async def deactivate_account(account_id: str, request: Request):
    """Soft-delete an account (set is_active=FALSE).

    The underlying database is NOT dropped — use the migration script or psql
    directly if you want to permanently remove the data.
    """
    _require_superadmin(request)
    from db.master_db import deactivate_account as _deactivate
    updated = _deactivate(account_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Account not found or already inactive")
    # Remove from cache immediately
    _account_cache_ref.pop(account_id, None)
    return {"status": "deactivated", "account_id": account_id}


@router.post("/accounts/{account_id}/clone")
async def clone_account(account_id: str, body: AccountCloneRequest, request: Request):
    """Clone an existing account into a new database."""
    _require_superadmin(request)

    safe = "".join(c for c in body.db_name if c.isalnum() or c == "_")
    if not safe or safe != body.db_name:
        raise HTTPException(status_code=400, detail="db_name may only contain letters, digits, and underscores")

    job_id = str(uuid.uuid4())
    _provision_jobs[job_id] = {
        "status": "queued",
        "display_name": body.display_name,
        "db_name": body.db_name,
        "source_account_id": account_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    t = threading.Thread(
        target=_provision_clone,
        args=(job_id, body.display_name, body.db_name, body.schema_name, account_id),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "queued"}


# ════════════════════════════════════════════════════════════════════════════
# SuperAdmin management endpoints
# ════════════════════════════════════════════════════════════════════════════


@router.get("/superadmins")
async def list_superadmins_endpoint(request: Request):
    """List all active superAdmins."""
    _require_superadmin(request)
    from db.master_db import list_superadmins
    return {"superadmins": list_superadmins()}


@router.post("/superadmins")
async def create_superadmin_endpoint(body: SuperAdminCreateRequest, request: Request):
    """Create a new superAdmin (local auth)."""
    _require_superadmin(request)
    from db.master_db import create_superadmin
    try:
        sa = create_superadmin(body.email, body.display_name, body.password)
    except Exception as exc:
        if "unique" in str(exc).lower():
            raise HTTPException(status_code=409, detail="SuperAdmin with this email already exists")
        raise HTTPException(status_code=500, detail=str(exc))
    return sa


# ════════════════════════════════════════════════════════════════════════════
# Cross-account user management
# ════════════════════════════════════════════════════════════════════════════


def _fetch_users_from_account_cfg(acct_cfg: dict) -> list:
    """Connect to a tenant DB and return its users (no password hashes)."""
    try:
        conn = psycopg2.connect(
            host=acct_cfg.get("host", "localhost"),
            port=int(acct_cfg.get("port", 5432)),
            dbname=acct_cfg.get("db_name") or acct_cfg.get("database", ""),
            user=acct_cfg.get("user", "postgres"),
            password=acct_cfg.get("password", ""),
            sslmode=acct_cfg.get("sslmode", "disable"),
            connect_timeout=5,
        )
        schema = acct_cfg.get("schema", "zcube")
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"SELECT id::text, email, display_name, auth_provider, role, "
                    f"is_active, hashed_password, created_at "
                    f"FROM {schema}.users ORDER BY email"
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Could not fetch users from %s: %s", acct_cfg.get("db_name", "?"), exc)
        return []


def _copy_user_to_account_cfg(user: dict, acct_cfg: dict) -> None:
    """Insert a user record into another account's DB (ON CONFLICT DO NOTHING)."""
    try:
        conn = psycopg2.connect(
            host=acct_cfg.get("host", "localhost"),
            port=int(acct_cfg.get("port", 5432)),
            dbname=acct_cfg.get("db_name") or acct_cfg.get("database", ""),
            user=acct_cfg.get("user", "postgres"),
            password=acct_cfg.get("password", ""),
            sslmode=acct_cfg.get("sslmode", "disable"),
            connect_timeout=5,
        )
        schema = acct_cfg.get("schema", "zcube")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {schema}.users "
                    f"(email, display_name, hashed_password, auth_provider, role, is_active) "
                    f"VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING",
                    (
                        user["email"],
                        user["display_name"],
                        user.get("hashed_password"),
                        user.get("auth_provider", "local"),
                        user.get("role", "user"),
                        user.get("is_active", True),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("Failed to copy user %s to %s: %s", user["email"], acct_cfg.get("db_name"), exc)


@router.get("/users")
async def list_all_users(request: Request):
    """List all users across all active accounts (superAdmin only).

    Scans every active tenant DB and merges results by email.
    Also enriches with account assignments from master.user_accounts.
    """
    _require_superadmin(request)
    from db.master_db import get_all_accounts, get_user_account_assignments

    accounts = get_all_accounts()
    assignments = get_user_account_assignments()   # {email: [{id, display_name}]}

    # Scan each tenant DB
    seen: dict = {}   # email -> user dict
    for acct in accounts:
        acct_cfg = _account_cache_ref.get(acct["id"])
        if not acct_cfg:
            continue
        for u in _fetch_users_from_account_cfg(acct_cfg):
            email = u["email"]
            if email not in seen:
                seen[email] = {
                    "email": email,
                    "display_name": u["display_name"],
                    "auth_provider": u["auth_provider"],
                    "role": u["role"],
                    "is_active": u["is_active"],
                    "created_at": str(u.get("created_at") or ""),
                    "accounts": [],
                }
            seen[email]["accounts"].append({
                "id": acct["id"],
                "display_name": acct["display_name"],
            })

    # Merge master.user_accounts assignments (may include extra not yet scanned)
    for email, accts in assignments.items():
        if email in seen:
            existing_ids = {a["id"] for a in seen[email]["accounts"]}
            for a in accts:
                if a["id"] not in existing_ids:
                    seen[email]["accounts"].append(a)
        # If email not in seen yet, it's in master but not in any scanned DB — skip

    return {"users": list(seen.values())}


@router.get("/users/assignments")
async def get_assignments(request: Request):
    """Return {email: [{id, display_name}]} from master.user_accounts."""
    _require_superadmin(request)
    from db.master_db import get_user_account_assignments
    return get_user_account_assignments()


class UserAccountsRequest(BaseModel):
    account_ids: List[str]


@router.put("/users/{email}/accounts")
async def set_user_accounts(email: str, body: UserAccountsRequest, request: Request):
    """Set account assignments for a user.

    Adds user to new accounts (copies user record) and removes from dropped ones.
    Removing only deletes the master.user_accounts row — the user record in the
    tenant DB is kept for audit purposes.
    """
    _require_superadmin(request)
    from db.master_db import (
        get_accounts_for_user, assign_user_to_account,
        remove_user_from_account,
    )

    email_clean = email.lower().strip()
    current = {a["id"] for a in get_accounts_for_user(email_clean)}
    desired = set(body.account_ids)

    to_add    = desired - current
    to_remove = current - desired

    # Find user record from any current account (for copy)
    source_user = None
    for acct_id in current:
        acct_cfg = _account_cache_ref.get(acct_id)
        if acct_cfg:
            users = _fetch_users_from_account_cfg(acct_cfg)
            source_user = next((u for u in users if u["email"] == email_clean), None)
            if source_user:
                break

    # If user not in master yet, try scanning desired accounts
    if source_user is None:
        for acct_id in desired:
            acct_cfg = _account_cache_ref.get(acct_id)
            if acct_cfg:
                users = _fetch_users_from_account_cfg(acct_cfg)
                source_user = next((u for u in users if u["email"] == email_clean), None)
                if source_user:
                    break

    for acct_id in to_add:
        acct_cfg = _account_cache_ref.get(acct_id)
        if acct_cfg and source_user:
            _copy_user_to_account_cfg(source_user, acct_cfg)
        assign_user_to_account(email_clean, acct_id)

    for acct_id in to_remove:
        remove_user_from_account(email_clean, acct_id)

    return {
        "email": email_clean,
        "account_ids": body.account_ids,
        "added": list(to_add),
        "removed": list(to_remove),
    }


# ════════════════════════════════════════════════════════════════════════════
# Cache management
# ════════════════════════════════════════════════════════════════════════════


@router.post("/cache/refresh")
async def refresh_cache(request: Request):
    """Force reload of account cache from master DB."""
    _require_superadmin(request)
    _refresh_cache_from_master()
    return {"status": "ok", "account_count": len(_account_cache_ref)}
