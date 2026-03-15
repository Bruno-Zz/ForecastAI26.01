"""
Authentication module for ForecastAI.
JWT-based auth with local password, Microsoft OAuth, and Google OAuth support.
"""

import json
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from jose import JWTError, jwt
from passlib.context import CryptContext

# ── Ensure project root is importable ──
_files_dir = Path(__file__).resolve().parent.parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

from db.db import get_conn, get_schema

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])

# ── Password hashing ──
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

ALGORITHM = "HS256"


# ────────────────────────────────────────────────────────
# Config helpers  (read from DB 'auth' parameter set)
# ────────────────────────────────────────────────────────


def _load_auth_from_db() -> dict:
    """Load the 'auth' parameter set from the database.

    Returns an empty dict if the DB is unavailable or the row doesn't exist yet
    (e.g. during initial schema creation).
    """
    try:
        schema = get_schema()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT parameters_set FROM {schema}.parameters "
                    f"WHERE parameter_type = 'auth' AND is_default = TRUE LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    return row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
        finally:
            conn.close()
    except Exception:
        pass
    return {}


def _jwt_secret() -> str:
    # Multi-tenancy: JWT secret lives in master DB.
    # Fall back to account-level secret for single-tenant installs.
    try:
        from db.master_db import get_master_jwt_secret
        secret = get_master_jwt_secret()
        if secret and secret not in ("CHANGE-ME", "CHANGE-ME-IN-PRODUCTION"):
            return secret
    except Exception:
        pass
    return _load_auth_from_db().get("jwt_secret", "CHANGE-ME")


def _token_expiry_minutes() -> int:
    try:
        from db.master_db import get_master_token_expiry_minutes
        return get_master_token_expiry_minutes()
    except Exception:
        pass
    return int(_load_auth_from_db().get("token_expiry_minutes", 480))


# ────────────────────────────────────────────────────────
# Pydantic request / response models
# ────────────────────────────────────────────────────────


class LoginRequest(BaseModel):
    email: str
    password: str
    account_id: Optional[str] = None  # UUID — required when superAdmin selects account


class MicrosoftTokenRequest(BaseModel):
    access_token: str
    account_id: Optional[str] = None  # Which tenant account this OAuth user belongs to


class GoogleTokenRequest(BaseModel):
    credential: str  # Google ID token (JWT from Google Identity Services)
    account_id: Optional[str] = None  # Which tenant account this OAuth user belongs to


class CreateUserRequest(BaseModel):
    email: str
    display_name: str
    password: str
    role: str = "user"
    allowed_segments: List[int] = []
    can_run_process: bool = False
    can_create_override: bool = False
    allowed_segments_edit: List[int] = []


class UpdateUserRequest(BaseModel):
    display_name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    allowed_segments: Optional[List[int]] = None
    can_run_process: Optional[bool] = None
    can_create_override: Optional[bool] = None
    allowed_segments_edit: Optional[List[int]] = None


class UserResponse(BaseModel):
    id: str
    email: str
    display_name: str
    auth_provider: str
    role: str
    is_active: bool
    allowed_segments: List[int] = []
    can_run_process: bool = False
    can_create_override: bool = False
    allowed_segments_edit: List[int] = []
    created_at: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: UserResponse


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


# ────────────────────────────────────────────────────────
# JWT helpers
# ────────────────────────────────────────────────────────


def _create_access_token(user_row: dict, account_id: Optional[str] = None) -> str:
    """Create a signed JWT for the given tenant user.

    account_id — UUID of the tenant account this token is scoped to.
    If None the token has no account scope (legacy single-tenant mode).
    """
    exp_minutes = _token_expiry_minutes()
    payload = {
        "jti": str(uuid.uuid4()),
        "sub": str(user_row["id"]),
        "email": user_row["email"],
        "role": user_row["role"],
        "display_name": user_row["display_name"],
        "auth_provider": user_row.get("auth_provider", "local"),
        "allowed_segments": user_row.get("allowed_segments", []) or [],
        "can_run_process": user_row.get("can_run_process", False) or False,
        "can_create_override": user_row.get("can_create_override", False) or False,
        "allowed_segments_edit": user_row.get("allowed_segments_edit", []) or [],
        "is_superadmin": False,
        "account_id": account_id,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=exp_minutes),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm=ALGORITHM)


def _create_superadmin_token(superadmin_row: dict, account_id: str) -> str:
    """Create a signed JWT for a superAdmin logged into a specific account."""
    exp_minutes = _token_expiry_minutes()
    payload = {
        "jti": str(uuid.uuid4()),
        "sub": str(superadmin_row["id"]),
        "email": superadmin_row["email"],
        "role": "superadmin",
        "display_name": superadmin_row["display_name"],
        "auth_provider": superadmin_row.get("auth_provider", "local"),
        "allowed_segments": [],
        "can_run_process": True,
        "can_create_override": True,
        "allowed_segments_edit": [],
        "is_superadmin": True,
        "account_id": account_id,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=exp_minutes),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm=ALGORITHM)


def _user_row_to_response(row: dict) -> UserResponse:
    return UserResponse(
        id=str(row["id"]),
        email=row["email"],
        display_name=row["display_name"],
        auth_provider=row["auth_provider"],
        role=row["role"],
        is_active=row["is_active"],
        allowed_segments=row.get("allowed_segments", []) or [],
        can_run_process=row.get("can_run_process", False) or False,
        can_create_override=row.get("can_create_override", False) or False,
        allowed_segments_edit=row.get("allowed_segments_edit", []) or [],
        created_at=str(row["created_at"]),
    )


def _db_fetch_user_by_email(email: str) -> Optional[dict]:
    """Look up a user by email. Returns dict or None."""
    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, email, display_name, hashed_password, auth_provider, "
                f"role, is_active, allowed_segments, can_run_process, can_create_override, "
                f"allowed_segments_edit, created_at, updated_at "
                f"FROM {schema}.users WHERE email = %s",
                (email,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


def _db_fetch_user_by_id(user_id: str) -> Optional[dict]:
    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, email, display_name, hashed_password, auth_provider, "
                f"role, is_active, allowed_segments, can_run_process, can_create_override, "
                f"allowed_segments_edit, created_at, updated_at "
                f"FROM {schema}.users WHERE id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


def _user_has_segment_access(user: dict, segment_id: int, edit: bool = False) -> bool:
    """Check if user has access to a specific segment."""
    if user.get("role") == "admin":
        return True
    if edit:
        allowed = user.get("allowed_segments_edit", []) or []
    else:
        allowed = user.get("allowed_segments", []) or []
    return segment_id in allowed


def _user_can_run_process(user: dict) -> bool:
    """Check if user can run processes."""
    if user.get("role") == "admin":
        return True
    return user.get("can_run_process", False) or False


def _user_can_create_override(user: dict) -> bool:
    """Check if user can create overrides."""
    if user.get("role") == "admin":
        return True
    return user.get("can_create_override", False) or False


# ────────────────────────────────────────────────────────
# Request helpers
# ────────────────────────────────────────────────────────


def get_current_user(request: Request) -> dict:
    """Extract authenticated user from request.state (set by middleware)."""
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def _require_admin(request: Request) -> dict:
    user = get_current_user(request)
    if user.get("role") not in ("admin", "superadmin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _require_superadmin(request: Request) -> dict:
    """Require the caller to be a superAdmin (not just a tenant admin)."""
    user = get_current_user(request)
    if user.get("role") != "superadmin":
        raise HTTPException(status_code=403, detail="SuperAdmin access required")
    return user


def _db_fetch_user_by_email_for_account(email: str, account_cfg: dict) -> Optional[dict]:
    """Look up a user by email using an explicit account DB config.

    Used during login before the ContextVar is set.
    """
    import psycopg2
    import psycopg2.extras
    schema = account_cfg.get("schema", "zcube")
    try:
        conn = psycopg2.connect(
            host=account_cfg["host"],
            port=account_cfg["port"],
            dbname=account_cfg["database"],
            user=account_cfg["user"],
            password=account_cfg["password"],
            sslmode=account_cfg.get("sslmode", "disable"),
            options=f"-c search_path={schema},public",
        )
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, email, display_name, hashed_password, auth_provider, "
                    f"role, is_active, allowed_segments, can_run_process, can_create_override, "
                    f"allowed_segments_edit, created_at, updated_at "
                    f"FROM {schema}.users WHERE email = %s",
                    (email.lower().strip(),),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
        finally:
            conn.close()
    except Exception as exc:
        logger.error("_db_fetch_user_by_email_for_account failed: %s", exc)
        return None


# ────────────────────────────────────────────────────────
# Auth endpoints
# ────────────────────────────────────────────────────────


def _get_account_cache() -> dict:
    """Return the in-memory account cache populated by main.py at startup.

    Returns an empty dict if multi-tenancy is not yet configured.
    """
    try:
        import api.main as _main
        return getattr(_main, "_account_cache", {})
    except Exception:
        return {}


def _log_auth_event(
    account_id: Optional[str],
    email: str,
    action: str,
    details: Optional[dict] = None,
) -> None:
    """Insert an auth audit-log row into the correct tenant DB (fire-and-forget).

    Uses the account_cache to resolve the DB config when the ContextVar
    may not yet be set (e.g. during login).  Silently swallows all errors.
    """
    import psycopg2

    try:
        # Prefer the ContextVar-set connection (works for logout where middleware
        # already resolved the account).  Fall back to explicit account_id lookup.
        try:
            conn = get_conn()
            schema = get_schema()
            _owns_conn = True
        except Exception:
            conn = None
            schema = None
            _owns_conn = False

        if conn is None and account_id:
            cache = _get_account_cache()
            cfg = cache.get(account_id)
            if cfg:
                schema = cfg.get("schema", "zcube")
                conn = psycopg2.connect(
                    host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                    user=cfg["user"], password=cfg["password"],
                    sslmode=cfg.get("sslmode", "disable"),
                )
                _owns_conn = True

        if conn is None or schema is None:
            return

        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {schema}.audit_log "
                    f"(entity_type, entity_id, action, new_value, changed_by) "
                    f"VALUES ('auth', NULL, %s, %s::jsonb, %s)",
                    (
                        action,
                        json.dumps(details, default=str) if details else None,
                        email,
                    ),
                )
            conn.commit()
        finally:
            if _owns_conn:
                conn.close()
    except Exception as exc:
        logger.debug("Auth audit log failed (non-fatal): %s", exc)


@router.get("/probe")
async def probe_superadmin(email: str):
    """Check whether an email belongs to a superAdmin and return the accounts list.

    This endpoint is PUBLIC (no auth required).  It does NOT validate the
    password — it only indicates whether the login screen should show the
    account-selection dropdown.

    Response: {"is_superadmin": bool, "accounts": [{"id": str, "display_name": str}]}
    """
    try:
        from db.master_db import get_superadmin_by_email, get_all_accounts, get_accounts_for_user
        sa = get_superadmin_by_email(email.lower().strip())
        if sa:
            accounts = get_all_accounts()
            return {
                "is_superadmin": True,
                "accounts": [
                    {"id": a["id"], "display_name": a["display_name"]}
                    for a in accounts
                ],
            }
        # Regular user with multiple accounts → also show the dropdown
        user_accts = get_accounts_for_user(email.lower().strip())
        if len(user_accts) > 1:
            return {
                "is_superadmin": False,
                "accounts": [
                    {"id": a["id"], "display_name": a["display_name"]}
                    for a in user_accts
                ],
            }
    except Exception as exc:
        logger.debug("probe: master DB not available: %s", exc)
    return {"is_superadmin": False, "accounts": []}


@router.post("/login")
async def login(body: LoginRequest):
    """Local email / password login — supports both tenant users and superAdmins.

    Multi-tenant behaviour:
    1. Always checks master DB first for a superAdmin match.
       a. SuperAdmin found, no account_id in body →
          returns {"status": "select_account", "accounts": [...]} (no JWT yet).
       b. SuperAdmin found, account_id provided →
          issues JWT with role='superadmin' and account_id.
    2. Not a superAdmin → finds the tenant DB for `account_id` (required) and
       authenticates the tenant user normally.
    3. Legacy single-tenant (no master DB) → original behaviour, no account_id needed.
    """
    email = body.email.lower().strip()

    # ── 1. Check for superAdmin in master DB ──
    superadmin = None
    try:
        from db.master_db import verify_superadmin, get_all_accounts
        superadmin = verify_superadmin(email, body.password)
    except Exception as exc:
        logger.debug("SuperAdmin check skipped (master DB unavailable): %s", exc)

    if superadmin:
        if not body.account_id:
            # Phase 1: return accounts list so frontend can show dropdown
            try:
                accounts = get_all_accounts()
            except Exception:
                accounts = []
            return {
                "status": "select_account",
                "accounts": [
                    {"id": a["id"], "display_name": a["display_name"]}
                    for a in accounts
                ],
            }
        # Phase 2: account selected — issue superAdmin JWT
        token = _create_superadmin_token(superadmin, body.account_id)
        sa_response = UserResponse(
            id=str(superadmin["id"]),
            email=superadmin["email"],
            display_name=superadmin["display_name"],
            auth_provider=superadmin.get("auth_provider", "local"),
            role="superadmin",
            is_active=True,
            allowed_segments=[],
            can_run_process=True,
            can_create_override=True,
            allowed_segments_edit=[],
            created_at=str(superadmin.get("created_at", "")),
        )
        _log_auth_event(body.account_id, superadmin["email"], "login",
                        {"role": "superadmin", "auth_provider": "local"})
        return TokenResponse(
            access_token=token,
            expires_in=_token_expiry_minutes() * 60,
            user=sa_response,
        )

    # ── 2. Tenant user login ──
    account_id = body.account_id
    account_cache = _get_account_cache()

    if account_id and account_id in account_cache:
        # Multi-tenant: use the specified account's DB
        account_cfg = account_cache[account_id]
        user = _db_fetch_user_by_email_for_account(email, account_cfg)
    else:
        # Check whether this user has multiple account assignments in master DB
        if not account_id and account_cache:
            try:
                from db.master_db import get_accounts_for_user
                user_accts = get_accounts_for_user(email)
                if len(user_accts) > 1:
                    # Phase 1 — ask frontend to pick an account
                    return {
                        "status": "select_account",
                        "accounts": [
                            {"id": a["id"], "display_name": a["display_name"]}
                            for a in user_accts
                        ],
                    }
                elif len(user_accts) == 1:
                    account_id = user_accts[0]["id"]
            except Exception:
                pass
        if account_id and account_id in account_cache:
            account_cfg = account_cache[account_id]
            user = _db_fetch_user_by_email_for_account(email, account_cfg)
        else:
            # Single-tenant fallback: use config.yaml DB
            user = _db_fetch_user_by_email(email)
            if not account_id and account_cache:
                account_id = next(iter(account_cache))

    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if user["auth_provider"] != "local":
        provider = user["auth_provider"].capitalize()
        raise HTTPException(
            status_code=400,
            detail=f"This account uses {provider} sign-in. Please use the {provider} button.",
        )
    if not user["hashed_password"]:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not pwd_context.verify(body.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="Account is disabled")

    token = _create_access_token(user, account_id=account_id)
    _log_auth_event(account_id, user["email"], "login",
                    {"role": user["role"], "auth_provider": "local"})
    return TokenResponse(
        access_token=token,
        expires_in=_token_expiry_minutes() * 60,
        user=_user_row_to_response(user),
    )


@router.post("/microsoft")
async def microsoft_login(body: MicrosoftTokenRequest):
    """Validate Microsoft OAuth token and create/find user.

    SuperAdmin check: if the validated MS email is in master.superadmins,
    we issue a superAdmin JWT (or ask for account selection first).
    """
    # Call Microsoft Graph to validate the token and get user profile
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {body.access_token}"},
        )

    if resp.status_code != 200:
        logger.warning(f"Microsoft Graph returned {resp.status_code}: {resp.text}")
        raise HTTPException(status_code=401, detail="Invalid Microsoft token")

    ms_profile = resp.json()
    email = (ms_profile.get("mail") or ms_profile.get("userPrincipalName", "")).lower()
    display_name = ms_profile.get("displayName", email.split("@")[0])

    if not email:
        raise HTTPException(
            status_code=400, detail="Could not determine email from Microsoft profile"
        )

    # ── SuperAdmin check: if email is in master.superadmins, issue superAdmin JWT ──
    try:
        from db.master_db import get_superadmin_by_email, get_all_accounts
        sa = get_superadmin_by_email(email)
        if sa:
            if not body.account_id:
                # Phase 1: ask frontend to pick an account
                try:
                    accounts = get_all_accounts()
                except Exception:
                    accounts = []
                return {
                    "status": "select_account",
                    "accounts": [
                        {"id": a["id"], "display_name": a["display_name"]}
                        for a in accounts
                    ],
                }
            # Phase 2: account selected — issue superAdmin JWT
            token = _create_superadmin_token(sa, body.account_id)
            sa_response = UserResponse(
                id=str(sa["id"]),
                email=sa["email"],
                display_name=sa["display_name"],
                auth_provider="microsoft",
                role="superadmin",
                is_active=True,
                allowed_segments=[],
                can_run_process=True,
                can_create_override=True,
                allowed_segments_edit=[],
                created_at=str(sa.get("created_at", "")),
            )
            _log_auth_event(body.account_id, sa["email"], "login",
                            {"role": "superadmin", "auth_provider": "microsoft"})
            return TokenResponse(
                access_token=token,
                expires_in=_token_expiry_minutes() * 60,
                user=sa_response,
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.debug("SuperAdmin MS check skipped: %s", exc)

    # ── Regular tenant user flow ──
    # Upsert user
    user = _db_fetch_user_by_email(email)
    schema = get_schema()
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            if user is None:
                # Create new user from Microsoft
                cur.execute(
                    f"INSERT INTO {schema}.users "
                    f"(email, display_name, auth_provider, role, is_active) "
                    f"VALUES (%s, %s, 'microsoft', 'user', TRUE) "
                    f"RETURNING id, email, display_name, hashed_password, auth_provider, "
                    f"role, is_active, created_at, updated_at",
                    (email, display_name),
                )
                row = cur.fetchone()
                cols = [d[0] for d in cur.description]
                user = dict(zip(cols, row))
                conn.commit()
                logger.info(f"New Microsoft user created: {email}")
            else:
                # Update display name if changed
                if user["display_name"] != display_name:
                    cur.execute(
                        f"UPDATE {schema}.users SET display_name = %s, updated_at = NOW() "
                        f"WHERE id = %s",
                        (display_name, user["id"]),
                    )
                    conn.commit()
                    user["display_name"] = display_name
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="Account is disabled")

    account_id = body.account_id
    if not account_id:
        account_cache = _get_account_cache()
        if account_cache:
            account_id = next(iter(account_cache))
    token = _create_access_token(user, account_id=account_id)
    _log_auth_event(account_id, user["email"], "login",
                    {"role": user["role"], "auth_provider": "microsoft"})
    return TokenResponse(
        access_token=token,
        expires_in=_token_expiry_minutes() * 60,
        user=_user_row_to_response(user),
    )


@router.post("/google")
async def google_login(body: GoogleTokenRequest):
    """Validate Google ID token and create/find user.

    SuperAdmin check: if the validated Google email is in master.superadmins,
    we issue a superAdmin JWT (or ask for account selection first).
    """
    # Verify the Google ID token
    auth_cfg = _load_auth_config()
    google_client_id = auth_cfg.get("google", {}).get("client_id", "")

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://oauth2.googleapis.com/tokeninfo?id_token={body.credential}"
        )

    if resp.status_code != 200:
        logger.warning(f"Google tokeninfo returned {resp.status_code}: {resp.text}")
        raise HTTPException(status_code=401, detail="Invalid Google token")

    token_info = resp.json()

    # Verify the audience matches our client ID (if configured)
    if google_client_id and token_info.get("aud") != google_client_id:
        logger.warning(f"Google token audience mismatch: {token_info.get('aud')}")
        raise HTTPException(status_code=401, detail="Invalid Google token audience")

    email = token_info.get("email", "").lower()
    if not email or not token_info.get("email_verified", False):
        raise HTTPException(status_code=400, detail="Google account email not verified")

    display_name = token_info.get("name", email.split("@")[0])

    # ── SuperAdmin check ──
    try:
        from db.master_db import get_superadmin_by_email, get_all_accounts
        sa = get_superadmin_by_email(email)
        if sa:
            if not body.account_id:
                try:
                    accounts = get_all_accounts()
                except Exception:
                    accounts = []
                return {
                    "status": "select_account",
                    "accounts": [
                        {"id": a["id"], "display_name": a["display_name"]}
                        for a in accounts
                    ],
                }
            token = _create_superadmin_token(sa, body.account_id)
            sa_response = UserResponse(
                id=str(sa["id"]),
                email=sa["email"],
                display_name=sa["display_name"],
                auth_provider="google",
                role="superadmin",
                is_active=True,
                allowed_segments=[],
                can_run_process=True,
                can_create_override=True,
                allowed_segments_edit=[],
                created_at=str(sa.get("created_at", "")),
            )
            return TokenResponse(
                access_token=token,
                expires_in=_token_expiry_minutes() * 60,
                user=sa_response,
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.debug("SuperAdmin Google check skipped: %s", exc)

    # ── Regular tenant user flow ──
    # Upsert user
    user = _db_fetch_user_by_email(email)
    schema = get_schema()
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            if user is None:
                cur.execute(
                    f"INSERT INTO {schema}.users "
                    f"(email, display_name, auth_provider, role, is_active) "
                    f"VALUES (%s, %s, 'google', 'user', TRUE) "
                    f"RETURNING id, email, display_name, hashed_password, auth_provider, "
                    f"role, is_active, created_at, updated_at",
                    (email, display_name),
                )
                row = cur.fetchone()
                cols = [d[0] for d in cur.description]
                user = dict(zip(cols, row))
                conn.commit()
                logger.info(f"New Google user created: {email}")
            else:
                if user["display_name"] != display_name:
                    cur.execute(
                        f"UPDATE {schema}.users SET display_name = %s, updated_at = NOW() "
                        f"WHERE id = %s",
                        (display_name, user["id"]),
                    )
                    conn.commit()
                    user["display_name"] = display_name
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="Account is disabled")

    account_id = body.account_id
    if not account_id:
        account_cache = _get_account_cache()
        if account_cache:
            account_id = next(iter(account_cache))
    token = _create_access_token(user, account_id=account_id)
    _log_auth_event(account_id, user["email"], "login",
                    {"role": user["role"], "auth_provider": "google"})
    return TokenResponse(
        access_token=token,
        expires_in=_token_expiry_minutes() * 60,
        user=_user_row_to_response(user),
    )


@router.get("/me", response_model=UserResponse)
async def get_me(request: Request):
    """Return current authenticated user info."""
    jwt_user = get_current_user(request)

    # SuperAdmins are stored in master DB — not in a tenant DB
    if jwt_user.get("is_superadmin") or jwt_user.get("role") == "superadmin":
        return UserResponse(
            id=str(jwt_user["id"]),
            email=jwt_user["email"],
            display_name=jwt_user.get("display_name", ""),
            auth_provider=jwt_user.get("auth_provider", "local"),
            role="superadmin",
            is_active=True,
            allowed_segments=[],
            can_run_process=True,
            can_create_override=True,
            allowed_segments_edit=[],
            created_at="",
        )

    user = _db_fetch_user_by_id(jwt_user["id"])
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return _user_row_to_response(user)


@router.post("/users", response_model=UserResponse)
async def create_user(body: CreateUserRequest, request: Request):
    """Admin: create a new local user."""
    _require_admin(request)

    if body.role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="Role must be 'admin' or 'user'")

    existing = _db_fetch_user_by_email(body.email)
    if existing:
        raise HTTPException(
            status_code=409, detail="A user with this email already exists"
        )

    hashed = pwd_context.hash(body.password)
    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.users "
                f"(email, display_name, hashed_password, auth_provider, role, is_active, "
                f"allowed_segments, can_run_process, can_create_override, allowed_segments_edit) "
                f"VALUES (%s, %s, %s, 'local', %s, TRUE, %s, %s, %s, %s) "
                f"RETURNING id, email, display_name, hashed_password, auth_provider, "
                f"role, is_active, allowed_segments, can_run_process, can_create_override, "
                f"allowed_segments_edit, created_at, updated_at",
                (
                    body.email,
                    body.display_name,
                    hashed,
                    body.role,
                    body.allowed_segments,
                    body.can_run_process,
                    body.can_create_override,
                    body.allowed_segments_edit,
                ),
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            user = dict(zip(cols, row))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # Auto-register in master.user_accounts so the user can be tracked cross-account
    try:
        current_account_id = getattr(request.state, "user", {}).get("account_id")
        if current_account_id:
            from db.master_db import assign_user_to_account
            assign_user_to_account(body.email.lower(), current_account_id)
    except Exception as _ua_exc:
        logger.warning("Could not register user in master.user_accounts: %s", _ua_exc)

    logger.info(f"Admin created user: {body.email} (role={body.role})")
    return _user_row_to_response(user)


@router.get("/users")
async def list_users(request: Request):
    """Admin: list all users."""
    _require_admin(request)

    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, email, display_name, auth_provider, role, is_active, "
                f"allowed_segments, can_run_process, can_create_override, allowed_segments_edit, "
                f"created_at, updated_at FROM {schema}.users ORDER BY created_at"
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    # Convert UUIDs and datetimes to strings
    for r in rows:
        r["id"] = str(r["id"])
        r["created_at"] = str(r["created_at"])
        r["updated_at"] = str(r["updated_at"]) if r.get("updated_at") else None

    return rows


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(user_id: str, body: UpdateUserRequest, request: Request):
    """Admin: update a user (role, active status, display name, permissions)."""
    _require_admin(request)

    user = _db_fetch_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    updates = []
    params = []
    if body.display_name is not None:
        updates.append("display_name = %s")
        params.append(body.display_name)
    if body.role is not None:
        if body.role not in ("admin", "user"):
            raise HTTPException(
                status_code=400, detail="Role must be 'admin' or 'user'"
            )
        updates.append("role = %s")
        params.append(body.role)
    if body.is_active is not None:
        updates.append("is_active = %s")
        params.append(body.is_active)
    if body.allowed_segments is not None:
        updates.append("allowed_segments = %s")
        params.append(body.allowed_segments)
    if body.can_run_process is not None:
        updates.append("can_run_process = %s")
        params.append(body.can_run_process)
    if body.can_create_override is not None:
        updates.append("can_create_override = %s")
        params.append(body.can_create_override)
    if body.allowed_segments_edit is not None:
        updates.append("allowed_segments_edit = %s")
        params.append(body.allowed_segments_edit)

    if not updates:
        return _user_row_to_response(user)

    updates.append("updated_at = NOW()")
    params.append(user_id)

    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {schema}.users SET {', '.join(updates)} WHERE id = %s "
                f"RETURNING id, email, display_name, hashed_password, auth_provider, "
                f"role, is_active, allowed_segments, can_run_process, can_create_override, "
                f"allowed_segments_edit, created_at, updated_at",
                params,
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            updated = dict(zip(cols, row))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return _user_row_to_response(updated)


@router.post("/change-password")
async def change_password(body: ChangePasswordRequest, request: Request):
    """User: change own password."""
    jwt_user = get_current_user(request)
    user = _db_fetch_user_by_id(jwt_user["id"])

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["auth_provider"] != "local":
        raise HTTPException(
            status_code=400, detail="Password change is only for local accounts"
        )
    if not pwd_context.verify(body.current_password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Current password is incorrect")

    new_hash = pwd_context.hash(body.new_password)
    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {schema}.users SET hashed_password = %s, updated_at = NOW() "
                f"WHERE id = %s",
                (new_hash, str(user["id"])),
            )
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {"detail": "Password changed successfully"}


@router.post("/logout")
async def logout(request: Request):
    """Revoke the current JWT."""
    jwt_user = get_current_user(request)
    # The jti is stored on request.state by the middleware
    jti = getattr(request.state, "jti", None)
    if not jti:
        return {"detail": "Logged out"}

    schema = get_schema()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.revoked_tokens (jti) VALUES (%s) ON CONFLICT DO NOTHING",
                (jti,),
            )
            # Audit log — same transaction cursor
            try:
                cur.execute(
                    f"INSERT INTO {schema}.audit_log "
                    f"(entity_type, entity_id, action, new_value, changed_by) "
                    f"VALUES ('auth', NULL, 'logout', NULL, %s)",
                    (jwt_user.get("email", "unknown"),),
                )
            except Exception:
                pass
            conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return {"detail": "Logged out"}


# ────────────────────────────────────────────────────────
# Admin seed (called on startup)
# ────────────────────────────────────────────────────────


def seed_default_admin(config_path=None) -> None:
    """Create the default admin user on first run if no users exist.

    Default admin credentials are read from the 'auth' parameter set in the DB
    (Settings -> System Configuration -> Auth).  The legacy *config_path*
    parameter is accepted but ignored.
    """
    try:
        auth_cfg = _load_auth_from_db()
        default_admin = auth_cfg.get("default_admin", {})
        if not default_admin.get("email"):
            logger.info("No default_admin configured in DB -- skipping seed")
            return

        schema = get_schema()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.users")
                count = cur.fetchone()[0]
                if count == 0:
                    hashed = pwd_context.hash(default_admin["password"])
                    cur.execute(
                        f"INSERT INTO {schema}.users "
                        f"(email, display_name, hashed_password, auth_provider, role, is_active) "
                        f"VALUES (%s, %s, %s, 'local', 'admin', TRUE)",
                        (
                            default_admin["email"],
                            default_admin.get("display_name", "Admin"),
                            hashed,
                        ),
                    )
                    conn.commit()
                    logger.info(f"Default admin user created: {default_admin['email']}")
                else:
                    logger.info(
                        f"Users table has {count} row(s) -- skipping admin seed"
                    )
        except Exception as e:
            conn.rollback()
            logger.warning(f"Admin seed failed (non-fatal): {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Admin seed config load failed (non-fatal): {e}")
