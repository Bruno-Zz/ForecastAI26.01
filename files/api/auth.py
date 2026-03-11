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
    return _load_auth_from_db().get("jwt_secret", "CHANGE-ME")


def _token_expiry_minutes() -> int:
    return int(_load_auth_from_db().get("token_expiry_minutes", 480))


# ────────────────────────────────────────────────────────
# Pydantic request / response models
# ────────────────────────────────────────────────────────


class LoginRequest(BaseModel):
    email: str
    password: str


class MicrosoftTokenRequest(BaseModel):
    access_token: str


class GoogleTokenRequest(BaseModel):
    credential: str  # Google ID token (JWT from Google Identity Services)


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


def _create_access_token(user_row: dict) -> str:
    """Create a signed JWT for the given user."""
    exp_minutes = _token_expiry_minutes()
    payload = {
        "jti": str(uuid.uuid4()),
        "sub": str(user_row["id"]),
        "email": user_row["email"],
        "role": user_row["role"],
        "display_name": user_row["display_name"],
        "auth_provider": user_row["auth_provider"],
        "allowed_segments": user_row.get("allowed_segments", []) or [],
        "can_run_process": user_row.get("can_run_process", False) or False,
        "can_create_override": user_row.get("can_create_override", False) or False,
        "allowed_segments_edit": user_row.get("allowed_segments_edit", []) or [],
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
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# ────────────────────────────────────────────────────────
# Auth endpoints
# ────────────────────────────────────────────────────────


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest):
    """Local email / password login."""
    user = _db_fetch_user_by_email(body.email)
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

    token = _create_access_token(user)
    return TokenResponse(
        access_token=token,
        expires_in=_token_expiry_minutes() * 60,
        user=_user_row_to_response(user),
    )


@router.post("/microsoft", response_model=TokenResponse)
async def microsoft_login(body: MicrosoftTokenRequest):
    """Validate Microsoft OAuth token and create/find user."""
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

    token = _create_access_token(user)
    return TokenResponse(
        access_token=token,
        expires_in=_token_expiry_minutes() * 60,
        user=_user_row_to_response(user),
    )


@router.post("/google", response_model=TokenResponse)
async def google_login(body: GoogleTokenRequest):
    """Validate Google ID token and create/find user."""
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

    token = _create_access_token(user)
    return TokenResponse(
        access_token=token,
        expires_in=_token_expiry_minutes() * 60,
        user=_user_row_to_response(user),
    )


@router.get("/me", response_model=UserResponse)
async def get_me(request: Request):
    """Return current authenticated user info."""
    jwt_user = get_current_user(request)
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
