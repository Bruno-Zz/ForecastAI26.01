"""
JWT authentication middleware for FastAPI.
Intercepts all requests, validates JWT, attaches user to request.state.

Multi-tenancy: after decoding the JWT the middleware reads the account_id
claim, resolves the tenant DB config from the in-memory account_cache, and
sets the per-request ContextVar in db.db so that every get_conn() call inside
the handler transparently uses the correct tenant database.
"""

import logging

from jose import JWTError, jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# Exact paths that do NOT require authentication
PUBLIC_PATHS = frozenset(
    {
        "/",
        "/docs",
        "/openapi.json",
        "/redoc",
    }
)

# Path prefixes that are public
PUBLIC_PREFIXES = (
    "/api/auth/login",
    "/api/auth/microsoft",
    "/api/auth/google",
    "/api/auth/probe",
)


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Validate Bearer JWT on every request (except whitelisted public paths).

    Parameters
    ----------
    secret_key : str
        HS256 signing secret — loaded from master.parameters at startup.
    schema : str
        Legacy single-tenant schema name used for revocation check fallback.
    account_cache : dict
        Mapping of account_id (str UUID) → DB config dict.
        Populated at startup from master.accounts; refreshed when accounts
        are added or removed.
    config_path : ignored (legacy parameter)
    """

    def __init__(
        self,
        app,
        secret_key: str,
        schema: str = "zcube",
        account_cache: dict = None,
        config_path=None,
    ):
        super().__init__(app)
        self.secret_key = secret_key
        self.schema = schema
        # Mutable dict — changes in main.py are reflected here automatically
        self.account_cache: dict = account_cache if account_cache is not None else {}

    async def dispatch(self, request, call_next):
        path = request.url.path

        # Skip auth for public paths
        if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return await call_next(request)

        # CORS preflight
        if request.method == "OPTIONS":
            return await call_next(request)

        # ── Extract token ──
        # 1. Authorization header (standard)
        # 2. ?token= query param (fallback for EventSource / SSE)
        auth_header = request.headers.get("Authorization", "")
        token = None
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        else:
            token = request.query_params.get("token")

        if not token:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid authorization header"},
            )

        # ── Decode JWT ──
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=["HS256"])
        except JWTError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )

        # ── Resolve tenant account context ──
        account_id: str | None = payload.get("account_id")
        ctx_token = None
        effective_schema = self.schema  # default (single-tenant fallback)

        if account_id and account_id in self.account_cache:
            account_cfg = self.account_cache[account_id]
            effective_schema = account_cfg.get("schema", self.schema)
            try:
                from db.db import set_account_context
                ctx_token = set_account_context(account_cfg)
            except Exception as exc:
                logger.warning("Failed to set account context for %s: %s", account_id, exc)

        # ── Check revocation ──
        jti = payload.get("jti")
        if jti:
            try:
                from db.db import get_conn

                conn = get_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT 1 FROM {effective_schema}.revoked_tokens WHERE jti = %s",
                            (jti,),
                        )
                        if cur.fetchone():
                            if ctx_token is not None:
                                try:
                                    from db.db import reset_account_context
                                    reset_account_context(ctx_token)
                                except Exception:
                                    pass
                            return JSONResponse(
                                status_code=401,
                                content={"detail": "Token has been revoked"},
                            )
                finally:
                    conn.close()
            except Exception as e:
                logger.debug("Revocation check skipped: %s", e)

        # ── Attach user to request state ──
        request.state.user = {
            "id": payload.get("sub"),
            "email": payload.get("email"),
            "role": payload.get("role"),
            "display_name": payload.get("display_name"),
            "auth_provider": payload.get("auth_provider"),
            "allowed_segments": payload.get("allowed_segments", []),
            "can_run_process": payload.get("can_run_process", False),
            "can_create_override": payload.get("can_create_override", False),
            "allowed_segments_edit": payload.get("allowed_segments_edit", []),
            # Multi-tenancy fields
            "account_id": account_id,
            "is_superadmin": payload.get("is_superadmin", False),
        }
        request.state.jti = jti

        try:
            response = await call_next(request)
        finally:
            # Always reset the ContextVar so it does not leak into the next request
            if ctx_token is not None:
                try:
                    from db.db import reset_account_context
                    reset_account_context(ctx_token)
                except Exception:
                    pass

        return response
