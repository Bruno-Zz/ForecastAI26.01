"""
JWT authentication middleware for FastAPI.
Intercepts all requests, validates JWT, attaches user to request.state.
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
)


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Validate Bearer JWT on every request (except whitelisted public paths)."""

    def __init__(self, app, secret_key: str, schema: str, config_path: str):
        super().__init__(app)
        self.secret_key = secret_key
        self.schema = schema
        self.config_path = config_path

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

        # ── Check revocation ──
        jti = payload.get("jti")
        if jti:
            try:
                from db.db import get_conn

                conn = get_conn(self.config_path)
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT 1 FROM {self.schema}.revoked_tokens WHERE jti = %s",
                            (jti,),
                        )
                        if cur.fetchone():
                            return JSONResponse(
                                status_code=401,
                                content={"detail": "Token has been revoked"},
                            )
                finally:
                    conn.close()
            except Exception as e:
                logger.debug(f"Revocation check skipped: {e}")

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
        }
        request.state.jti = jti

        return await call_next(request)
