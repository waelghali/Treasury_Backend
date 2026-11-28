# app/core/cloudflare_guard.py
"""
Cloudflare Origin Guard Middleware (Pure ASGI)

Rejects HTTP requests that arrive directly at the Render origin URL
(bypassing Cloudflare) unless the guard is disabled via environment variable.

Controlled by:
    ENFORCE_CLOUDFLARE_ONLY=true   → blocks direct-to-origin requests (default in production)
    ENFORCE_CLOUDFLARE_ONLY=false  → allows all requests (disable if issues arise)

Cloudflare always injects the CF-Connecting-IP header on proxied requests.
Render health-checks (which arrive without Cloudflare headers) are exempted
so the container is not mistakenly restarted.
"""
import os
import logging

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

# Paths that Render (or other uptime monitors) hit for health-checks.
# These must always be allowed through, even without Cloudflare headers.
_HEALTH_PATHS = frozenset({"/", "/health", "/healthz", "/ping"})


class CloudflareGuardMiddleware:
    """
    Pure ASGI middleware that blocks requests not arriving via Cloudflare.

    Toggle off instantly by setting ENFORCE_CLOUDFLARE_ONLY=false in Render
    dashboard — no code change or redeployment required (env var is read
    on every request so a service restart picks it up immediately).
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        # Check if the guard is enabled (default: enabled in production)
        is_prod = (
            os.getenv("ENVIRONMENT", "").lower() == "production"
            or os.getenv("RENDER") is not None
        )
        enforce = os.getenv("ENFORCE_CLOUDFLARE_ONLY", "true" if is_prod else "false")
        if enforce.lower() not in ("true", "1", "yes"):
            await self.app(scope, receive, send)
            return

        # Allow health-check paths (Render pings these directly)
        path = scope.get("path", "/")
        if path in _HEALTH_PATHS:
            await self.app(scope, receive, send)
            return

        # Look for Cloudflare's signature header
        headers = dict(scope.get("headers", []))
        has_cf = b"cf-connecting-ip" in headers

        if not has_cf:
            logger.warning(
                f"CloudflareGuard: blocked direct-to-origin request "
                f"path={path} (no CF-Connecting-IP header)"
            )
            # Return 403 Forbidden
            await send({
                "type": "http.response.start",
                "status": 403,
                "headers": [
                    [b"content-type", b"application/json"],
                ],
            })
            await send({
                "type": "http.response.body",
                "body": b'{"detail": "Direct access forbidden."}',
            })
            return

        await self.app(scope, receive, send)
