# app/core/security_headers.py
"""
OWASP Recommended HTTP Security Headers Middleware (Pure ASGI)
Attaches defense-in-depth headers to all outbound FastAPI HTTP responses
without BaseHTTPMiddleware stream/task-group allocation overhead.
"""
import os
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


def _is_https_request(scope: Scope) -> bool:
    """Returns True for direct HTTPS or trusted reverse-proxy HTTPS requests."""
    if scope.get("scheme") == "https":
        return True
    trust_forwarded = os.getenv(
        "TRUST_X_FORWARDED",
        "true" if os.getenv("RENDER") else "false",
    ).lower() == "true"
    if trust_forwarded:
        req_headers = Headers(scope=scope)
        proto = (req_headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
        return proto == "https"
    return False


class SecurityHeadersMiddleware:
    """
    Pure ASGI middleware that adds OWASP security headers to prevent common web vulnerabilities:
    - MIME-type sniffing (X-Content-Type-Options: nosniff)
    - Clickjacking (X-Frame-Options: SAMEORIGIN, except document preview stream)
    - Referrer leakage (Referrer-Policy: strict-origin-when-cross-origin)
    - Browser sensor/hardware APIs (Permissions-Policy, preserving microphone for voice Q&A)
    - Transport security (Strict-Transport-Security on HTTPS)
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        is_https = _is_https_request(scope)
        allow_frame_embed = path.endswith("/documents/download")

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.setdefault("X-Content-Type-Options", "nosniff")
                if not allow_frame_embed:
                    headers.setdefault("X-Frame-Options", "SAMEORIGIN")
                headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
                headers.setdefault(
                    "Permissions-Policy",
                    "geolocation=(), camera=(), payment=(), usb=()",
                )
                if is_https:
                    headers.setdefault(
                        "Strict-Transport-Security",
                        "max-age=31536000; includeSubDomains",
                    )
            await send(message)

        await self.app(scope, receive, send_wrapper)
