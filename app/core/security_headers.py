# app/core/security_headers.py
"""
OWASP Recommended HTTP Security Headers Middleware
Attaches defense-in-depth headers to all outbound FastAPI responses.
"""
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware that adds security headers to prevent common web vulnerabilities:
    - XSS & Sniffing attacks (X-Content-Type-Options, X-XSS-Protection)
    - Clickjacking (X-Frame-Options)
    - Transport Security (HSTS)
    - Sensitive token referrer leakage (Referrer-Policy)
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        response: Response = await call_next(request)

        # 1. Prevent MIME-type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # 2. Clickjacking protection (Permit embedding only by same origin)
        response.headers["X-Frame-Options"] = "SAMEORIGIN"

        # 3. Legacy XSS protection filter for older browsers
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # 4. Strict Referrer Policy: sends full URL only to same-origin
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # 5. HTTP Strict Transport Security (HSTS) for HTTPS
        if request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        return response
