# app/core/routing.py
"""
Centralized Backend Route & Deep-Link Helper
Provides canonical, type-safe frontend URLs for emails, background alerts, and notifications.
"""
import os
from typing import Optional


def get_frontend_base_url(request: Optional[object] = None) -> str:
    """
    Returns the canonical frontend base URL for the active environment.
    Priority:
    1. Origin header from incoming request (e.g. https://staging.growbusinessdevelopment.com)
    2. Referer header from incoming request
    3. X-Forwarded-Host or Host header from incoming request
    4. FRONTEND_URL environment variable
    5. Automatic environment detection (Staging DB or staging environment variables)
    6. Default: http://localhost:3000
    """
    if request and hasattr(request, "headers"):
        origin = request.headers.get("origin")
        if origin and origin.startswith("http"):
            return origin.rstrip("/")
        
        referer = request.headers.get("referer")
        if referer and referer.startswith("http"):
            from urllib.parse import urlparse
            parsed = urlparse(referer)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        
        host = request.headers.get("x-forwarded-host") or request.headers.get("host")
        if host:
            is_ssl = "growbusinessdevelopment.com" in str(host) or "render.com" in str(host) or "vercel.app" in str(host)
            proto = request.headers.get("x-forwarded-proto", "https" if is_ssl else "http")
            return f"{proto}://{host}".rstrip("/")

    env_val = os.getenv("FRONTEND_URL")
    if env_val and env_val.strip():
        return env_val.rstrip("/")

    # Detect staging environment from DB URL or environment variables
    db_url = os.getenv("DATABASE_URL", "").lower()
    if "staging" in db_url or os.getenv("ENVIRONMENT") == "staging" or os.getenv("FLASK_ENV") == "staging":
        return "https://staging.growbusinessdevelopment.com"

    return "http://localhost:3000"


def get_entity_link(entity_type: str, entity_id: Optional[int] = None, role: str = "end_user") -> str:
    """
    Returns the canonical frontend in-app route for a given entity type and role.
    
    Examples:
        get_entity_link("LG_RECORD", 123, "corporate_admin") -> "/corporate-admin/lg-records/123"
        get_entity_link("FACILITY", role="corporate_admin")   -> "/corporate-admin/issuance/facilities"
        get_entity_link("ISSUANCE_REQUEST", role="end_user")  -> "/end-user/issuance/requests"
    """
    is_corp = role in ("corporate_admin", "viewer")
    role_path = "corporate-admin" if is_corp else "end-user"

    normalized_type = entity_type.upper().strip()

    if normalized_type in ("LG_RECORD", "LG_RECORDS", "ISSUED_LG", "ISSUED_LGS"):
        if entity_id:
            return f"/{role_path}/lg-records/{entity_id}"
        return f"/{role_path}/lg-records"

    if normalized_type in ("FACILITY", "FACILITIES", "ISSUANCE_FACILITY"):
        return "/corporate-admin/issuance/facilities"

    if normalized_type in ("ISSUANCE_REQUEST", "ISSUANCE_REQUESTS", "REQUEST"):
        return f"/{role_path}/issuance/requests"

    if normalized_type in ("ISSUANCE_ISSUED_LG", "ISSUANCE_ISSUED_LGS"):
        return f"/{role_path}/issuance/issued-lgs"

    if normalized_type in ("APPROVAL", "APPROVAL_INBOX", "PENDING_APPROVALS"):
        return f"/{role_path}/approval-inbox" if is_corp else f"/{role_path}/pending-approvals"

    if normalized_type in ("ACTION_CENTER", "ACTIONCENTER"):
        return f"/{role_path}/action-center"

    if normalized_type in ("QUOTATION", "QUOTATIONS", "RFQ"):
        return f"/{role_path}/quotations/history"

    return f"/{role_path}/dashboard"


def get_public_quotation_link(token: str, base_url: Optional[str] = None) -> str:
    """Returns the full public quotation link for external bank partners."""
    host = base_url.rstrip("/") if base_url else get_frontend_base_url()
    return f"{host}/public-quotation/{token}"


def get_public_issuance_portal_link(base_url: Optional[str] = None) -> str:
    """Returns the public issuance self-service portal link."""
    host = base_url.rstrip("/") if base_url else get_frontend_base_url()
    return f"{host}/portal/issuance"
