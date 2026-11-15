# app/core/routing.py
"""
Centralized Backend Route & Deep-Link Helper
Provides canonical, type-safe frontend URLs for emails, background alerts, and notifications.
"""
import os
from typing import Optional


from urllib.parse import urlparse


def _is_trusted_grow_url(candidate_url: str, allow_backend_hosts: bool = False) -> bool:
    """
    Validates that a URL derived from request headers belongs strictly to official Grow
    domains (or localhost during non-production development) to prevent reset-link poisoning.
    """
    if not candidate_url:
        return False
    try:
        parsed = urlparse(candidate_url)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return False

    host = parsed.hostname.lower().strip(".")
    is_prod_env = bool(
        "grow_xl8z" in os.getenv("DATABASE_URL", "").lower()
        or os.getenv("ENVIRONMENT") == "production"
        or os.getenv("RENDER")
    )

    if host == "growbusinessdevelopment.com" or host.endswith(".growbusinessdevelopment.com"):
        return parsed.scheme == "https"

    if allow_backend_hosts and host.endswith(".onrender.com"):
        configured_backend = (os.getenv("BACKEND_URL") or os.getenv("API_BASE_URL") or "").lower()
        if configured_backend and host in configured_backend:
            return parsed.scheme == "https"

    if not is_prod_env and host in ("localhost", "127.0.0.1", "testserver"):
        return True

    # Also trust exact match against configured FRONTEND_URL / BACKEND_URL
    for env_key in ("FRONTEND_URL", "BACKEND_URL", "API_BASE_URL"):
        env_url = os.getenv(env_key, "").strip()
        if env_url:
            try:
                env_host = (urlparse(env_url).hostname or "").lower()
                if env_host and host == env_host:
                    return True
            except Exception:
                pass

    return False


def get_frontend_base_url(request: Optional[object] = None) -> str:
    """
    Returns the canonical frontend base URL for the active environment.
    Validates incoming headers against official Grow domains before use.
    """
    if request and hasattr(request, "headers"):
        origin = request.headers.get("origin")
        if origin and origin.startswith("http"):
            clean_origin = origin.rstrip("/")
            if _is_trusted_grow_url(clean_origin):
                return clean_origin

        referer = request.headers.get("referer")
        if referer and referer.startswith("http"):
            parsed = urlparse(referer)
            if parsed.scheme and parsed.netloc:
                clean_ref = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
                if _is_trusted_grow_url(clean_ref):
                    return clean_ref

        host = request.headers.get("x-forwarded-host") or request.headers.get("host")
        if host:
            is_ssl = "growbusinessdevelopment.com" in str(host) or "render.com" in str(host) or "vercel.app" in str(host)
            proto = request.headers.get("x-forwarded-proto", "https" if is_ssl else "http")
            candidate = f"{proto}://{host}".rstrip("/")
            if _is_trusted_grow_url(candidate):
                return candidate

    env_val = os.getenv("FRONTEND_URL")
    if env_val and env_val.strip():
        return env_val.rstrip("/")

    # Detect staging / production environment from DB URL or environment variables
    db_url = os.getenv("DATABASE_URL", "").lower()
    if "staging" in db_url or os.getenv("ENVIRONMENT") == "staging" or os.getenv("FLASK_ENV") == "staging":
        return "https://staging.growbusinessdevelopment.com"
    if "grow_xl8z" in db_url or os.getenv("ENVIRONMENT") == "production" or os.getenv("RENDER"):
        return "https://www.growbusinessdevelopment.com"

    return "http://localhost:3000"


def get_backend_base_url(request: Optional[object] = None) -> str:
    """
    Returns the canonical backend API base URL for document streaming and download endpoints.
    """
    env_backend = os.getenv("BACKEND_URL") or os.getenv("API_BASE_URL")
    if env_backend and env_backend.strip():
        return env_backend.rstrip("/")

    if request and hasattr(request, "headers"):
        host = request.headers.get("x-forwarded-host") or request.headers.get("host")
        if host:
            is_ssl = "growbusinessdevelopment.com" in str(host) or "render.com" in str(host) or "vercel.app" in str(host)
            proto = request.headers.get("x-forwarded-proto", "https" if is_ssl else "http")
            candidate = f"{proto}://{host}".rstrip("/")
            if _is_trusted_grow_url(candidate, allow_backend_hosts=True):
                return candidate

    db_url = os.getenv("DATABASE_URL", "").lower()
    if "staging" in db_url or os.getenv("ENVIRONMENT") == "staging":
        return "https://staging-api.growbusinessdevelopment.com"
    if "grow_xl8z" in db_url or os.getenv("ENVIRONMENT") == "production" or os.getenv("RENDER"):
        return "https://api.growbusinessdevelopment.com"

    return "http://localhost:8000"



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
