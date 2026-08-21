# app/core/routing.py
"""
Centralized Backend Route & Deep-Link Helper
Provides canonical, type-safe frontend URLs for emails, background alerts, and notifications.
"""
import os
from typing import Optional


def get_frontend_base_url() -> str:
    """Returns the configured frontend base URL from environment or default."""
    return os.getenv("FRONTEND_URL", "http://localhost:3000").rstrip("/")


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
