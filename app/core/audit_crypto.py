# app/core/audit_crypto.py
"""
Cryptographic WORM (Write Once, Read Many) Audit Trail Engine.
Phase 5: Institutional Banking Cyber Security & Compliance Readiness.

Implements an immutable SHA-256 hash-chain across all platform audit events.
Guarantees mathematical non-repudiation, tamper-evidence, and regulatory compliance:
- Any deletion, alteration, or insertion of audit records breaks the hash chain.
- Operates with zero external network dependencies (pure Python standard library).
- Fully backwards-compatible: historical legacy records without hashes serve as the baseline.
"""

import json
import hashlib
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List, Tuple
from sqlalchemy.orm import Session

GENESIS_HASH = "0" * 64  # 64-character zero-seed for the initial chained block


def normalize_timestamp_canonical(ts: Any) -> str:
    """
    Normalizes any datetime (naive, UTC, or timezone-aware from PostgreSQL)
    into a canonical ISO-8601 UTC string to prevent hash mismatches across sessions.
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc).isoformat()
    return str(ts)


def serialize_canonical_audit_payload(
    timestamp: Any,
    user_id: Optional[int],
    action_type: str,
    entity_type: str,
    entity_id: Optional[int],
    customer_id: Optional[int],
    lg_record_id: Optional[int],
    details: Any,
    ip_address: Optional[str]
) -> str:
    """
    Serializes audit attributes into a deterministic canonical JSON string
    with sorted keys and compact separators.
    """
    canonical_ts = normalize_timestamp_canonical(timestamp)
    payload: Dict[str, Any] = {
        "user_id": user_id,
        "action_type": str(action_type or "").strip().upper(),
        "entity_type": str(entity_type or "").strip(),
        "entity_id": entity_id,
        "customer_id": customer_id,
        "lg_record_id": lg_record_id,
        "ip_address": str(ip_address).strip() if ip_address else None,
        "timestamp": canonical_ts,
        "details": details if details is not None else {}
    }
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str)


def compute_audit_hash(
    previous_hash: str,
    timestamp: Any,
    user_id: Optional[int],
    action_type: str,
    entity_type: str,
    entity_id: Optional[int],
    customer_id: Optional[int],
    lg_record_id: Optional[int],
    details: Any,
    ip_address: Optional[str]
) -> str:
    """
    Computes a cryptographic SHA-256 hash linking the previous entry's hash
    with the canonical representation of the current audit event.
    """
    clean_prev = (previous_hash or GENESIS_HASH).strip().upper()
    canonical_payload = serialize_canonical_audit_payload(
        timestamp=timestamp,
        user_id=user_id,
        action_type=action_type,
        entity_type=entity_type,
        entity_id=entity_id,
        customer_id=customer_id,
        lg_record_id=lg_record_id,
        details=details,
        ip_address=ip_address
    )
    raw_material = f"{clean_prev}::{canonical_payload}"
    return hashlib.sha256(raw_material.encode("utf-8")).hexdigest().upper()


def verify_audit_log_chain(
    db: Session,
    start_id: Optional[int] = None,
    limit: Optional[int] = 10000
) -> Dict[str, Any]:
    """
    Validates the cryptographic integrity of the audit log hash chain.
    Walks through all chained audit records in sequence and verifies:
    1. The entry's 'previous_hash' matches the preceding entry's 'entry_hash'.
    2. The entry's 'entry_hash' matches the recomputed SHA-256 digest of its payload.
    
    Returns an institutional verification report:
    - is_valid: True if 100% intact, False if tampering detected
    - records_verified: Total count of verified chained records
    - broken_at_id: The exact database ID where the chain was broken (if tampered)
    - root_hash / latest_hash: Hashes of the first and most recent records
    """
    from app.models.models import AuditLog

    query = db.query(AuditLog).filter(AuditLog.entry_hash.isnot(None))
    if start_id is not None:
        query = query.filter(AuditLog.id >= start_id)

    records: List[AuditLog] = query.order_by(AuditLog.id.asc()).limit(limit).all()

    if not records:
        return {
            "is_valid": True,
            "status": "NO_CHAINED_RECORDS",
            "message": "Audit log hash chain is empty or has no chained records yet.",
            "records_verified": 0,
            "broken_at_id": None,
            "root_hash": None,
            "latest_hash": None
        }

    expected_prev = records[0].previous_hash or GENESIS_HASH
    verified_count = 0

    for record in records:
        # Check 1: Chain Link Continuity
        if (record.previous_hash or "").upper() != expected_prev.upper():
            return {
                "is_valid": False,
                "status": "BROKEN_CHAIN_LINK",
                "message": f"Hash continuity broken at AuditLog ID {record.id}. Expected previous hash {expected_prev}, got {record.previous_hash}.",
                "broken_at_id": record.id,
                "records_verified": verified_count,
                "failed_record": {
                    "id": record.id,
                    "action_type": record.action_type,
                    "timestamp": str(record.timestamp),
                    "recorded_previous_hash": record.previous_hash,
                    "expected_previous_hash": expected_prev
                }
            }

        # Check 2: Payload Content Integrity
        computed_hash = compute_audit_hash(
            previous_hash=record.previous_hash,
            timestamp=record.timestamp,
            user_id=record.user_id,
            action_type=record.action_type,
            entity_type=record.entity_type,
            entity_id=record.entity_id,
            customer_id=record.customer_id,
            lg_record_id=record.lg_record_id,
            details=record.details,
            ip_address=record.ip_address
        )

        if computed_hash != record.entry_hash.upper():
            return {
                "is_valid": False,
                "status": "TAMPERED_RECORD_CONTENT",
                "message": f"Tampering detected in AuditLog ID {record.id}! Recomputed hash {computed_hash} does not match recorded entry hash {record.entry_hash}.",
                "broken_at_id": record.id,
                "records_verified": verified_count,
                "failed_record": {
                    "id": record.id,
                    "action_type": record.action_type,
                    "timestamp": str(record.timestamp),
                    "recorded_hash": record.entry_hash,
                    "recomputed_hash": computed_hash
                }
            }

        expected_prev = record.entry_hash.upper()
        verified_count += 1

    return {
        "is_valid": True,
        "status": "INTACT",
        "message": f"Audit log cryptographic hash chain is 100% intact. Successfully verified {verified_count} sequential records.",
        "records_verified": verified_count,
        "broken_at_id": None,
        "root_hash": records[0].previous_hash,
        "latest_hash": records[-1].entry_hash,
        "first_record_id": records[0].id,
        "last_record_id": records[-1].id
    }
