# app/services/reconciliation_service.py
"""
LG Position Reconciliation Engine.

Pipeline:
  1. Upload → detect format → parse into normalized rows
  2. Column mapping: keyword match → cached per bank → AI fallback → manual
  3. Completeness check (count/total validation)
  4. Matching: positive (row ↔ IssuedLGRecord) + negative (system-only)
  5. Results → user resolution → corp admin approval for ADJUSTED → record update
"""

import logging
import io
import json
import re
from difflib import SequenceMatcher
from datetime import date, datetime
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_
from fastapi import HTTPException

from app.models.models_issuance import (
    ReconciliationSession, ReconciliationBankRow, ReconciliationResult,
    BankColumnMapping, IssuedLGRecord,
)
from app.models.models import Currency
from app.crud.crud import log_action

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Keyword → internal field map (case-insensitive matching)
COLUMN_KEYWORDS: Dict[str, List[str]] = {
    "bank_lg_number": [
        "lg number", "lg no", "lg#", "guarantee no", "guarantee number",
        "ref", "reference", "lg ref", "reference no", "lg reference",
        "رقم خطاب الضمان", "رقم الضمان",
    ],
    "beneficiary_name": [
        "beneficiary", "in favour of", "in favor of", "favour of",
        "المستفيد", "اسم المستفيد",
    ],
    "amount": [
        "amount", "value", "principal", "lg amount", "guarantee amount",
        "المبلغ", "القيمة",
    ],
    "currency_code": [
        "currency", "ccy", "curr", "العملة",
    ],
    "issue_date": [
        "issue date", "issuance date", "date of issue", "issued on",
        "تاريخ الإصدار",
    ],
    "expiry_date": [
        "expiry", "expiration", "maturity", "validity", "expiry date",
        "valid until", "expires", "تاريخ الانتهاء", "تاريخ الصلاحية",
    ],
}

LIVE_LG_STATUSES = [
    "ACTIVE", "LG_ISSUED", "DELIVERED_TO_BANK", "INTERNAL_PROCESSING",
]


class ReconciliationService:

    # ──────────────────────────────────────────────────
    # 1. CREATE SESSION
    # ──────────────────────────────────────────────────
    def create_session(
        self, db: Session, customer_id: int, bank_id: int,
        position_date: date, user_id: int,
        file_name: Optional[str] = None,
        file_format: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> ReconciliationSession:
        session = ReconciliationSession(
            customer_id=customer_id,
            bank_id=bank_id,
            position_date=position_date,
            original_file_name=file_name,
            file_format=file_format,
            status="CREATED",
            notes=notes,
            created_by_user_id=user_id,
        )
        db.add(session)
        db.commit()
        db.refresh(session)
        return session

    # ──────────────────────────────────────────────────
    # 2. PARSE FILE
    # ──────────────────────────────────────────────────
    async def parse_file(
        self, db: Session, session_id: int,
        file_bytes: bytes, file_name: str,
        customer_id: int, user_id: int,
    ) -> ReconciliationSession:
        """Parse uploaded file: detect format, map columns, create bank rows."""
        session = db.query(ReconciliationSession).filter(
            ReconciliationSession.id == session_id,
            ReconciliationSession.customer_id == customer_id,
        ).first()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        session.status = "PARSING"
        db.commit()

        try:
            ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""

            if ext in ("xlsx", "xls"):
                rows, method = self._parse_excel(db, file_bytes, session.bank_id, customer_id)
                session.file_format = "EXCEL"
            elif ext == "csv":
                rows, method = self._parse_csv(db, file_bytes, session.bank_id, customer_id)
                session.file_format = "CSV"
            elif ext == "pdf":
                rows, method = await self._parse_pdf(db, file_bytes, file_name, session.bank_id, customer_id, user_id)
                session.file_format = "PDF"
            elif ext in ("txt", "text"):
                rows, method = await self._parse_text(db, file_bytes, file_name, session.bank_id, customer_id, user_id)
                session.file_format = "TEXT"
            else:
                raise HTTPException(status_code=400,
                    detail=f"Unsupported file format: .{ext}. Upload Excel, CSV, PDF, or TXT.")

            session.parsing_method = method

            # Create bank rows
            for row_data in rows:
                bank_row = ReconciliationBankRow(
                    session_id=session.id,
                    bank_lg_number=self._clean_str(row_data.get("bank_lg_number")),
                    beneficiary_name=self._clean_str(row_data.get("beneficiary_name")),
                    amount=self._parse_amount(row_data.get("amount")),
                    currency_code=self._clean_str(row_data.get("currency_code")),
                    issue_date=self._parse_date(row_data.get("issue_date")),
                    expiry_date=self._parse_date(row_data.get("expiry_date")),
                    raw_data=row_data,
                )
                db.add(bank_row)

            session.total_bank_records = len(rows)
            session.status = "PARSED"
            db.commit()
            db.refresh(session)

            log_action(
                db, user_id=user_id,
                action_type="RECONCILIATION_FILE_PARSED",
                entity_type="ReconciliationSession",
                entity_id=session.id,
                details={
                    "file": file_name, "format": session.file_format,
                    "method": method, "rows": len(rows),
                },
                customer_id=customer_id
            )

            return session

        except HTTPException:
            raise
        except Exception as e:
            logger.exception(f"Reconciliation parse error: {e}")
            session.status = "FAILED"
            session.error_message = str(e)[:500]
            db.commit()
            raise HTTPException(status_code=500, detail=f"Parsing failed: {str(e)[:200]}")

    # ──────────────────────────────────────────────────
    # 3. RUN MATCHING
    # ──────────────────────────────────────────────────
    def run_matching(
        self, db: Session, session_id: int, customer_id: int, user_id: int,
    ) -> ReconciliationSession:
        """Positive + negative matching against live system LGs."""
        session = db.query(ReconciliationSession).filter(
            ReconciliationSession.id == session_id,
            ReconciliationSession.customer_id == customer_id,
        ).first()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        if session.status not in ("PARSED", "MATCHED"):
            raise HTTPException(status_code=400, detail="Session must be parsed before matching")

        session.status = "MATCHING"
        # Clear old results if re-matching
        db.query(ReconciliationResult).filter(
            ReconciliationResult.session_id == session.id
        ).delete()
        db.commit()

        # Load all bank rows
        bank_rows = db.query(ReconciliationBankRow).filter(
            ReconciliationBankRow.session_id == session.id
        ).all()

        # Load all live system LGs for this bank+customer (G1: eager-load currency)
        system_lgs = db.query(IssuedLGRecord).options(
            joinedload(IssuedLGRecord.currency)
        ).filter(
            IssuedLGRecord.customer_id == customer_id,
            IssuedLGRecord.bank_id == session.bank_id,
            IssuedLGRecord.status.in_(LIVE_LG_STATUSES),
        ).all()

        # Build lookup by bank_lg_number
        system_by_lg_number = {}
        for lg in system_lgs:
            if lg.bank_lg_number:
                normalised = lg.bank_lg_number.strip().upper()
                system_by_lg_number[normalised] = lg

        matched_system_ids = set()
        stats = {"matched": 0, "mismatched": 0, "bank_only": 0}

        # ── Positive matching ──
        for row in bank_rows:
            if not row.bank_lg_number:
                row.match_status = "BANK_ONLY"
                self._create_result(db, session.id, row.id, None,
                    "BANK_ONLY", "HIGH", "bank_lg_number",
                    "(no LG number)", None)
                stats["bank_only"] += 1
                continue

            key = row.bank_lg_number.strip().upper()
            lg = system_by_lg_number.get(key)

            if not lg:
                # Try fuzzy: strip non-alphanumeric
                key_clean = re.sub(r'[^A-Z0-9]', '', key)
                for sys_key, sys_lg in system_by_lg_number.items():
                    if re.sub(r'[^A-Z0-9]', '', sys_key) == key_clean:
                        lg = sys_lg
                        break

            if lg:
                matched_system_ids.add(lg.id)
                row.matched_lg_id = lg.id
                variances = self._compare_fields(row, lg)

                if variances:
                    row.match_status = "PARTIAL_MATCH"
                    row.variances = variances
                    for v in variances:
                        self._create_result(
                            db, session.id, row.id, lg.id,
                            v["type"], v["severity"], v["field"],
                            v["bank_value"], v["system_value"],
                        )
                    stats["mismatched"] += 1
                else:
                    row.match_status = "MATCHED"
                    stats["matched"] += 1
            else:
                row.match_status = "BANK_ONLY"
                self._create_result(db, session.id, row.id, None,
                    "BANK_ONLY", "HIGH", "bank_lg_number",
                    row.bank_lg_number, None)
                stats["bank_only"] += 1

            db.add(row)

        # ── Negative matching (system-only) ──
        system_only = 0
        for lg in system_lgs:
            if lg.id not in matched_system_ids:
                self._create_result(
                    db, session.id, None, lg.id,
                    "SYSTEM_ONLY", "MEDIUM", "presence",
                    None,
                    f"{lg.bank_lg_number or lg.lg_ref_number} — likely closed from beneficiary side",
                )
                system_only += 1

        # Update session stats
        session.matched_count = stats["matched"]
        session.mismatched_count = stats["mismatched"]
        session.bank_only_count = stats["bank_only"]
        session.system_only_count = system_only
        session.status = "MATCHED"

        # G3: Completeness check — store result on session for frontend visibility
        if session.bank_reported_count and session.bank_reported_count != session.total_bank_records:
            session.completeness_status = "COUNT_MISMATCH"
            session.completeness_note = (
                f"Bank reported {session.bank_reported_count} records "
                f"but parsed {session.total_bank_records}"
            )
            logger.warning(f"Reconciliation {session.id}: {session.completeness_note}")
        elif session.bank_reported_count:
            session.completeness_status = "OK"
            session.completeness_note = None
        else:
            session.completeness_status = "NOT_CHECKED"
            session.completeness_note = None

        db.commit()
        db.refresh(session)

        log_action(
            db, user_id=user_id,
            action_type="RECONCILIATION_MATCHING_COMPLETED",
            entity_type="ReconciliationSession",
            entity_id=session.id,
            details={
                "matched": stats["matched"], "mismatched": stats["mismatched"],
                "bank_only": stats["bank_only"], "system_only": system_only,
            },
            customer_id=customer_id
        )

        return session

    # ──────────────────────────────────────────────────
    # 4. RESOLVE RESULT
    # ──────────────────────────────────────────────────
    def resolve_result(
        self, db: Session, result_id: int,
        resolution: str, notes: Optional[str],
        user_id: int, customer_id: int,
    ) -> ReconciliationResult:
        """User resolves a mismatch: ADJUSTED, DISPUTE, or IGNORE."""
        result = db.query(ReconciliationResult).filter(
            ReconciliationResult.id == result_id,
        ).first()
        if not result:
            raise HTTPException(status_code=404, detail="Result not found")

        # Verify session belongs to customer
        session = db.query(ReconciliationSession).filter(
            ReconciliationSession.id == result.session_id,
            ReconciliationSession.customer_id == customer_id,
        ).first()
        if not session:
            raise HTTPException(status_code=403, detail="Not authorized")

        if resolution not in ("ADJUSTED", "DISPUTE", "IGNORE"):
            raise HTTPException(status_code=400, detail="Invalid resolution")

        result.user_resolution = resolution
        result.resolution_notes = notes
        result.resolved_by_user_id = user_id
        result.resolved_at = datetime.utcnow()

        if resolution == "ADJUSTED":
            # Phase 3 Governance: Context-Aware Auto-Generation
            converted_to_maintenance = False

            if result.issued_lg_id:
                from app.models.models_issuance import IssuedLGRecord
                lg = db.query(IssuedLGRecord).filter(
                    IssuedLGRecord.id == result.issued_lg_id
                ).first()

                if lg:
                    from app.services.issuance_maintenance_service import IssuanceMaintenanceService
                    maintenance_service = IssuanceMaintenanceService()
                    
                    # 1. EXPIRY DATE EXTENSION (Bank > System)
                    if result.mismatch_type == "EXPIRY" and result.field_name == "expiry_date":
                        try:
                            from datetime import date
                            bank_date = date.fromisoformat(result.bank_value) if result.bank_value and result.bank_value != "None" else None
                            sys_date = date.fromisoformat(result.system_value) if result.system_value and result.system_value != "None" else None
                            
                            if bank_date and sys_date and bank_date > sys_date:
                                maintenance_service.create_action(
                                    db=db,
                                    issued_lg_id=lg.id,
                                    action_type="EXTEND",
                                    action_data={"new_expiry_date": str(bank_date)},
                                    user_id=user_id,
                                    customer_id=customer_id,
                                    notes=f"Auto-generated from Reconciliation (Session {session.id}): Expiry extended from {sys_date} to {bank_date}.",
                                    initiation_source="INTERNAL_USER"
                                )
                                converted_to_maintenance = True
                        except Exception as e:
                            logger.error(f"Failed to auto-generate EXTEND maintenance action: {e}", exc_info=True)

                    # 2. AMOUNT DECREASE (System > Bank) => PARTIAL LIQUIDATION
                    elif result.mismatch_type == "AMOUNT" and result.field_name == "amount":
                        try:
                            from decimal import Decimal
                            bank_amt = Decimal(str(result.bank_value)) if result.bank_value and result.bank_value != "None" else Decimal(0)
                            sys_amt = Decimal(str(result.system_value)) if result.system_value and result.system_value != "None" else Decimal(0)
                            
                            if bank_amt > 0 and sys_amt > bank_amt:
                                reduction = sys_amt - bank_amt
                                maintenance_service.create_action(
                                    db=db,
                                    issued_lg_id=lg.id,
                                    action_type="LIQUIDATION",
                                    action_data={
                                        "liquidation_type": "PARTIAL",
                                        "liquidation_amount": str(reduction)
                                    },
                                    user_id=user_id,
                                    customer_id=customer_id,
                                    notes=f"Auto-generated from Reconciliation (Session {session.id}): Amount decreased from {sys_amt} to {bank_amt} (Reduction: {reduction}).",
                                    initiation_source="INTERNAL_USER"
                                )
                                converted_to_maintenance = True
                        except Exception as e:
                            logger.error(f"Failed to auto-generate LIQUIDATION maintenance action: {e}", exc_info=True)

            if converted_to_maintenance:
                result.approval_status = "CONVERTED_TO_MAINTENANCE"
                if not result.resolution_notes:
                    result.resolution_notes = ""
                result.resolution_notes += "\n\n[SYSTEM] Adjusted via auto-generated Maintenance Request."
            else:
                # Standard adjustment (Requires corporate admin approval before record update)
                result.approval_status = "PENDING_APPROVAL"
        else:
            result.approval_status = None
            result.record_updated = False

        db.add(result)
        db.commit()
        db.refresh(result)

        log_action(
            db, user_id=user_id,
            action_type=f"RECONCILIATION_RESULT_{resolution}",
            entity_type="ReconciliationResult",
            entity_id=result.id,
            details={
                "mismatch_type": result.mismatch_type,
                "field": result.field_name,
                "bank_value": result.bank_value,
                "system_value": result.system_value,
            },
            customer_id=customer_id,
        )

        return result

    # ──────────────────────────────────────────────────
    # 5. APPROVE ADJUSTED RESULT (Corp Admin)
    # ──────────────────────────────────────────────────
    def approve_adjustment(
        self, db: Session, result_id: int,
        admin_user_id: int, customer_id: int,
    ) -> ReconciliationResult:
        """Corporate admin approves the record update."""
        result = db.query(ReconciliationResult).filter(
            ReconciliationResult.id == result_id,
        ).first()
        if not result or result.approval_status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Not pending approval")

        session = db.query(ReconciliationSession).filter(
            ReconciliationSession.id == result.session_id,
            ReconciliationSession.customer_id == customer_id,
        ).first()
        if not session:
            raise HTTPException(status_code=403, detail="Not authorized")

        # Apply the change to IssuedLGRecord
        if result.issued_lg_id:
            lg = db.query(IssuedLGRecord).filter(
                IssuedLGRecord.id == result.issued_lg_id
            ).first()
            if lg:
                self._apply_adjustment(db, lg, result, admin_user_id)
                result.record_updated = True

        result.approval_status = "APPROVED"
        result.approved_by_user_id = admin_user_id
        result.approved_at = datetime.utcnow()

        db.add(result)
        db.commit()
        db.refresh(result)

        log_action(
            db, user_id=admin_user_id,
            action_type="RECONCILIATION_ADJUSTMENT_APPROVED",
            entity_type="ReconciliationResult",
            entity_id=result.id,
            details={"field": result.field_name, "new_value": result.bank_value},
            customer_id=customer_id,
        )

        return result

    # ──────────────────────────────────────────────────
    # 6. REJECT ADJUSTMENT (Corp Admin)
    # ──────────────────────────────────────────────────
    def reject_adjustment(
        self, db: Session, result_id: int,
        admin_user_id: int, customer_id: int,
        reason: Optional[str] = None,
    ) -> ReconciliationResult:
        result = db.query(ReconciliationResult).filter(
            ReconciliationResult.id == result_id,
        ).first()
        if not result or result.approval_status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Not pending approval")

        result.approval_status = "REJECTED"
        result.approved_by_user_id = admin_user_id
        result.approved_at = datetime.utcnow()
        result.user_resolution = None  # Revert — user must re-review
        result.resolution_notes = f"Rejected by admin: {reason or 'No reason given'}"
        result.record_updated = False

        db.add(result)
        db.commit()
        db.refresh(result)
        return result

    # ──────────────────────────────────────────────────
    # 7. COMPLETE SESSION
    # ──────────────────────────────────────────────────
    def complete_session(
        self, db: Session, session_id: int,
        user_id: int, customer_id: int,
    ) -> ReconciliationSession:
        session = db.query(ReconciliationSession).filter(
            ReconciliationSession.id == session_id,
            ReconciliationSession.customer_id == customer_id,
        ).first()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Check all results are resolved
        pending = db.query(ReconciliationResult).filter(
            ReconciliationResult.session_id == session.id,
            ReconciliationResult.user_resolution.is_(None),
        ).count()
        if pending > 0:
            raise HTTPException(status_code=400,
                detail=f"{pending} unresolved items remain. Resolve all before completing.")

        session.status = "COMPLETED"
        session.reviewed_by_user_id = user_id
        session.reviewed_at = datetime.utcnow()
        db.commit()
        db.refresh(session)
        return session

    # ══════════════════════════════════════════════════
    # PRIVATE: Parsing Helpers
    # ══════════════════════════════════════════════════

    def _parse_excel(self, db: Session, file_bytes: bytes, bank_id: int, customer_id: int
                     ) -> Tuple[List[Dict], str]:
        """Parse Excel file using openpyxl (no pandas requirement)."""
        import openpyxl

        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active

        all_rows = list(ws.iter_rows(values_only=True))
        if not all_rows:
            raise HTTPException(status_code=400, detail="Empty spreadsheet")

        best_col_map = {}
        best_method = ""
        best_headers = []
        header_row_idx = 0
        
        for idx, row in enumerate(all_rows[:20]):
            if not row or all(v is None or str(v).strip() == "" for v in row):
                continue
                
            temp_headers = [str(h).strip() if h is not None and str(h).strip() else f"col_{i}" for i, h in enumerate(row)]
            # Suppress mapping save during header search by running it without saving? 
            # Actually _map_columns saves if len >= 3. That's fine.
            temp_col_map, temp_method = self._map_columns(db, temp_headers, bank_id, customer_id)
            
            if len(temp_col_map) > len(best_col_map):
                best_col_map = temp_col_map
                best_method = temp_method
                best_headers = temp_headers
                header_row_idx = idx
                
            if len(best_col_map) >= 2:
                break
                
        if len(best_col_map) < 2:
            first_row = [str(h) for h in all_rows[0]] if all_rows else []
            raise HTTPException(status_code=400,
                detail=f"Could not map enough columns. Best attempt mapped: {best_col_map}. First row: {first_row}")

        headers = best_headers
        col_map = best_col_map
        method = best_method

        # Parse data rows
        rows = []
        for values in all_rows[header_row_idx + 1:]:
            if not values or all(v is None or str(v).strip() == "" for v in values):
                continue
            row_dict = {}
            raw = {}
            for i, val in enumerate(values):
                header = headers[i] if i < len(headers) else f"col_{i}"
                raw[header] = str(val).strip() if val is not None else None
                field = col_map.get(header)
                if field:
                    row_dict[field] = val
            row_dict["_raw"] = raw
            if row_dict.get("bank_lg_number") or row_dict.get("amount"):
                rows.append(row_dict)

        wb.close()
        return rows, method

    def _parse_csv(self, db: Session, file_bytes: bytes, bank_id: int, customer_id: int
                   ) -> Tuple[List[Dict], str]:
        """Parse CSV file."""
        import csv

        text = file_bytes.decode("utf-8-sig", errors="replace")
        reader = csv.reader(io.StringIO(text))
        all_rows = list(reader)
        if not all_rows:
            raise HTTPException(status_code=400, detail="Empty CSV file")

        best_col_map = {}
        best_method = ""
        best_headers = []
        header_row_idx = 0
        
        for idx, row in enumerate(all_rows[:20]):
            if not row or all(not v.strip() for v in row):
                continue
                
            temp_headers = [h.strip() if h.strip() else f"col_{i}" for i, h in enumerate(row)]
            temp_col_map, temp_method = self._map_columns(db, temp_headers, bank_id, customer_id)
            
            if len(temp_col_map) > len(best_col_map):
                best_col_map = temp_col_map
                best_method = temp_method
                best_headers = temp_headers
                header_row_idx = idx
                
            if len(best_col_map) >= 2:
                break
                
        if len(best_col_map) < 2:
            raise HTTPException(status_code=400,
                detail=f"Could not map enough columns. Best attempt mapped: {best_col_map}.")

        headers = best_headers
        col_map = best_col_map
        method = best_method

        rows = []
        for values in all_rows[header_row_idx + 1:]:
            if not values or all(not str(v).strip() for v in values):
                continue
            row_dict = {}
            raw = {}
            for i, val in enumerate(values):
                header = headers[i] if i < len(headers) else f"col_{i}"
                v_str = str(val).strip() if val else None
                raw[header] = v_str
                field = col_map.get(header)
                if field:
                    row_dict[field] = val if val else None
            row_dict["_raw"] = raw
            if row_dict.get("bank_lg_number") or row_dict.get("amount"):
                rows.append(row_dict)

        return rows, method

    async def _parse_pdf(self, db: Session, file_bytes: bytes, file_name: str,
                         bank_id: int, customer_id: int, user_id: int = None) -> Tuple[List[Dict], str]:
        """Parse PDF using Gemini AI extraction."""
        rows = await self._ai_extract_position_rows(file_bytes, file_name, db, customer_id, user_id)
        # Save mappings as AI-sourced (N/A for PDF — headers are implicit)
        return rows, "AI"

    async def _parse_text(self, db: Session, file_bytes: bytes, file_name: str,
                          bank_id: int, customer_id: int, user_id: int = None) -> Tuple[List[Dict], str]:
        """Parse text file using Gemini AI."""
        rows = await self._ai_extract_position_rows(file_bytes, file_name, db, customer_id, user_id)
        return rows, "AI"

    # ──────────────────────────────────────────────────
    # Column Mapping Engine
    # ──────────────────────────────────────────────────
    def _map_columns(self, db: Session, headers: List[str], bank_id: int, customer_id: int
                     ) -> Tuple[Dict[str, str], str]:
        """
        Map raw column headers to internal fields.
        Priority: 1) Cached mappings  2) Keyword match  3) AI (future)
        Returns: {source_header: internal_field}, method_used
        """
        # 1) Try cached mappings first
        cached = db.query(BankColumnMapping).filter(
            BankColumnMapping.bank_id == bank_id,
            BankColumnMapping.customer_id == customer_id,
        ).all()

        cached_map = {c.source_column.lower(): c.mapped_field for c in cached}

        col_map = {}
        mapped_fields = set()
        for header in headers:
            h_lower = header.lower().strip()
            if h_lower in cached_map and cached_map[h_lower] not in mapped_fields:
                col_map[header] = cached_map[h_lower]
                mapped_fields.add(cached_map[h_lower])

        if len(col_map) >= 3:
            return col_map, "CACHED"

        # 2) Keyword matching
        for header in headers:
            if header in col_map:
                continue
            h_lower = header.lower().strip()
            for field, keywords in COLUMN_KEYWORDS.items():
                if field in mapped_fields:
                    continue
                for kw in keywords:
                    if kw in h_lower or h_lower in kw:
                        col_map[header] = field
                        mapped_fields.add(field)
                        break
                if header in col_map:
                    break

        method = "AUTO" if len(col_map) >= 3 else "PARTIAL"

        # Save successful mappings for reuse
        if len(col_map) >= 3:
            self._save_column_mappings(db, bank_id, customer_id, col_map, method)

        return col_map, method

    def _save_column_mappings(self, db: Session, bank_id: int, customer_id: int,
                               col_map: Dict[str, str], source: str):
        """Persist column mappings for this bank+customer for future reuse."""
        for source_col, mapped_field in col_map.items():
            existing = db.query(BankColumnMapping).filter(
                BankColumnMapping.bank_id == bank_id,
                BankColumnMapping.customer_id == customer_id,
                BankColumnMapping.source_column == source_col,
            ).first()
            if existing:
                existing.mapped_field = mapped_field
                existing.mapping_source = source
                existing.updated_at = datetime.utcnow()
            else:
                db.add(BankColumnMapping(
                    bank_id=bank_id,
                    customer_id=customer_id,
                    source_column=source_col,
                    mapped_field=mapped_field,
                    mapping_source=source,
                    confidence=Decimal("0.95") if source == "AUTO" else Decimal("0.80"),
                ))
        db.flush()

    # ──────────────────────────────────────────────────
    # AI Extraction (PDF/Text)
    # ──────────────────────────────────────────────────
    async def _ai_extract_position_rows(self, file_bytes: bytes, file_name: str,
                                         db: Session = None, customer_id: int = None,
                                         user_id: int = None) -> List[Dict]:
        """Use Gemini AI to extract LG position rows from unstructured content."""
        try:
            from app.core.ai_integration import _get_gemini_model, log_ai_usage_sync
            import fitz  # PyMuPDF

            model = _get_gemini_model()
            if not model:
                raise HTTPException(status_code=503,
                    detail="AI model not available. Upload Excel/CSV instead.")

            # Extract text from PDF or read text directly
            if file_name.lower().endswith(".pdf"):
                doc = fitz.open(stream=file_bytes, filetype="pdf")
                text = "\n".join(page.get_text() for page in doc)
                total_pages = len(doc)
                doc.close()
            else:
                text = file_bytes.decode("utf-8-sig", errors="replace")
                total_pages = 1

            if len(text) < 50:
                raise HTTPException(status_code=400,
                    detail="File appears empty or unreadable.")

            # Truncate if too long
            if len(text) > 80000:
                text = text[:80000]

            prompt = f"""You are a banking document analyst. This document is an LG (Letter of Guarantee) 
position report from a bank. It lists all currently valid/active LGs.

Extract every LG entry as a JSON array. For each LG, extract:
- "bank_lg_number": The bank's reference/guarantee number (string)
- "beneficiary_name": Name of the beneficiary (string)
- "amount": The LG amount as a plain number (no commas, no currency symbols)
- "currency_code": 3-letter ISO currency code (e.g. "EGP", "USD")
- "issue_date": Issuance date in YYYY-MM-DD format (or null)
- "expiry_date": Expiry/maturity date in YYYY-MM-DD format (or null)

Rules:
- Return ONLY a valid JSON array. No explanations.
- If a field cannot be determined, use null.
- Dates must be YYYY-MM-DD format.
- Amount must be a plain number (remove commas, currency symbols).
- Include ALL LG entries found in the document.
- Skip any summary/total rows.

Document text:
{text}"""

            response = model.generate_content(prompt)
            response_text = response.text.strip()

            # H3: Log AI usage
            try:
                usage_meta = getattr(response, 'usage_metadata', None)
                if usage_meta and db is not None and customer_id is not None:
                    log_ai_usage_sync(
                        db, customer_id, user_id or 0, file_name,
                        prompt_tokens=getattr(usage_meta, 'prompt_token_count', 0),
                        completion_tokens=getattr(usage_meta, 'candidates_token_count', 0),
                        total_pages=total_pages,
                        call_type="reconciliation_pdf_parsing",
                    )
            except Exception as log_err:
                logger.warning(f"Failed to log reconciliation AI usage: {log_err}")

            # Clean markdown fences
            if response_text.startswith("```"):
                response_text = re.sub(r'^```(?:json)?\s*', '', response_text)
                response_text = re.sub(r'\s*```$', '', response_text)

            rows = json.loads(response_text)
            if not isinstance(rows, list):
                rows = [rows]

            return rows

        except json.JSONDecodeError as e:
            logger.error(f"AI returned invalid JSON: {e}")
            raise HTTPException(status_code=422,
                detail="AI could not parse the document into structured data. Try Excel/CSV instead.")
        except ImportError:
            raise HTTPException(status_code=503,
                detail="AI dependencies not available. Upload Excel/CSV.")
        except Exception as e:
            logger.exception(f"AI extraction failed: {e}")
            raise HTTPException(status_code=500,
                detail=f"AI extraction error: {str(e)[:200]}")

    # ══════════════════════════════════════════════════
    # PRIVATE: Matching Helpers
    # ══════════════════════════════════════════════════

    def _compare_fields(self, row: ReconciliationBankRow, lg: IssuedLGRecord) -> List[Dict]:
        """Compare bank row against system record, return list of variances."""
        variances = []

        # G4: Amount comparison with epsilon tolerance (0.01)
        if row.amount is not None and lg.current_amount is not None:
            bank_amt = Decimal(str(row.amount))
            sys_amt = Decimal(str(lg.current_amount))
            if abs(bank_amt - sys_amt) > Decimal("0.01"):
                variances.append({
                    "type": "AMOUNT", "severity": "HIGH", "field": "amount",
                    "bank_value": str(bank_amt), "system_value": str(sys_amt),
                })

        # G1: Currency comparison
        if row.currency_code and lg.currency:
            bank_ccy = row.currency_code.strip().upper()
            sys_ccy = (lg.currency.iso_code or "").strip().upper()
            if bank_ccy and sys_ccy and bank_ccy != sys_ccy:
                variances.append({
                    "type": "CURRENCY", "severity": "HIGH", "field": "currency_code",
                    "bank_value": bank_ccy, "system_value": sys_ccy,
                })

        # Expiry date comparison
        if row.expiry_date and lg.expiry_date:
            if row.expiry_date != lg.expiry_date:
                variances.append({
                    "type": "EXPIRY", "severity": "LOW", "field": "expiry_date",
                    "bank_value": str(row.expiry_date), "system_value": str(lg.expiry_date),
                })

        # Issue date comparison
        bank_issue = row.issue_date
        sys_issue = lg.bank_lg_issue_date or lg.issue_date
        if bank_issue and sys_issue:
            if bank_issue != sys_issue:
                variances.append({
                    "type": "INITIAL_DATA", "severity": "INFO", "field": "issue_date",
                    "bank_value": str(bank_issue), "system_value": str(sys_issue),
                })

        # G2: Beneficiary name fuzzy comparison (85% threshold)
        if row.beneficiary_name and lg.beneficiary_name:
            bank_ben = row.beneficiary_name.strip()
            sys_ben = lg.beneficiary_name.strip()
            if bank_ben and sys_ben:
                ratio = SequenceMatcher(None, bank_ben.lower(), sys_ben.lower()).ratio()
                if ratio < 0.85:
                    variances.append({
                        "type": "BENEFICIARY", "severity": "MEDIUM",
                        "field": "beneficiary_name",
                        "bank_value": bank_ben,
                        "system_value": f"{sys_ben} (match: {ratio:.0%})",
                    })

        return variances

    def _create_result(self, db: Session, session_id: int,
                       bank_row_id: Optional[int], issued_lg_id: Optional[int],
                       mismatch_type: str, severity: str, field_name: str,
                       bank_value: Optional[str], system_value: Optional[str]):
        db.add(ReconciliationResult(
            session_id=session_id,
            bank_row_id=bank_row_id,
            issued_lg_id=issued_lg_id,
            mismatch_type=mismatch_type,
            severity=severity,
            field_name=field_name,
            bank_value=bank_value,
            system_value=system_value,
        ))

    def _apply_adjustment(self, db: Session, lg: IssuedLGRecord,
                           result: ReconciliationResult, admin_user_id: int):
        """Apply the bank's value to the system record after admin approval."""
        before = {
            "status": lg.status,
            "current_amount": str(lg.current_amount) if lg.current_amount else None,
            "expiry_date": str(lg.expiry_date) if lg.expiry_date else None,
        }

        if result.field_name == "amount" and result.bank_value:
            try:
                lg.current_amount = Decimal(result.bank_value)
            except (ValueError, InvalidOperation):
                pass

        elif result.field_name == "expiry_date" and result.bank_value:
            try:
                lg.expiry_date = date.fromisoformat(result.bank_value)
            except ValueError:
                pass

        elif result.field_name == "issue_date" and result.bank_value:
            try:
                lg.bank_lg_issue_date = date.fromisoformat(result.bank_value)
            except ValueError:
                pass

        # Record in action history
        after = {
            "status": lg.status,
            "current_amount": str(lg.current_amount) if lg.current_amount else None,
            "expiry_date": str(lg.expiry_date) if lg.expiry_date else None,
        }
        history = lg.action_history or []
        history.append({
            "action_type": "RECONCILIATION_ADJUSTMENT",
            "field": result.field_name,
            "before": before,
            "after": after,
            "user_id": admin_user_id,
            "timestamp": str(datetime.utcnow()),
            "notes": f"Adjusted via reconciliation (result #{result.id})",
        })
        lg.action_history = list(history)
        db.add(lg)

    # ══════════════════════════════════════════════════
    # PRIVATE: Data Normalization
    # ══════════════════════════════════════════════════

    def _clean_str(self, val) -> Optional[str]:
        if val is None:
            return None
        s = str(val).strip()
        return s if s and s.lower() not in ("none", "nan", "null", "") else None

    def _parse_amount(self, val) -> Optional[Decimal]:
        if val is None:
            return None
        try:
            s = str(val).strip()
            s = re.sub(r'[^\d.\-]', '', s)  # Remove commas, currency symbols
            return Decimal(s) if s else None
        except (InvalidOperation, ValueError):
            return None

    def _parse_date(self, val) -> Optional[date]:
        if val is None:
            return None
        if isinstance(val, date):
            return val
        if isinstance(val, datetime):
            return val.date()
        s = str(val).strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y",
                     "%d.%m.%Y", "%Y%m%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    # ══════════════════════════════════════════════════
    # 3.3 HEADER DRIFT DETECTION
    # ══════════════════════════════════════════════════

    def detect_header_drift(
        self, db: Session, bank_id: int, customer_id: int, headers: List[str]
    ) -> Dict[str, Any]:
        """
        Compare new file column headers against cached BankColumnMapping.
        Returns drift info: new columns, missing cached columns, and mapping coverage.
        """
        # Load cached mappings
        cached = db.query(BankColumnMapping).filter(
            BankColumnMapping.bank_id == bank_id,
            BankColumnMapping.customer_id == customer_id,
        ).all()

        if not cached:
            return {
                "has_drift": False,
                "has_cached_mapping": False,
                "message": "No cached mapping found for this bank. First-time upload — headers will be auto-mapped.",
                "new_columns": [],
                "missing_columns": [],
                "cached_columns": [],
                "current_headers": headers,
            }

        cached_sources = {c.source_column.lower(): c.mapped_field for c in cached}
        current_lower = {h.lower().strip() for h in headers}

        new_columns = [h for h in headers if h.lower().strip() not in cached_sources]
        missing_columns = [
            {"column": c.source_column, "mapped_to": c.mapped_field}
            for c in cached
            if c.source_column.lower() not in current_lower
        ]

        has_drift = bool(new_columns or missing_columns)

        return {
            "has_drift": has_drift,
            "has_cached_mapping": True,
            "message": (
                f"Column drift detected: {len(new_columns)} new, {len(missing_columns)} missing. "
                "Consider re-analyzing the mapping."
                if has_drift else "No drift detected. Headers match cached mapping."
            ),
            "new_columns": new_columns,
            "missing_columns": missing_columns,
            "cached_columns": [
                {"column": c.source_column, "mapped_to": c.mapped_field}
                for c in cached
            ],
            "current_headers": headers,
        }

    def re_analyze_mapping(
        self, db: Session, bank_id: int, customer_id: int, headers: List[str]
    ) -> Dict[str, str]:
        """
        Clear cached mapping and re-run keyword mapping on new headers.
        Returns the new column map.
        """
        # Delete old cached mappings
        db.query(BankColumnMapping).filter(
            BankColumnMapping.bank_id == bank_id,
            BankColumnMapping.customer_id == customer_id,
        ).delete()
        db.flush()

        # Re-run keyword mapping (bypasses the cache since we just cleared it)
        col_map, method = self._map_columns(db, headers, bank_id, customer_id)
        db.commit()

        return {
            "mapping": col_map,
            "method": method,
            "mapped_count": len(col_map),
            "total_headers": len(headers),
        }


# Singleton
reconciliation_service = ReconciliationService()
