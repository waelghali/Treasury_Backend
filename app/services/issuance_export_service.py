import io
import json
from datetime import date, datetime, timezone
from typing import List, Dict, Any, Optional

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from sqlalchemy.orm import Session

from app.models.models import User, Bank, Currency, Customer, CustomerEntity
from app.models.models_issuance import (
    IssuedLGRecord,
    IssuanceRequest,
    IssuanceMaintenanceAction,
    IssuanceFacility,
    IssuanceFacilitySubLimit,
)

# ==============================================================================
# STYLE PALETTES & THEMES (Enterprise Financial Grade)
# ==============================================================================
COLOR_PRIMARY_NAVY = "1E3A8A"      # Deep Royal Navy
COLOR_SECONDARY_SLATE = "334155"   # Slate 700
COLOR_HEADER_TEXT = "FFFFFF"       # White
COLOR_ZEBRA_ROW = "F8FAFC"         # Soft slate 50
COLOR_BORDER = "CBD5E1"            # Slate 300
COLOR_KPI_BG = "F8FAFC"            # Slate 50
COLOR_KPI_BORDER = "94A3B8"        # Slate 400
COLOR_TEXT_MAIN = "0F172A"         # Slate 900
COLOR_TEXT_MUTED = "64748B"        # Slate 500
COLOR_TOTAL_ROW = "E2E8F0"         # Slate 200

FONT_NAME = "Segoe UI"

FONT_COMPANY = Font(name=FONT_NAME, size=15, bold=True, color=COLOR_PRIMARY_NAVY)
FONT_REPORT_TITLE = Font(name=FONT_NAME, size=12, bold=True, color=COLOR_SECONDARY_SLATE)
FONT_METADATA = Font(name=FONT_NAME, size=9, color=COLOR_TEXT_MUTED)
FONT_KPI_LABEL = Font(name=FONT_NAME, size=8, bold=True, color=COLOR_TEXT_MUTED)
FONT_KPI_VAL = Font(name=FONT_NAME, size=10.5, bold=True, color=COLOR_PRIMARY_NAVY)
FONT_HEADER = Font(name=FONT_NAME, size=10, bold=True, color=COLOR_HEADER_TEXT)
FONT_DATA = Font(name=FONT_NAME, size=9.5, color=COLOR_TEXT_MAIN)
FONT_DATA_BOLD = Font(name=FONT_NAME, size=9.5, bold=True, color=COLOR_TEXT_MAIN)
FONT_TOTAL = Font(name=FONT_NAME, size=10, bold=True, color=COLOR_PRIMARY_NAVY)

FILL_HEADER = PatternFill(start_color=COLOR_PRIMARY_NAVY, end_color=COLOR_PRIMARY_NAVY, fill_type="solid")
FILL_HEADER_ALT = PatternFill(start_color=COLOR_SECONDARY_SLATE, end_color=COLOR_SECONDARY_SLATE, fill_type="solid")
FILL_ZEBRA = PatternFill(start_color=COLOR_ZEBRA_ROW, end_color=COLOR_ZEBRA_ROW, fill_type="solid")
FILL_WHITE = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
FILL_KPI = PatternFill(start_color=COLOR_KPI_BG, end_color=COLOR_KPI_BG, fill_type="solid")
FILL_TOTAL = PatternFill(start_color=COLOR_TOTAL_ROW, end_color=COLOR_TOTAL_ROW, fill_type="solid")

BORDER_THIN = Border(
    left=Side(style="thin", color=COLOR_BORDER),
    right=Side(style="thin", color=COLOR_BORDER),
    top=Side(style="thin", color=COLOR_BORDER),
    bottom=Side(style="thin", color=COLOR_BORDER),
)

BORDER_TOTAL = Border(
    left=Side(style="thin", color=COLOR_BORDER),
    right=Side(style="thin", color=COLOR_BORDER),
    top=Side(style="thin", color=COLOR_BORDER),
    bottom=Side(style="double", color=COLOR_PRIMARY_NAVY),
)

BORDER_KPI = Border(
    left=Side(style="thin", color=COLOR_KPI_BORDER),
    right=Side(style="thin", color=COLOR_KPI_BORDER),
    top=Side(style="thin", color=COLOR_KPI_BORDER),
    bottom=Side(style="thin", color=COLOR_KPI_BORDER),
)

ALIGN_LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_RIGHT = Alignment(horizontal="right", vertical="center")
ALIGN_HEADER = Alignment(horizontal="center", vertical="center", wrap_text=True)

# ==============================================================================
# MASTER COLUMN REGISTRY (Select & Arrange)
# ==============================================================================
COLUMN_REGISTRY: Dict[str, Dict[str, Any]] = {
    # 1. Identification
    "internal_serial": {"label": "Internal Serial", "category": "Identification", "type": "string"},
    "lg_ref_number": {"label": "Customer LG Ref", "category": "Identification", "type": "string"},
    "bank_lg_number": {"label": "Bank LG Number", "category": "Identification", "type": "string"},
    "status": {"label": "Current Status", "category": "Identification", "type": "status"},
    "days_remaining": {"label": "Days Remaining", "category": "Identification", "type": "string"},

    # 2. Financials
    "current_amount": {"label": "Current Amount", "category": "Financials", "type": "currency"},
    "currency": {"label": "Currency", "category": "Financials", "type": "code"},
    "initial_amount": {"label": "Initial Requested Amount", "category": "Financials", "type": "currency"},
    "net_amount_change": {"label": "Net Amount Change", "category": "Financials", "type": "currency"},

    # 3. Key Dates
    "issue_date": {"label": "Bank Issue Date", "category": "Key Dates", "type": "date"},
    "expiry_date": {"label": "Current Expiry Date", "category": "Key Dates", "type": "date"},
    "original_expiry_date": {"label": "Original Expiry Date", "category": "Key Dates", "type": "date"},
    "request_date": {"label": "Request Date", "category": "Key Dates", "type": "date"},

    # 4. Parties & Facilities
    "bank_name": {"label": "Issuing Bank", "category": "Parties & Facilities", "type": "string"},
    "facility_name": {"label": "Credit Facility & Sub-Limit", "category": "Parties & Facilities", "type": "string"},
    "beneficiary_name": {"label": "Beneficiary Name", "category": "Parties & Facilities", "type": "string"},
    "beneficiary_country": {"label": "Beneficiary Country", "category": "Parties & Facilities", "type": "string"},
    "beneficiary_address": {"label": "Beneficiary Address", "category": "Parties & Facilities", "type": "string"},
    "issuing_entity": {"label": "Issuing Entity", "category": "Parties & Facilities", "type": "string"},

    # 5. Origination & Request
    "origination_source": {"label": "Origination Source", "category": "Origination & Request", "type": "string"},
    "request_serial": {"label": "Request Serial #", "category": "Origination & Request", "type": "string"},
    "requestor_name": {"label": "Requestor Name", "category": "Origination & Request", "type": "string"},
    "requestor_email": {"label": "Requestor Email", "category": "Origination & Request", "type": "string"},
    "department": {"label": "Department", "category": "Origination & Request", "type": "string"},
    "job_title": {"label": "Job Title", "category": "Origination & Request", "type": "string"},
    "lg_type": {"label": "LG Type", "category": "Origination & Request", "type": "string"},
    "lg_purpose": {"label": "LG Purpose / Description", "category": "Origination & Request", "type": "string"},
    "reference_type": {"label": "Underlying Contract Type", "category": "Origination & Request", "type": "string"},
    "reference_number": {"label": "Underlying Contract / PO #", "category": "Origination & Request", "type": "string"},
    "reference_amount": {"label": "Contract Ref Amount", "category": "Origination & Request", "type": "currency"},
    "reference_validity": {"label": "Reference Validity Status", "category": "Origination & Request", "type": "string"},

    # 6. Approvals & Governance (CLEAN single values)
    "approval_status": {"label": "Approval Status", "category": "Governance", "type": "string"},
    "approved_by": {"label": "Approved By", "category": "Governance", "type": "string"},
    "approval_date": {"label": "Approval Date", "category": "Governance", "type": "date"},

    # 7. Issuance & Logistics
    "issuance_method": {"label": "Issuance Method", "category": "Issuance & Logistics", "type": "string"},
    "issued_by": {"label": "Issued By (Treasury Officer)", "category": "Issuance & Logistics", "type": "string"},
    "execution_date": {"label": "Execution Date", "category": "Issuance & Logistics", "type": "date"},
    "delivery_date": {"label": "Delivery to Bank Date", "category": "Issuance & Logistics", "type": "date"},
    "delivery_method": {"label": "Delivery Method", "category": "Issuance & Logistics", "type": "string"},
    "delivery_notes": {"label": "Delivery Notes", "category": "Issuance & Logistics", "type": "string"},

    # 8. Confirmation & Verification
    "bank_reply_date": {"label": "Bank Reply Date", "category": "Verification & Custody", "type": "date"},
    "bank_confirmed_amount": {"label": "Bank Confirmed Amount", "category": "Verification & Custody", "type": "currency"},
    "verification_status": {"label": "Verification Status", "category": "Verification & Custody", "type": "string"},
    "verified_by": {"label": "Verified By", "category": "Verification & Custody", "type": "string"},
    "handover_date": {"label": "Handover to Beneficiary Date", "category": "Verification & Custody", "type": "date"},
    "recipient_name": {"label": "Recipient Name", "category": "Verification & Custody", "type": "string"},
    "custody_holder": {"label": "Custody Holder", "category": "Verification & Custody", "type": "string"},

    # 9. Maintenance & Lifecycle (CLEAN single values)
    "maint_count": {"label": "Maintenance Actions Count", "category": "Maintenance", "type": "integer"},
    "ext_count": {"label": "Extensions Count", "category": "Maintenance", "type": "integer"},
    "latest_maint_type": {"label": "Latest Action Type", "category": "Maintenance", "type": "string"},
    "latest_maint_date": {"label": "Latest Action Date", "category": "Maintenance", "type": "date"},
    "latest_maint_status": {"label": "Latest Action Status", "category": "Maintenance", "type": "string"},
}

PRESET_SUMMARY_COLS = [
    "internal_serial", "lg_ref_number", "status", "current_amount", "currency",
    "expiry_date", "days_remaining", "bank_name", "beneficiary_name", "lg_type", "department"
]

PRESET_FINANCIAL_COLS = [
    "internal_serial", "lg_ref_number", "bank_lg_number", "status", "current_amount", "currency",
    "initial_amount", "net_amount_change", "bank_name", "facility_name", "issue_date", "expiry_date", "days_remaining"
]

PRESET_OPERATIONS_COLS = [
    "internal_serial", "lg_ref_number", "bank_lg_number", "status", "bank_name", "beneficiary_name",
    "issuance_method", "issued_by", "delivery_date", "delivery_method", "handover_date", "recipient_name", "custody_holder"
]

PRESET_DETAILED_COLS = [
    "internal_serial", "lg_ref_number", "bank_lg_number", "status", "days_remaining",
    "current_amount", "currency", "initial_amount", "net_amount_change",
    "origination_source", "request_serial", "request_date", "requestor_name", "department",
    "issuing_entity", "lg_type", "lg_purpose",
    "reference_number", "reference_amount", "reference_validity",
    "approval_status", "approved_by", "approval_date",
    "beneficiary_name", "beneficiary_country", "bank_name", "facility_name",
    "issuance_method", "issued_by", "execution_date", "delivery_date", "delivery_method",
    "bank_reply_date", "issue_date", "expiry_date", "bank_confirmed_amount",
    "verification_status", "verified_by", "handover_date", "recipient_name", "custody_holder",
    "maint_count", "ext_count", "latest_maint_type", "latest_maint_date", "latest_maint_status"
]

# Lookup Maps
STATUS_LABELS: Dict[str, str] = {
    "ACTIVE": "Active",
    "INTERNAL_PROCESSING": "Internal Processing",
    "DELIVERED_TO_BANK": "Delivered to Bank",
    "BANK_INQUIRY": "Bank Inquiry",
    "BANK_REJECTED": "Rejected by Bank",
    "LG_ISSUED": "LG Issued by Bank",
    "EXPIRED": "Expired",
    "CANCELLED": "Cancelled",
    "PENDING_CLOSE": "Pending Close",
    "CLOSED": "Closed / Returned",
    "LIQUIDATED": "Liquidated",
    "SLA_EXCEEDED": "SLA Breach",
}

METHOD_LABELS: Dict[str, str] = {
    "BANK_FORM": "Pre-Printed Bank Form",
    "MANUAL_PDF": "Official Treasury Letter",
    "BANK_API": "Electronic Bank API",
    "PRE_PRINTED_FORM": "Pre-Printed Bank Form",
}

DELIVERY_LABELS: Dict[str, str] = {
    "HAND_DELIVERY": "Hand Delivery",
    "COURIER": "Courier Service",
    "EMAIL": "Secure Email / Portal",
    "OTHER": "Other",
}

VERIFICATION_LABELS: Dict[str, str] = {
    "MATCHED": "Verified & Matched",
    "DISCREPANCY": "Discrepancy Reported",
    "PENDING": "Pending Verification",
    "ACCEPTED": "Accepted with Discrepancy",
}


def _fmt_date(val: Any) -> str:
    if not val:
        return ""
    if isinstance(val, (datetime, date)):
        return val.strftime("%Y-%m-%d")
    s = str(val).split("T")[0].split(" ")[0]
    return s if s and s != "None" else ""


def _fmt_datetime(val: Any) -> str:
    if not val:
        return ""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")
    s = str(val).replace("T", " ")
    return s[:16] if s and s != "None" else ""


def _calculate_days_remaining(r: IssuedLGRecord, today: date) -> str:
    if r.is_open_ended:
        return "Open-Ended"
    if not r.expiry_date:
        return "Undated"
    delta = (r.expiry_date - today).days
    if delta > 0:
        return f"{delta} days remaining"
    elif delta == 0:
        return "Expires Today"
    else:
        return f"Expired ({abs(delta)} days ago)"


def _resolve_user(db: Session, user_id: Optional[int], cache: Dict[int, str]) -> str:
    if not user_id:
        return ""
    if user_id in cache:
        return cache[user_id]
    u = db.query(User).filter(User.id == user_id).first()
    display = u.email if u else f"User #{user_id}"
    cache[user_id] = display
    return display


# ==============================================================================
# MAIN EXPORT SERVICE
# ==============================================================================
class IssuanceExportService:

    @staticmethod
    def get_available_columns() -> List[Dict[str, Any]]:
        """Returns the full list of available columns categorized for the UI modal."""
        result = []
        for key, meta in COLUMN_REGISTRY.items():
            result.append({
                "id": key,
                "label": meta["label"],
                "category": meta["category"],
                "default": key in PRESET_SUMMARY_COLS,
            })
        return result

    @staticmethod
    def generate_excel_export(
        db: Session,
        customer_id: int,
        user_email: str,
        export_type: str = "summary",
        status_filter: Optional[str] = None,
        search: Optional[str] = None,
        custom_columns: Optional[List[str]] = None,
        include_event_log: bool = False,
        include_approvals: bool = False,
    ) -> io.BytesIO:
        """
        Builds a financial executive-grade Excel workbook for Issued LGs.
        Supports presets (summary, detailed, full_audit) or dynamic custom columns.
        """
        today = date.today()
        user_cache: Dict[int, str] = {}

        # 1. Fetch Customer info
        cust = db.query(Customer).filter(Customer.id == customer_id).first()
        company_name = cust.name if cust else "Corporate Treasury"

        # 2. Fetch Issued LG records
        query = db.query(IssuedLGRecord).filter(IssuedLGRecord.customer_id == customer_id)
        if status_filter and status_filter.upper() != "ALL":
            query = query.filter(IssuedLGRecord.status == status_filter.upper())
        records: List[IssuedLGRecord] = query.order_by(IssuedLGRecord.created_at.desc()).all()

        if search:
            s = search.lower()
            records = [
                r for r in records
                if (r.lg_ref_number and s in r.lg_ref_number.lower())
                or (r.beneficiary_name and s in r.beneficiary_name.lower())
                or (r.internal_serial and s in r.internal_serial.lower())
                or (r.bank_lg_number and s in r.bank_lg_number.lower())
            ]

        # 3. Determine active columns and title
        if custom_columns and len(custom_columns) > 0:
            active_cols = [c for c in custom_columns if c in COLUMN_REGISTRY]
            report_title = "Issued Letters of Guarantee - Custom Position Report"
            sheet_title = "Custom Position Register"
        elif export_type == "summary":
            active_cols = PRESET_SUMMARY_COLS
            report_title = "Issued Letters of Guarantee - Executive Summary"
            sheet_title = "Executive Summary"
        elif export_type == "detailed":
            active_cols = PRESET_DETAILED_COLS
            report_title = "Issued Letters of Guarantee - Detailed Position Register"
            sheet_title = "Position Register"
        else:  # full_audit
            active_cols = PRESET_DETAILED_COLS
            report_title = "Issued Letters of Guarantee - Master Audit Register"
            sheet_title = "Master LG Register"
            include_event_log = True
            include_approvals = True

        # 4. Create Workbook
        wb = openpyxl.Workbook()
        wb.remove(wb.active)  # Remove default blank sheet

        # Sheet 1: Master / Position Register
        IssuanceExportService._build_register_sheet(
            wb=wb,
            db=db,
            records=records,
            active_cols=active_cols,
            company_name=company_name,
            report_title=report_title,
            sheet_title=sheet_title,
            user_email=user_email,
            status_filter=status_filter,
            today=today,
            user_cache=user_cache,
        )

        # Sheet 2: Lifecycle Event Log (if requested)
        if include_event_log:
            IssuanceExportService._build_lifecycle_ledger_sheet(
                wb=wb, db=db, records=records, company_name=company_name,
                user_email=user_email, today=today, user_cache=user_cache
            )

        # Sheet 3: Approvals Audit Trail (if requested)
        if include_approvals:
            IssuanceExportService._build_approvals_sheet(
                wb=wb, db=db, records=records, company_name=company_name,
                user_email=user_email, today=today, user_cache=user_cache
            )

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return buf

    # --------------------------------------------------------------------------
    # SHEET BUILDER: Register (Master / Detailed / Custom / Summary)
    # --------------------------------------------------------------------------
    @staticmethod
    def _build_register_sheet(
        wb, db, records, active_cols, company_name, report_title, sheet_title,
        user_email, status_filter, today, user_cache
    ):
        ws = wb.create_sheet(title=sheet_title)
        ws.views.sheetView[0].showGridLines = True

        headers = [COLUMN_REGISTRY[col_id]["label"] for col_id in active_cols]

        # Header & KPI block
        start_row = IssuanceExportService._write_title_and_kpis(
            ws, company_name, report_title, user_email, status_filter, records, len(headers), today
        )

        # Table Header
        header_row = start_row
        ws.row_dimensions[header_row].height = 28
        for col_idx, h in enumerate(headers, 1):
            cell = ws.cell(row=header_row, column=col_idx, value=h)
            cell.font = FONT_HEADER
            cell.fill = FILL_HEADER
            cell.alignment = ALIGN_HEADER
            cell.border = BORDER_THIN

        # Data Rows
        row_idx = header_row + 1
        for i, r in enumerate(records):
            ws.row_dimensions[row_idx].height = 21
            fill = FILL_ZEBRA if i % 2 == 1 else FILL_WHITE

            # Linked models
            req = db.query(IssuanceRequest).filter(IssuanceRequest.id == r.request_id).first() if r.request_id else None
            maints = db.query(IssuanceMaintenanceAction).filter(
                IssuanceMaintenanceAction.issued_lg_id == r.id
            ).order_by(IssuanceMaintenanceAction.id.asc()).all()

            # Pre-extract values for all possible columns
            val_map = IssuanceExportService._extract_row_values(r, req, maints, db, today, user_cache)

            for col_idx, col_id in enumerate(active_cols, 1):
                val = val_map.get(col_id, "")
                col_meta = COLUMN_REGISTRY[col_id]
                col_type = col_meta.get("type", "string")

                cell = ws.cell(row=row_idx, column=col_idx, value=val)
                cell.font = FONT_DATA
                cell.fill = fill
                cell.border = BORDER_THIN

                if col_type == "currency":
                    if isinstance(val, (int, float)):
                        cell.number_format = "#,##0.00"
                        cell.alignment = ALIGN_RIGHT
                    else:
                        cell.alignment = ALIGN_RIGHT
                elif col_type in ("date", "status", "code") or col_id in ("internal_serial", "lg_ref_number", "bank_lg_number", "days_remaining", "request_serial"):
                    cell.alignment = ALIGN_CENTER
                elif col_type == "integer":
                    cell.number_format = "#,##0"
                    cell.alignment = ALIGN_CENTER
                else:
                    cell.alignment = ALIGN_LEFT

            row_idx += 1

        # Summary Row (Formulas for currency columns)
        if records:
            ws.row_dimensions[row_idx].height = 22
            # Find first currency column to put total label
            first_curr_idx = next((idx for idx, c in enumerate(active_cols, 1) if COLUMN_REGISTRY[c].get("type") == "currency"), None)
            label_col = max(1, first_curr_idx - 1) if first_curr_idx and first_curr_idx > 1 else 1

            ws.cell(row=row_idx, column=label_col, value="Total Portfolio:").font = FONT_TOTAL
            ws.cell(row=row_idx, column=label_col).alignment = ALIGN_RIGHT

            for col_idx, col_id in enumerate(active_cols, 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.fill = FILL_TOTAL
                cell.border = BORDER_TOTAL
                if COLUMN_REGISTRY[col_id].get("type") == "currency":
                    col_letter = get_column_letter(col_idx)
                    cell.value = f"=SUM({col_letter}{header_row+1}:{col_letter}{row_idx-1})"
                    cell.font = FONT_TOTAL
                    cell.number_format = "#,##0.00"
                    cell.alignment = ALIGN_RIGHT

        ws.freeze_panes = f"A{header_row + 1}"
        ws.auto_filter.ref = f"A{header_row}:{get_column_letter(len(headers))}{max(row_idx, header_row + 1)}"
        IssuanceExportService._auto_fit_columns(ws, len(headers))

    # --------------------------------------------------------------------------
    # DATA EXTRACTION: Clean, single-value cells
    # --------------------------------------------------------------------------
    @staticmethod
    def _extract_row_values(
        r: IssuedLGRecord,
        req: Optional[IssuanceRequest],
        maints: List[IssuanceMaintenanceAction],
        db: Session,
        today: date,
        user_cache: Dict[int, str]
    ) -> Dict[str, Any]:
        """Extracts clean, single-value fields for an LG record."""
        # 1. Request details
        req_sn = req.serial_number if req else "N/A"
        orig_src = f"Request ({req.serial_number})" if req else "Direct Entry / Import"
        req_date = _fmt_date(req.created_at) if req else _fmt_date(r.created_at)
        req_name = req.requestor_name if req and req.requestor_name else (req.requestor_email if req else "")
        req_email = req.requestor_email if req else ""
        dept = r.department or (req.department if req else "") or "General"
        job_title = req.job_title if req and req.job_title else ""
        entity_name = r.issuing_entity.entity_name if r.issuing_entity else (req.issuing_entity.entity_name if req and req.issuing_entity else "")
        lg_type = (r.lg_type.name if r.lg_type else (req.lg_type.name if req and req.lg_type else "")) or "Guarantee"
        purpose = (req.lg_purpose if req else r.lg_purpose) or ""

        # Financials
        initial_amt = float(req.amount) if req and req.amount else float(r.current_amount or 0)
        curr_amt = float(r.current_amount or 0)
        net_amt_change = curr_amt - initial_amt
        orig_expiry = _fmt_date(req.requested_expiry_date if req and req.requested_expiry_date else r.expiry_date)

        # Contract reference
        ref_type = (req.reference_type if req else "Contract") or ""
        ref_num = (r.reference_number or (req.reference_number if req else "")) or ""
        ref_amt = float(req.reference_amount) if req and req.reference_amount else ""
        ref_validity = r.reference_validity_flag or ("Valid" if req and req.reference_end_date and r.expiry_date and r.expiry_date <= req.reference_end_date else "")

        # Internal Approvals (CLEAN single values!)
        approval_status = "Direct Entry / Pre-approved"
        approved_by = "N/A"
        approval_date = ""
        if req and req.approval_chain_audit and isinstance(req.approval_chain_audit, list):
            # Find final approval step or latest action
            final_step = next((s for s in reversed(req.approval_chain_audit) if isinstance(s, dict) and "APPROVED" in s.get("action", "")), None)
            if final_step:
                approval_status = "Fully Approved"
                uid = final_step.get("user_id")
                approved_by = _resolve_user(db, uid, user_cache) if uid else "System"
                approval_date = _fmt_date(final_step.get("timestamp"))
            elif any(isinstance(s, dict) and s.get("action") == "SUBMITTED" for s in req.approval_chain_audit):
                approval_status = "Pending Approval"
                first_step = next(s for s in req.approval_chain_audit if isinstance(s, dict) and s.get("action") == "SUBMITTED")
                uid = first_step.get("user_id")
                approved_by = f"Submitted by {_resolve_user(db, uid, user_cache)}"
                approval_date = _fmt_date(first_step.get("timestamp"))

        # Parties & Facilities
        bank_name = r.bank.name if r.bank else "N/A"
        facility_name = r.sub_limit.facility.facility_name if r.sub_limit and r.sub_limit.facility else ""
        if r.sub_limit and r.sub_limit.limit_name:
            facility_name = f"{facility_name} ({r.sub_limit.limit_name})"

        # Maintenance (CLEAN single values!)
        maint_count = len(maints)
        ext_count = sum(1 for m in maints if m.action_type == "EXTEND")
        latest_maint_type = "None"
        latest_maint_date = ""
        latest_maint_status = "None"
        if maints:
            latest = maints[-1]
            latest_maint_type = latest.action_type.replace("_", " ").title()
            latest_maint_date = _fmt_date(latest.created_at or latest.delivery_date or latest.bank_reply_date)
            latest_maint_status = latest.status.replace("_", " ").title()

        return {
            "internal_serial": r.internal_serial or f"LG-{r.id:04d}",
            "lg_ref_number": r.lg_ref_number,
            "bank_lg_number": r.bank_lg_number or "Awaiting Bank Number",
            "status": STATUS_LABELS.get(r.status, r.status or "Active"),
            "days_remaining": _calculate_days_remaining(r, today),
            "current_amount": curr_amt,
            "currency": r.currency.iso_code if r.currency else "EGP",
            "initial_amount": initial_amt,
            "net_amount_change": net_amt_change,
            "issue_date": _fmt_date(r.bank_lg_issue_date or r.issue_date),
            "expiry_date": _fmt_date(r.bank_lg_expiry_date or r.expiry_date),
            "original_expiry_date": orig_expiry,
            "request_date": req_date,
            "bank_name": bank_name,
            "facility_name": facility_name,
            "beneficiary_name": r.beneficiary_name,
            "beneficiary_country": r.beneficiary_country or "",
            "beneficiary_address": r.beneficiary_address or "",
            "issuing_entity": entity_name,
            "origination_source": orig_src,
            "request_serial": req_sn,
            "requestor_name": req_name,
            "requestor_email": req_email,
            "department": dept,
            "job_title": job_title,
            "lg_type": lg_type,
            "lg_purpose": purpose,
            "reference_type": ref_type,
            "reference_number": ref_num,
            "reference_amount": ref_amt,
            "reference_validity": ref_validity,
            "approval_status": approval_status,
            "approved_by": approved_by,
            "approval_date": approval_date,
            "issuance_method": METHOD_LABELS.get(r.issuance_method, r.issuance_method or ""),
            "issued_by": _resolve_user(db, r.issued_by_user_id, user_cache),
            "execution_date": _fmt_date(r.created_at),
            "delivery_date": _fmt_date(r.delivery_date),
            "delivery_method": DELIVERY_LABELS.get(r.delivery_method, r.delivery_method or ""),
            "delivery_notes": r.delivery_notes or "",
            "bank_reply_date": _fmt_date(r.bank_reply_date),
            "bank_confirmed_amount": float(r.bank_lg_amount) if r.bank_lg_amount else curr_amt,
            "verification_status": VERIFICATION_LABELS.get(r.verification_status, r.verification_status or "Pending"),
            "verified_by": _resolve_user(db, r.verified_by_user_id, user_cache),
            "handover_date": _fmt_date(r.handover_date),
            "recipient_name": r.recipient_name or "",
            "custody_holder": r.custody_holder or "",
            "maint_count": maint_count,
            "ext_count": ext_count,
            "latest_maint_type": latest_maint_type,
            "latest_maint_date": latest_maint_date,
            "latest_maint_status": latest_maint_status,
        }

    # --------------------------------------------------------------------------
    # SHEET BUILDER: Lifecycle Event Log (Auditor Tab)
    # --------------------------------------------------------------------------
    @staticmethod
    def _build_lifecycle_ledger_sheet(wb, db, records, company_name, user_email, today, user_cache):
        ws = wb.create_sheet(title="Lifecycle Event Log")
        ws.views.sheetView[0].showGridLines = True

        headers = [
            "Customer LG Ref", "Internal Serial", "Bank LG #", "Beneficiary", "Bank",
            "Event Date", "Lifecycle Stage", "Action / Milestone", "Actor / User",
            "Previous Value", "New Value", "Status", "Notes & Details"
        ]

        ws.cell(row=1, column=1, value=f"{company_name} - Complete Lifecycle Audit Ledger").font = FONT_COMPANY
        ws.cell(row=2, column=1, value=f"Generated on {_fmt_datetime(datetime.now())} | Full chronological history of all lifecycle events").font = FONT_METADATA

        header_row = 4
        ws.row_dimensions[header_row].height = 26
        for col_idx, h in enumerate(headers, 1):
            cell = ws.cell(row=header_row, column=col_idx, value=h)
            cell.font = FONT_HEADER
            cell.fill = FILL_HEADER_ALT
            cell.alignment = ALIGN_HEADER
            cell.border = BORDER_THIN

        row_idx = header_row + 1
        event_count = 0

        for r in records:
            req = db.query(IssuanceRequest).filter(IssuanceRequest.id == r.request_id).first() if r.request_id else None
            maints = db.query(IssuanceMaintenanceAction).filter(
                IssuanceMaintenanceAction.issued_lg_id == r.id
            ).order_by(IssuanceMaintenanceAction.id.asc()).all()

            events = []

            # 1. Request Origination
            if req:
                events.append({
                    "date": _fmt_date(req.created_at),
                    "stage": "1. Request Origination",
                    "action": "Issuance Request Submitted",
                    "actor": req.requestor_name or req.requestor_email or "Requestor",
                    "prev": "-",
                    "new": f"{float(req.amount or 0):,.2f} {r.currency.iso_code if r.currency else ''}",
                    "status": "Submitted",
                    "notes": f"Serial: {req.serial_number}, Dept: {req.department}, Purpose: {req.lg_purpose or '-'}"
                })

                if req.approval_chain_audit and isinstance(req.approval_chain_audit, list):
                    for step in req.approval_chain_audit:
                        if isinstance(step, dict):
                            act = step.get("action", "")
                            uid = step.get("user_id")
                            u_str = _resolve_user(db, uid, user_cache) if uid else "System"
                            events.append({
                                "date": _fmt_date(step.get("timestamp")),
                                "stage": "2. Internal Approval",
                                "action": act.replace("_", " ").title(),
                                "actor": u_str,
                                "prev": "-",
                                "new": f"Step {step.get('step', '-')}",
                                "status": "Approved" if "APPROVED" in act else act.title(),
                                "notes": step.get("reason", "") or step.get("notes", "") or "Approval Workflow Step"
                            })

            # 2. Execution
            events.append({
                "date": _fmt_date(r.created_at),
                "stage": "3. Treasury Execution",
                "action": f"Executed via {METHOD_LABELS.get(r.issuance_method, r.issuance_method or 'Treasury')}",
                "actor": _resolve_user(db, r.issued_by_user_id, user_cache),
                "prev": "-",
                "new": f"{float(r.current_amount or 0):,.2f}",
                "status": "Issued to Bank",
                "notes": f"Bank: {r.bank.name if r.bank else 'N/A'}, Facility: {r.sub_limit.facility.facility_name if r.sub_limit and r.sub_limit.facility else '-'}"
            })

            # 3. Delivery
            if r.delivery_date:
                events.append({
                    "date": _fmt_date(r.delivery_date),
                    "stage": "4. Bank Delivery",
                    "action": f"Delivered via {DELIVERY_LABELS.get(r.delivery_method, r.delivery_method or 'Courier')}",
                    "actor": _resolve_user(db, r.issued_by_user_id, user_cache),
                    "prev": "-",
                    "new": "-",
                    "status": "Delivered to Bank",
                    "notes": r.delivery_notes or "Physical form submitted to bank branch"
                })

            # 4. Bank Confirmation
            if r.bank_reply_date or r.bank_lg_number:
                events.append({
                    "date": _fmt_date(r.bank_reply_date or r.issue_date),
                    "stage": "5. Bank Confirmation",
                    "action": "LG Issued & Confirmed by Bank",
                    "actor": r.bank.name if r.bank else "Bank",
                    "prev": "-",
                    "new": f"Bank LG #{r.bank_lg_number or 'Confirmed'}",
                    "status": STATUS_LABELS.get(r.status, r.status or "Active"),
                    "notes": f"Bank Amount: {float(r.bank_lg_amount or r.current_amount or 0):,.2f}, Expiry: {_fmt_date(r.bank_lg_expiry_date or r.expiry_date)}"
                })

            # 5. Verification
            if r.verified_at or r.verification_status:
                events.append({
                    "date": _fmt_date(r.verified_at),
                    "stage": "6. Document Verification",
                    "action": VERIFICATION_LABELS.get(r.verification_status, r.verification_status or "Verified"),
                    "actor": _resolve_user(db, r.verified_by_user_id, user_cache),
                    "prev": "-",
                    "new": "-",
                    "status": r.verification_status or "Matched",
                    "notes": r.verification_notes or "LG copy scan verified against bank issuance parameters"
                })

            # 6. Handover
            if r.handover_date:
                events.append({
                    "date": _fmt_date(r.handover_date),
                    "stage": "7. Beneficiary Handover",
                    "action": "Original LG Handed Over",
                    "actor": _resolve_user(db, r.handover_by_user_id, user_cache),
                    "prev": "-",
                    "new": r.recipient_name or "Recipient",
                    "status": "Handover Complete",
                    "notes": f"Recipient: {r.recipient_name or ''} ({r.recipient_department or ''}), Custody: {r.custody_holder or ''}"
                })

            # 7. Maintenance Actions
            for m in maints:
                data = m.action_data or {}
                m_type = m.action_type.replace("_", " ").title()
                p_val = "-"
                n_val = "-"
                if "new_amount" in data:
                    n_val = f"{float(data['new_amount']):,.2f}"
                    p_val = f"{float(data.get('snapshot_amount', r.current_amount)):,.2f}"
                elif "new_expiry_date" in data:
                    n_val = str(data["new_expiry_date"])
                    p_val = str(data.get("snapshot_expiry_date", r.expiry_date or ""))
                elif "liquidation_amount" in data:
                    n_val = f"{float(data['liquidation_amount']):,.2f}"

                events.append({
                    "date": _fmt_date(m.created_at or m.delivery_date or m.bank_reply_date),
                    "stage": f"8. Maintenance: {m_type}",
                    "action": f"{m_type} ({m.letter_serial_number or 'Instruction'})",
                    "actor": _resolve_user(db, m.initiated_by_user_id, user_cache),
                    "prev": p_val,
                    "new": n_val,
                    "status": m.status.replace("_", " ").title(),
                    "notes": m.notes or f"Instruction Status: {m.instruction_status or '-'}"
                })

            for ev in events:
                event_count += 1
                ws.row_dimensions[row_idx].height = 20
                fill = FILL_ZEBRA if event_count % 2 == 1 else FILL_WHITE

                row_vals = [
                    r.lg_ref_number,
                    r.internal_serial or f"LG-{r.id:04d}",
                    r.bank_lg_number or "-",
                    r.beneficiary_name,
                    r.bank.name if r.bank else "N/A",
                    ev["date"],
                    ev["stage"],
                    ev["action"],
                    ev["actor"],
                    ev["prev"],
                    ev["new"],
                    ev["status"],
                    ev["notes"]
                ]

                for col_idx, val in enumerate(row_vals, 1):
                    cell = ws.cell(row=row_idx, column=col_idx, value=val)
                    cell.font = FONT_DATA
                    cell.fill = fill
                    cell.border = BORDER_THIN
                    if col_idx in (1, 2, 3, 6, 12):
                        cell.alignment = ALIGN_CENTER
                    elif col_idx in (10, 11) and ("," in str(val) or "." in str(val)):
                        cell.alignment = ALIGN_RIGHT
                    else:
                        cell.alignment = ALIGN_LEFT

                row_idx += 1

        ws.freeze_panes = f"A{header_row + 1}"
        ws.auto_filter.ref = f"A{header_row}:{get_column_letter(len(headers))}{max(row_idx, header_row + 1)}"
        IssuanceExportService._auto_fit_columns(ws, len(headers))

    # --------------------------------------------------------------------------
    # SHEET BUILDER: Approvals Audit Trail (Governance Tab)
    # --------------------------------------------------------------------------
    @staticmethod
    def _build_approvals_sheet(wb, db, records, company_name, user_email, today, user_cache):
        ws = wb.create_sheet(title="Approvals Audit Trail")
        ws.views.sheetView[0].showGridLines = True

        headers = [
            "Customer LG Ref", "Internal Serial", "Scope / Context", "Step Number",
            "Action Decision", "Approver Name / Email", "Timestamp", "Decision Status", "Audit Notes"
        ]

        ws.cell(row=1, column=1, value=f"{company_name} - Internal Approvals Governance Log").font = FONT_COMPANY
        ws.cell(row=2, column=1, value=f"Generated on {_fmt_datetime(datetime.now())} | Full internal audit trail for requests and maintenance approvals").font = FONT_METADATA

        header_row = 4
        ws.row_dimensions[header_row].height = 26
        for col_idx, h in enumerate(headers, 1):
            cell = ws.cell(row=header_row, column=col_idx, value=h)
            cell.font = FONT_HEADER
            cell.fill = FILL_HEADER_ALT
            cell.alignment = ALIGN_HEADER
            cell.border = BORDER_THIN

        row_idx = header_row + 1
        approval_count = 0

        for r in records:
            req = db.query(IssuanceRequest).filter(IssuanceRequest.id == r.request_id).first() if r.request_id else None
            maints = db.query(IssuanceMaintenanceAction).filter(
                IssuanceMaintenanceAction.issued_lg_id == r.id
            ).order_by(IssuanceMaintenanceAction.id.asc()).all()

            # 1. Request Approvals
            if req and req.approval_chain_audit and isinstance(req.approval_chain_audit, list):
                for item in req.approval_chain_audit:
                    if isinstance(item, dict):
                        approval_count += 1
                        ws.row_dimensions[row_idx].height = 20
                        fill = FILL_ZEBRA if approval_count % 2 == 1 else FILL_WHITE
                        uid = item.get("user_id")
                        user_str = _resolve_user(db, uid, user_cache) if uid else "System Automated"
                        act = item.get("action", "")

                        row_vals = [
                            r.lg_ref_number,
                            r.internal_serial or f"LG-{r.id:04d}",
                            f"New LG Request ({req.serial_number})",
                            item.get("step", 1),
                            act.replace("_", " ").title(),
                            user_str,
                            _fmt_datetime(item.get("timestamp")),
                            "Approved" if "APPROVED" in act else act.title(),
                            item.get("reason") or item.get("notes") or "Step verification"
                        ]

                        for col_idx, val in enumerate(row_vals, 1):
                            cell = ws.cell(row=row_idx, column=col_idx, value=val)
                            cell.font = FONT_DATA
                            cell.fill = fill
                            cell.border = BORDER_THIN
                            if col_idx in (1, 2, 4, 7, 8):
                                cell.alignment = ALIGN_CENTER
                            else:
                                cell.alignment = ALIGN_LEFT
                        row_idx += 1

            # 2. Maintenance Approvals
            for m in maints:
                if m.approval_history and isinstance(m.approval_history, list):
                    for item in m.approval_history:
                        if isinstance(item, dict):
                            approval_count += 1
                            ws.row_dimensions[row_idx].height = 20
                            fill = FILL_ZEBRA if approval_count % 2 == 1 else FILL_WHITE
                            uid = item.get("user_id")
                            user_str = _resolve_user(db, uid, user_cache) if uid else "System Automated"
                            dec = item.get("decision", "APPROVED")

                            row_vals = [
                                r.lg_ref_number,
                                r.internal_serial or f"LG-{r.id:04d}",
                                f"Maintenance: {m.action_type} ({m.letter_serial_number or m.id})",
                                item.get("step", 1),
                                dec.replace("_", " ").title(),
                                user_str,
                                _fmt_datetime(item.get("timestamp")),
                                dec.title(),
                                item.get("notes") or "Maintenance action governance approval"
                            ]

                            for col_idx, val in enumerate(row_vals, 1):
                                cell = ws.cell(row=row_idx, column=col_idx, value=val)
                                cell.font = FONT_DATA
                                cell.fill = fill
                                cell.border = BORDER_THIN
                                if col_idx in (1, 2, 4, 7, 8):
                                    cell.alignment = ALIGN_CENTER
                                else:
                                    cell.alignment = ALIGN_LEFT
                            row_idx += 1

        ws.freeze_panes = f"A{header_row + 1}"
        ws.auto_filter.ref = f"A{header_row}:{get_column_letter(len(headers))}{max(row_idx, header_row + 1)}"
        IssuanceExportService._auto_fit_columns(ws, len(headers))

    # --------------------------------------------------------------------------
    # COMMON: Title, Metadata, KPI Cards Banner (CLEAN & MERGED)
    # --------------------------------------------------------------------------
    @staticmethod
    def _write_title_and_kpis(ws, company_name, report_title, user_email, status_filter, records, col_count, today) -> int:
        # Row 1: Company Title
        ws.row_dimensions[1].height = 24
        ws.cell(row=1, column=1, value=f"{company_name} - Corporate Treasury Management").font = FONT_COMPANY

        # Row 2: Report Title
        ws.row_dimensions[2].height = 20
        ws.cell(row=2, column=1, value=report_title).font = FONT_REPORT_TITLE

        # Row 3: Metadata
        ws.row_dimensions[3].height = 18
        scope_str = status_filter.upper() if status_filter and status_filter.upper() != "ALL" else "All Portfolio Statuses"
        meta_str = f"Export Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} | Requested By: {user_email} | Scope: {scope_str} | Total Records: {len(records)}"
        ws.cell(row=3, column=1, value=meta_str).font = FONT_METADATA

        # Row 4: KPI Summary Cards (Clean Merged Cells)
        ws.row_dimensions[4].height = 26
        active_count = sum(1 for r in records if r.status == "ACTIVE")
        expiring_30 = sum(1 for r in records if r.status == "ACTIVE" and r.expiry_date and 0 <= (r.expiry_date - today).days <= 30)

        # Aggregate total liability
        currency_totals: Dict[str, float] = {}
        for r in records:
            if r.status in ("ACTIVE", "LG_ISSUED", "INTERNAL_PROCESSING", "DELIVERED_TO_BANK"):
                c = r.currency.iso_code if r.currency else "EGP"
                currency_totals[c] = currency_totals.get(c, 0.0) + float(r.current_amount or 0)

        total_liability_str = " | ".join(f"{c} {amt:,.2f}" for c, amt in currency_totals.items()) if currency_totals else "0.00"

        kpis = [
            ("TOTAL RECORDS", f"{len(records)} LGs"),
            ("ACTIVE LGs", f"{active_count} Active"),
            ("PORTFOLIO LIABILITY", total_liability_str),
            ("EXPIRING SOON", f"{expiring_30} LGs"),
        ]

        # Allocate 2-3 merged columns per card so text doesn't clip
        card_width = max(2, min(4, col_count // len(kpis)))
        current_col = 1
        for label, val in kpis:
            end_col = min(current_col + card_width - 1, col_count)
            # Merge cells for this card
            if end_col > current_col:
                ws.merge_cells(start_row=4, start_column=current_col, end_row=4, end_column=end_col)

            main_cell = ws.cell(row=4, column=current_col, value=f"{label}: {val}")
            main_cell.font = FONT_KPI_VAL
            main_cell.alignment = ALIGN_CENTER

            # Style all cells in the merged span
            for c in range(current_col, end_col + 1):
                cell = ws.cell(row=4, column=c)
                cell.fill = FILL_KPI
                cell.border = BORDER_KPI

            current_col = end_col + 2
            if current_col > col_count:
                break

        # Row 5: Spacer
        ws.row_dimensions[5].height = 10
        return 6

    # --------------------------------------------------------------------------
    # COMMON: Auto-fit Column Widths
    # --------------------------------------------------------------------------
    @staticmethod
    def _auto_fit_columns(ws, max_cols: int):
        for col_idx in range(1, max_cols + 1):
            col_letter = get_column_letter(col_idx)
            max_len = 0
            for row_idx in range(6, ws.max_row + 1):
                cell_val = str(ws.cell(row=row_idx, column=col_idx).value or "")
                if "\n" in cell_val:
                    lines = cell_val.split("\n")
                    max_len = max(max_len, max(len(l) for l in lines))
                else:
                    max_len = max(max_len, len(cell_val))
            # Header length
            hdr_val = str(ws.cell(row=6, column=col_idx).value or "")
            max_len = max(max_len, len(hdr_val))
            ws.column_dimensions[col_letter].width = max(min(max_len + 4, 50), 13)
