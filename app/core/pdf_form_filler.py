# app/core/pdf_form_filler.py
"""
PDF Form Filler Utility for Bank Submission Methods.

Handles two modes:
1. FILLABLE_PDF: Fills existing interactive form fields in a PDF
2. PHYSICAL_OVERLAY: Generates a text-only PDF overlay for pre-printed bank forms

Uses pypdf for reading/writing PDF forms.
"""

import io
import logging
from typing import Dict, Any, Optional, List
from datetime import date
from decimal import Decimal

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Date Formatting Helpers
# ---------------------------------------------------------------------------
DATE_FORMAT_MAP = {
    "DD/MM/YYYY": "%d/%m/%Y",
    "DD-MM-YYYY": "%d-%m-%Y",
    "MM/DD/YYYY": "%m/%d/%Y",
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYYMMDD": "%Y%m%d",
    "DD-MMM-YYYY": "%d-%b-%Y",
    "DD.MM.YYYY": "%d.%m.%Y",
    "YYYY/MM/DD": "%Y/%m/%d",
    # 2-digit year variants (for forms with limited character boxes)
    "DD/MM/YY": "%d/%m/%y",
    "DD-MM-YY": "%d-%m-%y",
    "MM/DD/YY": "%m/%d/%y",
    "DDMMYY": "%d%m%y",
    "DDMMYYYY": "%d%m%Y",
    "YY/MM/DD": "%y/%m/%d",
    # Single component variants for split-box forms
    "DD": "%d",
    "MM": "%m",
    "YYYY": "%Y",
    "YY": "%y",
}

def _format_date(value, date_format: str = None) -> str:
    """Format a date value according to the specified format string."""
    if not isinstance(value, date):
        return str(value) if value else ""
    
    if date_format and date_format.upper() in DATE_FORMAT_MAP:
        return value.strftime(DATE_FORMAT_MAP[date_format.upper()])
    
    # Default format
    return value.strftime("%d/%m/%Y")


# ---------------------------------------------------------------------------
# Number to Words Converter (supports EGP, USD, EUR)
# ---------------------------------------------------------------------------
def _number_to_words(amount: float, currency_code: str = "EGP") -> str:
    """Convert a number to its English word representation with currency."""
    ones = ['', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine',
            'Ten', 'Eleven', 'Twelve', 'Thirteen', 'Fourteen', 'Fifteen', 'Sixteen',
            'Seventeen', 'Eighteen', 'Nineteen']
    tens = ['', '', 'Twenty', 'Thirty', 'Forty', 'Fifty', 'Sixty', 'Seventy', 'Eighty', 'Ninety']
    
    currency_names = {
        "EGP": ("Egyptian Pound", "Egyptian Pounds", "Piaster", "Piasters"),
        "USD": ("US Dollar", "US Dollars", "Cent", "Cents"),
        "EUR": ("Euro", "Euros", "Cent", "Cents"),
        "GBP": ("British Pound", "British Pounds", "Penny", "Pence"),
        "AED": ("UAE Dirham", "UAE Dirhams", "Fil", "Fils"),
        "SAR": ("Saudi Riyal", "Saudi Riyals", "Halala", "Halalas"),
    }
    
    def _int_to_words(n):
        if n == 0:
            return "Zero"
        if n < 0:
            return "Minus " + _int_to_words(-n)
        
        parts = []
        if n >= 1_000_000:
            millions = n // 1_000_000
            parts.append(_int_to_words(millions) + " Million")
            n %= 1_000_000
        if n >= 1_000:
            thousands = n // 1_000
            parts.append(_int_to_words(thousands) + " Thousand")
            n %= 1_000
        if n >= 100:
            hundreds = n // 100
            parts.append(ones[hundreds] + " Hundred")
            n %= 100
        if n >= 20:
            parts.append(tens[n // 10])
            if n % 10:
                parts.append(ones[n % 10])
        elif n > 0:
            parts.append(ones[n])
        
        return " ".join(parts)
    
    whole = int(amount)
    fraction = round((amount - whole) * 100)
    
    curr = currency_names.get(currency_code.upper(), (currency_code, currency_code, "unit", "units"))
    major_singular, major_plural, minor_singular, minor_plural = curr
    
    result = _int_to_words(whole) + " " + (major_singular if whole == 1 else major_plural)
    if fraction > 0:
        result += " and " + _int_to_words(fraction) + " " + (minor_singular if fraction == 1 else minor_plural)
    result += " Only"
    return result


# ---------------------------------------------------------------------------
# Core: Fill PDF Form
# ---------------------------------------------------------------------------
def fill_pdf_form(
    template_pdf_bytes: bytes,
    field_mapping: List[Dict[str, Any]],
    request_data: Dict[str, Any],
    lg_language: str = None,
) -> bytes:
    """
    Fills a PDF form by writing values into interactive form fields.
    
    Supports:
    - Text fields with smart date formatting
    - Checkbox/radio fields with boolean matching
    - Amount-in-words generation
    
    Args:
        template_pdf_bytes: The blank bank PDF form as bytes
        field_mapping: List of mappings with optional date_format, field_type, fill_strategy
        request_data: Dict of issuance request data (keys match mapped_to values)
    
    Returns:
        Filled PDF as bytes
    """
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        try:
            from PyPDF2 import PdfReader, PdfWriter
        except ImportError:
            logger.error("Neither pypdf nor PyPDF2 is installed. Cannot fill PDF forms.")
            raise ImportError("PDF library required. Install: pip install pypdf")

    reader = PdfReader(io.BytesIO(template_pdf_bytes))
    writer = PdfWriter()

    # CRITICAL: clone_reader_document_root preserves the /AcroForm dictionary
    # (form field definitions). Using add_page() only copies page content but
    # drops the form structure, resulting in an empty form.
    writer.clone_reader_document_root(reader)

    # Build field values from mapping
    field_values = {}
    checkbox_values = {}  # Separate handling for checkboxes
    
    for mapping_entry in field_mapping:
        pdf_field = mapping_entry.get("pdf_field_name")
        mapped_to = mapping_entry.get("mapped_to")
        
        if not pdf_field or not mapped_to:
            continue
        
        # Language filtering: skip fields for the opposite language
        if lg_language:
            field_lang = (mapping_entry.get("language") or "shared").lower()
            if field_lang not in ("shared", lg_language.lower()[:2]):
                logger.debug(f"SKIP field '{pdf_field}' (lang={field_lang}, request={lg_language})")
                continue
        
        field_type = mapping_entry.get("field_type", "text").lower()
        fill_strategy = mapping_entry.get("fill_strategy", "").lower()
        date_format = mapping_entry.get("date_format")
        
        # Get value from request data
        value = request_data.get(mapped_to, "")
        if value is None:
            value = ""
        
        # --- CHECKBOX / BOOLEAN MATCH ---
        if field_type == "checkbox" or fill_strategy == "boolean_match":
            # For checkboxes, the value should be boolean-like
            is_checked = bool(value) and str(value).lower() not in ("", "0", "false", "no", "none")
            checkbox_values[pdf_field] = is_checked
            # Also set as text for update_page_form_field_values
            field_values[pdf_field] = "/Yes" if is_checked else "/Off"
            continue
        
        # --- DATE FIELDS ---
        if field_type == "date" or isinstance(value, date):
            # Convert string dates to date objects for proper formatting
            if isinstance(value, str) and value:
                try:
                    # Try ISO format first (YYYY-MM-DD)
                    from datetime import datetime as dt
                    parsed = dt.strptime(value.strip()[:10], "%Y-%m-%d").date()
                    value = _format_date(parsed, date_format)
                except (ValueError, TypeError):
                    # Try DD/MM/YYYY
                    try:
                        parsed = dt.strptime(value.strip()[:10], "%d/%m/%Y").date()
                        value = _format_date(parsed, date_format)
                    except (ValueError, TypeError):
                        # Can't parse, leave as-is
                        pass
            elif isinstance(value, date):
                value = _format_date(value, date_format)
            field_values[pdf_field] = str(value)
            logger.debug(f"PDF Fill: {pdf_field} ({mapped_to}) → '{value}' [date, fmt={date_format}]")
            continue
        
        # --- REGULAR TEXT ---
        # Format numbers
        if isinstance(value, (int, float, Decimal)):
            value = f"{float(value):,.2f}" if isinstance(value, (float, Decimal)) else str(value)
        
        field_values[pdf_field] = str(value)
        logger.debug(f"PDF Fill: {pdf_field} ({mapped_to}) → '{str(value)[:60]}' [text]")

    # Debug dump all field values before writing
    logger.info(f"=== PDF FILL: {len(field_values)} text fields, {len(checkbox_values)} checkboxes ===")
    for fn, fv in field_values.items():
        logger.info(f"  FIELD '{fn}' → '{str(fv)[:80]}'")
    for fn, fv in checkbox_values.items():
        logger.info(f"  CHECKBOX '{fn}' → {fv}")

    # Fill text form fields across all pages
    if field_values:
        for page_num, page in enumerate(writer.pages):
            try:
                # Resolve IndirectObject references in annotation /Rect values.
                # Some PDFs store Rect coordinates as indirect refs, causing
                # pypdf's arithmetic to fail inside update_page_form_field_values.
                if '/Annots' in page:
                    from pypdf.generic import ArrayObject, NameObject as _NO
                    for annot_ref in page['/Annots']:
                        try:
                            annot = annot_ref.get_object() if hasattr(annot_ref, 'get_object') else annot_ref
                            if '/Rect' in annot:
                                rect = annot['/Rect']
                                resolved_rect = ArrayObject([
                                    (v.get_object() if hasattr(v, 'get_object') else v)
                                    for v in rect
                                ])
                                annot[_NO('/Rect')] = resolved_rect
                        except Exception:
                            pass
                writer.update_page_form_field_values(page, field_values)
            except Exception as page_err:
                logger.warning(f"Page {page_num} form fill failed: {page_err}")
    
    # Handle checkboxes separately using low-level annotation manipulation
    if checkbox_values:
        _fill_checkboxes(writer, checkbox_values)

    # Write output
    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    
    filled_bytes = output.read()
    logger.info(
        f"Successfully filled PDF form: {len(field_values)} text fields, "
        f"{len(checkbox_values)} checkboxes ({len(filled_bytes)} bytes)"
    )
    return filled_bytes


def _fill_checkboxes(writer, checkbox_values: Dict[str, bool]):
    """
    Set checkbox fields in the PDF by manipulating annotations directly.
    pypdf's update_page_form_field_values doesn't handle checkboxes well.
    """
    try:
        from pypdf.generic import NameObject, BooleanObject
    except ImportError:
        from PyPDF2.generic import NameObject, BooleanObject
    
    for page in writer.pages:
        if '/Annots' not in page:
            continue
        for annot_ref in page['/Annots']:
            try:
                annot = annot_ref.get_object()
                field_name = str(annot.get('/T', ''))
                # Clean up field name
                if field_name.startswith('(') and field_name.endswith(')'):
                    field_name = field_name[1:-1]
                
                if field_name in checkbox_values:
                    is_checked = checkbox_values[field_name]
                    check_val = NameObject("/Yes") if is_checked else NameObject("/Off")
                    annot.update({
                        NameObject("/V"): check_val,
                        NameObject("/AS"): check_val,
                    })
            except Exception as e:
                logger.debug(f"Checkbox fill error for annotation: {e}")


# ---------------------------------------------------------------------------
# Extract PDF form field names
# ---------------------------------------------------------------------------
def get_pdf_form_fields(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Extracts all interactive form field names from a PDF.
    Used during AI analysis to identify available fields.
    
    Returns: List of {"field_name": "...", "field_type": "text|checkbox|...", "page": 0}
    """
    try:
        from pypdf import PdfReader
    except ImportError:
        from PyPDF2 import PdfReader

    reader = PdfReader(io.BytesIO(pdf_bytes))
    fields = []
    
    for page_num, page in enumerate(reader.pages):
        if '/Annots' in page:
            for annotation in page['/Annots']:
                annot = annotation.get_object()
                field_type = annot.get('/FT', '')
                field_name = annot.get('/T', '')
                
                if field_name:
                    # Clean up the field name
                    field_name = str(field_name)
                    if field_name.startswith('(') and field_name.endswith(')'):
                        field_name = field_name[1:-1]
                    
                    field_type_str = str(field_type) if field_type else "unknown"
                    type_map = {'/Tx': 'text', '/Btn': 'checkbox', '/Ch': 'dropdown'}
                    
                    fields.append({
                        "field_name": field_name,
                        "field_type": type_map.get(field_type_str, field_type_str),
                        "page": page_num,
                    })

    # Also check the global fields dict
    if reader.get_fields():
        existing_names = {f["field_name"] for f in fields}
        for name, field_obj in reader.get_fields().items():
            if name not in existing_names:
                fields.append({
                    "field_name": name,
                    "field_type": "text",
                    "page": 0,
                })

    logger.info(f"Extracted {len(fields)} form fields from PDF")
    return fields


# ---------------------------------------------------------------------------
# Build comprehensive request data dictionary
# ---------------------------------------------------------------------------
def build_request_data_dict(request, db=None, bank_id=None) -> Dict[str, Any]:
    """
    Builds a comprehensive data dictionary from an IssuanceRequest object.
    Used to populate both signed letters and fillable PDF forms.
    Includes request fields, customer data, AND bank account details.
    
    Args:
        request: IssuanceRequest ORM object
        db: SQLAlchemy session (for bank account lookup)
        bank_id: Optional bank_id to resolve bank account details
    """
    data = {
        # Request basics
        "request_id": request.serial_number or f"REQ-{request.id}",
        "serial_number": request.serial_number or "",
        "status": request.status or "",
        "transaction_type": request.transaction_type or "",
        
        # Beneficiary
        "beneficiary_name": request.beneficiary_name or "",
        "beneficiary_address": request.beneficiary_address or "",
        "beneficiary_contact_person": request.beneficiary_contact_person or "",
        "beneficiary_phone": request.beneficiary_phone or "",
        "beneficiary_email": request.beneficiary_email or "",
        "beneficiary_country": request.beneficiary_country or "",
        "beneficiary_id_number": request.beneficiary_id_number or "",
        
        # LG Core
        "amount": float(request.amount) if request.amount else 0,
        "lg_purpose": request.lg_purpose or "",
        "purpose": request.lg_purpose or "",
        "operational_status": request.operational_status or "",
        
        # Dates  
        "requested_issue_date": request.requested_issue_date,
        "requested_expiry_date": request.requested_expiry_date,
        "expiry_date": request.requested_expiry_date,
        
        # Reference
        "reference_type": request.reference_type or "",
        "reference_number": request.reference_number or "",
        "reference_amount": float(request.reference_amount) if request.reference_amount else 0,
        
        # Requestor
        "requestor_name": request.requestor_name or "",
        "requestor_email": request.requestor_email or "",
        "department": request.department or "",
        "phone_number": request.phone_number or "",
        "employee_id": request.employee_id or "",
        
        # Custom fields
        "custom_field_1_value": request.custom_field_1_value or "",
        "custom_field_2_value": request.custom_field_2_value or "",
        
        # Date helpers
        "current_date": date.today(),
        
        # Cross-border indicators  
        "is_cross_border": bool(getattr(request, 'is_cross_border', False)),
        "is_local_lg": not bool(getattr(request, 'is_cross_border', False)),
        
        # LG Format defaults (mutually exclusive)
        "lg_format_is_special": bool(getattr(request, 'requires_special_wording', False)),
        "lg_format_is_bank_standard": not bool(getattr(request, 'requires_special_wording', False)),
        
        # Third-party issuance
        "is_third_party": bool(getattr(request, 'is_third_party', False)),
        "is_in_own_name": not bool(getattr(request, 'is_third_party', False)),
        "third_party_name": getattr(request, 'third_party_name', '') or "",
        "third_party_address": getattr(request, 'third_party_address', '') or "",
        "third_party_relationship": getattr(request, 'third_party_relationship', '') or "",
        
        # LG Language booleans (for language selection checkboxes on bank forms)
        "lg_language_is_arabic": getattr(request, 'lg_language', 'AR') == 'AR',
        "lg_language_is_english": getattr(request, 'lg_language', 'AR') == 'EN',
        
        # Bank account defaults (will be enriched below if bank_id provided)
        "bank_branch": "",
        "bank_account_number": "",
        "customer_cif_number": "",
        "iban": "",
        "account_name": "",
        
        # C1: Applicable Rules
        "applicable_rules": getattr(request, 'applicable_rules', None) or "",
        "applicable_rules_text": {
            "URDG_758": "Subject to URDG 758 (ICC Uniform Rules for Demand Guarantees)",
            "ISP_98": "Subject to ISP98 (International Standby Practices)",
            "LOCAL_LAW": "Subject to local governing law",
        }.get(getattr(request, 'applicable_rules', None) or "", ""),
    }
    
    # --- Compute additional_conditions ---
    conditions_parts = []
    if bool(getattr(request, 'requires_special_wording', False)):
        conditions_parts.append("As per attached special wording")
    if bool(getattr(request, 'is_cross_border', False)):
        conditions_parts.append("Cross-border Letter of Guarantee")
    if data.get("applicable_rules_text"):
        conditions_parts.append(data["applicable_rules_text"])
    data["additional_conditions"] = " / ".join(conditions_parts)
    
    # --- C2: Cross-border details enrichment ---
    cbd = getattr(request, 'cross_border_details', None) or {}
    if cbd:
        data["advising_bank_name"] = cbd.get("advising_bank_name", "")
        data["advising_bank_country"] = cbd.get("advising_bank_country", "")
        data["advising_bank_swift"] = cbd.get("advising_bank_swift", "")
        data["governing_law_country"] = cbd.get("governing_law_country", "")
        data["place_of_jurisdiction"] = cbd.get("place_of_jurisdiction", "")
        data["delivery_channel"] = cbd.get("delivery_channel", "")
        data["beneficiary_bank_name"] = cbd.get("beneficiary_bank_name", "")
        data["beneficiary_bank_swift"] = cbd.get("beneficiary_bank_swift", "")
    
    # --- C3: Treasury enrichment override ---
    te = getattr(request, 'treasury_enrichment', None) or {}
    if te.get("margin_instructions"):
        data["margin_instructions"] = te["margin_instructions"]
    if te.get("internal_notes"):
        data["treasury_notes"] = te["internal_notes"]
    
    # --- Add relationship data if loaded ---
    currency_code = ""
    if hasattr(request, 'currency') and request.currency:
        currency_code = request.currency.iso_code or ""
        data["currency_code"] = currency_code
        data["currency_name"] = request.currency.name or ""
    
    # Amount with currency & amount in words
    amount_val = float(request.amount) if request.amount else 0
    data["amount_with_currency"] = f"{currency_code} {amount_val:,.2f}".strip()
    _words_only = _number_to_words(amount_val, currency_code or "EGP")
    # Always format as: "EGP 100,000.00 — One Hundred Thousand Egyptian Pounds Only"
    data["amount_in_words"] = (
        f"{currency_code} {amount_val:,.2f} \u2014 {_words_only}" if currency_code else _words_only
    )
    
    # LG Type — text AND boolean flags for checkbox matching
    lg_type_name = ""
    if hasattr(request, 'lg_type') and request.lg_type:
        lg_type_name = (request.lg_type.name or "").strip()
        data["lg_type"] = lg_type_name
        data["guarantee_type"] = lg_type_name
    
    lt_lower = lg_type_name.lower()
    op_status = (data.get("operational_status") or "").lower()
    is_advance = any(kw in lt_lower for kw in ["advance", "payment advance", "دفعة مقدمة"])
    
    data["lg_type_is_bid_bond"] = any(kw in lt_lower for kw in ["bid bond", "tender", "ابتدائي"])
    data["lg_type_is_performance"] = any(kw in lt_lower for kw in ["performance", "final", "نهائي"])
    data["lg_type_is_advance_payment"] = is_advance
    data["lg_type_is_payment_guarantee"] = any(kw in lt_lower for kw in ["payment guarantee", "ضمان دفع"])
    # Conditioned Advance Payment = Advance Payment + Non-Operative (conditional)
    data["lg_type_is_advance_conditioned"] = is_advance and "non" in op_status
    # Unconditioned Advance Payment = Advance Payment + Operative (unconditional)  
    data["lg_type_is_advance_unconditioned"] = is_advance and "non" not in op_status and op_status != ""
    
    if hasattr(request, 'customer') and request.customer:
        data["customer_name"] = request.customer.name or ""
        data["company_name"] = request.customer.name or ""
        data["customer_address"] = getattr(request.customer, 'address', '') or ""
        data["customer_phone"] = getattr(request.customer, 'contact_phone', '') or ""
        data["customer_email"] = getattr(request.customer, 'contact_email', '') or ""
    
    if hasattr(request, 'issuing_entity') and request.issuing_entity:
        entity_name = getattr(request.issuing_entity, 'entity_name', '') or getattr(request.issuing_entity, 'name', '') or ""
        data["entity_name"] = entity_name
        data["entity_address"] = getattr(request.issuing_entity, 'address', '') or ""
    
    if hasattr(request, 'project') and request.project:
        data["project_name"] = request.project.name or ""
    
    # --- Bank Account Enrichment ---
    if db and bank_id:
        try:
            from app.models.models_issuance import CustomerBankAccount
            
            # Try entity-specific account first, then fallback to default
            account = None
            entity_id = getattr(request, 'issuing_entity_id', None)
            
            if entity_id:
                account = db.query(CustomerBankAccount).filter(
                    CustomerBankAccount.customer_id == request.customer_id,
                    CustomerBankAccount.bank_id == bank_id,
                    CustomerBankAccount.entity_id == entity_id,
                    CustomerBankAccount.is_active == True,
                    CustomerBankAccount.is_deleted == False,
                ).first()
            
            if not account:
                # Fallback: default account for this customer+bank pair
                account = db.query(CustomerBankAccount).filter(
                    CustomerBankAccount.customer_id == request.customer_id,
                    CustomerBankAccount.bank_id == bank_id,
                    CustomerBankAccount.is_active == True,
                    CustomerBankAccount.is_deleted == False,
                ).order_by(CustomerBankAccount.is_default.desc()).first()
            
            if account:
                data["bank_branch"] = account.branch_name or ""
                data["bank_account_number"] = account.account_number or ""
                data["customer_cif_number"] = account.customer_number or ""
                data["iban"] = account.iban or ""
                data["account_name"] = account.account_name or ""
                logger.info(f"Enriched form data with bank account: branch={account.branch_name}, acct=***{(account.account_number or '')[-4:]}")
            else:
                logger.info(f"No bank account found for customer_id={request.customer_id}, bank_id={bank_id}")
        except Exception as e:
            logger.warning(f"Failed to enrich bank account data: {e}")
    
    # --- Facility Enrichment ---
    # Check if the customer has an active facility at this bank
    if db and bank_id:
        try:
            from app.models.models_issuance import IssuanceFacility
            facility = db.query(IssuanceFacility).filter(
                IssuanceFacility.customer_id == request.customer_id,
                IssuanceFacility.bank_id == bank_id,
                IssuanceFacility.status == "ACTIVE",
                IssuanceFacility.is_deleted == False,
            ).first()
            data["has_facility_at_bank"] = bool(facility)
            if facility:
                data["facility_reference"] = facility.reference_number or ""
                logger.info(f"Customer has active facility at bank_id={bank_id}: ref={facility.reference_number}")
            else:
                logger.info(f"No active facility found for customer_id={request.customer_id}, bank_id={bank_id}")
        except Exception as e:
            logger.warning(f"Failed to check facility: {e}")
            data["has_facility_at_bank"] = ""  # Unknown — will surface as missing field
    
    return data


# ---------------------------------------------------------------------------
# Physical Overlay PDF Generator
# ---------------------------------------------------------------------------

def generate_overlay_pdf(
    template_pdf_bytes: bytes,
    field_mapping: List[Dict[str, Any]],
    request_data: Dict[str, Any],
    lg_language: str = None,
) -> bytes:
    """
    Generates a text-only overlay PDF for pre-printed physical bank forms.
    
    Instead of filling interactive form fields (FILLABLE_PDF mode), this creates
    a blank PDF page matching the template's dimensions and places text at the
    x,y coordinates specified in the field mapping.
    
    The user prints this overlay on top of their pre-printed physical form.
    
    Args:
        template_pdf_bytes: Original bank form PDF (used only to get page size)
        field_mapping: List of mappings with x, y, font_size, width fields
        request_data: Dict of issuance request data
    
    Returns:
        Overlay PDF as bytes
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import mm
    
    try:
        from pypdf import PdfReader
    except ImportError:
        from PyPDF2 import PdfReader
    
    # Get page dimensions from the template PDF
    reader = PdfReader(io.BytesIO(template_pdf_bytes))
    if reader.pages:
        page = reader.pages[0]
        media_box = page.mediabox
        page_width = float(media_box.width)
        page_height = float(media_box.height)
    else:
        # Fallback to A4
        page_width, page_height = A4
    
    num_template_pages = len(reader.pages)
    
    # Create overlay PDF in memory
    output = io.BytesIO()
    c = canvas.Canvas(output, pagesize=(page_width, page_height))
    
    # Group fields by page
    fields_by_page = {}
    for entry in field_mapping:
        pg = entry.get("page", 0)
        if pg not in fields_by_page:
            fields_by_page[pg] = []
        fields_by_page[pg].append(entry)
    
    filled_count = 0
    
    for page_num in range(max(num_template_pages, max(fields_by_page.keys(), default=0) + 1)):
        # Set page size (may vary per page in some PDFs)
        if page_num < len(reader.pages):
            pg = reader.pages[page_num]
            mb = pg.mediabox
            pw, ph = float(mb.width), float(mb.height)
            c.setPageSize((pw, ph))
        
        page_fields = fields_by_page.get(page_num, [])
        
        for entry in page_fields:
            mapped_to = entry.get("mapped_to", "")
            font_size = entry.get("font_size", 10)
            
            # --- Resolve coordinates: percentage-based (new) or absolute (legacy) ---
            x_pct = entry.get("x_pct")
            y_pct = entry.get("y_pct")
            x_abs = entry.get("x")
            y_abs = entry.get("y")
            
            if x_pct is not None and y_pct is not None:
                # Percentage-based: convert to absolute PDF points
                # x_pct: 0=left, 100=right → multiply by page width
                # y_pct: 0=TOP, 100=BOTTOM → invert for PDF (origin = bottom-left)
                x = float(x_pct) / 100.0 * pw
                y = (1.0 - float(y_pct) / 100.0) * ph  # Invert Y axis (top→bottom to bottom→top)
                width_abs = float(entry.get("width_pct", 30)) / 100.0 * pw
            elif x_abs is not None and y_abs is not None:
                # Legacy absolute PDF points
                x = float(x_abs)
                y = float(y_abs)
                width_abs = float(entry.get("width", 200))
            else:
                continue  # No coordinates, skip
            
            if not mapped_to:
                continue
            
            # Language filtering
            if lg_language:
                field_lang = (entry.get("language") or "shared").lower()
                if field_lang not in ("shared", lg_language.lower()[:2]):
                    continue
            
            field_type = entry.get("field_type", "text").lower()
            date_format = entry.get("date_format")
            fill_strategy = entry.get("fill_strategy", "").lower()
            
            # Get value first (needed for strikethrough check)
            value = request_data.get(mapped_to, "")
            if value is None:
                value = ""
            
            # --- Strikethrough strategy: draw a line to cross out text ---
            if fill_strategy == "strikethrough":
                is_active = bool(value) and str(value).lower() not in ("", "0", "false", "no", "none")
                if is_active:
                    line_y = y + font_size * 0.35  # center of text
                    c.setStrokeColorRGB(0, 0, 0)
                    c.setLineWidth(1.5)
                    c.line(x, line_y, x + width_abs, line_y)
                    filled_count += 1
                    logger.debug(f"Overlay: STRIKETHROUGH ({x:.0f},{y:.0f}) w={width_abs:.0f} p{page_num} [{mapped_to}]")
                continue
            
            # Format value based on type
            if field_type == "checkbox":
                is_checked = bool(value) and str(value).lower() not in ("", "0", "false", "no", "none")
                value = "✓" if is_checked else ""
            elif field_type == "date" or isinstance(value, date):
                if isinstance(value, str) and value:
                    try:
                        from datetime import datetime as dt
                        parsed = dt.strptime(value.strip()[:10], "%Y-%m-%d").date()
                        value = _format_date(parsed, date_format)
                    except (ValueError, TypeError):
                        pass
                elif isinstance(value, date):
                    value = _format_date(value, date_format)
            elif isinstance(value, (int, float, Decimal)):
                value = f"{float(value):,.2f}" if isinstance(value, (float, Decimal)) else str(value)
            
            value = str(value)
            if not value:
                continue
            
            # Draw text at the specified position
            c.setFont("Helvetica", font_size)
            c.drawString(x, y, value)
            filled_count += 1
            logger.debug(
                f"Overlay: p{page_num} '{mapped_to}' → '{value[:30]}' | "
                f"pct=({x_pct or x_abs},{y_pct or y_abs}) → pts=({x:.1f},{y:.1f}) "
                f"page=({pw:.0f}x{ph:.0f}) fs={font_size}"
            )
        
        c.showPage()  # Move to next page
    
    c.save()
    output.seek(0)
    overlay_bytes = output.read()
    
    logger.info(f"Generated overlay PDF: {filled_count} fields placed, {page_num + 1} pages ({len(overlay_bytes)} bytes)")
    return overlay_bytes


def generate_scanned_fill_pdf(
    template_pdf_bytes: bytes,
    field_mapping: List[Dict[str, Any]],
    request_data: Dict[str, Any],
    lg_language: str = None,
) -> bytes:
    """
    Generates a filled PDF by merging a text overlay onto the original scanned form.
    
    Unlike PHYSICAL_OVERLAY (which produces a blank text-only page for printing
    on top of a physical form), this merges the text directly onto the scanned
    PDF image — the result is a complete, filled-looking PDF.
    
    Used when: the form is non-fillable (scanned/flat) but the bank accepts
    a printed copy (not requiring the physical pre-printed form).
    """
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        from PyPDF2 import PdfReader, PdfWriter
    
    # Step 1: Generate the text-only overlay
    overlay_bytes = generate_overlay_pdf(template_pdf_bytes, field_mapping, request_data, lg_language=lg_language)
    
    # Step 2: Merge overlay onto original scanned pages
    original_reader = PdfReader(io.BytesIO(template_pdf_bytes))
    overlay_reader = PdfReader(io.BytesIO(overlay_bytes))
    writer = PdfWriter()
    
    for page_num in range(len(original_reader.pages)):
        base_page = original_reader.pages[page_num]
        
        # Merge overlay page if it exists
        if page_num < len(overlay_reader.pages):
            base_page.merge_page(overlay_reader.pages[page_num])
        
        writer.add_page(base_page)
    
    # Write merged result
    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    merged_bytes = output.read()
    
    logger.info(f"Generated scanned-fill PDF: merged overlay onto {len(original_reader.pages)} pages ({len(merged_bytes)} bytes)")
    return merged_bytes
