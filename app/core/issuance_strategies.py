# app/core/issuance_strategies.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from app.models_issuance import IssuanceRequest, IssuanceFacility, IssuedLGRecord
from sqlalchemy.orm import Session
from app.core.document_generator import generate_pdf_from_html
import datetime

class IssuanceStrategy(ABC):
    @abstractmethod
    async def execute(self, db: Session, request: IssuanceRequest, facility: IssuanceFacility, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns a dict. 
        If output_type is 'BYTES', output_data will be the raw PDF bytes.
        """
        pass

class ManualPdfStrategy(IssuanceStrategy):
    async def execute(self, db: Session, request: IssuanceRequest, facility: IssuanceFacility, config: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Load Template (Assumed to be a string in config or fetched from DB)
        # In a real scenario, you might query a 'Template' table here using config['template_id']
        # For this snippet, we assume the HTML string is passed or hardcoded for the demo
        template_html = config.get("template_html", "<html><body><p>Error: No Template Found</p></body></html>")
        
        # 2. Prepare Data (The "Custody" way - Flat Dictionary)
        # specific logic to format dates/amounts as user expects
        data = {
            "{{ref_number}}": f"REQ-{request.id}",
            "{{date}}": datetime.date.today().strftime("%d-%b-%Y"),
            "{{bank_name}}": facility.bank.name,
            "{{beneficiary_name}}": request.beneficiary_name,
            "{{amount}}": f"{request.amount:,.2f}",
            "{{currency}}": str(request.currency_id), # Ideally fetch Currency Code (USD/EUR)
            "{{expiry_date}}": request.requested_expiry_date.strftime("%d-%b-%Y") if request.requested_expiry_date else "Open Ended",
        }

        # Add business details (Custom fields)
        if request.business_details:
             for k, v in request.business_details.items():
                 data[f"{{{{details.{k}}}}}]"] = str(v)

        # 3. Simple Replacement (No Jinja2)
        filled_html = template_html
        for key, value in data.items():
            filled_html = filled_html.replace(key, value)

        # 4. Generate PDF Bytes (Async)
        pdf_bytes = await generate_pdf_from_html(filled_html, filename_hint=f"req_{request.id}")
        
        if not pdf_bytes:
            raise Exception("Failed to generate PDF")

        return {
            "status": "WAITING_DELIVERY",
            "output_type": "BYTES",
            "output_data": pdf_bytes, # RAW BYTES
            "filename": f"LG_Request_{request.id}.pdf",
            "message": "PDF Generated."
        }

# --- STRATEGY 2: API INTEGRATION (Bank B) ---
class BankApiStrategy(IssuanceStrategy):
    def execute(self, db: Session, request: IssuanceRequest, facility: IssuanceFacility, user_id: int) -> Dict[str, Any]:
        endpoint = facility.issuance_method_config.get("api_endpoint")
        
        # 1. Call External Bank API
        # response = requests.post(endpoint, json=request.business_details)
        # bank_ref = response.json().get("id")
        bank_ref = f"BANK-API-{request.id}" # Placeholder
        
        return {
            "status": "PROCESSING_BANK", # Automatically sent
            "output_type": "API_REF",
            "output_data": bank_ref,
            "message": "Request submitted to Bank Portal. Authorized Signatory must approve there."
        }

# --- STRATEGY 3: PRE-PRINTED FORM (Bank D) ---
class PrePrintedFormStrategy(IssuanceStrategy):
    def execute(self, db: Session, request: IssuanceRequest, facility: IssuanceFacility, user_id: int) -> Dict[str, Any]:
        # 1. Generate PDF with NO Background (Just text overlay)
        # coordinate_map = facility.issuance_method_config.get("coordinates")
        pdf_path = f"generated_lgs/overlay_{request.id}.pdf"
        
        return {
            "status": "WAITING_PRINT",
            "output_type": "FILE",
            "output_data": pdf_path,
            "message": "Overlay generated. Insert Bank Form in printer tray 1."
        }

# --- THE FACTORY ---
class IssuanceStrategyFactory:
    @staticmethod
    def get_strategy(method_name: str) -> IssuanceStrategy:
        if method_name == "MANUAL_PDF":
            return ManualPdfStrategy()
        elif method_name == "BANK_API_V1":
            return BankApiStrategy()
        elif method_name == "PRE_PRINTED_FORM":
            return PrePrintedFormStrategy()
        else:
            # Default fallback
            return ManualPdfStrategy()