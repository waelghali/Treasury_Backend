# app/services/issuance_service.py

from typing import List, Optional, Tuple, Dict, Any
from decimal import Decimal
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from sqlalchemy import func

from app.crud.crud_issuance import crud_issuance_request, crud_issuance_facility
from app.models_issuance import IssuanceRequest, IssuanceFacility, IssuanceFacilitySubLimit, IssuedLGRecord
from app.schemas.schemas_issuance import IssuanceRequestUpdate

from datetime import date

class IssuanceService:
    
    # ==========================================================================
    # 1. UTILIZATION LOGIC (The "Engine")
    # ==========================================================================

    def calculate_facility_utilization(self, db: Session, facility_id: int) -> Dict[int, Dict[str, Decimal]]:
        """
        Calculates the used and available amounts for ALL sub-limits in a facility.
        Returns a Dictionary: { sub_limit_id: { "limit": X, "used": Y, "available": Z } }
        """
        # 1. Get the Facility and its Sub-Limits
        facility = crud_issuance_facility.get(db, id=facility_id)
        if not facility:
            raise HTTPException(status_code=404, detail="Facility not found")

        result_map = {}

        for sub_limit in facility.sub_limits:
            # 2. Query Sum of all ACTIVE Issued LGs linked to this Sub-Limit
            # Note: We sum 'current_amount' from IssuedLGRecord
            used_amount = db.query(func.sum(IssuedLGRecord.current_amount)).filter(
                IssuedLGRecord.facility_sub_limit_id == sub_limit.id,
                IssuedLGRecord.status == "ACTIVE"
            ).scalar() or Decimal(0)

            # 3. Query Sum of all PENDING Requests (Approved but not yet Issued)
            # This is crucial to prevent "double spending" the limit
            pending_amount = db.query(func.sum(IssuanceRequest.amount)).filter(
                IssuanceRequest.selected_facility_id == facility.id, # We'll need to link requests to sub-limits eventually
                IssuanceRequest.status.in_(["APPROVED_INTERNAL", "PROCESSING_BANK"]),
                IssuanceRequest.transaction_type == "NEW_ISSUANCE"
            ).scalar() or Decimal(0)

            total_used = used_amount + pending_amount
            available = sub_limit.limit_amount - total_used

            result_map[sub_limit.id] = {
                "limit_name": sub_limit.limit_name,
                "total_limit": sub_limit.limit_amount,
                "used_amount": total_used,
                "available_amount": available
            }

        return result_map

    # ==========================================================================
    # 2. WORKFLOW ACTIONS
    # ==========================================================================

    def submit_for_approval(self, db: Session, request_id: int, user_id: int) -> IssuanceRequest:
        """ Moves a request from DRAFT to PENDING_APPROVAL. """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        
        if request.status != "DRAFT":
            raise HTTPException(status_code=400, detail="Only DRAFT requests can be submitted")
        
        updated_request = crud_issuance_request.update(
            db, db_obj=request, obj_in=IssuanceRequestUpdate(status="PENDING_APPROVAL")
        )
        return updated_request

    def approve_request(self, db: Session, request_id: int, approver_user_id: int) -> IssuanceRequest:
        """ Moves a request from PENDING_APPROVAL to APPROVED_INTERNAL. """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")

        if request.status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Request is not pending approval")
        
        updated_request = crud_issuance_request.update(
            db, db_obj=request, obj_in=IssuanceRequestUpdate(status="APPROVED_INTERNAL")
        )
        return updated_request

    def reject_request(self, db: Session, request_id: int, user_id: int) -> IssuanceRequest:
        """ Rejects the request. """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")

        updated_request = crud_issuance_request.update(
            db, db_obj=request, obj_in=IssuanceRequestUpdate(status="REJECTED")
        )
        return updated_request

    # ==========================================================================
    # 3. INTELLIGENT SELECTION
    # ==========================================================================

    def find_suitable_facilities(self, db: Session, request_id: int) -> List[Dict[str, Any]]:
        """
        THE SMART ENGINE: Finds facilities that match the request criteria AND have available limit.
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")

        # 1. Fetch all active facilities for this customer
        facilities = crud_issuance_facility.get_multi_by_customer(db, customer_id=request.customer_id)
        
        suitable_options = []

        for facility in facilities:
            # Check 1: Currency match
            if facility.currency_id != request.currency_id:
                continue

            # Check 2: Calculate Utilization for this facility
            # This returns the REAL available amounts
            utilization_map = self.calculate_facility_utilization(db, facility.id)

            # Check 3: Check each Sub-Limit against the Request Amount
            for sub_limit in facility.sub_limits:
                stats = utilization_map.get(sub_limit.id)
                if not stats:
                    continue

                available = stats["available_amount"]
                
                # Logic: Does the sub-limit have enough money?
                if available >= request.amount:
                    suitable_options.append({
                        "facility_id": facility.id,
                        "facility_bank": facility.bank.name,
                        "facility_ref": facility.reference_number,
                        "sub_limit_id": sub_limit.id,
                        "sub_limit_name": sub_limit.limit_name,
                        "limit_total": sub_limit.limit_amount,
                        "limit_available": available, # <-- This is the key value for the user!
                        "price_commission": sub_limit.default_commission_rate,
                        "cash_margin": sub_limit.default_cash_margin_pct
                    })
        
        # Sort options: Cheapest Commission first, then Highest Availability
        suitable_options.sort(key=lambda x: (x['price_commission'] or 100, -x['limit_available']))
        
        return suitable_options

    def execute_issuance(
        self, 
        db: Session, 
        request_id: int, 
        user_id: int, 
        selected_facility_sub_limit_id: int,
        issued_ref_number: str,
        issue_date: date,
        expiry_date: Optional[date] = None
    ) -> IssuedLGRecord:
        """
        Finalizes the request:
        1. Creates the Master IssuedLGRecord.
        2. Links the Request to it.
        3. Updates Request status to ISSUED.
        """
        # 1. Get the Approved Request
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        
        if request.status != "APPROVED_INTERNAL":
             raise HTTPException(status_code=400, detail="Request must be APPROVED_INTERNAL before issuance.")

        # 2. Create the Master Record (The "Live" LG)
        # Note: We link it to the specific Sub-Limit chosen by the user
        new_lg_record = IssuedLGRecord(
            lg_ref_number=issued_ref_number,
            customer_id=request.customer_id,
            facility_sub_limit_id=selected_facility_sub_limit_id,
            beneficiary_name=request.beneficiary_name,
            current_amount=request.amount,
            currency_id=request.currency_id,
            issue_date=issue_date,
            expiry_date=expiry_date,
            status="ACTIVE"
        )
        db.add(new_lg_record)
        db.flush() # Get the ID
        
        # 3. Link Request to Record and Close it
        # This is CRITICAL: This is what makes the utilization calculation work later
        request.lg_record_id = new_lg_record.id
        request.status = "ISSUED"
        db.add(request)
        
        db.commit()
        db.refresh(new_lg_record)
        return new_lg_record

    def prepare_application_document_data(self, db: Session, request_id: int) -> Dict[str, Any]:
        """
        Prepares the data dictionary required to fill the HTML template.
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")

        if not request.selected_facility_id:
             raise HTTPException(status_code=400, detail="No facility selected. Cannot generate application without knowing the bank.")
             
        facility = crud_issuance_facility.get(db, id=request.selected_facility_id)
        
        # Format currency amounts
        formatted_amount = f"{request.amount:,.2f}"
        
        data = {
            "date": date.today().strftime("%d-%b-%Y"),
            "bank_name": facility.bank.name,
            "bank_address": facility.bank.address or "Head Office",
            "company_name": request.customer.name,
            "beneficiary_name": request.beneficiary_name,
            "currency": request.currency.code,
            "amount": formatted_amount,
            "expiry_date": request.requested_expiry_date.strftime("%d-%b-%Y") if request.requested_expiry_date else "Open Ended",
            "purpose": request.business_details.get("project_name", "General Business Operations"),
            "ref_number": f"REQ-{request.id}",
            "tender_ref": request.business_details.get("tender_reference", "N/A"),
            # Add specific clauses if they exist in business details
            "special_conditions": request.business_details.get("special_clauses", "As per standard bank text.")
        }
        return data

    def render_application_html(self, data: Dict[str, Any]) -> str:
        """
        Constructs a simple HTML string for the bank application.
        In the future, this should load a Jinja2 template from the 'Template' table.
        """
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Helvetica', sans-serif; font-size: 12pt; padding: 40px; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .title {{ font-size: 16pt; font-weight: bold; text-decoration: underline; }}
                .content {{ line-height: 1.6; text-align: justify; }}
                .field {{ font-weight: bold; }}
                .signature-section {{ margin-top: 60px; }}
                .row {{ display: flex; justify-content: space-between; margin-top: 40px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <div class="title">Application for Letter of Guarantee</div>
                <div>Date: {data['date']}</div>
            </div>

            <div class="content">
                <p>To: <strong>{data['bank_name']}</strong><br>
                {data['bank_address']}</p>

                <p>Dear Sirs,</p>

                <p>Please issue a Letter of Guarantee on our behalf with the following details:</p>

                <ul>
                    <li><span class="field">Beneficiary:</span> {data['beneficiary_name']}</li>
                    <li><span class="field">Amount:</span> {data['currency']} {data['amount']}</li>
                    <li><span class="field">Expiry Date:</span> {data['expiry_date']}</li>
                    <li><span class="field">Purpose/Project:</span> {data['purpose']}</li>
                    <li><span class="field">Tender Reference:</span> {data['tender_ref']}</li>
                </ul>

                <p><strong>Special Conditions / Text Requirements:</strong><br>
                {data['special_conditions']}</p>

                <p>Please debit our account for the applicable commission and margin as per our facility agreement.</p>
            </div>

            <div class="signature-section">
                <p>Sincerely,</p>
                <br><br>
                <p>__________________________<br>
                <strong>{data['company_name']}</strong><br>
                Authorized Signatory</p>
            </div>
        </body>
        </html>
        """
        return html_content

issuance_service = IssuanceService()