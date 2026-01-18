# app/services/issuance_service.py

from typing import List, Optional, Tuple, Dict, Any
from decimal import Decimal
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from sqlalchemy import func

from app.crud.crud_issuance import crud_issuance_request, crud_issuance_facility
from app.models_issuance import IssuanceRequest, IssuanceFacility, IssuanceFacilitySubLimit, IssuedLGRecord, IssuanceWorkflowPolicy, BankIssuanceOption
from app.models_reconciliation import BankPositionBatch, BankPositionRow
from app.schemas.schemas_issuance import IssuanceRequestUpdate, SuitableFacilityOut, BankIssuanceOptionOut
from app.core.issuance_strategies import IssuanceStrategyFactory

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
        """ 
        Moves request from DRAFT to PENDING_APPROVAL. 
        Calculates the approval chain based on Workflow Policies.
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        
        if request.status != "DRAFT":
            raise HTTPException(status_code=400, detail="Only DRAFT requests can be submitted")

        # 1. Determine Policies based on Amount
        policies = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == request.customer_id,
            IssuanceWorkflowPolicy.min_amount <= request.amount,
            (IssuanceWorkflowPolicy.max_amount == None) | (IssuanceWorkflowPolicy.max_amount >= request.amount)
        ).order_by(IssuanceWorkflowPolicy.step_sequence.asc()).all()

        next_status = "PENDING_APPROVAL"
        next_role = None
        next_step = 0

        # 2. Assign First Approver
        if policies:
            first_policy = policies[0]
            next_role = first_policy.approver_role_name
            next_step = first_policy.step_sequence
        else:
            # Fallback: If no policies defined, auto-route to default Admin or just mark pending
            next_role = "CORPORATE_ADMIN" 
            next_step = 1

        updated_request = crud_issuance_request.update(
            db, db_obj=request, obj_in=IssuanceRequestUpdate(
                status=next_status
            )
        )
        # Update extra fields manually as they might not be in the Schema yet
        updated_request.current_approval_step = next_step
        updated_request.pending_approver_role = next_role
        db.add(updated_request)
        db.commit()
        
        return updated_request

    def approve_request(self, db: Session, request_id: int, approver_user_id: int) -> IssuanceRequest:
        """ 
        Approves the current step. 
        If there is a next step in the policy, moves to that.
        If last step, moves to APPROVED_INTERNAL.
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")

        if request.status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Request is not pending approval")
        
        # 1. Check for Next Step
        current_step = request.current_approval_step or 0
        
        next_policy = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == request.customer_id,
            IssuanceWorkflowPolicy.min_amount <= request.amount,
            (IssuanceWorkflowPolicy.max_amount == None) | (IssuanceWorkflowPolicy.max_amount >= request.amount),
            IssuanceWorkflowPolicy.step_sequence > current_step
        ).order_by(IssuanceWorkflowPolicy.step_sequence.asc()).first()

        # 2. Log this approval (Audit)
        audit_entry = {
            "step": current_step,
            "approver_id": approver_user_id,
            "role": request.pending_approver_role,
            "timestamp": str(date.today())
        }
        current_audit = request.approval_chain_audit or []
        current_audit.append(audit_entry)
        request.approval_chain_audit = current_audit

        # 3. Transition
        if next_policy:
            # Move to next approver
            request.current_approval_step = next_policy.step_sequence
            request.pending_approver_role = next_policy.approver_role_name
            # Status remains PENDING_APPROVAL
        else:
            # Final Approval
            request.status = "APPROVED_INTERNAL"
            request.pending_approver_role = None

        db.add(request)
        db.commit()
        db.refresh(request)
        return request

    # ==========================================================================
    # 3. SMART FACILITY SELECTION
    # ==========================================================================

    def get_suitable_facilities(self, db: Session, request_id: int) -> List[SuitableFacilityOut]:
        """
        Smart Engine v2:
        - Iterates through SUB-LIMITS (e.g., "Standard LGs", "Bid Bonds").
        - Calculates Costs (Commission, Margin).
        - Generates Tags ("BEST_PRICE", "NO_MARGIN").
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")

        # 1. Fetch Facilities
        facilities = crud_issuance_facility.get_multi_by_customer(db, customer_id=request.customer_id)
        
        candidates = []
        
        for fac in facilities:
            # Basic Facility Filters
            if fac.currency_id != request.currency_id: 
                continue
            if fac.expiry_date and fac.expiry_date < date.today(): 
                continue

            # 2. Iterate Sub-Limits (The actual buckets where price lives)
            for sub in fac.sub_limits:
                # Filter: Does this sub-limit support the requested LG Type?
                # (Assuming sub.lg_type_id matches request.business_details['lg_type_id'] or similar)
                # For now, allowing all sub-limits to be candidates if they have space.

                # Calculate Usage
                # Note: In a real scenario, you'd sum up 'IssuedLGRecord' for this sub_limit_id
                # For this snippet, we assume full sub-limit is available for simplicity or fetch dynamic usage
                # usage = self.get_sub_limit_usage(db, sub.id) 
                # available = sub.limit_amount - usage
                available = sub.limit_amount # Placeholder: assuming empty for now

                if available < request.amount:
                    continue

                # 3. Calculate Financials
                comm_rate = sub.default_commission_rate or 0.0
                margin_pct = sub.default_cash_margin_pct or 0.0
                
                est_comm = float(request.amount) * (comm_rate / 100.0)
                req_margin = float(request.amount) * (margin_pct / 100.0)

                # 4. Generate Tags
                tags = []
                if margin_pct == 0:
                    tags.append("NO_MARGIN")
                if comm_rate < 1.0: # Arbitrary threshold for "Cheap"
                    tags.append("COMPETITIVE_RATE")
                if fac.sla_agreement_days and fac.sla_agreement_days <= 2:
                    tags.append("FAST_TRACK")

                # --- NEW: Fetch Issuance Options for this Bank ---
                bank_options = db.query(BankIssuanceOption).filter(
                    BankIssuanceOption.bank_id == fac.bank_id,
                    BankIssuanceOption.is_active == True
                ).all()

                method_dtos = [
                    BankIssuanceOptionOut(
                        id=opt.id, 
                        display_name=opt.display_name, 
                        strategy_code=opt.strategy_code
                    ) for opt in bank_options
                ]

                candidates.append(SuitableFacilityOut(
                    facility_id=fac.id,
                    available_methods=method_dtos,
                    facility_bank=fac.bank.name,
                    sub_limit_id=sub.id,
                    sub_limit_name=sub.limit_name,
                    limit_available=float(available),
                    
                    price_commission_rate=comm_rate,
                    price_cash_margin_pct=margin_pct,
                    estimated_commission_cost=est_comm,
                    required_cash_margin_amount=req_margin,
                    
                    recommendation_tags=tags
                ))

        # 5. Ranking Logic (Sort by Cheapest Commission)
        # Identify the absolute best price to tag it
        if candidates:
            lowest_cost = min(c.estimated_commission_cost for c in candidates)
            for c in candidates:
                if c.estimated_commission_cost == lowest_cost:
                    c.recommendation_tags.insert(0, "BEST_PRICE")

        # Sort: "BEST_PRICE" first, then by Cost Ascending
        candidates.sort(key=lambda x: x.estimated_commission_cost)
        
        return candidates


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
        
    # ==========================================================================
    # 4. FINAL EXECUTION (ISSUANCE)
    # ==========================================================================

    async def issue_lg_from_request(self, db: Session, request_id: int, facility_id: int, issuance_option_id: int, user_id: int):
        """
        Executes Issuance using the specific Option selected by the User.
        """
        request = crud_issuance_request.get(db, id=request_id)
        facility = crud_issuance_facility.get(db, id=facility_id)

        if request.status != "APPROVED_INTERNAL":
             raise HTTPException(status_code=400, detail="Request must be internally approved first")

        # 1. Fetch the Specific Option the User Chose
        option = db.query(BankIssuanceOption).filter(BankIssuanceOption.id == issuance_option_id).first()
        if not option:
            raise HTTPException(status_code=400, detail="Invalid issuance method selected")

        # 2. Delegate to Strategy Factory using the OPTION's code
        # e.g., if code is 'BANK_API_V1', we get the BankApiStrategy class
        strategy = IssuanceStrategyFactory.get_strategy(option.strategy_code)
        
        # 3. Execute with Specific Config
        # We pass option.configuration (e.g. { "api_url": "..." }) instead of the generic facility config
        execution_result = await strategy.execute(db, request, facility, option.configuration)
        
        # 4. Create the Record (Common Part)
        new_lg = IssuedLGRecord(
            customer_id=request.customer_id,
            facility_id=facility.id,
            lg_type_id=1,
            ref_number=f"TEMP-{request.id}", 
            status="ACTIVE",
            amount=request.amount,
            currency_id=request.currency_id,
            issue_date=date.today(),
            expiry_date=request.requested_expiry_date,
            beneficiary_name=request.beneficiary_name,
            created_by_user_id=user_id
        )
        
        # 5. Handle Artifacts (e.g. Save PDF path if generated)
        if execution_result.get("output_type") == "FILE":
            # Assuming you might want to save the path in the business_details or a new column
            # new_lg.document_path = execution_result["output_data"]
            pass

        db.add(new_lg)
        
        # 6. Close the Request
        request.status = "COMPLETED"
        request.lg_record_id = new_lg.id
        request.selected_issuance_option_id = option.id # Save the choice history
        
        db.add(request)
        db.commit()
        db.refresh(new_lg)
        
        return {
            "lg_record": new_lg,
            "execution_result": result # Contains the PDF bytes if generated
        }

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
    # 5. RECONCILIATION SERVICE
    # ==========================================================================

    def process_reconciliation_batch(self, db: Session, batch_id: int):
        """
        Iterates through uploaded bank rows and matches them against IssuedLGRecords.
        Updates row status: MATCHED, MISMATCH, or MISSING_IN_SYSTEM.
        """
        # 1. Get the Batch
        batch = db.query(BankPositionBatch).filter(BankPositionBatch.id == batch_id).first()
        if not batch:
            raise HTTPException(status_code=404, detail="Reconciliation Batch not found")

        # 2. Get all rows in this batch
        rows = db.query(BankPositionRow).filter(BankPositionRow.batch_id == batch_id).all()
        
        matched_count = 0
        
        for row in rows:
            # 3. Clean and Search Reference
            # Strip whitespace to ensure clean match
            clean_ref = str(row.ref_number).strip()
            
            # Find the system record
            system_record = db.query(IssuedLGRecord).filter(
                IssuedLGRecord.lg_ref_number == clean_ref,
                IssuedLGRecord.customer_id == batch.bank_id  # Assuming validation: Bank should match facility bank, optional check
            ).first()
            
            # Note: Ideally we check if system_record.facility.bank_id == batch.bank_id, 
            # but simpler lookup on unique Ref Number is usually sufficient.
            system_record = db.query(IssuedLGRecord).filter(IssuedLGRecord.lg_ref_number == clean_ref).first()

            if not system_record:
                row.recon_status = "MISSING_IN_SYSTEM"
                row.recon_note = f"Ref '{clean_ref}' does not exist in our Issued Records."
            
            else:
                # 4. Compare Financials (Amount)
                # Using a small epsilon for float/decimal comparison safety
                diff = abs(float(system_record.current_amount) - float(row.amount))
                
                if diff < 0.01:
                    row.recon_status = "MATCHED"
                    row.recon_note = "Perfect match."
                    matched_count += 1
                else:
                    row.recon_status = "MISMATCH"
                    row.recon_note = f"Amount mismatch. Bank: {row.amount:,.2f}, System: {system_record.current_amount:,.2f}"

                # Optional: Check Validity Status
                # If Bank says 'Expired' but we say 'Active', flag it? 
                # (Can be added here later)

        # 5. Update Batch Statistics
        batch.total_records = len(rows)
        batch.matched_records = matched_count
        
        db.commit()
        db.refresh(batch)
        
        return {
            "batch_id": batch.id,
            "total": batch.total_records,
            "matched": batch.matched_records,
            "status": "COMPLETED"
        }

issuance_service = IssuanceService()