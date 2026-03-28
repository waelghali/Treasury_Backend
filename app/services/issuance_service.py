# app/services/issuance_service.py

import logging
logger = logging.getLogger(__name__)

from typing import List, Optional, Tuple, Dict, Any
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from fastapi import HTTPException, status
from sqlalchemy import func

from app.crud.crud_issuance import crud_issuance_request
from app.crud.crud_facility import crud_facility
from app.models.models_issuance import (
    IssuanceRequest, IssuanceFacility, IssuanceFacilitySubLimit, 
    IssuedLGRecord, IssuanceWorkflowPolicy, BankIssuanceOption,
    IssuanceExposureEntry
)
from app.models.models import CurrencyExchangeRate, Currency
from app.models.models_reconciliation import BankPositionBatch, BankPositionRow
from app.schemas.schemas_issuance import IssuanceRequestUpdate, SuitableFacilityOut, BankIssuanceOptionOut
from app.core.issuance_strategies import IssuanceStrategyFactory
from app.crud.base import log_action

from datetime import date

class IssuanceService:
    
    # ==========================================================================
    # 0. UTILITIES
    # ==========================================================================

    def _get_user_label(self, db: Session, user_id: int) -> str:
        """Resolve user_id to a display name for audit trail entries."""
        if not user_id:
            return None
        from app.models import User
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            return user.email
        return f"User #{user_id}"

    def _get_fx_rate(self, db: Session, from_currency_id: int, to_currency_code: str = "EGP") -> Decimal:
        """
        LEGACY WRAPPER — delegates to the centralized FxService.
        Kept for backward compatibility with callers that use the old signature.
        Returns 1.0 if same currency OR if no rate found (preserves old behavior).
        """
        # Resolve to_currency_code to an ID for the new service
        to_currency = db.query(Currency).filter(
            Currency.iso_code == to_currency_code
        ).first()
        if not to_currency:
            logger.warning(f"Currency code '{to_currency_code}' not found. Using 1.0")
            return Decimal("1.0")

        from app.services.fx_service import fx_service
        rate = fx_service.get_rate(
            db, from_currency_id, to_currency.id,
            allow_ai=False  # Legacy callers don't expect AI calls
        )
        return rate if rate is not None else Decimal("1.0")

    def _generate_lg_serial(self, db: Session, customer_id: int, issuing_entity_id: int = None) -> str:
        """
        Generates an auto-incrementing internal serial number for an issued LG.
        Format: YYYY-XXXX-NNNNSSS
          YYYY = current year
          XXXX = entity code (from CustomerEntity.code, e.g., "ACME")
          NNNN = LG sequence per entity per year (0001, 0002, ...)
          SSS  = sub-serial (always 000 for original issuance)
        """
        from datetime import datetime as dt
        from app.models import CustomerEntity, Customer

        current_year = dt.now().year

        # Resolve entity code
        entity_code = None
        if issuing_entity_id:
            entity = db.query(CustomerEntity).filter(CustomerEntity.id == issuing_entity_id).first()
            if entity and entity.code:
                entity_code = entity.code.upper()

        if not entity_code:
            # Fallback: first 4 chars of customer name
            customer = db.query(Customer).filter(Customer.id == customer_id).first()
            if customer:
                entity_code = customer.name.replace(" ", "").upper()[:4].ljust(4, "X")
            else:
                entity_code = "XXXX"

        # Find the highest existing serial for this entity+year
        prefix = f"{current_year}-{entity_code}-"
        last_serial = db.query(IssuedLGRecord.internal_serial).filter(
            IssuedLGRecord.internal_serial.like(f"{prefix}%"),
            IssuedLGRecord.customer_id == customer_id,
        ).order_by(IssuedLGRecord.internal_serial.desc()).first()

        if last_serial and last_serial[0]:
            try:
                # Extract NNNNSSS part and get NNNN
                serial_part = last_serial[0].split("-", 2)[2]  # "0001000"
                last_seq = int(serial_part[:4])
                next_seq = last_seq + 1
            except (IndexError, ValueError):
                next_seq = 1
        else:
            next_seq = 1

        return f"{current_year}-{entity_code}-{next_seq:04d}000"

    # ==========================================================================
    # 1. UTILIZATION LOGIC (The "Engine")
    # ==========================================================================

    def calculate_facility_utilization(self, db: Session, facility_id: int) -> Dict[int, Dict[str, Decimal]]:
        """
        Calculates the used and available amounts for ALL sub-limits in a facility.
        Returns a Dictionary: { sub_limit_id: { "limit": X, "used": Y, "available": Z } }
        """
        # 1. Get the Facility and its Sub-Limits
        facility = crud_facility.get(db, id=facility_id)
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
                IssuanceRequest.selected_sub_limit_id == sub_limit.id,
                IssuanceRequest.status.in_(["APPROVED_INTERNAL", "FACILITY_RESERVED", "INTERNAL_PROCESSING"]),
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
    # 2. WORKFLOW ACTIONS (The Advanced Matrix Engine)
    # ==========================================================================

    def _evaluate_condition(self, db: Session, request: IssuanceRequest, policy: IssuanceWorkflowPolicy) -> bool:
        """Evaluates if a specific workflow step applies to this request.
        
        For AMOUNT_OVER and AMOUNT_RANGE: if the policy has a currency_id set,
        the request amount is converted to that currency before comparison.
        If FX conversion fails, the condition is treated as True (fail-safe:
        approval is required when we can't determine the amount).
        """
        if policy.condition_type in ["ALWAYS", "ANY_DEPARTMENT"]:
            return True

        if policy.condition_type == "AMOUNT_OVER":
            try:
                threshold = Decimal(str(policy.condition_value))
                comparison_amount = self._get_fx_adjusted_amount(db, request, policy)
                if comparison_amount is None:
                    return True  # Fail-safe: can't convert → require approval
                return comparison_amount > threshold
            except Exception:
                return False

        if policy.condition_type == "AMOUNT_RANGE":
            try:
                # Handle formats: "MIN-MAX", "MIN,MAX", "(MIN,MAX)", "(MIN-MAX)"
                raw = str(policy.condition_value).strip().strip("()")
                # Try comma first (UI format), then dash
                if "," in raw:
                    parts = raw.split(",")
                else:
                    parts = raw.split("-")
                min_val = Decimal(parts[0].strip()) if parts[0].strip() else Decimal("0")
                max_val = Decimal(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else None
                
                comparison_amount = self._get_fx_adjusted_amount(db, request, policy)
                if comparison_amount is None:
                    return True  # Fail-safe: can't convert → require approval
                
                if comparison_amount < min_val:
                    return False
                if max_val is not None and comparison_amount > max_val:
                    return False
                return True
            except Exception:
                return False
                
        if policy.condition_type == "DEPT_MATCH":
            # condition_value stores the department ID, request.department stores the name
            # Look up the department by name to get its ID for a proper comparison
            from app.models.models import Department
            from sqlalchemy import func as sa_func
            req_dept_name = request.department
            if not req_dept_name:
                return False
            dept = db.query(Department).filter(
                Department.customer_id == request.customer_id,
                sa_func.lower(Department.name) == str(req_dept_name).lower(),
                Department.is_deleted == False
            ).first()
            if dept:
                return str(dept.id) == str(policy.condition_value)
            return False
            
        if policy.condition_type == "CROSS_BORDER":
            return bool(request.is_cross_border)

        if policy.condition_type == "THIRD_PARTY":
            return bool(request.is_third_party)

        if policy.condition_type == "REFERENCE_TYPE_MATCH":
            # condition_value stores the reference type name (e.g., "Contract", "Project")
            if not request.reference_type or not policy.condition_value:
                return False
            return str(request.reference_type).lower() == str(policy.condition_value).lower()
            
        return False

    def _get_fx_adjusted_amount(self, db: Session, request: IssuanceRequest, policy: IssuanceWorkflowPolicy) -> Optional[Decimal]:
        """
        Returns the request amount converted to the policy's currency.
        If the policy has no currency_id, returns the raw request amount.
        If FX conversion fails, returns None (caller decides: usually fail-safe → True).
        """
        if not policy.currency_id or not request.currency_id:
            return request.amount

        # Same currency → no conversion needed
        if policy.currency_id == request.currency_id:
            return request.amount

        # Convert request amount to policy currency
        from app.services.fx_service import fx_service
        converted, rate = fx_service.convert(
            db,
            Decimal(str(request.amount)),
            request.currency_id,
            policy.currency_id,
            allow_ai=False,  # Approval evaluation should be fast — CBE only
        )

        if converted is not None:
            logger.debug(
                f"FX-adjusted amount: {request.amount} (currency_id={request.currency_id}) "
                f"→ {converted} (policy currency_id={policy.currency_id}) at rate {rate}"
            )
            return converted

        logger.warning(
            f"FX conversion failed for request {request.id}: "
            f"currency_id={request.currency_id} → policy currency_id={policy.currency_id}"
        )
        return None

    def _resolve_approvers(self, db: Session, request: IssuanceRequest, policy: IssuanceWorkflowPolicy, requestor_user_id: int = None) -> List[int]:
        """Resolves the policy's approver rules into a concrete list of User IDs.
        Excludes the requestor (they should never approve their own request).
        Deduplicates across groups automatically via set."""
        from app.models.models import User, Department, ApprovalGroup
        from sqlalchemy import func
        
        approver_ids = set()
        
        if policy.approver_type == "USERS":
            approver_ids.update([int(uid) for uid in policy.approver_values if str(uid).isdigit()])
            
        elif policy.approver_type == "ROLE":
            roles = [str(r).lower() for r in policy.approver_values]
            users = db.query(User.id).filter(
                User.customer_id == request.customer_id,
                User.role.in_(roles),
                User.is_deleted == False
            ).all()
            approver_ids.update([u[0] for u in users])
            
        elif policy.approver_type == "DEPT_HEAD":
            req_dept_name = request.department
            if req_dept_name:
                dept = db.query(Department).filter(
                    Department.customer_id == request.customer_id,
                    func.lower(Department.name) == str(req_dept_name).lower(),
                    Department.is_deleted == False
                ).first()
                if dept and dept.manager_id:
                    approver_ids.add(dept.manager_id)
                    
        elif policy.approver_type == "GROUP":
            group_ids = [int(gid) for gid in policy.approver_values if str(gid).isdigit()]
            groups = db.query(ApprovalGroup).filter(
                ApprovalGroup.id.in_(group_ids),
                ApprovalGroup.customer_id == request.customer_id,
                ApprovalGroup.is_deleted == False
            ).all()
            for group in groups:
                for u in group.users:
                    if not u.is_deleted:
                        approver_ids.add(u.id)
        
        # RULE 1: Requestor cannot approve their own request
        if requestor_user_id and requestor_user_id in approver_ids:
            approver_ids.discard(requestor_user_id)

        # RULE 2: Remove users who have already signed this request IN THE CURRENT ROUND (no double-dipping)
        # Only consider signatures after the last re-approval trigger or resubmission
        current_audit = request.approval_chain_audit or []
        last_reset_idx = -1
        for i, entry in enumerate(current_audit):
            if entry.get("action") in ("RE_APPROVAL_TRIGGERED", "RESUBMITTED"):
                last_reset_idx = i
        current_round_audit = current_audit[last_reset_idx + 1:] if last_reset_idx >= 0 else current_audit
        already_signed_users = {entry.get("user_id") for entry in current_round_audit if entry.get("action") == "APPROVED_STEP"}
        
        return list(approver_ids - already_signed_users)

    def _find_next_step(self, db: Session, request: IssuanceRequest, start_sequence: int = 0):
        """
        Evaluates policies sequentially. Auto-skips policies that apply but have 0 eligible approvers.
        Applies anti-deadlock: if eligible approvers < required_signatures, lowers the requirement.
        
        PRE-DEDUP: Before selecting the first step, looks ahead at ALL applicable steps.
        If a user appears in multiple steps, removes them from steps where other approvers
        can still fulfill the requirement, keeping them in steps where they're critical.
        Priority: remove from multi-approver steps first, then from later steps.
        
        Returns (next_policy, eligible_approver_ids) or (None, []) if fully approved.
        """
        policies = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == request.customer_id,
            IssuanceWorkflowPolicy.is_active == True,
            IssuanceWorkflowPolicy.step_sequence > start_sequence
        ).order_by(IssuanceWorkflowPolicy.step_sequence.asc()).all()
        
        current_audit = request.approval_chain_audit or []

        # ── PRE-DEDUP PHASE: Look ahead at all applicable steps ──
        # Collect which policies apply and their raw approver pools
        applicable = []  # List of (policy, approver_ids_set)
        for policy in policies:
            if self._evaluate_condition(db, request, policy):
                approver_ids = set(self._resolve_approvers(db, request, policy, request.requestor_user_id))
                applicable.append((policy, approver_ids))
        
        if len(applicable) > 1:
            # Find users appearing in more than one step
            user_step_map = {}  # user_id -> list of indices in applicable[]
            for idx, (policy, approvers) in enumerate(applicable):
                for uid in approvers:
                    user_step_map.setdefault(uid, []).append(idx)
            
            shared_users = {uid: indices for uid, indices in user_step_map.items() if len(indices) > 1}
            
            if shared_users:
                # For each shared user: keep in ALL steps where they're critical,
                # remove ONLY from steps where other approvers can still fulfill the requirement.
                # Goal: maximize the number of distinct approvers across the full chain.
                for uid, indices in shared_users.items():
                    # Classify each step as safe-to-remove or must-keep
                    safe_to_remove = []  # Steps where removing this user still leaves enough approvers
                    must_keep = []       # Steps where this user is critical (sole/essential approver)
                    
                    for idx in indices:
                        policy, approvers = applicable[idx]
                        remaining_after_removal = len(approvers) - 1
                        if remaining_after_removal >= policy.required_signatures:
                            safe_to_remove.append(idx)
                        else:
                            must_keep.append(idx)
                    
                    if must_keep:
                        # User is critical in some steps — remove from all safe-to-remove steps
                        for idx in safe_to_remove:
                            applicable[idx][1].discard(uid)
                    else:
                        # User is safe to remove from ALL steps — keep in the most critical one
                        # (fewest other approvers, earliest step as tiebreak)
                        safe_to_remove.sort(key=lambda idx: (
                            len(applicable[idx][1]) - 1,           # fewest other approvers first
                            applicable[idx][0].step_sequence       # earlier step first
                        ))
                        # Keep in the first (most critical), remove from the rest
                        for idx in safe_to_remove[1:]:
                            applicable[idx][1].discard(uid)
                
                # Log the pre-dedup in audit trail
                dedup_log = []
                for uid, indices in shared_users.items():
                    kept_steps = []
                    removed_steps = []
                    for idx in indices:
                        policy, approvers = applicable[idx]
                        if uid in approvers:
                            kept_steps.append(policy.step_sequence)
                        else:
                            removed_steps.append(policy.step_sequence)
                    if removed_steps:
                        dedup_log.append(f"User {uid}: kept in step(s) {kept_steps}, removed from step(s) {removed_steps}")
                
                if dedup_log:
                    current_audit.append({
                        "action": "APPROVER_DEDUP",
                        "details": dedup_log,
                        "reason": "Shared approvers pre-assigned to steps where they are most critical",
                        "timestamp": str(date.today())
                    })
                    request.approval_chain_audit = list(current_audit)
                    flag_modified(request, 'approval_chain_audit')

        # ── SEQUENTIAL STEP SELECTION (with pre-deduped pools) ──
        for policy, approver_ids in applicable:
            if approver_ids:
                approver_list = list(approver_ids)
                # ANTI-DEADLOCK: If eligible approvers < required signatures,
                # lower the requirement to match available approvers
                effective_sigs = min(policy.required_signatures, len(approver_list))
                if effective_sigs < policy.required_signatures:
                    current_audit.append({
                        "action": "ADJUSTED_SIGNATURES",
                        "step": policy.step_sequence,
                        "original_required": policy.required_signatures,
                        "adjusted_to": effective_sigs,
                        "reason": f"Only {len(approver_list)} eligible approvers (requestor excluded or group too small)",
                        "timestamp": str(date.today())
                    })
                    request.approval_chain_audit = list(current_audit)
                    flag_modified(request, 'approval_chain_audit')
                
                return policy, approver_list
            else:
                # Auto-Skip: Condition met, but no eligible users
                current_audit.append({
                    "action": "SKIPPED_STEP",
                    "step": policy.step_sequence,
                    "reason": "No eligible approvers found (requestor excluded, empty group, or all double-dipped)",
                    "timestamp": str(date.today())
                })
                request.approval_chain_audit = list(current_audit)
                flag_modified(request, 'approval_chain_audit')

                # --- Notify corp admins about the skip ---
                try:
                    from app.schemas.all_schemas import SystemNotificationCreate
                    from app.crud.crud import crud_notification
                    import datetime as _dt
                    _now = _dt.datetime.utcnow()
                    notif = SystemNotificationCreate(
                        content=f"Approval step {policy.step_sequence} was auto-skipped for request {request.serial_number}: no eligible approvers.",
                        notification_type="APPROVAL_SKIP",
                        target_roles=["corporate_admin"],
                        target_customer_ids=[request.customer_id],
                        start_date=_now,
                        end_date=_now + _dt.timedelta(days=14),
                        link=f"/corporate-admin/issuance/requests"
                    )
                    crud_notification.create_notification(db, obj_in=notif)
                except Exception as e:
                    logger.warning(f"Failed to create skip notification: {e}")
                # Loop continues to the next policy
        
        return None, []

    def get_approval_roadmap(self, db: Session, request_id: int) -> dict:
        """
        Computes the full lifecycle approval roadmap for a request.
        Evaluates ALL workflow policies and returns their status
        (completed, active, skipped, pending) with approver details.
        """
        from app.models.models import User, Department, ApprovalGroup
        
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            return {"steps": []}

        # Get ALL workflow policies for this customer (ordered)
        policies = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == request.customer_id,
            IssuanceWorkflowPolicy.is_active == True
        ).order_by(IssuanceWorkflowPolicy.step_sequence.asc()).all()

        audit = request.approval_chain_audit or []
        is_fully_approved = any(e.get("action") == "FULLY_APPROVED" for e in audit)
        
        # ── KEY FIX: After a RE_APPROVAL_TRIGGERED, only look at entries from the current round ──
        # Previous-round approvals should not show as "completed" in the roadmap.
        last_restart_idx = -1
        for i, entry in enumerate(audit):
            if entry.get("action") in ("RE_APPROVAL_TRIGGERED", "RESUBMITTED"):
                last_restart_idx = i
        current_round_audit = audit[last_restart_idx + 1:] if last_restart_idx >= 0 else audit
        # Also check is_fully_approved only in current round
        is_fully_approved = any(e.get("action") == "FULLY_APPROVED" for e in current_round_audit)
        
        # Build lookup maps for user names
        all_user_ids = set()
        for entry in current_round_audit:
            if entry.get("user_id"):
                all_user_ids.add(entry["user_id"])
        
        user_name_map = {}
        if all_user_ids:
            users = db.query(User).filter(User.id.in_(all_user_ids)).all()
            user_name_map = {u.id: u.email for u in users}

        # Group audit entries by step (current round only)
        step_audit = {}
        for entry in current_round_audit:
            s = entry.get("step")
            if s is not None:
                step_audit.setdefault(s, []).append(entry)

        steps = []
        for policy in policies:
            seq = policy.step_sequence
            step_entries = step_audit.get(seq, [])
            
            # Determine if this step's condition applies
            condition_applies = self._evaluate_condition(db, request, policy)
            
            # Build condition label
            condition_label = self._get_condition_label(db, policy, request)
            
            # Build approver label  
            approver_label = self._get_approver_label(db, policy, request)
            
            # Determine step status
            approvals = [e for e in step_entries if e.get("action") == "APPROVED_STEP"]
            has_adjusted = next((e for e in step_entries if e.get("action") == "ADJUSTED_SIGNATURES"), None)
            
            required_sigs = has_adjusted["adjusted_to"] if has_adjusted else policy.required_signatures
            
            # Status determination — uses FRESH condition evaluation (not historical audit)
            # This ensures we always show the correct roadmap even if old buggy code
            # skipped steps incorrectly.
            
            if not condition_applies:
                status = "skipped"
            elif is_fully_approved:
                status = "completed"
            elif len(approvals) >= required_sigs:
                status = "completed"
            elif seq < (request.current_approval_step or 0):
                status = "completed"
            elif seq == (request.current_approval_step or 0) and request.status == "PENDING_APPROVAL":
                status = "active"
            else:
                status = "pending"
            
            logger.debug(f"Roadmap: Request {request_id}, Seq {seq}: cond={policy.condition_type}({policy.condition_value}), applies={condition_applies}, approvals={len(approvals)}, is_fully_approved={is_fully_approved}, current_step={request.current_approval_step}, req_status={request.status} -> {status}")

            # Build actions list (for completed/active steps)
            actions = []
            for a in approvals:
                uid = a.get("user_id")
                actions.append({
                    "user_id": uid,
                    "user_name": user_name_map.get(uid, f"User #{uid}"),
                    "action": "Approved",
                    "timestamp": a.get("timestamp", "")
                })

            # Build expected approvers (for active/pending steps)
            expected_approvers = []
            if status in ("active", "pending") and condition_applies:
                approver_ids = self._resolve_approvers(db, request, policy, request.requestor_user_id)
                if approver_ids:
                    approver_users = db.query(User).filter(User.id.in_(approver_ids)).all()
                    expected_approvers = [{"id": u.id, "name": u.email} for u in approver_users]

            step_data = {
                "sequence": seq,
                "condition_type": policy.condition_type,
                "condition_label": condition_label,
                "approver_type": policy.approver_type,
                "approver_label": approver_label,
                "required_signatures": required_sigs,
                "status": status,
                "actions": actions,
                "expected_approvers": expected_approvers,
            }
            
            if status == "skipped":
                skip_entry = next((e for e in step_entries if e.get("action") == "SKIPPED_STEP"), None)
                step_data["skip_reason"] = skip_entry.get("reason", "Condition not met") if skip_entry else "Condition not met"
            
            if status == "active":
                step_data["signatures_collected"] = request.signatures_collected or 0

            steps.append(step_data)

        return {
            "request_id": request_id,
            "request_status": request.status,
            "steps": steps
        }

    def _get_condition_label(self, db, policy, request):
        """Returns a human-readable label for a policy's condition."""
        if policy.condition_type == "ALWAYS":
            return "Always"
        if policy.condition_type == "AMOUNT_RANGE":
            return f"Amount {policy.condition_value}"
        if policy.condition_type == "AMOUNT_OVER":
            return f"Amount over {policy.condition_value}"
        if policy.condition_type == "DEPT_MATCH":
            from app.models.models import Department
            dept = db.query(Department).filter(
                Department.customer_id == request.customer_id,
                Department.id == int(policy.condition_value) if policy.condition_value else 0,
                Department.is_deleted == False
            ).first()
            return f"Department: {dept.name}" if dept else f"Department #{policy.condition_value}"
        if policy.condition_type == "CROSS_BORDER":
            return "Cross-Border Transaction"
        if policy.condition_type == "THIRD_PARTY":
            return "Third-Party Issuance"
        return policy.condition_type

    def _get_approver_label(self, db, policy, request):
        """Returns a human-readable label for a policy's approver type."""
        from app.models.models import User, ApprovalGroup
        
        if policy.approver_type == "DEPT_HEAD":
            return "Department Manager"
        if policy.approver_type == "USERS":
            user_ids = [int(uid) for uid in policy.approver_values if str(uid).isdigit()]
            users = db.query(User).filter(User.id.in_(user_ids)).all()
            names = [u.email for u in users]
            return ", ".join(names) if names else "Specific Individuals"
        if policy.approver_type == "GROUP":
            group_ids = [int(gid) for gid in policy.approver_values if str(gid).isdigit()]
            groups = db.query(ApprovalGroup).filter(
                ApprovalGroup.id.in_(group_ids),
                ApprovalGroup.customer_id == request.customer_id
            ).all()
            names = [g.name for g in groups]
            return ", ".join(names) if names else "Approval Group"
        if policy.approver_type == "ROLE":
            return f"Role: {', '.join(policy.approver_values)}"
        return policy.approver_type


    def submit_for_approval(self, db: Session, request_id: int, user_id: int) -> IssuanceRequest:
        """
        Unified submit flow: DRAFT -> creates V1 snapshot -> runs approval matrix
        -> PENDING_APPROVAL or APPROVED_INTERNAL.
        """
        from app.models.models_issuance import IssuanceRequestSnapshot, IssuanceRequestVersion

        request = crud_issuance_request.get(db, id=request_id)
        if not request or request.status not in ("DRAFT", "REVISION_REQUIRED"):
            raise HTTPException(status_code=400, detail="Only DRAFT or REVISION_REQUIRED requests can be submitted")

        # Determine start step: resume from returned step or start from 0
        resume_from_step = 0
        is_resubmission = False
        if request.status == "REVISION_REQUIRED" and request.returned_from_step is not None:
            is_resubmission = True
            # Subtract 1 so _find_next_step (which uses > start_sequence) re-evaluates the returning step
            resume_from_step = max(0, request.returned_from_step - 1)
            # Clear revision tracking on resubmission
            request.revision_notes = None
            request.returned_from_step = None

        # --- Business Rule Validation ---
        if request.lg_type_id == 3 and not request.operational_status:
            raise HTTPException(status_code=400, detail="Advance Payment LGs require operational_status")

        # --- Create Immutable V1 Snapshot ---
        existing_snapshot = db.query(IssuanceRequestSnapshot).filter(
            IssuanceRequestSnapshot.request_id == request.id
        ).first()
        if not existing_snapshot:
            snapshot_data = {
                col.name: str(getattr(request, col.name)) if getattr(request, col.name) is not None else None
                for col in request.__table__.columns
            }
            snapshot = IssuanceRequestSnapshot(
                request_id=request.id,
                snapshot_data=snapshot_data
            )
            db.add(snapshot)
            db.flush()

        # --- Initialize/Update Audit Trail ---
        if is_resubmission:
            # Resubmission after revision — preserve existing audit trail
            # Try to get change_reason from:
            # 1. EDIT_REASON_PENDING entry in audit trail (public portal edits)
            # 2. IssuanceRequestVersion record (internal edits)
            current_audit = list(request.approval_chain_audit or [])

            # Check for pending edit reason from public portal
            resubmit_reason = None
            cleaned_audit = []
            for entry in current_audit:
                if entry.get("action") == "EDIT_REASON_PENDING":
                    resubmit_reason = entry.get("reason")
                    # Don't carry the pending entry forward — it's consumed here
                else:
                    cleaned_audit.append(entry)
            current_audit = cleaned_audit

            # Fallback: check IssuanceRequestVersion (internal edit flow)
            if not resubmit_reason:
                latest_version = db.query(IssuanceRequestVersion).filter(
                    IssuanceRequestVersion.request_id == request.id
                ).order_by(IssuanceRequestVersion.version_number.desc()).first()
                resubmit_reason = latest_version.change_reason if latest_version else None

            current_audit.append({
                "action": "RESUBMITTED",
                "user_id": user_id,
                "user_name": self._get_user_label(db, user_id) or request.requestor_name or request.requestor_email,
                "timestamp": str(date.today()),
                "resumed_from_step": resume_from_step,
                "reason": resubmit_reason
            })
            request.approval_chain_audit = current_audit
            flag_modified(request, 'approval_chain_audit')
        else:
            request.approval_chain_audit = [{
                "action": "SUBMITTED",
                "user_id": user_id,
                "user_name": self._get_user_label(db, user_id) or request.requestor_name or request.requestor_email,
                "timestamp": str(date.today())
            }]
            flag_modified(request, 'approval_chain_audit')

        # --- Run Approval Matrix ---
        next_policy, approver_ids = self._find_next_step(db, request, start_sequence=resume_from_step)

        if next_policy:
            request.current_approval_step = next_policy.step_sequence
            request.pending_approver_users = approver_ids
            request.signatures_collected = 0
            request.status = "PENDING_APPROVAL"
        else:
            # Distinguish: "no policies configured" vs "policies exist but none match"
            has_any_policies = db.query(IssuanceWorkflowPolicy).filter(
                IssuanceWorkflowPolicy.customer_id == request.customer_id,
                IssuanceWorkflowPolicy.is_active == True,
            ).count() > 0

            # Check if at least one policy was evaluated and skipped (auto-skip with audit entry)
            audit_entries = request.approval_chain_audit or []
            any_step_was_evaluated = any(
                e.get("action") in ("SKIPPED_STEP", "ADJUSTED_SIGNATURES")
                for e in audit_entries
            )

            if has_any_policies and not any_step_was_evaluated:
                # COVERAGE GAP: Policies exist but NONE matched this request.
                # This means the request falls outside all configured approval rules
                # (e.g., amount exceeds all AMOUNT_RANGE thresholds with no catch-all).
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "This request does not match any configured approval workflow rule. "
                        "No approval path exists for this combination of amount, department, "
                        "and request attributes. Please contact your Corporate Admin to review "
                        "the approval matrix configuration."
                    )
                )

            # Either no policies configured (auto-approve) or all matched policies
            # were evaluated and auto-skipped (all approvers excluded) → approve
            request.status = "APPROVED_INTERNAL"
            request.current_approval_step = 999
            request.pending_approver_users = []
            request.signatures_collected = 0
            
            current_audit = request.approval_chain_audit or []
            reason = "No workflow policies configured" if not has_any_policies else "All applicable steps auto-skipped (no eligible approvers)"
            current_audit.append({
                "action": "FULLY_APPROVED",
                "reason": reason,
                "timestamp": str(date.today())
            })
            request.approval_chain_audit = list(current_audit)
            flag_modified(request, 'approval_chain_audit')

        db.add(request)
        db.commit()
        db.refresh(request)

        log_action(
            db, user_id=user_id,
            action_type="ISSUANCE_REQUEST_SUBMITTED",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={
                "serial_number": request.serial_number,
                "status_after": request.status,
                "amount": str(request.amount),
                "currency_id": request.currency_id,
                "beneficiary": request.beneficiary_name
            },
            customer_id=request.customer_id
        )

        return request

    def approve_request(self, db: Session, request_id: int, approver_user_id: int) -> IssuanceRequest:
        """ Records a signature. If enough signatures are collected, moves to the next step. """
        request = crud_issuance_request.get(db, id=request_id)
        if not request or request.status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Request is not pending approval")
            
        # 1. Verify Authorization (type-safe: cast JSONB values to int)
        allowed_users = [int(uid) for uid in (request.pending_approver_users or [])]
        
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[APPROVE] Request {request_id}: status={request.status}, step={request.current_approval_step}, pending_approvers={request.pending_approver_users}, approver={approver_user_id}")
        
        # Auto-recalculate if pending_approver_users is empty (stale from re-approval)
        if not allowed_users:
            logger.info(f"[APPROVE] pending_approver_users is empty, recalculating from step 0 (to include step 1)")
            try:
                next_policy, recalc_ids = self._find_next_step(db, request, start_sequence=0)
                logger.info(f"[APPROVE] Recalculated: policy={next_policy}, approver_ids={recalc_ids}")
                if recalc_ids:
                    request.pending_approver_users = recalc_ids
                    allowed_users = [int(uid) for uid in recalc_ids]
                    if next_policy:
                        request.current_approval_step = next_policy.step_sequence
                    db.flush()
                    logger.info(f"[APPROVE] Updated pending_approver_users to {recalc_ids}")
            except Exception as e:
                logger.error(f"[APPROVE] Recalculation failed: {e}", exc_info=True)
        
        if approver_user_id not in allowed_users:
            raise HTTPException(status_code=403, detail="You are not authorized to approve this step.")

        # Block self-approval: requestor cannot approve their own request
        if request.requestor_user_id and approver_user_id == request.requestor_user_id:
            raise HTTPException(status_code=403, detail="You cannot approve your own request.")

        current_audit = request.approval_chain_audit or []
        # Only check for duplicate signatures AFTER the last resubmission or re-approval trigger
        # (previous approval-round signatures don't count)
        last_resubmit_idx = -1
        for i, entry in enumerate(current_audit):
            if entry.get("action") in ("RESUBMITTED", "RE_APPROVAL_TRIGGERED"):
                last_resubmit_idx = i
        recent_entries = current_audit[last_resubmit_idx + 1:] if last_resubmit_idx >= 0 else current_audit
        for entry in recent_entries:
            if entry.get("action") == "APPROVED_STEP" and entry.get("step") == request.current_approval_step and entry.get("user_id") == approver_user_id:
                raise HTTPException(status_code=400, detail="You have already signed this step.")

        # 2. Record Signature
        request.signatures_collected = (request.signatures_collected or 0) + 1
        
        logger.debug(f"Approve: Request {request_id}: user={approver_user_id}, step={request.current_approval_step}, signatures_collected={request.signatures_collected}")
        
        current_audit.append({
            "action": "APPROVED_STEP",
            "step": request.current_approval_step,
            "user_id": approver_user_id,
            "user_name": self._get_user_label(db, approver_user_id),
            "timestamp": str(date.today())
        })
        request.approval_chain_audit = list(current_audit)
        flag_modified(request, 'approval_chain_audit')

        # 3. Check if Step is Complete
        current_policy = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == request.customer_id,
            IssuanceWorkflowPolicy.step_sequence == request.current_approval_step
        ).first()
        
        policy_sigs = current_policy.required_signatures if current_policy else 1
        
        # Anti-deadlock: use effective required signatures if adjusted
        # (set by _find_next_step when eligible approvers < policy requirement)
        effective_sigs = None
        # Check audit trail for adjustment record for this step — CURRENT ROUND ONLY
        for entry in recent_entries:
            if entry.get("action") == "ADJUSTED_SIGNATURES" and entry.get("step") == request.current_approval_step:
                effective_sigs = entry.get("adjusted_to", policy_sigs)
                break
        required_sigs = effective_sigs if effective_sigs is not None else policy_sigs

        logger.debug(f"Approve: Request {request_id}: policy_sigs={policy_sigs}, effective_sigs={effective_sigs}, required_sigs={required_sigs}, collected={request.signatures_collected}, advancing={request.signatures_collected >= required_sigs}")

        if request.signatures_collected >= required_sigs:
            # STEP COMPLETE -> Find Next Step
            next_policy, approver_ids = self._find_next_step(db, request, start_sequence=request.current_approval_step)

            if next_policy:
                logger.debug(f"Approve: Request {request_id}: ADVANCING to step {next_policy.step_sequence}, approvers={approver_ids}")
                request.current_approval_step = next_policy.step_sequence
                request.pending_approver_users = approver_ids
                request.signatures_collected = 0
            else:
                logger.debug(f"Approve: Request {request_id}: FULLY APPROVED — no more steps")
                # Final Approval
                request.status = "APPROVED_INTERNAL"
                request.pending_approver_users = []
                request.signatures_collected = 0
                
                current_audit = request.approval_chain_audit or []
                current_audit.append({
                    "action": "FULLY_APPROVED",
                    "timestamp": str(date.today())
                })
                request.approval_chain_audit = list(current_audit)
                flag_modified(request, 'approval_chain_audit')
        else:
            logger.debug(f"Approve: Request {request_id}: WAITING for more signatures ({request.signatures_collected}/{required_sigs})")

        db.add(request)
        db.commit()
        db.refresh(request)

        log_action(
            db, user_id=approver_user_id,
            action_type="ISSUANCE_REQUEST_APPROVED",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={
                "step": request.current_approval_step,
                "status_after": request.status,
                "signatures_collected": request.signatures_collected,
                "required_signatures": required_sigs,
                "approver_user_id": approver_user_id
            },
            customer_id=request.customer_id
        )

        return request

    def reject_request(self, db: Session, request_id: int, user_id: int, rejection_reason: str = None) -> IssuanceRequest:
        """ Rejects the request and clears pending approvers. """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")

        request.status = "REJECTED"
        request.pending_approver_users = []
        request.revision_notes = rejection_reason  # Store reason for requestor visibility
        
        current_audit = request.approval_chain_audit or []
        current_audit.append({
            "action": "REJECTED",
            "user_id": user_id,
            "user_name": self._get_user_label(db, user_id),
            "notes": rejection_reason,
            "timestamp": str(date.today())
        })
        request.approval_chain_audit = list(current_audit)
        flag_modified(request, 'approval_chain_audit')

        db.add(request)
        db.commit()
        db.refresh(request)

        log_action(
            db, user_id=user_id,
            action_type="ISSUANCE_REQUEST_REJECTED",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={
                "serial_number": request.serial_number,
                "status_after": "REJECTED",
                "rejection_reason": rejection_reason
            },
            customer_id=request.customer_id
        )

        return request

    def return_for_revision(self, db: Session, request_id: int, user_id: int, revision_notes: str = None) -> IssuanceRequest:
        """
        Returns a request for revision instead of rejecting it.
        The requestor can edit and resubmit — approval resumes from the step that returned it.
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        if request.status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Only PENDING_APPROVAL requests can be returned for revision")

        # Store the step that returned it so resubmission resumes here
        request.returned_from_step = request.current_approval_step
        request.revision_notes = revision_notes
        request.status = "REVISION_REQUIRED"
        request.pending_approver_users = []
        request.signatures_collected = 0
        
        current_audit = request.approval_chain_audit or []
        current_audit.append({
            "action": "REVISION_REQUIRED",
            "step": request.current_approval_step,
            "user_id": user_id,
            "user_name": self._get_user_label(db, user_id),
            "notes": revision_notes,
            "timestamp": str(date.today())
        })
        request.approval_chain_audit = list(current_audit)
        flag_modified(request, 'approval_chain_audit')

        db.add(request)
        db.commit()
        db.refresh(request)

        log_action(
            db, user_id=user_id,
            action_type="ISSUANCE_REQUEST_RETURNED_FOR_REVISION",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={
                "serial_number": request.serial_number,
                "returned_from_step": request.returned_from_step,
                "revision_notes": revision_notes
            },
            customer_id=request.customer_id
        )

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
        facilities = crud_facility.get_multi_by_customer(db, customer_id=request.customer_id)

        # C3: Load scoring weights from GlobalConfiguration system (once, outside loop)
        from app.crud.crud_config import crud_customer_configuration
        from app.constants import GlobalConfigKey
        is_urgent = getattr(request, 'is_urgent', False)

        def _get_weight(key_enum, fallback):
            """Resolve weight from customer override → global default → hardcoded fallback."""
            cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
                db, request.customer_id, key_enum
            )
            if cfg and cfg.get('effective_value') is not None:
                try:
                    return float(cfg['effective_value'])
                except (ValueError, TypeError):
                    pass
            return fallback

        if is_urgent:
            raw_weights = {
                'cost': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_URGENT_COST, 15),
                'margin': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_URGENT_MARGIN, 10),
                'sla': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_URGENT_SLA, 40),
                'capacity': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_URGENT_CAPACITY, 15),
                'currency_match': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_URGENT_CURRENCY_MATCH, 20),
            }
        else:
            raw_weights = {
                'cost': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_COST, 30),
                'margin': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_MARGIN, 15),
                'sla': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_SLA, 15),
                'capacity': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_CAPACITY, 20),
                'currency_match': _get_weight(GlobalConfigKey.FACILITY_SCORE_WEIGHT_CURRENCY_MATCH, 20),
            }

        total_w = sum(raw_weights.values()) or 1.0
        w_cost = raw_weights['cost'] / total_w
        w_margin = raw_weights['margin'] / total_w
        w_sla = raw_weights['sla'] / total_w
        w_capacity = raw_weights['capacity'] / total_w
        w_currency = raw_weights['currency_match'] / total_w
        
        candidates = []
        
        logger.info(
            f"[FACILITY MATCH] Request {request_id}: lg_type_id={request.lg_type_id}, "
            f"currency_id={request.currency_id}, amount={request.amount}, "
            f"is_cross_border={request.is_cross_border}, is_third_party={request.is_third_party}, "
            f"total facilities to evaluate={len(facilities)}"
        )

        for fac in facilities:
            fac_label = f"Fac[{fac.id}] '{fac.facility_name}'"

            # ── Hard filters at facility level ──
            if getattr(fac, 'is_deleted', False):
                logger.info(f"[FACILITY MATCH]   SKIP {fac_label}: is_deleted=True")
                continue
            if fac.status != 'ACTIVE':
                logger.info(f"[FACILITY MATCH]   SKIP {fac_label}: status='{fac.status}' (not ACTIVE)")
                continue
            if fac.expiry_date and fac.expiry_date < date.today(): 
                logger.info(f"[FACILITY MATCH]   SKIP {fac_label}: expired {fac.expiry_date} < today {date.today()}")
                continue
            
            # Currency: must match OR facility allows multi-currency
            if fac.currency_id != request.currency_id and not fac.multi_currency_allowed:
                logger.info(
                    f"[FACILITY MATCH]   SKIP {fac_label}: currency mismatch "
                    f"fac.currency_id={fac.currency_id} vs request.currency_id={request.currency_id}, "
                    f"multi_currency_allowed={fac.multi_currency_allowed}"
                )
                continue
            
            # Cross-border: if request is cross-border, facility must allow it
            if getattr(request, 'is_cross_border', False) and not fac.allow_cross_border:
                logger.info(f"[FACILITY MATCH]   SKIP {fac_label}: cross_border request but facility forbids it")
                continue
            
            # Third-party: if request is third-party, facility must allow it
            if getattr(request, 'is_third_party', False) and not fac.allow_third_party_issuance:
                logger.info(f"[FACILITY MATCH]   SKIP {fac_label}: third_party request but facility forbids it")
                continue

            logger.info(f"[FACILITY MATCH]   PASS {fac_label}: facility-level filters OK, checking {len(fac.sub_limits)} sub-limits")

            # 2. Iterate Sub-Limits
            for sub in fac.sub_limits:
                sub_label = f"Sub[{sub.id}] '{sub.limit_name}'"

                # Filter: LG type (type-safe: JSONB may store strings or ints)
                if sub.lg_type_ids:
                    str_lg_type_ids = [str(x) for x in sub.lg_type_ids]
                    if str(request.lg_type_id) not in str_lg_type_ids:
                        logger.info(
                            f"[FACILITY MATCH]     SKIP {sub_label}: lg_type_id mismatch "
                            f"request={request.lg_type_id} not in sub={sub.lg_type_ids}"
                        )
                        continue
                else:
                    logger.info(f"[FACILITY MATCH]     WARN {sub_label}: lg_type_ids is empty/null — sub-limit accepts ALL types")

                try:
                    # Filter: Country restrictions on sub-limit
                    if sub.allowed_countries:
                        country_rules = sub.allowed_countries
                        req_country = getattr(request, 'issuance_country', None) or getattr(request, 'beneficiary_country', None)
                        if req_country and isinstance(country_rules, dict):
                            rule_type = country_rules.get('type', 'ALLOW')
                            country_list = country_rules.get('list', country_rules.get('countries', []))
                            if country_list:  # Empty list = no restrictions
                                country_list_upper = [c.upper() for c in country_list]
                                req_upper = req_country.upper().strip()
                                if rule_type == 'ALLOW' and req_upper not in country_list_upper:
                                    logger.info(
                                        f"[FACILITY MATCH]     SKIP {sub_label}: country ALLOW filter reject "
                                        f"req_country='{req_upper}' not in allowed={country_list_upper}"
                                    )
                                    continue
                                if rule_type == 'EXCLUDE' and req_upper in country_list_upper:
                                    logger.info(
                                        f"[FACILITY MATCH]     SKIP {sub_label}: country EXCLUDE filter reject "
                                        f"req_country='{req_upper}' in excluded={country_list_upper}"
                                    )
                                    continue

                    # Filter: Dedication — if sub-limit is earmarked for specific projects, only match those
                    if sub.dedicated_project_ids and isinstance(sub.dedicated_project_ids, list):
                        req_project_id = getattr(request, 'project_id', None)
                        if sub.dedicated_project_ids and req_project_id not in sub.dedicated_project_ids:
                            logger.info(
                                f"[FACILITY MATCH]     SKIP {sub_label}: dedication mismatch "
                                f"req_project_id={req_project_id} not in {sub.dedicated_project_ids}"
                            )
                            continue

                    # Calculate REAL Utilization (sub-limit level)
                    used_amount = db.query(func.coalesce(func.sum(IssuedLGRecord.current_amount), 0)).filter(
                        IssuedLGRecord.facility_sub_limit_id == sub.id,
                        IssuedLGRecord.status.in_(["ACTIVE", "INTERNAL_PROCESSING"])
                    ).scalar()

                    pending_exposure = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)).filter(
                        IssuanceExposureEntry.sub_limit_id == sub.id,
                        IssuanceExposureEntry.is_active == True
                    ).scalar()

                    total_used = max(float(used_amount), float(pending_exposure))
                    initial_util = float(getattr(sub, 'initial_utilization', 0) or 0)
                    total_used += initial_util
                    available = float(sub.limit_amount) - total_used
                    limit_total = float(sub.limit_amount)

                    # Bug C fix: Calculate utilization against FACILITY total, not sub-limit cap
                    facility_total = float(fac.total_limit_amount)
                    # Facility-level total used (across ALL sub-limits)
                    facility_total_used_q = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)).filter(
                        IssuanceExposureEntry.facility_id == fac.id,
                        IssuanceExposureEntry.is_active == True
                    ).scalar()
                    facility_total_used = float(facility_total_used_q)
                    for sl in fac.sub_limits:
                        facility_total_used += float(getattr(sl, 'initial_utilization', 0) or 0)
                    facility_available = facility_total - facility_total_used
                    utilization = (facility_total_used / facility_total * 100) if facility_total > 0 else 0.0

                    # Sufficient = fits in BOTH sub-limit cap AND facility total
                    has_sufficient = available >= float(request.amount) and facility_available >= float(request.amount)

                    # 3. Calculate Financials (proper cost formula)
                    comm_rate = float(sub.default_commission_rate or 0)
                    min_comm = float(sub.default_min_commission or 0)
                    flat_fee = float(sub.default_flat_fee or 0)
                    margin_pct = float(sub.default_cash_margin_pct or 0)
                    
                    amount = float(request.amount)
                    # Commission = max(percentage-based, minimum floor) + flat fee
                    pct_comm = amount * (comm_rate / 100.0)
                    est_comm = max(pct_comm, min_comm) + flat_fee
                    req_margin = amount * (margin_pct / 100.0)
                    # Total cost to the customer
                    total_cost = est_comm + req_margin

                    # 4. Generate Tags
                    tags = []
                    if not has_sufficient:
                        # Distinguish: sub-limit has room but facility total is full
                        if available >= float(request.amount) and facility_available < float(request.amount):
                            tags.append("FACILITY_FULL")
                        else:
                            tags.append("INSUFFICIENT_LIMIT")
                    if sub.dedicated_project_ids and isinstance(sub.dedicated_project_ids, list):
                        tags.append("DEDICATED")
                    if margin_pct == 0:
                        tags.append("NO_MARGIN")
                    elif margin_pct <= 5:
                        tags.append("LOW_MARGIN")
                    if fac.sla_agreement_days and fac.sla_agreement_days <= 2:
                        tags.append("FAST_TRACK")

                    # 5. Compute multi-factor score (0-100, higher is better)
                    # C3: Weights (w_cost, w_margin, w_sla, w_capacity, w_currency) pre-loaded from GlobalConfig
                    
                    # Cost score: lower cost = higher score (normalize against amount)
                    cost_ratio = total_cost / amount if amount > 0 else 0
                    cost_score = max(0, 100 - (cost_ratio * 100 * 5))  # 5x penalty
                    
                    # Margin score: lower margin = higher score
                    margin_score = max(0, 100 - margin_pct * 4)  # 25% margin = 0 score
                    
                    # SLA score: faster = higher score
                    sla_days = fac.sla_agreement_days or 7  # default 7 days
                    sla_score = max(0, 100 - (sla_days * 10))  # 10 days = 0 score
                    
                    # Capacity score: more available = higher score (encourages risk distribution)
                    capacity_ratio = available / limit_total if limit_total > 0 else 0
                    capacity_score = min(100, capacity_ratio * 100)

                    # C2: Currency match score — same currency = 100, multi-currency with FX = 50, mismatch = 0
                    if fac.currency_id == request.currency_id:
                        currency_match_score = 100.0
                    elif fac.multi_currency_allowed:
                        # Multi-currency facility: partial score (FX conversion adds cost/risk)
                        currency_match_score = 50.0
                    else:
                        currency_match_score = 0.0
                    
                    facility_score = round(
                        cost_score * w_cost + 
                        margin_score * w_margin + 
                        sla_score * w_sla + 
                        capacity_score * w_capacity +
                        currency_match_score * w_currency, 
                        1
                    )

                    # Fetch Issuance Options for this Bank
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
                        bank_id=fac.bank_id,
                        sub_limit_id=sub.id,
                        sub_limit_name=sub.limit_name,
                        limit_available=max(0, available),
                        total_limit=limit_total,
                        total_used=total_used,
                        utilization_pct=round(utilization, 1),
                        has_sufficient_limit=has_sufficient,
                        
                        price_commission_rate=float(comm_rate),
                        price_cash_margin_pct=float(margin_pct),
                        estimated_commission_cost=est_comm,
                        required_cash_margin_amount=req_margin,
                        facility_score=facility_score,
                        
                        recommendation_tags=tags
                    ))
                except Exception as e:
                    import logging
                    logging.warning(f"Facility matching error for sub-limit {sub.id}: {e}")

        # 5. Ranking Logic
        if candidates:
            sufficient = [c for c in candidates if c.has_sufficient_limit]
            
            # BEST_PRICE: lowest total cost among sufficient options
            if len(sufficient) > 1:
                lowest_cost = min(c.estimated_commission_cost for c in sufficient)
                for c in sufficient:
                    if c.estimated_commission_cost == lowest_cost:
                        c.recommendation_tags.insert(0, "BEST_PRICE")
            
            # BEST_OVERALL: highest composite score
            if sufficient:
                best_score = max(c.facility_score for c in sufficient)
                for c in sufficient:
                    if c.facility_score == best_score:
                        if "BEST_PRICE" not in c.recommendation_tags:
                            c.recommendation_tags.insert(0, "BEST_OVERALL")
                        else:
                            c.recommendation_tags.insert(1, "BEST_OVERALL")

        # Sort by: sufficient first, then by composite score (descending)
        candidates.sort(key=lambda x: (0 if x.has_sufficient_limit else 1, -x.facility_score))
        
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

    # ==========================================================================
    # 4. FACILITY RESERVATION & UNIFIED ISSUANCE EXECUTION ENGINE
    # ==========================================================================

    def reserve_facility(
        self,
        db: Session,
        request_id: int,
        user_id: int,
        sub_limit_id: int
    ) -> IssuanceRequest:
        """
        Reserve facility capacity for a request without creating the LG record.
        APPROVED_INTERNAL → FACILITY_RESERVED
        Creates an exposure entry of type RESERVATION.
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found.")

        if request.status != "APPROVED_INTERNAL":
            raise HTTPException(
                status_code=400,
                detail=f"Only APPROVED_INTERNAL requests can be reserved. Current: {request.status}"
            )

        # Subscription limit check — fail fast before locking any facility
        from app.models import Customer as _CustomerModel
        _customer = db.query(_CustomerModel).filter(_CustomerModel.id == request.customer_id).first()
        if _customer and _customer.subscription_plan and _customer.active_issuance_lg_count >= _customer.subscription_plan.max_issuance_records:
            raise HTTPException(
                status_code=400,
                detail=f"Issuance LG limit ({_customer.subscription_plan.max_issuance_records}) exceeded for this customer's subscription plan. Cannot reserve a facility."
            )

        # Verify sub-limit exists — WITH ROW LOCK (C7: prevents concurrent over-reservation)
        sub_limit = db.query(IssuanceFacilitySubLimit).filter(
            IssuanceFacilitySubLimit.id == sub_limit_id
        ).with_for_update().first()
        if not sub_limit:
            raise HTTPException(status_code=404, detail="Selected facility sub-limit not found.")

        # Load facility for currency context (C1: multi-currency utilization fix)
        facility = db.query(IssuanceFacility).filter(
            IssuanceFacility.id == sub_limit.facility_id
        ).first()
        if not facility:
            raise HTTPException(status_code=404, detail="Facility not found.")

        # C4: Respect FX_SUSPENDED status
        if facility.status == "FX_SUSPENDED":
            raise HTTPException(
                status_code=400,
                detail="This facility is suspended due to FX breach. Contact your Corporate Admin."
            )

        # --- DEFENSIVE VALIDATION: Re-check sub-limit constraints (prevents bypass) ---
        # 1. LG Type check
        if sub_limit.lg_type_ids:
            request_lg_type_id = request.lg_type_id
            # str-coerce both sides: JSONB may return strings OR ints depending on driver
            str_sub_type_ids = [str(x) for x in sub_limit.lg_type_ids]
            if request_lg_type_id and str(request_lg_type_id) not in str_sub_type_ids:
                raise HTTPException(
                    status_code=400,
                    detail=f"This sub-limit does not support the requested LG type. "
                           f"Allowed LG type IDs: {sub_limit.lg_type_ids}"
                )

        # 2. Country check (ALLOW / EXCLUDE mode)
        if sub_limit.allowed_countries:
            country_rule = sub_limit.allowed_countries
            rule_type = country_rule.get("type", "ALLOW") if isinstance(country_rule, dict) else "ALLOW"
            # Frontend saves key as 'list'; older records may use 'countries' — support both
            country_list = country_rule.get("list", country_rule.get("countries", [])) if isinstance(country_rule, dict) else []
            request_country = (request.beneficiary_country or "").upper()

            if country_list and request_country:
                upper_list = [c.upper() for c in country_list]
                if rule_type == "ALLOW" and request_country not in upper_list:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Beneficiary country '{request_country}' is not in the allowed countries for this sub-limit."
                    )
                elif rule_type == "EXCLUDE" and request_country in upper_list:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Beneficiary country '{request_country}' is excluded from this sub-limit."
                    )

        # 3. Dedicated project check
        if sub_limit.dedicated_project_ids:
            request_project_id = getattr(request, 'project_id', None)
            if request_project_id and request_project_id not in sub_limit.dedicated_project_ids:
                raise HTTPException(
                    status_code=400,
                    detail="This sub-limit is dedicated to specific projects. "
                           "The request's project does not match."
                )

        # C1: Convert request amount to facility currency for capacity comparison
        from app.services.fx_service import fx_service
        facility_equivalent_amount, fx_rate = fx_service.convert(
            db,
            Decimal(str(request.amount)),
            request.currency_id,
            facility.currency_id,
            allow_ai=False,  # Reservation should be fast — CBE only
        )

        if facility_equivalent_amount is None:
            raise HTTPException(
                status_code=400,
                detail="Cannot determine FX rate to convert request amount to facility currency. "
                       "Please ensure exchange rates are up to date."
            )

        # Check sub-limit availability (amounts now in facility currency)
        used_amount = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0))\
            .filter(
                IssuanceExposureEntry.sub_limit_id == sub_limit_id,
                IssuanceExposureEntry.is_active == True
            ).scalar()
        # Add initial utilization
        used_amount = float(used_amount) + float(getattr(sub_limit, 'initial_utilization', 0) or 0)

        available_amount = float(sub_limit.limit_amount) - used_amount
        if float(facility_equivalent_amount) > available_amount:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient sub-limit capacity. Available: {available_amount:,.2f} "
                       f"({facility.currency.iso_code if facility.currency else 'N/A'}), "
                       f"Required: {float(facility_equivalent_amount):,.2f}"
            )

        # Check FACILITY-level total cap
        facility_total_used_q = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0))\
            .filter(
                IssuanceExposureEntry.facility_id == facility.id,
                IssuanceExposureEntry.is_active == True
            ).scalar()
        facility_total_used = float(facility_total_used_q)
        # Add initial utilization from all sub-limits
        for sl in facility.sub_limits:
            facility_total_used += float(getattr(sl, 'initial_utilization', 0) or 0)
        facility_available = float(facility.total_limit_amount) - facility_total_used
        if float(facility_equivalent_amount) > facility_available:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient facility total limit. Facility available: {facility_available:,.2f}, "
                       f"Required: {float(facility_equivalent_amount):,.2f}"
            )

        # Check max per LG (compare in facility currency)
        if sub_limit.max_amount_per_lg and float(facility_equivalent_amount) > float(sub_limit.max_amount_per_lg):
            raise HTTPException(
                status_code=400,
                detail=f"Amount exceeds maximum per LG ({float(sub_limit.max_amount_per_lg):,.2f}) for this sub-limit."
            )

        # Create exposure reservation (amounts in facility currency)
        from datetime import date as date_cls

        exposure_entry = IssuanceExposureEntry(
            facility_id=sub_limit.facility_id,
            sub_limit_id=sub_limit_id,
            request_id=request_id,
            entry_type="RESERVATION",
            original_amount_delta=request.amount,
            original_currency_id=request.currency_id,
            fx_rate_used=fx_rate,
            facility_equivalent_delta=facility_equivalent_amount,
            is_active=True,
            effective_date=date_cls.today()
        )
        db.add(exposure_entry)

        # Update request
        request.status = "FACILITY_RESERVED"
        request.selected_sub_limit_id = sub_limit_id

        db.add(request)
        db.commit()
        db.refresh(request)

        log_action(
            db, user_id=user_id,
            action_type="FACILITY_RESERVED",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={
                "sub_limit_id": sub_limit_id,
                "facility_name": sub_limit.limit_name,
                "amount": str(request.amount)
            },
            customer_id=request.customer_id
        )

        return request

    def release_reservation(
        self,
        db: Session,
        request_id: int,
        user_id: int
    ) -> IssuanceRequest:
        """
        Release a facility reservation, freeing the held capacity.
        FACILITY_RESERVED → APPROVED_INTERNAL
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found.")

        if request.status != "FACILITY_RESERVED":
            raise HTTPException(
                status_code=400,
                detail=f"Only FACILITY_RESERVED requests can be released. Current: {request.status}"
            )

        # Deactivate exposure entries for this request
        db.query(IssuanceExposureEntry).filter(
            IssuanceExposureEntry.request_id == request_id,
            IssuanceExposureEntry.is_active == True
        ).update({"is_active": False})

        # Revert status
        request.status = "APPROVED_INTERNAL"
        request.selected_sub_limit_id = None

        db.add(request)
        db.commit()
        db.refresh(request)

        log_action(
            db, user_id=user_id,
            action_type="FACILITY_RESERVATION_RELEASED",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={"reason": "Manual release by user"},
            customer_id=request.customer_id
        )

        return request

    # ================================================================
    # C5: Pre-Execution FX Drift Check
    # ================================================================
    def pre_execution_check(self, db: Session, request_id: int) -> Dict[str, Any]:
        """
        Runs pre-execution checks for a reserved request before issue_lg.
        Returns warnings (FX drift, etc.) the frontend can display.
        """
        from app.services.fx_service import fx_service
        from app.constants import GlobalConfigKey

        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found.")

        result = {
            "request_id": request_id,
            "status": request.status,
            "warnings": [],
            "fx_drift": None,
        }

        # Only check FX drift for requests that have a reserved facility
        if request.status != "FACILITY_RESERVED" or not request.selected_sub_limit_id:
            return result

        # Find the active exposure entry for this reservation
        exposure = db.query(IssuanceExposureEntry).filter(
            IssuanceExposureEntry.request_id == request_id,
            IssuanceExposureEntry.is_active == True,
            IssuanceExposureEntry.entry_type == "RESERVATION",
        ).first()

        if not exposure or not exposure.fx_rate_used or exposure.fx_rate_used == 1:
            # No FX conversion was involved (same currency) — no drift possible
            return result

        # Get current FX rate for the same currency pair
        sub_limit = db.query(IssuanceFacilitySubLimit).filter(
            IssuanceFacilitySubLimit.id == request.selected_sub_limit_id
        ).first()
        if not sub_limit:
            return result

        facility = db.query(IssuanceFacility).filter(
            IssuanceFacility.id == sub_limit.facility_id
        ).first()
        if not facility or facility.currency_id == request.currency_id:
            # Same currency — no drift
            return result

        current_amount, current_rate = fx_service.convert(
            db,
            Decimal(str(request.amount)),
            request.currency_id,
            facility.currency_id,
            allow_ai=False,
        )

        if current_rate is None or current_amount is None:
            result["warnings"].append("Unable to fetch current FX rate for drift comparison.")
            return result

        # Calculate drift percentage
        reserved_rate = float(exposure.fx_rate_used)
        current_rate_f = float(current_rate)
        if reserved_rate == 0:
            return result

        drift_pct = abs(current_rate_f - reserved_rate) / reserved_rate * 100

        # Get threshold from config (default 2%)
        threshold = 2.0
        try:
            threshold_val = crud_customer_configuration.get_customer_config_or_global_fallback(
                db, request.customer_id, GlobalConfigKey.FX_DRIFT_WARNING_THRESHOLD
            )
            if threshold_val is not None:
                threshold = float(threshold_val)
        except Exception:
            pass  # Use default

        reserved_equivalent = float(exposure.facility_equivalent_delta)
        current_equivalent = float(current_amount)
        cost_impact = current_equivalent - reserved_equivalent

        fx_drift_info = {
            "reserved_rate": round(reserved_rate, 6),
            "current_rate": round(current_rate_f, 6),
            "drift_pct": round(drift_pct, 2),
            "threshold_pct": threshold,
            "exceeds_threshold": drift_pct > threshold,
            "reserved_equivalent": round(reserved_equivalent, 2),
            "current_equivalent": round(current_equivalent, 2),
            "cost_impact": round(cost_impact, 2),
            "facility_currency": facility.currency.code if facility.currency else "N/A",
            "request_currency": request.currency.code if request.currency else "N/A",
        }

        result["fx_drift"] = fx_drift_info

        if fx_drift_info["exceeds_threshold"]:
            direction = "increased" if cost_impact > 0 else "decreased"
            result["warnings"].append(
                f"FX rate has changed {drift_pct:.1f}% since reservation "
                f"({reserved_rate:.4f} → {current_rate_f:.4f}). "
                f"Cost has {direction} by {abs(cost_impact):,.2f} {fx_drift_info['facility_currency']}."
            )

        return result

    async def issue_lg(
        self,
        db: Session,
        request_id: int,
        user_id: int,
        sub_limit_id: Optional[int] = None,
        issued_ref_number: str = "",
        issue_date: date = None,
        expiry_date: Optional[date] = None,
        issuance_method: Optional[str] = "MANUAL",
        bank_method_id: Optional[int] = None,
        bank_id: Optional[int] = None,
        manual_pricing: Optional[Dict[str, Any]] = None  # D3: cost data for no-facility LGs
    ) -> IssuedLGRecord:
        """
        The single, unified execution method. Performs:
        1. Validates approval status
        2. Acquires atomic lock (prevents double-execution)
        3. Checks facility availability (if sub_limit_id provided)
        4. Creates exposure reservation entry (if sub_limit_id provided)
        5. Creates IssuedLGRecord with full accountability
        6. Transitions request to PENDING_BANK_CONFIRMATION
        """
        import logging
        logger = logging.getLogger(__name__)

        # --- STEP 1: Fetch & Validate (with row-level lock to prevent concurrent issuance) ---
        # Lock only the main table to avoid "FOR UPDATE cannot be applied to nullable outer join"
        request = db.query(IssuanceRequest).filter(
            IssuanceRequest.id == request_id
        ).with_for_update(of=IssuanceRequest).first()
        if not request:
            raise HTTPException(status_code=404, detail="Request not found.")

        if request.status not in ("APPROVED_INTERNAL", "FACILITY_RESERVED"):
            raise HTTPException(
                status_code=400,
                detail=f"Request must be APPROVED_INTERNAL or FACILITY_RESERVED before issuance. Current status: {request.status}"
            )

        # Guard: prevent duplicate issuance if LG record already exists
        if request.lg_record_id:
            raise HTTPException(
                status_code=409,
                detail=f"This request has already been issued (LG record #{request.lg_record_id}). Cannot issue again."
            )
        # Also check by request_id to catch orphaned records — but exclude failed/terminal LGs
        # (SLA_EXCEEDED, BANK_REJECTED, CANCELLED are closed attempts — retry is allowed)
        terminal_statuses = ("SLA_EXCEEDED", "BANK_REJECTED", "CANCELLED")
        existing_lg = db.query(IssuedLGRecord).filter(
            IssuedLGRecord.request_id == request_id,
            IssuedLGRecord.status.notin_(terminal_statuses)
        ).first()
        if existing_lg:
            # Auto-heal: link and mark as issued
            request.lg_record_id = existing_lg.id
            request.status = "LG_ISSUED"
            db.flush()
            raise HTTPException(
                status_code=409,
                detail=f"An active LG record already exists for this request (#{existing_lg.id}, status={existing_lg.status}). Request status has been corrected."
            )

        already_reserved = request.status == "FACILITY_RESERVED"
        # If already reserved, use the stored sub_limit_id
        if already_reserved and not sub_limit_id:
            sub_limit_id = request.selected_sub_limit_id

        # --- STEP 2: Atomic Lock (prevents double-click / concurrent execution) ---
        if request.locked_for_issuance:
            # Check if this was pre-locked by the form endpoint for the same bank
            meta = dict(request.metadata_json or {})
            form_locked_bank = meta.get("locked_bank_id")
            if form_locked_bank and bank_id and int(form_locked_bank) == int(bank_id):
                # Form-endpoint lock for same bank → proceed (this is the expected flow)
                pass
            elif form_locked_bank and bank_id and int(form_locked_bank) != int(bank_id):
                raise HTTPException(
                    status_code=409,
                    detail=f"This request is locked to bank #{form_locked_bank}. Cannot issue from a different bank."
                )
            else:
                raise HTTPException(
                    status_code=409,
                    detail="This request is currently being processed by another user. Please wait."
                )
        else:
            request.locked_for_issuance = True
        db.flush()  # Push lock to DB immediately within this transaction

        try:
            # --- STEP 3: Verify Subscription Limits ---
            # Fetch the customer and their active subscription
            from app.models import Customer
            customer = db.query(Customer).filter(Customer.id == request.customer_id).first()
            if not customer:
                db.rollback()
                request.locked_for_issuance = False
                db.commit()
                raise HTTPException(status_code=404, detail="Customer not found.")

            # If they have a plan, verify they haven't exceeded their limit
            if customer.subscription_plan and customer.active_issuance_lg_count >= customer.subscription_plan.max_issuance_records:
                db.rollback()
                request.locked_for_issuance = False
                db.commit()
                raise HTTPException(
                    status_code=400, 
                    detail=f"Issuance LG limit ({customer.subscription_plan.max_issuance_records}) exceeded for this customer's subscription plan. Cannot execute new issuance."
                )

            # --- STEP 4: Facility Check & Exposure (conditional on sub_limit_id) ---
            sub_limit = None
            if sub_limit_id:
                sub_limit = db.query(IssuanceFacilitySubLimit).filter(
                    IssuanceFacilitySubLimit.id == sub_limit_id
                ).first()
                if not sub_limit:
                    raise HTTPException(status_code=404, detail="Selected facility sub-limit not found.")

                if not already_reserved:
                    # Only check availability and create exposure if NOT already reserved
                    # (reserved requests already have an active exposure entry)
                    
                    # C1: Convert request amount to facility currency for capacity comparison
                    facility = db.query(IssuanceFacility).filter(
                        IssuanceFacility.id == sub_limit.facility_id
                    ).first()

                    from app.services.fx_service import fx_service
                    facility_equivalent_amount, fx_rate = fx_service.convert(
                        db,
                        Decimal(str(request.amount)),
                        request.currency_id,
                        facility.currency_id if facility else request.currency_id,
                        allow_ai=False,
                    )
                    if facility_equivalent_amount is None:
                        raise HTTPException(
                            status_code=400,
                            detail="Cannot determine FX rate for facility currency conversion."
                        )

                    used_amount = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0))\
                        .filter(
                            IssuanceExposureEntry.sub_limit_id == sub_limit_id,
                            IssuanceExposureEntry.is_active == True
                        ).scalar()
                    used_amount = float(used_amount) + float(getattr(sub_limit, 'initial_utilization', 0) or 0)

                    available_amount = float(sub_limit.limit_amount) - used_amount

                    if float(facility_equivalent_amount) > available_amount:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Insufficient sub-limit capacity. Available: {available_amount:,.2f}, Required: {float(facility_equivalent_amount):,.2f}"
                        )

                    # Check FACILITY-level total cap
                    if facility:
                        facility_total_used_q = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0))\
                            .filter(
                                IssuanceExposureEntry.facility_id == facility.id,
                                IssuanceExposureEntry.is_active == True
                            ).scalar()
                        facility_total_used = float(facility_total_used_q)
                        for sl in facility.sub_limits:
                            facility_total_used += float(getattr(sl, 'initial_utilization', 0) or 0)
                        facility_available = float(facility.total_limit_amount) - facility_total_used
                        if float(facility_equivalent_amount) > facility_available:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Insufficient facility total limit. Available: {facility_available:,.2f}, Required: {float(facility_equivalent_amount):,.2f}"
                            )

                    # Check max amount per LG if configured
                    if sub_limit.max_amount_per_lg and float(facility_equivalent_amount) > float(sub_limit.max_amount_per_lg):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Amount exceeds maximum per LG ({float(sub_limit.max_amount_per_lg):,.2f}) for this sub-limit."
                        )

                    # Create Exposure Entry (RESERVATION)
                    from datetime import date as date_cls

                    exposure_entry = IssuanceExposureEntry(
                        facility_id=sub_limit.facility_id,
                        sub_limit_id=sub_limit_id,
                        request_id=request_id,
                        entry_type="RESERVATION",
                        original_amount_delta=request.amount,
                        original_currency_id=request.currency_id,
                        fx_rate_used=fx_rate,
                        facility_equivalent_delta=facility_equivalent_amount,
                        is_active=True,
                        effective_date=date_cls.today()
                    )
                    db.add(exposure_entry)
                    db.flush()

            # --- STEP 5: Create IssuedLGRecord ---
            # D1: Copy ALL relevant fields from request → LG record (self-contained)
            # D2: issue_date = None (populated from bank reply, not at execution)
            # Handle retry: if previous failed LGs exist for this request, append suffix
            final_ref_number = issued_ref_number
            previous_attempts = db.query(IssuedLGRecord).filter(
                IssuedLGRecord.request_id == request_id,
                IssuedLGRecord.status.in_(("SLA_EXCEEDED", "BANK_REJECTED", "CANCELLED"))
            ).count()
            if previous_attempts > 0:
                final_ref_number = f"{issued_ref_number}-R{previous_attempts + 1}"

            new_lg_record = IssuedLGRecord(
                lg_ref_number=final_ref_number,
                customer_id=request.customer_id,
                facility_sub_limit_id=sub_limit_id,
                bank_id=bank_id,
                request_id=request_id,
                # Core LG data
                beneficiary_name=request.beneficiary_name,
                beneficiary_address=getattr(request, 'beneficiary_address', None),
                beneficiary_country=getattr(request, 'beneficiary_country', None),
                current_amount=request.amount,
                currency_id=request.currency_id,
                issue_date=None,  # D2: Set to NULL — populated from bank reply
                requested_issue_date=issue_date or request.requested_issue_date,
                expiry_date=expiry_date or request.requested_expiry_date,
                status="INTERNAL_PROCESSING",
                # Accountability
                issued_by_user_id=user_id,
                issuance_method=issuance_method,
                current_owner_user_id=getattr(request, 'requestor_user_id', None),
                # D1: Entity & organizational context
                issuing_entity_id=getattr(request, 'issuing_entity_id', None),
                lg_type_id=getattr(request, 'lg_type_id', None),
                department=getattr(request, 'department', None),
                project_id=getattr(request, 'project_id', None),
                # D1: Conditions & flags
                is_cross_border=getattr(request, 'is_cross_border', False),
                is_third_party=getattr(request, 'is_third_party', False),
                reference_type=getattr(request, 'reference_type', None),
                lg_purpose=getattr(request, 'lg_purpose', None),
                lg_payable_currency_id=getattr(request, 'payable_currency_id', None),
                # D1 continued: newly migrated fields
                operational_status=getattr(request, 'operational_status', None),
                lg_language=getattr(request, 'lg_language', 'AR'),
                reference_number=getattr(request, 'reference_number', None),
                reference_amount=getattr(request, 'reference_amount', None),
                reference_currency_id=getattr(request, 'reference_currency_id', None),
                reference_start_date=getattr(request, 'reference_start_date', None),
                reference_end_date=getattr(request, 'reference_end_date', None),
                applicable_rules=getattr(request, 'applicable_rules', None),
                is_auto_reducing=getattr(request, 'is_auto_reducing', False),
                reduction_trigger=getattr(request, 'reduction_trigger', None),
                beneficiary_contact_person=getattr(request, 'beneficiary_contact_person', None),
                beneficiary_phone=getattr(request, 'beneficiary_phone', None),
                beneficiary_email=getattr(request, 'beneficiary_email', None),
                # D3: Manual pricing for no-facility LGs
                manual_pricing=manual_pricing if not sub_limit_id else None,
            )
            db.add(new_lg_record)
            db.flush()  # Get the ID

            # --- STEP 5b: Generate Internal Serial Number ---
            try:
                issuing_entity_id = getattr(request, 'issuing_entity_id', None)
                internal_serial = self._generate_lg_serial(db, request.customer_id, issuing_entity_id)
                new_lg_record.internal_serial = internal_serial
                db.flush()
                logger.info(f"LG internal serial assigned: {internal_serial}")
            except Exception as e:
                logger.warning(f"Failed to generate internal serial: {e}. LG created without serial.")

            # --- STEP 6: Update Request ---
            request.status = "INTERNAL_PROCESSING"
            request.lg_record_id = new_lg_record.id
            request.selected_sub_limit_id = sub_limit_id
            # Lock stays True until bank confirms or request is cancelled

            # --- STEP 6b: Increment active issuance count ---
            customer.active_issuance_lg_count = (customer.active_issuance_lg_count or 0) + 1

            # --- NEW: STEP 7: Execute Bank Instruction Strategy ---
            if bank_method_id:
                from app.models.models_issuance import BankIssuanceOption
                method = db.query(BankIssuanceOption).filter(BankIssuanceOption.id == bank_method_id).first()
                if method and method.is_active:
                    from app.core.issuance_strategies import IssuanceStrategyFactory
                    strategy = IssuanceStrategyFactory.get_strategy(method.strategy_code)
                    
                    config = method.configuration or {}
                    # Add necessary runtime data to config
                    config["lg_record_id"] = new_lg_record.id
                    config["request_id"] = request_id
                    
                    # Execute strategy (await to ensure completion)
                    await strategy.execute(db, request, config)
                    
                    logger.info(f"Executed strategy {method.strategy_code} for LG record {new_lg_record.id}")

            db.commit()
            db.refresh(new_lg_record)

            logger.info(
                f"LG Issued: request_id={request_id}, lg_record_id={new_lg_record.id}, "
                f"by user_id={user_id}, sub_limit_id={sub_limit_id}, amount={request.amount}"
            )

            # --- AUDIT LOG ---
            log_action(
                db, user_id=user_id,
                action_type="ISSUANCE_LG_ISSUED",
                entity_type="IssuanceRequest",
                entity_id=request_id,
                details={
                    "lg_record_id": new_lg_record.id,
                    "lg_ref_number": issued_ref_number,
                    "internal_serial": new_lg_record.internal_serial,
                    "sub_limit_id": sub_limit_id,
                    "amount": str(request.amount),
                    "currency_id": request.currency_id,
                    "issuance_method": issuance_method,
                    "status_before": "APPROVED_INTERNAL",
                    "status_after": "INTERNAL_PROCESSING"
                },
                customer_id=request.customer_id
            )

            return new_lg_record

        except HTTPException:
            # On validation failure, release the lock and rollback
            db.rollback()
            # Re-acquire the request to release the lock cleanly
            request = crud_issuance_request.get(db, id=request_id)
            if request:
                request.locked_for_issuance = False
                db.commit()
            raise
        except Exception as e:
            db.rollback()
            request = crud_issuance_request.get(db, id=request_id)
            if request:
                request.locked_for_issuance = False
                db.commit()
            logger.error(f"Issuance execution failed for request_id={request_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Issuance execution failed: {str(e)}")

    def cancel_request(
        self,
        db: Session,
        request_id: int,
        user_id: int,
        reason: str
    ) -> IssuanceRequest:
        """
        Cancels a request and releases any facility reservation.
        Allowed from: APPROVED_INTERNAL, INTERNAL_PROCESSING
        """
        import logging
        logger = logging.getLogger(__name__)

        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found.")

        cancellable_statuses = ("APPROVED_INTERNAL", "INTERNAL_PROCESSING")
        if request.status not in cancellable_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel request in status '{request.status}'. Must be one of: {cancellable_statuses}"
            )

        # Release any active exposure reservations
        active_entries = db.query(IssuanceExposureEntry).filter(
            IssuanceExposureEntry.request_id == request_id,
            IssuanceExposureEntry.is_active == True
        ).all()

        for entry in active_entries:
            entry.is_active = False

        # Cancel the linked LG record if it exists
        if request.lg_record_id:
            lg_record = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == request.lg_record_id).first()
            if lg_record and lg_record.status == "INTERNAL_PROCESSING":
                lg_record.status = "CANCELLED"
            
            # Decrement active issuance count (was incremented at execution)
            from app.models import Customer
            customer = db.query(Customer).filter(Customer.id == request.customer_id).first()
            if customer and (customer.active_issuance_lg_count or 0) > 0:
                customer.active_issuance_lg_count -= 1

        # Update request
        request.status = "CANCELLED"
        request.cancellation_reason = reason
        request.locked_for_issuance = False

        db.commit()
        db.refresh(request)

        logger.info(f"Request {request_id} cancelled by user {user_id}. Reason: {reason}")

        # --- AUDIT LOG ---
        log_action(
            db, user_id=user_id,
            action_type="ISSUANCE_REQUEST_CANCELLED",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={
                "reason": reason,
                "status_before": "INTERNAL_PROCESSING" if request.lg_record_id else "APPROVED_INTERNAL",
                "reservations_released": len(active_entries),
                "lg_record_cancelled": request.lg_record_id is not None
            },
            customer_id=request.customer_id
        )

        return request


    def request_cancellation(
        self,
        db: Session,
        request_id: int,
        user_id: int,
        reason: str
    ) -> IssuanceRequest:
        """
        End user or requestor submits a cancellation request (goes to admin for approval).
        Allowed from: PENDING_APPROVAL, APPROVED_INTERNAL, FACILITY_RESERVED
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found.")

        cancellable = ("PENDING_APPROVAL", "APPROVED_INTERNAL", "FACILITY_RESERVED")
        if request.status not in cancellable:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot request cancellation in status '{request.status}'. Allowed: {cancellable}"
            )

        # Save previous status so we can restore on rejection
        meta = request.metadata_json or {}
        meta["pre_cancel_status"] = request.status
        request.metadata_json = meta

        request.status = "CANCELLATION_REQUESTED"
        request.cancellation_reason = reason

        # Audit chain entry
        audit = request.approval_chain_audit or []
        audit.append({
            "action": "CANCELLATION_REQUESTED",
            "user_id": user_id,
            "reason": reason,
            "previous_status": meta["pre_cancel_status"],
            "timestamp": __import__('datetime').datetime.utcnow().isoformat()
        })
        request.approval_chain_audit = audit

        db.commit()
        db.refresh(request)

        logger.info(f"Cancellation requested for request {request_id} by user {user_id}. Reason: {reason}")

        # Audit log
        log_action(
            db, user_id=user_id,
            action_type="CANCELLATION_REQUESTED",
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={"reason": reason, "previous_status": meta["pre_cancel_status"]},
            customer_id=request.customer_id
        )

        # Notify admins
        try:
            from app.schemas.all_schemas import SystemNotificationCreate
            from app.crud.crud import crud_notification
            _now = __import__('datetime').datetime.utcnow()
            notif = SystemNotificationCreate(
                content=f"Cancellation requested for {request.serial_number}: {reason}",
                notification_type="CANCELLATION_REQUEST",
                target_roles=["corporate_admin"],
                target_customer_ids=[request.customer_id],
                start_date=_now,
                end_date=_now + __import__('datetime').timedelta(days=30),
                link=f"/corporate-admin/issuance/requests"
            )
            crud_notification.create_notification(db, obj_in=notif)
        except Exception as e:
            logger.warning(f"Failed to create cancellation notification: {e}")

        return request

    def resolve_cancellation(
        self,
        db: Session,
        request_id: int,
        admin_user_id: int,
        approved: bool,
        note: str = None
    ) -> IssuanceRequest:
        """
        Admin approves or rejects a cancellation request.
        Approve → cancel the request (release reservations, etc.)
        Reject  → restore previous status
        """
        request = crud_issuance_request.get(db, id=request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found.")

        if request.status != "CANCELLATION_REQUESTED":
            raise HTTPException(
                status_code=400,
                detail=f"Request is not in CANCELLATION_REQUESTED status (current: {request.status})."
            )

        meta = request.metadata_json or {}
        previous_status = meta.get("pre_cancel_status", "APPROVED_INTERNAL")

        if approved:
            # Release exposure reservations
            active_entries = db.query(IssuanceExposureEntry).filter(
                IssuanceExposureEntry.request_id == request_id,
                IssuanceExposureEntry.is_active == True
            ).all()
            for entry in active_entries:
                entry.is_active = False

            # Cancel linked LG record if any
            if request.lg_record_id:
                lg_record = db.query(IssuedLGRecord).filter(
                    IssuedLGRecord.id == request.lg_record_id
                ).first()
                if lg_record and lg_record.status == "INTERNAL_PROCESSING":
                    lg_record.status = "CANCELLED"

            request.status = "CANCELLED"
            request.locked_for_issuance = False
            action_type = "CANCELLATION_APPROVED"

            logger.info(f"Cancellation APPROVED for request {request_id} by admin {admin_user_id}")
        else:
            # Reject — restore previous status
            request.status = previous_status
            request.cancellation_reason = None  # Clear the reason
            action_type = "CANCELLATION_REJECTED"

            logger.info(f"Cancellation REJECTED for request {request_id} by admin {admin_user_id}")

        # Clean up metadata
        meta.pop("pre_cancel_status", None)
        request.metadata_json = meta

        # Audit chain entry
        audit = request.approval_chain_audit or []
        audit.append({
            "action": action_type,
            "user_id": admin_user_id,
            "note": note,
            "previous_status": previous_status,
            "timestamp": __import__('datetime').datetime.utcnow().isoformat()
        })
        request.approval_chain_audit = audit

        db.commit()
        db.refresh(request)

        # Audit log
        log_action(
            db, user_id=admin_user_id,
            action_type=action_type,
            entity_type="IssuanceRequest",
            entity_id=request_id,
            details={
                "approved": approved,
                "note": note,
                "cancellation_reason": request.cancellation_reason,
                "previous_status": previous_status
            },
            customer_id=request.customer_id
        )

        # Notify the requestor
        try:
            from app.schemas.all_schemas import SystemNotificationCreate
            from app.crud.crud import crud_notification
            _now = __import__('datetime').datetime.utcnow()
            status_word = "approved" if approved else "rejected"
            notif = SystemNotificationCreate(
                content=f"Your cancellation request for {request.serial_number} has been {status_word}." + (f" Note: {note}" if note else ""),
                notification_type="CANCELLATION_RESOLVED",
                target_user_ids=[request.requestor_user_id] if request.requestor_user_id else [],
                target_customer_ids=[request.customer_id],
                start_date=_now,
                end_date=_now + __import__('datetime').timedelta(days=30),
                link=f"/corporate-admin/issuance/requests"
            )
            crud_notification.create_notification(db, obj_in=notif)
        except Exception as e:
            logger.warning(f"Failed to create cancellation resolution notification: {e}")

        return request


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

    async def generate_issuance_letter(
        self,
        db: Session,
        request_id: int,
        customer_id: int,
        additional_text: str = "",
        use_special_wording: bool = False,
        field_overrides: dict = None,
    ) -> Dict[str, Any]:
        """
        Generates a signed letter PDF for an issuance request.
        Uses the custody template system: customer-specific template → global fallback.
        
        Returns: { "pdf_bytes": bytes, "filename": str, "template_name": str }
        """
        import logging
        from app.crud.crud import crud_template
        from app.core.document_generator import generate_pdf_from_html
        from sqlalchemy.orm import selectinload

        logger = logging.getLogger(__name__)

        # 1. Fetch the request with all relationships
        request = db.query(IssuanceRequest).options(
            selectinload(IssuanceRequest.currency),
            selectinload(IssuanceRequest.lg_type),
            selectinload(IssuanceRequest.issuing_entity),
            selectinload(IssuanceRequest.customer),
            selectinload(IssuanceRequest.project),
        ).filter(
            IssuanceRequest.id == request_id,
            IssuanceRequest.customer_id == customer_id,
        ).first()

        if not request:
            raise HTTPException(status_code=404, detail="Issuance request not found.")

        # 2. Resolve template: customer-specific first, then global (language-aware)
        lg_lang = getattr(request, 'lg_language', 'EN') or 'EN'
        template = crud_template.get_single_template(
            db,
            action_type="LG_ISSUANCE_REQUEST",
            is_global=False,
            customer_id=customer_id,
            is_notification_template=False,
            language=lg_lang,
        )
        if not template:
            template = crud_template.get_single_template(
                db,
                action_type="LG_ISSUANCE_REQUEST",
                is_global=True,
                is_notification_template=False,
                language=lg_lang,
            )
        if not template:
            raise HTTPException(
                status_code=404,
                detail="No template found for LG_ISSUANCE_REQUEST. Please create one in System Owner → Templates."
            )

        # 3. Resolve the bank name and bank account from the selected facility
        bank_name = "N/A"
        bank_account = None
        facility = None
        if request.selected_sub_limit_id:
            sub_limit = db.query(IssuanceFacilitySubLimit).options(
                selectinload(IssuanceFacilitySubLimit.facility).selectinload(IssuanceFacility.bank),
                selectinload(IssuanceFacilitySubLimit.facility).selectinload(IssuanceFacility.bank_account),
            ).filter(IssuanceFacilitySubLimit.id == request.selected_sub_limit_id).first()
            if sub_limit and sub_limit.facility:
                facility = sub_limit.facility
                if facility.bank:
                    bank_name = facility.bank.name
                # Get bank account: facility-linked first, then default for this bank
                if facility.bank_account:
                    bank_account = facility.bank_account
        
        # Fallback: find a default bank account for this customer+bank
        if not bank_account and facility and facility.bank_id:
            from app.models.models_issuance import CustomerBankAccount
            bank_account = db.query(CustomerBankAccount).filter(
                CustomerBankAccount.customer_id == customer_id,
                CustomerBankAccount.bank_id == facility.bank_id,
                CustomerBankAccount.is_default == True,
                CustomerBankAccount.is_deleted == False,
            ).first()
            # If no default, try any active for this bank
            if not bank_account:
                bank_account = db.query(CustomerBankAccount).filter(
                    CustomerBankAccount.customer_id == customer_id,
                    CustomerBankAccount.bank_id == facility.bank_id,
                    CustomerBankAccount.is_active == True,
                    CustomerBankAccount.is_deleted == False,
                ).first()

        # 4. Get form config for custom field labels
        from app.models.models_issuance import CustomerFormConfiguration
        form_config = db.query(CustomerFormConfiguration).filter(
            CustomerFormConfiguration.customer_id == customer_id,
            CustomerFormConfiguration.is_deleted == False,
        ).first()

        custom_field_1_label = ""
        custom_field_2_label = ""
        if form_config and form_config.custom_field_1_config:
            custom_field_1_label = form_config.custom_field_1_config.get("label", "")
        if form_config and form_config.custom_field_2_config:
            custom_field_2_label = form_config.custom_field_2_config.get("label", "")

        # 5. Build placeholder data dictionary
        amount_val = float(request.amount) if request.amount else 0
        currency_code = request.currency.iso_code if request.currency else "N/A"
        currency_name = request.currency.name if request.currency else "N/A"

        # Amount in words
        def amount_to_words(amount: float) -> str:
            try:
                from num2words import num2words
                integer_part = int(amount)
                decimal_part = round((amount - integer_part) * 100)
                words = num2words(integer_part).title()
                if decimal_part > 0:
                    words += f" and {num2words(decimal_part).title()} Cents"
                return words
            except ImportError:
                return f"{amount:,.2f}"

        # LG Wording clause — override from user or use request flag
        if use_special_wording or request.requires_special_wording:
            lg_wording_clause = "As Per Attached Special Wording Document"
        else:
            lg_wording_clause = "Bank's Standard Format"

        # Other instructions — combine request field + runtime additional text
        parts = []
        if request.other_conditions:
            parts.append(request.other_conditions)
        if additional_text:
            parts.append(additional_text)
        other_instructions = "\n".join(parts)

        # Conditions acceptance clause
        conditions_acceptance = (
            "This request is considered as our acceptance of the bank's standard terms and conditions "
            "applicable at the time of issuance of this Letter of Guarantee."
        )

        placeholder_data = {
            # Guarantee Core
            "beneficiary_name": request.beneficiary_name or "",
            "beneficiary_address": request.beneficiary_address or "",
            "amount": f"{amount_val:,.2f}",
            "amount_in_words": amount_to_words(amount_val),
            "currency_code": currency_code,
            "currency_name": currency_name,
            "lg_type": request.lg_type.name if request.lg_type else "N/A",
            "expiry_date": request.requested_expiry_date.strftime("%d-%b-%Y") if request.requested_expiry_date else "N/A",
            "issue_date": request.requested_issue_date.strftime("%d-%b-%Y") if request.requested_issue_date else date.today().strftime("%d-%b-%Y"),
            "purpose": request.lg_purpose or "",
            "reference_type": request.reference_type or "N/A",
            "reference_number": request.reference_number or "N/A",
            "serial_number": request.serial_number or f"REQ-{request.id}",
            
            # Bank Details
            "bank_name": bank_name,
            "account_name": bank_account.account_name if bank_account else "",
            "account_number": bank_account.account_number if bank_account else "",
            "customer_number": bank_account.customer_number if bank_account else "",
            "branch_name": bank_account.branch_name if bank_account else "",
            "iban": bank_account.iban if bank_account else "",

            # Customer / Company
            "customer_name": request.customer.name if request.customer else "N/A",
            "company_name": request.customer.name if request.customer else "N/A",
            "entity_name": request.issuing_entity.entity_name if request.issuing_entity else "",
            "customer_address": request.issuing_entity.address if request.issuing_entity and hasattr(request.issuing_entity, 'address') else "",
            "requestor_name": request.requestor_name or "",
            
            # Wording, Conditions & Free Text
            "lg_wording_clause": lg_wording_clause,
            "other_instructions": other_instructions,
            "other_instructions_section": (
                f'<div class="instructions"><p><strong>Other Instructions:</strong></p><p>{other_instructions}</p></div>'
                if other_instructions else ""
            ),
            "conditions_acceptance": conditions_acceptance,

            # Date & Extras
            "current_date": date.today().strftime("%d-%b-%Y"),
            "platform_name": "Treasury Management Platform",
            "custom_field_1_label": custom_field_1_label,
            "custom_field_1_value": request.custom_field_1_value or "",
            "custom_field_2_label": custom_field_2_label,
            "custom_field_2_value": request.custom_field_2_value or "",
        }

        # 5b. Apply user overrides from missing fields panel
        if field_overrides:
            for key, value in field_overrides.items():
                if key in placeholder_data and value and str(value).strip():
                    placeholder_data[key] = value

        # 6. Fill template — process conditional blocks first, then placeholders
        generated_html = template.content

        # 6a. Process {{#if key}}...{{/if}} conditional blocks
        import re
        def _process_conditionals(html, data):
            pattern = r'\{\{#if\s+(\w+)\}\}(.*?)\{\{/if\}\}'
            def replacer(match):
                field_key = match.group(1)
                inner_html = match.group(2)
                val = str(data.get(field_key, '') or '').strip()
                if val and val not in ('N/A', 'None', ''):
                    return inner_html
                return ''  # Strip entire block
            return re.sub(pattern, replacer, html, flags=re.DOTALL)

        generated_html = _process_conditionals(generated_html, placeholder_data)

        # 6b. Replace {{key}} placeholders
        for key, value in placeholder_data.items():
            str_value = str(value) if value is not None else ""
            generated_html = generated_html.replace(f"{{{{{key}}}}}", str_value)

        # 7. Generate PDF
        pdf_bytes = await generate_pdf_from_html(
            generated_html,
            filename_hint=f"lg_issuance_letter_{request.serial_number}"
        )
        if not pdf_bytes:
            raise HTTPException(
                status_code=500,
                detail="Failed to generate PDF from template."
            )

        logger.info(f"Generated issuance letter for request {request.serial_number} using template '{template.name}' ({len(pdf_bytes)} bytes)")

        return {
            "pdf_bytes": pdf_bytes,
            "filename": f"LG_Issuance_Letter_{request.serial_number}.pdf",
            "template_name": template.name,
            "template_id": template.id,
        }

    def get_similarity_matches(
        self,
        db: Session,
        customer_id: int,
        reference_type: Optional[str] = None,
        reference_number: Optional[str] = None,
        beneficiary_name: Optional[str] = None,
        amount: Optional[float] = None,
        currency: Optional[str] = None,
        lg_type_id: Optional[int] = None,
        requested_expiry_date: Optional[date] = None,
        exclude_request_id: Optional[int] = None
    ) -> dict:
        from app.models.models_issuance import IssuanceRequest, IssuedLGRecord
        from difflib import SequenceMatcher
        from datetime import datetime, timedelta
        
        cutoff = datetime.utcnow() - timedelta(days=365)
        
        # 1. Fetch Issued LGs
        lg_query = db.query(IssuedLGRecord).filter(
            IssuedLGRecord.customer_id == customer_id,
            IssuedLGRecord.created_at >= cutoff
        )
        if exclude_request_id:
            lg_query = lg_query.filter(IssuedLGRecord.request_id != exclude_request_id)
        issued_lgs = lg_query.all()
        
        # 2. Fetch Active Requests (Pending / Approved)
        req_query = db.query(IssuanceRequest).filter(
            IssuanceRequest.customer_id == customer_id,
            IssuanceRequest.created_at >= cutoff,
            IssuanceRequest.status.notin_(["ISSUED", "REJECTED_INTERNAL", "REJECTED_BANK", "CANCELLED", "DRAFT"])
        )
        if exclude_request_id:
            req_query = req_query.filter(IssuanceRequest.id != exclude_request_id)
        active_requests = req_query.all()
        
        results = []
        
        # Create a unified list of items to compare against
        compare_items = []
        for lg in issued_lgs:
            linked_req = None
            if lg.request_id:
                linked_req = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()
            
            compare_items.append({
                "type": "issued_lg",
                "obj": lg,
                "linked_req": linked_req,
                "ref_num": lg.lg_ref_number,
                "id": lg.id,
                "ben_name": lg.beneficiary_name,
                "amt": float(lg.current_amount) if lg.current_amount else 0.0,
                "currency": lg.currency.iso_code if lg.currency else "",
                "issue_date": lg.issue_date,
                "expiry_date": lg.expiry_date,
                "status": lg.status
            })
            
        for req in active_requests:
            # Skip if this request already birthed an LG that we are checking
            if any(item["linked_req"] and item["linked_req"].id == req.id for item in compare_items):
                continue
                
            compare_items.append({
                "type": "request",
                "obj": req,
                "linked_req": req,
                "ref_num": req.serial_number,
                "id": req.id,
                "ben_name": req.beneficiary_name,
                "amt": float(req.amount) if req.amount else 0.0,
                "currency": req.currency.iso_code if req.currency else "",
                "issue_date": req.requested_issue_date,
                "expiry_date": req.requested_expiry_date,
                "status": f"PENDING ({req.status})"
            })
            
        for item in compare_items:
            score = 0.0
            breakdown = {}
            total_possible = 100.0
            linked_req = item["linked_req"]
            
            # 1. Reference (10%)
            if reference_type and reference_number:
                if linked_req and linked_req.reference_type and linked_req.reference_number:
                    if (linked_req.reference_type.lower().strip() == reference_type.lower().strip() and 
                            linked_req.reference_number.lower().strip() == reference_number.lower().strip()):
                        score += 10
                        breakdown["reference"] = {"matched": True, "score": 10}
                    else:
                        breakdown["reference"] = {"matched": False, "score": 0}
                else:
                    # Historical record has no reference to compare against
                    total_possible -= 10
                    breakdown["reference"] = {"matched": None, "score": 0, "ignored": True}
            else:
                total_possible -= 10
                breakdown["reference"] = {"matched": None, "score": 0, "ignored": True}
                
            # 2. Beneficiary (35%)
            if beneficiary_name and item["ben_name"]:
                ratio = SequenceMatcher(
                    None, 
                    str(beneficiary_name).lower().strip(),
                    str(item["ben_name"]).lower().strip()
                ).ratio()
                if ratio >= 0.8:
                    ns = round(ratio * 35, 1)
                    score += ns
                    breakdown["beneficiary"] = {"matched": True, "score": ns, "similarity": round(ratio * 100)}
                else:
                    breakdown["beneficiary"] = {"matched": False, "score": 0, "similarity": round(ratio * 100)}
            else:
                total_possible -= 35
                breakdown["beneficiary"] = {"matched": None, "score": 0, "ignored": True}
                
            # 3. Amount & Currency (30%)
            if amount and item["amt"]:
                req_amt = float(amount)
                item_amt = float(item["amt"])
                
                req_curr = currency.upper().strip() if currency else None
                item_curr = str(item.get("currency", "")).upper().strip() if item.get("currency") else None
                
                # Check currency strict match if both are present
                currency_match = True
                if req_curr and item_curr and req_curr != item_curr:
                    currency_match = False
                
                if req_amt > 0 and item_amt > 0 and currency_match:
                    diff_pct = abs(req_amt - item_amt) / max(req_amt, item_amt)
                    if diff_pct <= 0.05:
                        ams = round((1 - diff_pct / 0.05) * 30, 1)
                        score += ams
                        breakdown["amount"] = {"matched": True, "score": ams, "lg_amount": str(item_amt)}
                    else:
                        breakdown["amount"] = {"matched": False, "score": 0, "lg_amount": str(item_amt)}
                else:
                    breakdown["amount"] = {"matched": False, "score": 0, "lg_amount": str(item_amt)}
            else:
                total_possible -= 30
                breakdown["amount"] = {"matched": None, "score": 0, "ignored": True}
                
            # 4. LG Type (10%)
            if lg_type_id and linked_req and hasattr(linked_req, 'lg_type_id') and linked_req.lg_type_id:
                if int(lg_type_id) == int(linked_req.lg_type_id):
                    score += 10
                    breakdown["lg_type"] = {"matched": True, "score": 10}
                else:
                    breakdown["lg_type"] = {"matched": False, "score": 0}
            else:
                total_possible -= 10
                breakdown["lg_type"] = {"matched": None, "score": 0, "ignored": True}
                
            # 5. Expiry (15%)
            if requested_expiry_date and item["expiry_date"]:
                req_exp = requested_expiry_date.date() if hasattr(requested_expiry_date, 'date') else requested_expiry_date
                item_exp = item["expiry_date"].date() if hasattr(item["expiry_date"], 'date') else item["expiry_date"]
                
                delta_days = abs((req_exp - item_exp).days)
                if delta_days <= 30:
                    es = round((1 - delta_days / 30) * 15, 1)
                    score += es
                    breakdown["expiry"] = {"matched": True, "score": es, "days_diff": delta_days}
                else:
                    breakdown["expiry"] = {"matched": False, "score": 0, "days_diff": delta_days}
            else:
                total_possible -= 15
                breakdown["expiry"] = {"matched": None, "score": 0, "ignored": True}
                
            if total_possible >= 65:
                final_score = round((score / total_possible) * 100, 1)
                
                if final_score >= 70:
                    exact_ref = False
                    if (reference_type and reference_number and 
                        linked_req and linked_req.reference_type and linked_req.reference_number and
                        linked_req.reference_type.lower() == reference_type.lower() and 
                        linked_req.reference_number.lower() == reference_number.lower()):
                        exact_ref = True
                    
                    results.append({
                        "lg_ref_number": item["ref_num"],
                        "lg_id": item["id"] if item["type"] == "issued_lg" else None,
                        "request_id": item["id"] if item["type"] == "request" else None,
                        "match_type": item["type"],
                        "beneficiary_name": item["ben_name"],
                        "amount": str(item["amt"]),
                        "currency": item.get("currency", ""),
                        "issue_date": str(item["issue_date"]) if item["issue_date"] else None,
                        "expiry_date": str(item["expiry_date"]) if item["expiry_date"] else None,
                        "status": item["status"],
                        "score": final_score,
                        "breakdown": breakdown,
                        "exact_ref": exact_ref,
                        "recall_data": {
                            "reference_amount": str(linked_req.reference_amount) if linked_req and hasattr(linked_req, 'reference_amount') and linked_req.reference_amount else None,
                            "reference_currency_id": getattr(linked_req, 'reference_currency_id', None) if linked_req else None,
                            "reference_start_date": str(linked_req.reference_start_date) if linked_req and hasattr(linked_req, 'reference_start_date') and linked_req.reference_start_date else None,
                            "reference_end_date": str(linked_req.reference_end_date) if linked_req and hasattr(linked_req, 'reference_end_date') and linked_req.reference_end_date else None,
                            "project_id": getattr(linked_req, 'project_id', None) if linked_req else None,
                        } if exact_ref else None
                    })
        results.sort(key=lambda x: x["score"], reverse=True)
        return {
            "found": len(results) > 0,
            "matches": results,
            "total_issued_compared": len(issued_lgs),
            "total_requests_compared": len(active_requests)
        }

issuance_service = IssuanceService()