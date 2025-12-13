# app/crud/crud_reports.py

import json
from datetime import date, datetime, timedelta
import decimal
from typing import Any, Dict, List, Optional, Tuple, Type
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func, and_, case, Date, cast, or_
from fastapi import HTTPException, status
from sqlalchemy.sql import func as sql_func
from decimal import Decimal
from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.models import (
    LGRecord, LGInstruction, Customer, SubscriptionPlan, User, AuditLog, LGCategory,
    CustomerEntity, Bank, LgStatus, LgType, InternalOwnerContact,
    UserCustomerEntityAssociation, LGCategoryCustomerEntityAssociation, Currency
)
from app.schemas.all_schemas import (
    ReportFilterBase,
    SystemUsageOverviewReportOut, SystemUsageOverviewReportItemOut,
    CustomerLGPerformanceReportOut, CustomerLGPerformanceReportItemOut,
    MyLGDashboardReportOut, MyLGDashboardReportItemOut
)
from app.constants import (
    UserRole, LgStatusEnum, GlobalConfigKey,
    ACTION_TYPE_LG_EXTEND, ACTION_TYPE_LG_RELEASE, ACTION_TYPE_LG_LIQUIDATE, ACTION_TYPE_LG_AMEND,
    ACTION_TYPE_LG_DECREASE_AMOUNT, ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
    AUDIT_ACTION_TYPE_LG_INSTRUCTION_DELIVERED, AUDIT_ACTION_TYPE_LG_BANK_REPLY_RECORDED, ACTION_TYPE_LG_RECORD_DELIVERY, ACTION_TYPE_LG_RECORD_BANK_REPLY
)

import logging
logger = logging.getLogger(__name__)


class CRUDReports(CRUDBase):
    def __init__(self, model: Type[LGRecord], crud_customer_configuration_instance: Any, crud_user_instance: Any):
        super().__init__(model)
        self.crud_customer_configuration_instance = crud_customer_configuration_instance
        self.crud_user_instance = crud_user_instance

    def _apply_common_filters(
        self,
        query: Any, # SQLAlchemy query object
        user_context: Dict[str, Any],
        lg_record_model: Type[LGRecord] = LGRecord
    ) -> Any:
        """Applies filters based on user role and report filter criteria."""
        user_role = user_context['role']
        user_customer_id = user_context['customer_id']
        user_entity_ids = user_context['entity_ids']
        user_has_all_entity_access = user_context['has_all_entity_access']
        
        # Always exclude deleted LG records
        query = query.filter(lg_record_model.is_deleted == False)

        # Role-based access control
        if user_role == UserRole.END_USER:
            query = query.filter(lg_record_model.customer_id == user_customer_id)
            if not user_has_all_entity_access:
                query = query.filter(lg_record_model.beneficiary_corporate_id.in_(user_entity_ids))
        elif user_role == UserRole.CORPORATE_ADMIN:
            query = query.filter(lg_record_model.customer_id == user_customer_id)
        # System Owner does not need filtering here as they see everything by default

        return query

    # --- NEW: System Owner Report - System Usage Overview ---
    def get_system_usage_overview_report(
        self, db: Session, user_context: Dict[str, Any]
    ) -> SystemUsageOverviewReportOut:
        logger.info(f"Generating System Usage Overview for System Owner {user_context['user_id']}.")

        if user_context['role'] != UserRole.SYSTEM_OWNER:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only System Owners can access this report.")

        total_customers = db.query(Customer).filter(Customer.is_deleted == False).count()
        total_lgs = db.query(LGRecord).filter(LGRecord.is_deleted == False).count()
        total_users = db.query(User).filter(User.is_deleted == False).count()
        total_instructions = db.query(LGInstruction).filter(LGInstruction.is_deleted == False).count()

        # Count emails sent by checking AuditLog
        email_actions = [
            "NOTIFICATION_SENT", "PASSWORD_RESET_INITIATED", "LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT"
        ]
        total_emails_sent = db.query(AuditLog).filter(AuditLog.action_type.in_(email_actions)).count()

        report_data = SystemUsageOverviewReportItemOut(
            total_customers=total_customers,
            total_lgs_managed=total_lgs,
            total_users=total_users,
            total_instructions_issued=total_instructions,
            total_emails_sent=total_emails_sent
        )

        return SystemUsageOverviewReportOut(
            report_date=date.today(),
            data=report_data
        )

    # --- NEW: Corporate Admin Report - Customer LG Performance ---
    def get_customer_lg_performance_report(
        self, db: Session, user_context: Dict[str, Any]
    ) -> CustomerLGPerformanceReportOut:
        logger.info(f"Generating Customer LG Performance report for customer {user_context['customer_id']}.")

        if user_context['role'] not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER]:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")
            
        customer_id = user_context['customer_id']
        if user_context['role'] == UserRole.SYSTEM_OWNER and 'customer_id' in user_context:
            customer_id = user_context['customer_id'] # Allow System Owner to filter

        # 1. LGs by Status
        lgs_by_status_query = db.query(
            LgStatus.name,
            func.count(LGRecord.id)
        ).join(LGRecord).filter(
            LGRecord.customer_id == customer_id,
            LGRecord.is_deleted == False
        ).group_by(LgStatus.name).all()
        lgs_by_status = {name: count for name, count in lgs_by_status_query}

        # 2. Instructions by Type
        instructions_by_type_query = db.query(
            LGInstruction.instruction_type,
            func.count(LGInstruction.id)
        ).join(LGRecord).filter(
            LGRecord.customer_id == customer_id,
            LGRecord.is_deleted == False,
            LGInstruction.is_deleted == False
        ).group_by(LGInstruction.instruction_type).all()
        instructions_by_type = {type: count for type, count in instructions_by_type_query}

        # 3. Total Value of Active LGs
        active_lg_value_query = db.query(
            func.sum(LGRecord.lg_amount),
            Currency.iso_code
        ).join(Currency, LGRecord.lg_currency_id == Currency.id).filter(
            LGRecord.customer_id == customer_id,
            LGRecord.is_deleted == False,
            LGRecord.lg_status_id == LgStatusEnum.VALID.value
        ).group_by(Currency.iso_code).all()
        total_lg_value_active = {iso_code: amount for amount, iso_code in active_lg_value_query}

        # 4. User Actions
        users_and_actions_query = db.query(
            User.email,
            func.count(AuditLog.id)
        ).outerjoin(AuditLog, AuditLog.user_id == User.id).filter(
            User.customer_id == customer_id,
            User.is_deleted == False,
            AuditLog.customer_id == customer_id
        ).group_by(User.email).all()

        users_with_actions = {email: count for email, count in users_and_actions_query}


        report_data = CustomerLGPerformanceReportItemOut(
            lgs_by_status=lgs_by_status,
            instructions_by_type=instructions_by_type,
            total_value_of_active_lgs=total_lg_value_active,
            users_with_action_counts=users_with_actions
        )

        return CustomerLGPerformanceReportOut(
            report_date=date.today(),
            data=report_data
        )
    
    def get_my_lg_dashboard_report(
        self, db: Session, user_context: Dict[str, Any]
    ) -> MyLGDashboardReportOut:
        logger.info(f"Generating My LG Dashboard report for End User {user_context['user_id']}.")

        if user_context['role'] not in [UserRole.END_USER, UserRole.CORPORATE_ADMIN]:
             # Allow Admins to see the dashboard too (for the Safety View)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")

        user_id = user_context['user_id']
        customer_id = user_context['customer_id']
        user_has_all_entity_access = user_context['has_all_entity_access']
        user_entity_ids = user_context['entity_ids']

        # Determine the user's LG access filter
        lg_filter_conditions = [LGRecord.is_deleted == False, LGRecord.customer_id == customer_id]
        if not user_has_all_entity_access:
            lg_filter_conditions.append(LGRecord.beneficiary_corporate_id.in_(user_entity_ids))
        
        # --- 1. ORIGINAL LOGIC: My LGs List ---
        my_lg_records = db.query(LGRecord).join(InternalOwnerContact).filter(
            InternalOwnerContact.email == user_context['email'],
            *lg_filter_conditions
        ).options(
            selectinload(LGRecord.lg_status),
            selectinload(LGRecord.lg_currency),
            selectinload(LGRecord.beneficiary_corporate)
        ).all()
        my_lgs_count = len(my_lg_records)
        

        # --- 2. ORIGINAL LOGIC: LGs Nearing Expiry ---
        config_key = GlobalConfigKey.AUTO_RENEWAL_DAYS_BEFORE_EXPIRY
        days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, customer_id, config_key
        )
        configurable_days = int(days_config.get('effective_value', 60)) if days_config else 60
        expiry_cutoff_date = date.today() + timedelta(days=configurable_days)
        
        lgs_near_expiry_query = db.query(LGRecord).filter(
            LGRecord.expiry_date >= date.today(),
            LGRecord.expiry_date <= expiry_cutoff_date,
            *lg_filter_conditions
        ).all()
        lgs_near_expiry_count = len(lgs_near_expiry_query)


        # --- 3. ORIGINAL LOGIC: Instructions Not Delivered ---
        report_start_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED
        )
        report_start_days = int(report_start_days_config.get('effective_value', 3)) if report_start_days_config else 3

        undelivered_instructions_query = db.query(LGInstruction).join(LGRecord).filter(
            LGInstruction.is_deleted == False,
            LGInstruction.delivery_date.is_(None),
            LGInstruction.maker_user_id == user_id,
            func.date(LGInstruction.instruction_date) <= (date.today() - timedelta(days=report_start_days)),
            *lg_filter_conditions
        ).all()
        undelivered_instructions_count = len(undelivered_instructions_query)
        

        # --- 4. ORIGINAL LOGIC: Recent Actions ---
        recent_actions_query = db.query(AuditLog).filter(
            AuditLog.user_id == user_id
        ).order_by(AuditLog.timestamp.desc()).limit(10).all()
        
        recent_actions_list = []
        for log in recent_actions_query:
            details_summary = str(log.details) if log.details else "No details."
            if len(details_summary) > 100:
                details_summary = details_summary[:97] + "..."
            recent_actions_list.append(
                f"{log.timestamp.strftime('%Y-%m-%d %H:%M')}: {log.action_type} on {log.entity_type} (ID: {log.entity_id}). Details: {details_summary}"
            )

        # =================================================================================
        # --- NEW LOGIC: UPCOMING EXPIRIES LIST & SAFETY SCORE ---
        # =================================================================================
        
        # A. Fetch the actual list of expiring LGs (Reuse the query from step 2 but sort it)
        # We sort by date to show the most urgent ones first
        upcoming_expiries_list = []
        lgs_sorted = sorted(lgs_near_expiry_query, key=lambda x: x.expiry_date)
        
        for lg in lgs_sorted:
            days_left = (lg.expiry_date - date.today()).days
            upcoming_expiries_list.append({
                "lg_number": lg.lg_number,
                "bank_name": lg.issuing_bank.name if lg.issuing_bank else "Unknown Bank",
                "expiry_date": lg.expiry_date,
                "days_remaining": days_left
            })

        # B. Calculate Safety Score (Simple Logic)
        # Base score 100. Minus 10 points for every expired item. Minus 2 points for every expiring item.
        # (You can adjust this formula later)
        score = 100
        # Check for *actual* expired items (past today) which are risky
        actually_expired_count = db.query(LGRecord).filter(
             LGRecord.expiry_date < date.today(), 
             LGRecord.status == LgStatusEnum.ACTIVE,
             *lg_filter_conditions
        ).count()
        
        score -= (actually_expired_count * 10)
        score -= (lgs_near_expiry_count * 2) 
        score = max(0, min(100, score)) # Clamp between 0 and 100

        # C. Determine Risk Label
        if score >= 90:
            risk_status = "Stable"
            risk_color = "green"
        elif score >= 70:
            risk_status = "Attention"
            risk_color = "yellow"
        else:
            risk_status = "Critical"
            risk_color = "red"


        # --- COMBINE EVERYTHING ---
        report_data = MyLGDashboardReportItemOut(
            # Original Data
            my_lgs_count=my_lgs_count,
            lgs_near_expiry_count=lgs_near_expiry_count,
            undelivered_instructions_count=undelivered_instructions_count,
            recent_actions=recent_actions_list,
            
            # New Data
            safety_score=score,
            risk_status=risk_status,
            risk_color=risk_color,
            upcoming_expiries=upcoming_expiries_list
        )

        return MyLGDashboardReportOut(
            report_date=date.today(),
            data=report_data
        )
    def get_chart_data(self, db: Session, report_type: str, user_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Retrieves data for the dashboard charts based on report type and user context.
        """
        query = None
        customer_id = user_context.get('customer_id')

        # LG Type Mix
        if report_type == "lg_type_mix":
            base_lg_query = db.query(models.LGRecord).filter(models.LGRecord.is_deleted == False)
            if customer_id:
                base_lg_query = base_lg_query.filter(models.LGRecord.customer_id == customer_id)
            query = base_lg_query.with_entities(
                models.LgType.name,
                func.count(models.LGRecord.id)
            ).join(models.LgType).group_by(models.LgType.name)

        # Bank Processing Times
        elif report_type == "bank_processing_times":
            instruction_query = db.query(models.LGInstruction).filter(
                models.LGInstruction.is_deleted == False,
                models.LGInstruction.delivery_date.isnot(None),
                models.LGInstruction.bank_reply_date.isnot(None),
            ).join(models.LGRecord)
            
            if customer_id:
                instruction_query = instruction_query.filter(models.LGRecord.customer_id == customer_id)

            query = instruction_query.with_entities(
                models.Bank.short_name,
                func.avg(models.LGInstruction.bank_reply_date - models.LGInstruction.delivery_date).label('avg_timedelta')
            ).join(models.Bank, models.LGRecord.issuing_bank_id == models.Bank.id).group_by(models.Bank.short_name)

        # Bank Market Share
        elif report_type == "bank_market_share":
            base_lg_query = db.query(models.LGRecord).filter(models.LGRecord.is_deleted == False)
            if customer_id:
                base_lg_query = base_lg_query.filter(models.LGRecord.customer_id == customer_id)
            query = base_lg_query.with_entities(
                models.Bank.short_name,
                func.count(models.LGRecord.id)
            ).join(models.Bank, models.LGRecord.issuing_bank_id == models.Bank.id).group_by(models.Bank.short_name)

        # Average Delivery Days
        elif report_type == "avg_delivery_days":
            instruction_query = db.query(models.LGInstruction).filter(
                models.LGInstruction.is_deleted == False,
                models.LGInstruction.delivery_date.isnot(None),
                models.LGInstruction.instruction_date.isnot(None),
            ).join(models.LGRecord)
            
            if customer_id:
                instruction_query = instruction_query.filter(models.LGRecord.customer_id == customer_id)

            query = instruction_query.with_entities(
                func.avg(models.LGInstruction.delivery_date - models.LGInstruction.instruction_date)
            )

        # Average Days to Action
        elif report_type == "avg_days_to_action":
            action_types = [
                ACTION_TYPE_LG_EXTEND, ACTION_TYPE_LG_RELEASE, ACTION_TYPE_LG_LIQUIDATE, ACTION_TYPE_LG_DECREASE_AMOUNT
            ]
            instruction_query = db.query(models.LGInstruction).filter(
                models.LGInstruction.is_deleted == False,
                models.LGInstruction.instruction_type.in_(action_types)
            ).join(models.LGRecord)

            if customer_id:
                instruction_query = instruction_query.filter(models.LGRecord.customer_id == customer_id)
            
            query = instruction_query.with_entities(
                func.avg(models.LGRecord.expiry_date - models.LGInstruction.instruction_date)
            )

        if query:
            results = query.all()
            if report_type == "bank_processing_times":
                return [{"name": row.short_name, "value": row.avg_timedelta.total_seconds() / 86400} for row in results]
            elif report_type in ["avg_delivery_days", "avg_days_to_action"]:
                avg_timedelta = results[0][0] if results and results[0][0] is not None else None
                avg_days = avg_timedelta.total_seconds() / 86400 if avg_timedelta else None
                return {'average_days': avg_days} if avg_days is not None else None
            return [{"name": row[0], "value": row[1]} for row in results]
        return []
    
def get_all_lg_lifecycle_history(
    db: Session,
    customer_id: int,
    user_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    action_types: Optional[List[str]] = None,
    lg_record_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieves the full lifecycle history for the given customer.
    Fixes '0E-10' by converting amounts to formatted strings.
    """
    
    # 1. Base Query
    query = db.query(models.AuditLog, models.LGRecord, models.User).join(
        models.LGRecord, models.AuditLog.lg_record_id == models.LGRecord.id
    ).join(
        models.User, models.AuditLog.user_id == models.User.id, isouter=True
    ).filter(
        models.LGRecord.is_deleted == False
    ).options(
        selectinload(models.LGRecord.customer),
        selectinload(models.LGRecord.beneficiary_corporate),
        selectinload(models.LGRecord.issuing_bank),
        selectinload(models.LGRecord.lg_type),
        selectinload(models.LGRecord.lg_category),
        selectinload(models.LGRecord.lg_currency), 
        selectinload(models.LGRecord.internal_owner_contact),
    )
    
    # 2. Filtering
    query = query.filter(models.LGRecord.customer_id == customer_id)
    
    if start_date:
        query = query.filter(func.date(models.AuditLog.timestamp) >= start_date)
    
    if end_date:
        query = query.filter(func.date(models.AuditLog.timestamp) <= end_date)

    if action_types:
        query = query.filter(models.AuditLog.action_type.in_(action_types))

    if lg_record_ids:
        query = query.filter(models.LGRecord.id.in_(lg_record_ids))

    query = query.order_by(models.AuditLog.timestamp.desc())

    # 3. Execute and Format
    results = []
    raw_data = query.all()

    for log, lg, user in raw_data:
        # Safely get relational data
        ben_name = lg.beneficiary_corporate.entity_name if lg.beneficiary_corporate else None
        bank_name = lg.issuing_bank.name if lg.issuing_bank else lg.foreign_bank_name if lg.foreign_bank_name else None
        currency_code = lg.lg_currency.iso_code if lg.lg_currency else None
        
        # --- Initialize fields ---
        instruction_serial = None
        delivery_date = None
        bank_reply_date = None
        old_expiry_date = None
        new_expiry_date = None
        
        # Amounts
        old_amount_dec: Optional[Decimal] = None
        new_amount_dec: Optional[Decimal] = None
        amount_change: Optional[float] = None 
        
        reason = None
        log_details: Dict[str, Any] = log.details if log.details else {}
        summary_description = log.action_type.replace('_', ' ').title() if log.action_type else "Action Performed"
        
        # --- Extraction Logic ---
        
        # 1. Logistics Fields 
        if log.action_type == AUDIT_ACTION_TYPE_LG_INSTRUCTION_DELIVERED:
            instruction_serial = log_details.get("instruction_serial") or log_details.get("serial_number")
            date_str = log_details.get("delivery_date")
            try: delivery_date = date.fromisoformat(date_str) if date_str else None
            except ValueError: delivery_date = None
            
            delivery_date_str = delivery_date.isoformat() if delivery_date else "N/A"
            summary_description = f"LG Instruction Delivered to Bank on {delivery_date_str}."

        elif log.action_type == AUDIT_ACTION_TYPE_LG_BANK_REPLY_RECORDED:
            instruction_serial = log_details.get("instruction_serial") or log_details.get("serial_number")
            date_str = log_details.get("bank_reply_date")
            try: bank_reply_date = date.fromisoformat(date_str) if date_str else None
            except ValueError: bank_reply_date = None
            
            reason = log_details.get("reply_details", "") or log_details.get("reason", "") 
            
            reply_date_str = bank_reply_date.isoformat() if bank_reply_date else "N/A"
            detail_note = f" (Details: {reason})" if reason else ""
            summary_description = f"Bank Reply Recorded on {reply_date_str}.{detail_note}"
        
        # 2. Time Amendment Fields
        elif log.action_type in [ACTION_TYPE_LG_EXTEND, ACTION_TYPE_LG_AMEND]:
            old_expiry_date_str = log_details.get("old_expiry_date")
            new_expiry_date_str = log_details.get("new_expiry_date")
            
            try: old_expiry_date = date.fromisoformat(old_expiry_date_str) if old_expiry_date_str else None
            except ValueError: old_expiry_date = None
            try: new_expiry_date = date.fromisoformat(new_expiry_date_str) if new_expiry_date_str else None
            except ValueError: new_expiry_date = None
            
            reason = log_details.get("reason", "")
            if reason:
                 summary_description = f"LG Amended. Reason: {reason}."
            
        # 3. Financial Amendment Fields
        elif log.action_type in [ACTION_TYPE_LG_DECREASE_AMOUNT, ACTION_TYPE_LG_LIQUIDATE]:
            try:
                old_amount_dec = Decimal(str(log_details.get("old_amount"))) if log_details.get("old_amount") is not None else None
                new_amount_dec = Decimal(str(log_details.get("new_amount"))) if log_details.get("new_amount") is not None else None
            except (decimal.InvalidOperation, TypeError):
                old_amount_dec = None
                new_amount_dec = None
            
            if old_amount_dec is not None and new_amount_dec is not None:
                amount_change = float(old_amount_dec - new_amount_dec)
            else:
                amount_change = None
            
            reason = log_details.get("reason", "")
            
            if amount_change is not None and currency_code:
                new_total_str = f"{new_amount_dec:,.2f}" if new_amount_dec is not None else "N/A"
                summary_description = (
                    f"Decreased by {abs(amount_change):,.2f} {currency_code}. "
                    f"New Total: {new_total_str} {currency_code}."
                )
            elif reason:
                 summary_description = f"LG Decreased/Liquidated. Reason: {reason}."
            else:
                summary_description = "LG Financial Change (Details N/A)."

        # --- Summary Cleanup ---
        summary_description = log_details.get("summary_description") or log_details.get("summary") or summary_description
        
        if "N/A" in summary_description or "n/a" in summary_description:
            if "Reason: N/A" in summary_description and reason:
                 summary_description = summary_description.replace("Reason: N/A.", f"Reason: {reason}.")
            elif "Decreased by N/A" in summary_description and amount_change is not None and currency_code:
                summary_description = summary_description.replace("Decreased by N/A.", f"Decreased by {abs(amount_change):,.2f} {currency_code}.")
            elif "Serial: N/A" in summary_description and instruction_serial:
                summary_description = summary_description.replace("Serial: N/A.", f"Serial: {instruction_serial}.")
        
        
        # --- CRITICAL FIX: CONVERT TO STRING TO STOP 0E-10 ---
        def format_amount_to_string(value: Optional[Decimal]) -> Optional[str]:
            if value is None:
                return None
            try:
                val_dec = Decimal(str(value))
                return f"{val_dec:.2f}"
            except:
                return str(value)

        lg_amount_str = format_amount_to_string(lg.lg_amount)
        old_amount_str = format_amount_to_string(old_amount_dec)
        new_amount_str = format_amount_to_string(new_amount_dec)
        
        results.append({
            "lg_record_id": lg.id,
            "lg_number": lg.lg_number,
            "issuer_name": lg.customer.name if lg.customer else None,
            "beneficiary_name": ben_name,
            "internal_owner_email": lg.internal_owner_contact.email if lg.internal_owner_contact else None,
            "issuing_bank_name": bank_name,
            "issuance_date": lg.issuance_date.date() if lg.issuance_date else None,
            "lg_type_name": lg.lg_type.name if lg.lg_type else None,
            "lg_category_name": lg.lg_category.name if lg.lg_category else None,
            
            # These are now Strings, so they will export exactly as "0.00" or "500.00"
            "lg_amount": lg_amount_str,
            "lg_currency": currency_code,
            
            "action_type": log.action_type,
            "timestamp": log.timestamp,
            "user_email": user.email if user else None,
            "details": log_details, 
            
            "instruction_serial": instruction_serial,
            "delivery_date": delivery_date,
            "bank_reply_date": bank_reply_date,
            "old_expiry_date": old_expiry_date,
            "new_expiry_date": new_expiry_date,
            
            "old_amount": old_amount_str, 
            "new_amount": new_amount_str,
            
            "amount_change": amount_change,
            "summary_description": summary_description, 
        })

    return results