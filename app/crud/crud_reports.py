# app/crud/crud_reports.py

import json
from datetime import date, datetime, timedelta
import decimal
from typing import Any, Dict, List, Optional, Tuple, Type
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func, and_, case, Date, cast, or_
from fastapi import HTTPException, status
from sqlalchemy.sql import func as sql_func

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
    AUDIT_ACTION_TYPE_LG_INSTRUCTION_DELIVERED, AUDIT_ACTION_TYPE_LG_BANK_REPLY_RECORDED
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
    
    # --- NEW: End User Report - My LG Dashboard ---
    def get_my_lg_dashboard_report(
        self, db: Session, user_context: Dict[str, Any]
    ) -> MyLGDashboardReportOut:
        logger.info(f"Generating My LG Dashboard report for End User {user_context['user_id']}.")

        if user_context['role'] != UserRole.END_USER:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only End Users can access this report.")

        user_id = user_context['user_id']
        customer_id = user_context['customer_id']
        user_has_all_entity_access = user_context['has_all_entity_access']
        user_entity_ids = user_context['entity_ids']

        # Determine the user's LG access filter
        lg_filter_conditions = [LGRecord.is_deleted == False, LGRecord.customer_id == customer_id]
        if not user_has_all_entity_access:
            lg_filter_conditions.append(LGRecord.beneficiary_corporate_id.in_(user_entity_ids))
        
        # 1. My LGs List (LGs assigned to this user)
        # Assuming an LG is "assigned" if the user is the internal owner contact
        my_lg_records = db.query(LGRecord).join(InternalOwnerContact).filter(
            InternalOwnerContact.email == user_context['email'],
            *lg_filter_conditions
        ).options(
            selectinload(LGRecord.lg_status),
            selectinload(LGRecord.lg_currency),
            selectinload(LGRecord.beneficiary_corporate)
        ).all()
        my_lgs_count = len(my_lg_records)
        

        # 2. LGs Nearing Expiry
        # Get configurable days from customer config or global fallback
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

        # 3. Instructions Not Delivered
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
        

        # 4. Recent Actions
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


        report_data = MyLGDashboardReportItemOut(
            my_lgs_count=my_lgs_count,
            lgs_near_expiry_count=lgs_near_expiry_count,
            undelivered_instructions_count=undelivered_instructions_count,
            recent_actions=recent_actions_list
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
            ).join(models.Bank).group_by(models.Bank.short_name)

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