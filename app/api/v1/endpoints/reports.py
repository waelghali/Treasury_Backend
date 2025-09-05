# app/api/v1/endpoints/reports.py

import os
import sys
import importlib.util
from datetime import date, datetime, timedelta
from typing import List, Optional, Any, Dict
import io
import csv
import decimal
import json
from fastapi import APIRouter, Depends, HTTPException, status, Query, Response, Request
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas.all_schemas import (
    SystemUsageOverviewReportOut,
    CustomerLGPerformanceReportOut,
    MyLGDashboardReportOut,
    LgTypeMixReportOut,
    AvgBankProcessingTimeReportOut,
    BankMarketShareReportOut,
    AvgDeliveryDaysReportOut,
    AvgDaysToActionEventReportOut,
    AvgDaysToActionEventOut,
    DemoRequestCreate,
)
from app.crud.crud import crud_reports, log_action
from app.constants import UserRole, GlobalConfigKey

import logging
logger = logging.getLogger(__name__)

# --- Corrected Imports for security and user context ---
try:
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_file_dir, '..', '..', '..'))
    
    security_module_path = os.path.join(project_root, 'core', 'security.py')
    if not os.path.exists(security_module_path):
        raise FileNotFoundError(f"Expected core/security.py at {security_module_path} but it was not found.")
    
    spec = importlib.util.spec_from_file_location("app.core.security", security_module_path)
    core_security = importlib.util.module_from_spec(spec)
    sys.modules["app.core.security"] = core_security
    spec.loader.exec_module(core_security)
    from app.core.security import (
        TokenData,
        HasPermission,
        get_current_active_user,
    )
except Exception as e:
    logger.critical(f"FATAL ERROR (reports.py): Could not import core.security module directly. Error: {e}", exc_info=True)
    raise

router = APIRouter(prefix="/reports", tags=["Reports"])

# FIX: Refactor to a self-contained dependency to avoid injection issues
async def get_current_report_user_context(
    current_user_active: TokenData = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """
    Dependency that extracts relevant user context for reporting based on a single dependency.
    """
    if current_user_active.role == UserRole.SYSTEM_OWNER:
        return {
            "user_id": current_user_active.user_id,
            "email": current_user_active.email,
            "role": current_user_active.role,
            "customer_id": None,
            "has_all_entity_access": True,
            "entity_ids": [],
        }
    elif current_user_active.role == UserRole.CORPORATE_ADMIN:
        return {
            "user_id": current_user_active.user_id,
            "email": current_user_active.email,
            "role": current_user_active.role,
            "customer_id": current_user_active.customer_id,
            "has_all_entity_access": current_user_active.has_all_entity_access,
            "entity_ids": current_user_active.entity_ids,
        }
    elif current_user_active.role == UserRole.END_USER:
        return {
            "user_id": current_user_active.user_id,
            "email": current_user_active.email,
            "role": current_user_active.role,
            "customer_id": current_user_active.customer_id,
            "has_all_entity_access": current_user_active.has_all_entity_access,
            "entity_ids": current_user_active.entity_ids,
        }
    else:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for report access.")

def _log_report_access(
    db: Session,
    user_context: Dict[str, Any],
    report_name: str,
    export_format: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None
):
    """Internal helper to log report access."""
    # Convert date/datetime objects in filters to ISO format strings for JSON serialization
    log_details = {"report_name": report_name, "export_format": export_format, "user_role": user_context['role'].value}
    if filters:
        for key, value in filters.items():
            if isinstance(value, (date, datetime)):
                filters[key] = value.isoformat()
        log_details['filters'] = filters
    
    log_action(
        db,
        user_id=user_context['user_id'],
        action_type=f"REPORT_ACCESS_{report_name.upper().replace(' ', '_')}",
        entity_type="Report",
        entity_id=None,
        details=log_details,
        customer_id=user_context['customer_id'],
        lg_record_id=None
    )

def _export_to_csv(data: List[Dict[str, Any]], filename: str) -> Response:
    """Helper to convert a list of dicts to CSV and return as a FastAPI Response."""
    if not data:
        return Response(content="No data to export.", media_type="text/plain", status_code=status.HTTP_204_NO_CONTENT)

    output = io.StringIO()
    if len(data) == 1 and isinstance(data[0], dict):
        all_items = []
        for key, value in data[0].items():
            if isinstance(value, (dict, list)):
                if isinstance(value, dict):
                    all_items.extend([{'key': f"{key}.{k}", 'value': v} for k, v in value.items()])
                elif isinstance(value, list):
                    all_items.extend([{'key': f"{key}.{i}", 'value': item} for i, item in enumerate(value)])
            else:
                all_items.append({'key': key, 'value': value})
        
        fieldnames = ['key', 'value']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_items)

    else:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            clean_row = {
                k: (v.isoformat() if isinstance(v, (date, datetime)) else float(v) if isinstance(v, decimal.Decimal) else v) 
                for k, v in row.items()
            }
            writer.writerow(clean_row)
    
    response_content = output.getvalue()
    output.close()

    headers = {
        "Content-Disposition": f"attachment; filename={filename}.csv",
        "Content-Type": "text/csv",
    }
    return Response(content=response_content, headers=headers, media_type="text/csv")


# FIX: Remove the redundant /reports prefix from all endpoint decorators.
@router.get("/system-owner/system-usage-overview", response_model=SystemUsageOverviewReportOut)
async def get_system_usage_overview(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
    export_format: Optional[str] = Query(None, description="Set to 'csv' to export as CSV. Default: JSON."),
):
    """
    **Report: System Usage Overview**
    Provides a high-level summary of business traction, growth, and adoption.
    """
    if user_context['role'] != UserRole.SYSTEM_OWNER:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only System Owners can access this report.")

    _log_report_access(db, user_context, "System Usage Overview", export_format)

    report_data_out = crud_reports.get_system_usage_overview_report(db, user_context)

    if export_format and export_format.lower() == 'csv':
        list_of_dicts = [report_data_out.data.model_dump(by_alias=True)]
        return _export_to_csv(list_of_dicts, "system_usage_overview")

    return report_data_out

@router.get("/customer-lg-type-mix", response_model=LgTypeMixReportOut, summary="LG type mix per customer (Pie Chart)")
def get_customer_lg_type_mix(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
):
    if user_context['role'] not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")
    
    customer_data = crud_reports.get_chart_data(db, "lg_type_mix", user_context)
    global_user_context = {"role": UserRole.SYSTEM_OWNER} 
    global_data = crud_reports.get_chart_data(db, "lg_type_mix", global_user_context)

    return {
        "report_date": date.today(),
        "data": {
            "customer_lg_type_mix": customer_data,
            "global_lg_type_mix": global_data
        }
    }

@router.get("/avg-bank-processing-time", response_model=AvgBankProcessingTimeReportOut, summary="Average processing times by bank (Bar Chart)")
def get_avg_bank_processing_time(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
):
    if user_context['role'] not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")

    # Modified logic: Check the user's role and fetch global data if the user is a CORPORATE_ADMIN
    if user_context['role'] == UserRole.CORPORATE_ADMIN:
        global_user_context = {"role": UserRole.SYSTEM_OWNER} # Create a new context to bypass customer filtering
        data = crud_reports.get_chart_data(db, "bank_processing_times", global_user_context)
    else:
        # For System Owners, the default user_context is already global
        data = crud_reports.get_chart_data(db, "bank_processing_times", user_context)

    return {"report_date": date.today(), "data": data}

@router.get("/bank-market-share", response_model=BankMarketShareReportOut, summary="Bank market share (Pie Chart)")
def get_bank_market_share(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
):
    if user_context['role'] not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")

    customer_data = crud_reports.get_chart_data(db, "bank_market_share", user_context)
    global_user_context = {"role": UserRole.SYSTEM_OWNER} 
    global_data = crud_reports.get_chart_data(db, "bank_market_share", global_user_context)

    return {
        "report_date": date.today(), 
        "data": {
            "customer_market_share": customer_data,
            "global_market_share": global_data
        }
    }

# FIX: Remove the redundant /reports prefix from all endpoint decorators.
@router.get("/corporate-admin/lg-performance", response_model=CustomerLGPerformanceReportOut)
async def get_customer_lg_performance(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
    export_format: Optional[str] = Query(None, description="Set to 'csv' to export as CSV. Default: JSON."),
    customer_id: Optional[int] = Query(None, description="Filter by customer ID (System Owner only).")
):
    """
    **Report: Customer LG Performance**
    Provides a comprehensive overview of LG activity and user performance for a single customer.
    """
    if user_context['role'] == UserRole.SYSTEM_OWNER:
        if not customer_id:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="System Owners must provide a customer ID to run this report.")
        user_context['customer_id'] = customer_id
    elif user_context['role'] not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")

    _log_report_access(db, user_context, "Customer LG Performance", export_format)

    report_data_out = crud_reports.get_customer_lg_performance_report(db, user_context)

    if export_format and export_format.lower() == 'csv':
        list_of_dicts = [report_data_out.data.model_dump(by_alias=True)]
        return _export_to_csv(list_of_dicts, "customer_lg_performance")

    return report_data_out


# FIX: Remove the redundant /reports prefix from all endpoint decorators.
@router.get("/end-user/my-lg-dashboard", response_model=MyLGDashboardReportOut)
async def get_my_lg_dashboard(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
    export_format: Optional[str] = Query(None, description="Set to 'csv' to export as CSV. Default: JSON."),
):
    """
    **Report: My LG Dashboard**
    Provides a personalized dashboard for an End User to manage their assigned LG workload and pending actions.
    """
    if user_context['role'] != UserRole.END_USER:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only End Users can access this report.")

    _log_report_access(db, user_context, "My LG Dashboard", export_format)

    report_data_out = crud_reports.get_my_lg_dashboard_report(db, user_context)

    if export_format and export_format.lower() == 'csv':
        list_of_dicts = [report_data_out.data.model_dump(by_alias=True)]
        return _export_to_csv(list_of_dicts, "my_lg_dashboard")

    return report_data_out

# app/api/v1/endpoints/reports.py

@router.get("/avg-delivery-days", response_model=AvgDeliveryDaysReportOut, summary="Average delivery days (per customer vs. overall)")
def get_avg_delivery_days(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
):
    if user_context['role'] not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")
    
    # Corrected: Pass user_context unconditionally to let crud handle the filter
    customer_avg_data = crud_reports.get_chart_data(db, "avg_delivery_days", user_context) # Pass the user_context here
    customer_avg = customer_avg_data['average_days'] if customer_avg_data else None
    
    overall_avg_data = crud_reports.get_chart_data(db, "avg_delivery_days", {"role": UserRole.SYSTEM_OWNER})
    overall_avg = overall_avg_data['average_days'] if overall_avg_data else None

    return {
        "customer_avg": customer_avg,
        "overall_avg": overall_avg,
    }


@router.get("/avg-days-to-action", response_model=AvgDaysToActionEventOut, summary="Average days before expiry when action is taken")
def get_avg_days_to_action(
    db: Session = Depends(get_db),
    user_context: Dict[str, Any] = Depends(get_current_report_user_context),
):
    if user_context['role'] not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized role for this report.")

    # Corrected: Pass user_context unconditionally to let crud handle the filter
    customer_avg_data = crud_reports.get_chart_data(db, "avg_days_to_action", user_context) # Pass the user_context here
    customer_avg = customer_avg_data['average_days'] if customer_avg_data else None
    
    overall_avg_data = crud_reports.get_chart_data(db, "avg_days_to_action", {"role": UserRole.SYSTEM_OWNER})
    overall_avg = overall_avg_data['average_days'] if overall_avg_data else None

    return {
        "customer_avg": customer_avg,
        "overall_avg": overall_avg,
    }

@router.post("/demo-requests", status_code=status.HTTP_201_CREATED)
async def submit_demo_request(
    demo_request: DemoRequestCreate,
    db: Session = Depends(get_db),
    request: Request = None
):
    """
    Receives and stores demo request form data.
    This is a public endpoint and does not require authentication.
    """
    try:
        # Define a private directory for storing the data, relative to the project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        storage_dir = os.path.join(project_root, 'private_data')
        os.makedirs(storage_dir, exist_ok=True)
        
        # Use a unique file name to avoid conflicts
        file_path = os.path.join(storage_dir, 'demo_requests.jsonl')
        
        # Prepare the data with a timestamp and IP address
        data_to_store = demo_request.model_dump()
        data_to_store['timestamp'] = datetime.now().isoformat()
        data_to_store['ip_address'] = request.client.host if request else None

        # Append the JSON data to the file
        with open(file_path, 'a') as f:
            f.write(json.dumps(data_to_store) + '\n')
        
        # Log the action for audit purposes
        log_action(
            db,
            user_id=None,  # No user is logged in for this public action
            action_type="DEMO_REQUEST_SUBMITTED",
            entity_type="DemoRequest",
            entity_id=None,
            details=data_to_store,
            customer_id=None,
            lg_record_id=None
        )

        return {"message": "Demo request submitted successfully."}
    except Exception as e:
        logger.error(f"Error processing demo request: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing your request."
        )

# Add a simple, authenticated endpoint to retrieve the data
@router.get("/demo-requests", summary="Retrieve all demo requests (Admin only)")
async def get_all_demo_requests(
    current_user: TokenData = Depends(HasPermission("system_owner:view_dashboard")), # Protect with a relevant permission
    request: Request = None
):
    """
    Retrieves all submitted demo requests from the private storage file.
    This endpoint requires System Owner permissions.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    file_path = os.path.join(project_root, 'private_data', 'demo_requests.jsonl')

    if not os.path.exists(file_path):
        return {"message": "No demo requests have been submitted yet."}

    requests_list = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                requests_list.append(json.loads(line))
        return {"demo_requests": requests_list}
    except Exception as e:
        logger.error(f"Error reading demo requests file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving the demo requests."
        )