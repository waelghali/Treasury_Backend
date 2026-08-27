# app/api/v1/endpoints/system_holidays_endpoints.py
"""
System Owner Holidays & Business Calendar Configuration Endpoints
Allows System Owners to manage centralized holiday calendars, view upcoming banking holidays,
add corporate shutdown periods, and configure the regional weekend scheme.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.core.security import TokenData, get_current_system_owner, get_current_user
from app.models.models import SystemHoliday, GlobalConfiguration
from app.services.business_calendar_service import business_calendar_service, WEEKEND_CONFIGURATIONS

logger = logging.getLogger("app.system_holidays_api")
router = APIRouter(prefix="/system-owner/holidays", tags=["System Owner - Holidays & Calendar"])


# --- Schemas ---
class SystemHolidayCreate(BaseModel):
    holiday_date: date
    name: str = Field(..., max_length=255, description="Holiday or shutdown name")
    is_recurring: bool = Field(False, description="Whether this holiday repeats annually on same date")
    notes: Optional[str] = None


class SystemHolidayOut(BaseModel):
    id: int
    holiday_date: date
    name: str
    is_recurring: bool
    notes: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CalendarSettingsOut(BaseModel):
    weekend_scheme: str
    weekend_days: List[int]
    weekend_days_names: List[str]
    default_country: str
    custom_holidays_count: int


class CalendarSettingsUpdate(BaseModel):
    weekend_scheme: Optional[str] = Field(None, description="MIDDLE_EAST (Fri-Sat) or WESTERN (Sat-Sun)")
    default_country: Optional[str] = Field(None, max_length=10, description="Country code, e.g. EG, SA, AE, US")


# --- Endpoints ---

@router.get("", response_model=List[SystemHolidayOut])
def list_system_holidays(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """Lists all custom System Holiday override entries."""
    return db.query(SystemHoliday).filter(
        SystemHoliday.is_deleted == False
    ).order_by(SystemHoliday.holiday_date.asc()).all()


@router.post("", response_model=SystemHolidayOut, status_code=status.HTTP_201_CREATED)
def create_system_holiday(
    payload: SystemHolidayCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_system_owner)
):
    """Creates a new custom holiday date at the System Owner level."""
    existing = db.query(SystemHoliday).filter(
        SystemHoliday.holiday_date == payload.holiday_date,
        SystemHoliday.is_deleted == False
    ).first()

    if existing:
        existing.name = payload.name
        existing.is_recurring = payload.is_recurring
        existing.notes = payload.notes
        db.commit()
        db.refresh(existing)
        return existing

    new_h = SystemHoliday(
        holiday_date=payload.holiday_date,
        name=payload.name,
        is_recurring=payload.is_recurring,
        notes=payload.notes
    )
    db.add(new_h)
    db.commit()
    db.refresh(new_h)
    return new_h


@router.delete("/{holiday_id}")
def delete_system_holiday(
    holiday_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_system_owner)
):
    """Soft-deletes a custom system holiday."""
    h = db.query(SystemHoliday).filter(
        SystemHoliday.id == holiday_id,
        SystemHoliday.is_deleted == False
    ).first()

    if not h:
        raise HTTPException(status_code=404, detail="Holiday not found")

    h.is_deleted = True
    db.commit()
    return {"message": "Holiday deleted successfully", "id": holiday_id}


@router.get("/calendar-settings", response_model=CalendarSettingsOut)
def get_calendar_settings(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """Returns the current regional weekend convention and active country settings."""
    cfg_weekend = db.query(GlobalConfiguration).filter(
        GlobalConfiguration.key == "WEEKEND_DAYS_SCHEME"
    ).first()
    cfg_country = db.query(GlobalConfiguration).filter(
        GlobalConfiguration.key == "DEFAULT_CALENDAR_COUNTRY"
    ).first()

    scheme = cfg_weekend.value_default if cfg_weekend and cfg_weekend.value_default else "MIDDLE_EAST"
    country = cfg_country.value_default if cfg_country and cfg_country.value_default else "EG"

    weekend_days = list(WEEKEND_CONFIGURATIONS.get(scheme, WEEKEND_CONFIGURATIONS["MIDDLE_EAST"]))
    day_names_map = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}
    weekend_names = [day_names_map[d] for d in sorted(weekend_days)]

    custom_count = db.query(SystemHoliday).filter(SystemHoliday.is_deleted == False).count()

    return CalendarSettingsOut(
        weekend_scheme=scheme,
        weekend_days=weekend_days,
        weekend_days_names=weekend_names,
        default_country=country,
        custom_holidays_count=custom_count
    )


@router.put("/calendar-settings", response_model=CalendarSettingsOut)
def update_calendar_settings(
    payload: CalendarSettingsUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_system_owner)
):
    """Updates the regional weekend scheme or country calendar at the System Owner level."""
    if payload.weekend_scheme:
        scheme_upper = payload.weekend_scheme.upper().strip()
        if scheme_upper not in WEEKEND_CONFIGURATIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid weekend scheme. Must be one of: {list(WEEKEND_CONFIGURATIONS.keys())}"
            )
        cfg_w = db.query(GlobalConfiguration).filter(
            GlobalConfiguration.key == "WEEKEND_DAYS_SCHEME"
        ).first()
        if cfg_w:
            cfg_w.value_default = scheme_upper
        else:
            db.add(GlobalConfiguration(
                key="WEEKEND_DAYS_SCHEME",
                value_default=scheme_upper,
                description="Regional weekend scheme (MIDDLE_EAST vs WESTERN)"
            ))

    if payload.default_country:
        country_clean = payload.default_country.upper().strip()
        cfg_c = db.query(GlobalConfiguration).filter(
            GlobalConfiguration.key == "DEFAULT_CALENDAR_COUNTRY"
        ).first()
        if cfg_c:
            cfg_c.value_default = country_clean
        else:
            db.add(GlobalConfiguration(
                key="DEFAULT_CALENDAR_COUNTRY",
                value_default=country_clean,
                description="Default country code for automated bank holidays"
            ))

    db.commit()
    return get_calendar_settings(db=db, current_user=current_user)
