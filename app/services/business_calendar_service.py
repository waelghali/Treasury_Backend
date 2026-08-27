# app/services/business_calendar_service.py
"""
Business Calendar & Turnaround Time Service
Computes net business days between dates, taking into account regional weekend standards
(e.g., Sun-Thu banking work week in Egypt/GCC vs Mon-Fri) and official country public holidays
merged with custom system-owner holiday dates.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Set, Optional, Dict, Any
from sqlalchemy.orm import Session

try:
    import holidays
    HAS_HOLIDAYS_LIB = True
except ImportError:
    HAS_HOLIDAYS_LIB = False

from app.models.models import SystemHoliday, GlobalConfiguration

logger = logging.getLogger("app.business_calendar")

# Regional weekend day configurations (Python weekday(): 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun)
WEEKEND_CONFIGURATIONS = {
    "MIDDLE_EAST": {4, 5},     # Friday (4) and Saturday (5) off -> Sun to Thu work week
    "WESTERN": {5, 6},         # Saturday (5) and Sunday (6) off -> Mon to Fri work week
    "FRIDAY_ONLY": {4},        # Friday off only
    "SATURDAY_SUNDAY": {5, 6}
}


class BusinessCalendarService:
    def __init__(self, default_country: str = "EG", default_weekend_scheme: str = "MIDDLE_EAST"):
        self.default_country = default_country
        self.default_weekend_scheme = default_weekend_scheme

    def get_weekend_days(self, db: Optional[Session] = None) -> Set[int]:
        """
        Returns the set of integer weekdays that are non-working weekend days.
        Configurable via global_configurations key 'WEEKEND_DAYS_SCHEME' (default: MIDDLE_EAST = Fri, Sat).
        """
        scheme = self.default_weekend_scheme
        if db:
            try:
                cfg = db.query(GlobalConfiguration).filter(
                    GlobalConfiguration.key == "WEEKEND_DAYS_SCHEME"
                ).first()
                if cfg and cfg.value_default:
                    scheme = cfg.value_default.upper().strip()
            except Exception as e:
                logger.debug(f"Could not load weekend scheme config: {e}")

        return WEEKEND_CONFIGURATIONS.get(scheme, WEEKEND_CONFIGURATIONS["MIDDLE_EAST"])

    def get_holiday_dates(
        self,
        db: Optional[Session],
        start_year: int,
        end_year: int,
        country_code: Optional[str] = None
    ) -> Set[date]:
        """
        Returns all official public holidays and custom SystemHoliday dates between start_year and end_year.
        """
        country = (country_code or self.default_country).upper()
        holiday_dates: Set[date] = set()

        # 1. Automated National Public Holidays (via python holidays lib)
        if HAS_HOLIDAYS_LIB:
            try:
                years = list(range(start_year, end_year + 1))
                country_holidays = holidays.country_holidays(country, years=years)
                for h_date in country_holidays.keys():
                    if isinstance(h_date, date):
                        holiday_dates.add(h_date)
            except Exception as e:
                logger.warning(f"Failed to fetch national holidays for {country}: {e}")

        # 2. Database System Holidays (System Owner custom overrides)
        if db:
            try:
                db_holidays = db.query(SystemHoliday).filter(
                    SystemHoliday.is_deleted == False
                ).all()

                for h in db_holidays:
                    if h.is_recurring:
                        # Add for every relevant year in range
                        for y in range(start_year, end_year + 1):
                            try:
                                holiday_dates.add(date(y, h.holiday_date.month, h.holiday_date.day))
                            except ValueError:
                                pass  # Handle leap year Feb 29 edge cases
                    else:
                        holiday_dates.add(h.holiday_date)
            except Exception as e:
                logger.warning(f"Failed to fetch system holidays from db: {e}")

        return holiday_dates

    def is_business_day(
        self,
        target_date: date,
        db: Optional[Session] = None,
        country_code: Optional[str] = None
    ) -> bool:
        """Returns True if the given date is a working business day."""
        weekend_days = self.get_weekend_days(db)
        if target_date.weekday() in weekend_days:
            return False

        holidays_set = self.get_holiday_dates(db, target_date.year, target_date.year, country_code)
        return target_date not in holidays_set

    def calculate_business_turnaround_days(
        self,
        start_date: Optional[date],
        end_date: Optional[date],
        db: Optional[Session] = None,
        country_code: Optional[str] = None
    ) -> float:
        """
        Calculates the net business days elapsed between start_date (delivery to bank)
        and end_date (bank reply/issue date).
        
        Rules:
        - If start_date == end_date on a business day -> returns 1.0 (same-day turnaround).
        - Skips all weekend days (e.g. Fri/Sat) and holidays.
        - Minimum turnaround is 1.0 day.
        """
        if not start_date or not end_date:
            return 1.0

        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()

        if end_date < start_date:
            return 1.0

        weekend_days = self.get_weekend_days(db)
        holidays_set = self.get_holiday_dates(db, start_date.year, end_date.year, country_code)

        if start_date == end_date:
            return 1.0

        current = start_date
        business_days = 0

        while current < end_date:
            current += timedelta(days=1)
            # Check if this day is a working business day
            if current.weekday() not in weekend_days and current not in holidays_set:
                business_days += 1

        return max(1.0, float(business_days))


business_calendar_service = BusinessCalendarService()
