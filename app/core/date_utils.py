# app/core/date_utils.py
import calendar
from datetime import date, timedelta
from typing import Optional
from dateutil.relativedelta import relativedelta

def calculate_projected_expiry(start_date: Optional[date], value: Optional[int], unit: Optional[str]) -> Optional[date]:
    """
    Calculates projected expiry date from start_date according to validity_period_value and validity_period_unit.
    Enforces banking month-end convention:
    - If start_date is the last calendar day of the month, the projected expiry date when adding months/years
      is also clamped to the last calendar day of the target month.
    - Prevents rolling over into an extra billing quarter (e.g. Jan 31 + 3 months = April 30, not May 1).
    """
    if not start_date or not value or value <= 0:
        return start_date

    unit_norm = (unit or "DAYS").upper().strip()
    
    if unit_norm in ("DAY", "DAYS"):
        return start_date + timedelta(days=value)
    
    elif unit_norm in ("MONTH", "MONTHS"):
        is_month_end = (start_date.day == calendar.monthrange(start_date.year, start_date.month)[1])
        res = start_date + relativedelta(months=value)
        if is_month_end:
            target_max_day = calendar.monthrange(res.year, res.month)[1]
            res = res.replace(day=target_max_day)
        return res

    elif unit_norm in ("YEAR", "YEARS"):
        is_month_end = (start_date.day == calendar.monthrange(start_date.year, start_date.month)[1])
        res = start_date + relativedelta(years=value)
        if is_month_end:
            target_max_day = calendar.monthrange(res.year, res.month)[1]
            res = res.replace(day=target_max_day)
        return res

    return start_date + timedelta(days=value)


def format_period_sentence(value: Optional[int], unit: Optional[str], language: str = "AR") -> str:
    """
    Returns the formal legal period sentence in Arabic or English.
    e.g. Arabic: "٣ أشهر من تاريخ الإصدار" or "سنة واحدة من تاريخ الإصدار"
    e.g. English: "3 Months from date of issuance"
    """
    if not value or value <= 0:
        return ""
    
    unit_norm = (unit or "MONTHS").upper().strip()
    is_ar = (language or "AR").upper().startswith("AR")
    
    # Arabic numeral helper
    arabic_numerals = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")
    ar_val = str(value).translate(arabic_numerals)
    
    if is_ar:
        if unit_norm in ("DAY", "DAYS"):
            if value == 1:
                return "يوم واحد من تاريخ الإصدار"
            elif value == 2:
                return "يومان من تاريخ الإصدار"
            elif 3 <= value <= 10:
                return f"{ar_val} أيام من تاريخ الإصدار"
            else:
                return f"{ar_val} يوماً من تاريخ الإصدار"
        elif unit_norm in ("MONTH", "MONTHS"):
            if value == 1:
                return "شهر واحد من تاريخ الإصدار"
            elif value == 2:
                return "شهران من تاريخ الإصدار"
            elif 3 <= value <= 10:
                return f"{ar_val} أشهر من تاريخ الإصدار"
            else:
                return f"{ar_val} شهراً من تاريخ الإصدار"
        elif unit_norm in ("YEAR", "YEARS"):
            if value == 1:
                return "سنة واحدة من تاريخ الإصدار"
            elif value == 2:
                return "سنتان من تاريخ الإصدار"
            elif 3 <= value <= 10:
                return f"{ar_val} سنوات من تاريخ الإصدار"
            else:
                return f"{ar_val} سنة من تاريخ الإصدار"
    else:
        u_label = "Day" if unit_norm in ("DAY", "DAYS") else ("Month" if unit_norm in ("MONTH", "MONTHS") else "Year")
        if value > 1:
            u_label += "s"
        return f"{value} {u_label} from date of issuance"
    
    return f"{value} {unit_norm} from date of issuance"
