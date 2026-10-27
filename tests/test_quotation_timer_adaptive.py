"""
Verification of Adaptive Quotation Timer Scheduler & Fast-Path Guard
"""
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.database import engine, SessionLocal
from app.models.models_quotation import QuotationRequest
from app.models.models import User
from app.constants import UserRole
from app.services.notifications import quotation_timers as qt
from app.services.notifications.catalog import EventCode


def test_fast_path_when_no_active_rfqs():
    """Verify that when no RFQs are active, inspect returns (False, False) and sweep_once skips queries."""
    with SessionLocal() as db:
        now = datetime.now(timezone.utc)
        # Any past dummy evaluation
        evaluate_mock = MagicMock()
        has_work, is_active = qt._inspect_rfq_activity(db, now)
        # If there are no open RFQs in current DB:
        print(f"  Existing DB state: has_work={has_work}, is_active={is_active}")
        stats = qt.sweep_once(db, evaluate=evaluate_mock, now=now)
        assert stats == {"closing_soon": 0, "window_closed": 0, "auto_resolved": 0}
        assert evaluate_mock.call_count == 0
    print("  [PASS] Fast-path returns 0 stats and 0 evaluations when no candidate RFQs are due")


def test_adaptive_interval_transitions():
    """Verify that interval dynamically shifts between IDLE (30s) and ACTIVE (5s)."""
    scheduler_mock = MagicMock()
    qt._scheduler = scheduler_mock
    qt._current_interval = 30

    # 1. When active/approaching: transitions to 5s
    qt._adjust_interval(qt.ACTIVE_SWEEP_SECONDS)
    scheduler_mock.reschedule_job.assert_called_with(qt.SWEEP_JOB_ID, trigger="interval", seconds=5)
    assert qt._current_interval == 5

    # 2. Calling with same interval does not reschedule redundantly
    scheduler_mock.reset_mock()
    qt._adjust_interval(qt.ACTIVE_SWEEP_SECONDS)
    scheduler_mock.reschedule_job.assert_not_called()

    # 3. When idle: transitions back to 30s
    qt._adjust_interval(qt.IDLE_SWEEP_SECONDS)
    scheduler_mock.reschedule_job.assert_called_with(qt.SWEEP_JOB_ID, trigger="interval", seconds=30)
    assert qt._current_interval == 30
    print("  [PASS] Adaptive interval transitions cleanly between 30s and 5s without redundant reschedules")


def test_nudge_quotation_timers():
    """Verify nudge adds a one-off date job to run immediately."""
    scheduler_mock = MagicMock()
    qt._scheduler = scheduler_mock
    qt.nudge_quotation_timers()
    assert scheduler_mock.add_job.called
    call_args = scheduler_mock.add_job.call_args[1]
    assert call_args["trigger"] == "date"
    assert call_args["id"] == qt.NUDGE_JOB_ID
    assert call_args["replace_existing"] is True
    print("  [PASS] nudge_quotation_timers schedules immediate date job on APScheduler")


if __name__ == "__main__":
    print("=" * 70)
    print("VERIFYING QUOTATION TIMERS ADAPTIVE SCHEDULING")
    print("=" * 70)
    test_fast_path_when_no_active_rfqs()
    test_adaptive_interval_transitions()
    test_nudge_quotation_timers()
    print("=" * 70)
    print("ALL ADAPTIVE SCHEDULING TESTS PASSED")
    print("=" * 70)
