# tests/test_lg_custody_notifications.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch
from app.services.notifications.catalog import (
    EventCode,
    CATALOG,
    get_spec,
    LinkTarget,
    Urgency,
)
from app.services.notifications.audiences import AudienceKey, resolve_lg_audience
from app.services.notifications.lg_custody_masking import mask_lg_number, mask_issuer_name
from app.services.notifications.emit import build_link, emit_lg
from app.services.notifications.lg_custody_service import (
    _has_active_custody_action,
    _dispatch_bucket_with_burst_protection,
    resolve_stale_custody_notifications,
)
from app.constants import UserRole


class TestLGCustodyNotifications(unittest.TestCase):

    def test_mask_lg_number(self):
        self.assertEqual(mask_lg_number(None), "—")
        self.assertEqual(mask_lg_number(""), "—")
        self.assertEqual(mask_lg_number("LG-2024-8412"), "...8412")
        self.assertEqual(mask_lg_number("8412"), "...8412")
        self.assertEqual(mask_lg_number("12"), "...12")

    def test_mask_issuer_name(self):
        self.assertEqual(mask_issuer_name(None), "—")
        self.assertEqual(mask_issuer_name(""), "—")
        self.assertEqual(mask_issuer_name("Hassan Ahmed Contracting Co."), "Hassan A*** C*** C***")
        self.assertEqual(mask_issuer_name("شركة حسن أحمد للمقاولات"), "شركة ح*** أ*** ل***")
        self.assertEqual(mask_issuer_name("Siemens"), "Sie***")

    def test_catalog_specs_confidentiality_and_audiences(self):
        """Ensure no financial amount or currency field exists in any LG Custody event spec, and End User events target CUSTOMER_END_USERS."""
        lg_custody_events = [
            EventCode.LG_CUSTODY_INACTION_ESCALATION,
            EventCode.LG_CUSTODY_UNDELIVERED_LETTER,
            EventCode.LG_CUSTODY_APPROVAL_REQUIRED,
            EventCode.LG_CUSTODY_DAY_ZERO,
            EventCode.LG_CUSTODY_BANK_DEFECT,
            EventCode.LG_USER_EXPIRY_ACTION_NOTICE,
            EventCode.LG_USER_PRE_ESCALATION_WARNING,
            EventCode.LG_USER_REQUEST_DECIDED,
            EventCode.LG_CUSTODY_PORTFOLIO_BURST_DIGEST,
        ]

        forbidden_fields = {"amount", "currency", "rate", "price", "value", "balance"}

        for code in lg_custody_events:
            spec = get_spec(code)
            self.assertIsNotNone(spec)
            self.assertEqual(spec.module, "CUSTODY")
            for field in spec.allowed_fields:
                self.assertNotIn(field.lower(), forbidden_fields, f"Forbidden field '{field}' in {code}")
            self.assertNotIn("amount", spec.title.lower())
            self.assertNotIn("currency", spec.title.lower())
            self.assertNotIn("amount", spec.body.lower())
            self.assertNotIn("currency", spec.body.lower())

        # Verify End User events strictly target CUSTOMER_END_USERS (not InternalOwnerContact)
        self.assertEqual(
            get_spec(EventCode.LG_USER_EXPIRY_ACTION_NOTICE).audiences,
            (AudienceKey.CUSTOMER_END_USERS,),
        )
        self.assertEqual(
            get_spec(EventCode.LG_USER_PRE_ESCALATION_WARNING).audiences,
            (AudienceKey.CUSTOMER_END_USERS,),
        )

    def test_build_link_lg_custody(self):
        mock_lg = MagicMock()
        mock_lg.id = 42

        admin_link = build_link(LinkTarget.LG_CUSTODY_DETAIL, mock_lg, UserRole.CORPORATE_ADMIN)
        self.assertEqual(admin_link, "/corporate-admin/lg-records/42")

        user_link = build_link(LinkTarget.LG_CUSTODY_DETAIL, mock_lg, UserRole.END_USER)
        self.assertEqual(user_link, "/end-user/lg-records/42")

        list_admin = build_link(LinkTarget.LG_CUSTODY_LIST, mock_lg, UserRole.CORPORATE_ADMIN)
        self.assertEqual(list_admin, "/corporate-admin/lg-records")

        list_user = build_link(LinkTarget.LG_CUSTODY_LIST, mock_lg, UserRole.END_USER)
        self.assertEqual(list_user, "/end-user/lg-records")

        approval_link = build_link(LinkTarget.LG_CUSTODY_APPROVALS, mock_lg, UserRole.CORPORATE_ADMIN)
        self.assertEqual(approval_link, "/corporate-admin/approval-requests")

    def test_emit_lg_staging_and_masking(self):
        """Test emit_lg creates UserNotification rows with confidential masking."""
        mock_db = MagicMock()
        mock_admin = MagicMock()
        mock_admin.id = 101
        mock_admin.role = UserRole.CORPORATE_ADMIN
        mock_admin.customer_id = 1
        mock_admin.is_deleted = False

        mock_db.query.return_value.filter.return_value.all.side_effect = [
            [(101,)],      # admin ids
            [mock_admin],  # user objects
        ]

        mock_lg = MagicMock()
        mock_lg.id = 77
        mock_lg.customer_id = 1
        mock_lg.lg_number = "LG-2024-9988"
        mock_lg.issuer_name = "Saudi Binladin Group"

        added_rows = []
        mock_db.add_all.side_effect = lambda rows: added_rows.extend(rows)

        staged_count = emit_lg(
            db=mock_db,
            code=EventCode.LG_CUSTODY_INACTION_ESCALATION,
            lg_record=mock_lg,
            extra={"days": 14},
        )

        self.assertGreater(staged_count, 0)
        self.assertEqual(len(added_rows), 1)
        row = added_rows[0]
        self.assertEqual(row.module, "CUSTODY")
        self.assertEqual(row.event_type, EventCode.LG_CUSTODY_INACTION_ESCALATION.value)
        self.assertEqual(row.urgency, Urgency.HIGH.value)
        self.assertIn("...9988", row.message)
        self.assertIn("Saudi B*** G***", row.message)
        self.assertIn("14d", row.message)
        self.assertEqual(row.link, "/corporate-admin/lg-records/77")
        self.assertNotIn("SAR", row.message)
        self.assertNotIn("USD", row.message)

    def test_historical_extension_does_not_suppress_new_cycle(self):
        """An LG extended 11 months ago (Confirmed by Bank) must not block current renewal escalations."""
        today = date(2026, 10, 29)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None  # no audit release, no pending AR

        old_ext = MagicMock()
        old_ext.is_deleted = False
        old_ext.status = "Confirmed by Bank"
        old_ext.instruction_type = "LG_EXTENSION"
        old_ext.bank_reply_date = today - timedelta(days=320)
        old_ext.instruction_date = today - timedelta(days=330)

        mock_lg = MagicMock()
        mock_lg.id = 50
        mock_lg.instructions = [old_ext]

        # Old confirmed extension from 11 months ago should return False
        self.assertFalse(_has_active_custody_action(mock_db, mock_lg, today=today))

        # Recent extension issued 5 days ago should return True
        recent_ext = MagicMock()
        recent_ext.is_deleted = False
        recent_ext.status = "Instruction Issued"
        recent_ext.instruction_type = "LG_EXTENSION"
        recent_ext.bank_reply_date = None
        recent_ext.instruction_date = today - timedelta(days=5)
        mock_lg.instructions = [old_ext, recent_ext]
        self.assertTrue(_has_active_custody_action(mock_db, mock_lg, today=today))

    @patch("app.services.notifications.lg_custody_service.emit_lg")
    def test_portfolio_burst_protection(self, mock_emit_lg):
        """When > 3 LGs trigger the same sweep event, only 2 individual alerts + 1 burst digest are emitted."""
        mock_emit_lg.return_value = 1
        mock_db = MagicMock()

        today = date(2026, 10, 29)
        items = []
        for i in range(1, 8):  # 7 LGs expiring together
            lg = MagicMock()
            lg.id = i
            lg.customer_id = 10
            lg.lg_number = f"LG-2026-000{i}"
            lg.issuer_name = "Orascom Construction"
            lg.expiry_date = today + timedelta(days=14)
            items.append((lg, {"days": 14}))

        staged = _dispatch_bucket_with_burst_protection(
            db=mock_db,
            customer_id=10,
            event_code=EventCode.LG_CUSTODY_INACTION_ESCALATION,
            bucket_key="LG_CUSTODY_INACTION_ESCALATION_14D",
            items=items,
        )

        # 2 individual + 1 burst digest = 3 emit_lg calls instead of 7!
        self.assertEqual(staged, 3)
        self.assertEqual(mock_emit_lg.call_count, 3)
        digest_call = mock_emit_lg.call_args_list[2]
        self.assertEqual(digest_call.kwargs["code"], EventCode.LG_CUSTODY_PORTFOLIO_BURST_DIGEST)
        self.assertEqual(digest_call.kwargs["extra"]["count"], 7)
        self.assertEqual(digest_call.kwargs["reference_key_override"], "10:LG_CUSTODY_INACTION_ESCALATION_14D")

    def test_auto_resolve_stale_notifications(self):
        """Verify resolve_stale_custody_notifications marks unread rows read and expires them."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.update.return_value = 2

        resolved = resolve_stale_custody_notifications(
            mock_db,
            lg_record_id=88,
            event_codes=[EventCode.LG_CUSTODY_APPROVAL_REQUIRED],
        )
        self.assertEqual(resolved, 2)
        mock_db.query.return_value.filter.return_value.update.assert_called_once()


if __name__ == "__main__":
    unittest.main()
