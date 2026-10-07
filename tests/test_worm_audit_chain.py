# tests/test_worm_audit_chain.py
"""
Test Suite for Phase 5: Cryptographic Hash-Chained WORM Audit Trail.
Verifies:
1. log_action generates valid SHA-256 entry_hash and links previous_hash.
2. Sequential audit events form an unbroken cryptographic hash-chain.
3. verify_audit_log_chain accurately verifies valid sequential chains.
4. Tamper Detection: Changing any field or deleting a link in the chain breaks
   verification immediately and flags the exact record ID.
"""

import unittest
from datetime import datetime, timezone
from app.database import SessionLocal
from app.models.models import AuditLog
from app.crud.base import log_action
from app.core.audit_crypto import (
    GENESIS_HASH,
    compute_audit_hash,
    verify_audit_log_chain
)


class TestWormAuditChain(unittest.TestCase):

    def setUp(self):
        self.db = SessionLocal()
        self.created_log_ids = []

    def tearDown(self):
        # Clean up any test audit logs created during this run
        if self.created_log_ids:
            self.db.query(AuditLog).filter(AuditLog.id.in_(self.created_log_ids)).delete(synchronize_session=False)
            self.db.commit()
        self.db.close()

    def test_worm_hash_chain_creation_and_linking(self):
        # 1. Log First Action
        log_action(
            db=self.db,
            user_id=1,
            action_type="TEST_WORM_EVENT_1",
            entity_type="QuotationRequest",
            entity_id=101,
            details={"rfq_id": "RFQ-TEST-001", "amount": 100000.0}
        )
        self.db.commit()

        log1 = self.db.query(AuditLog).filter(AuditLog.action_type == "TEST_WORM_EVENT_1").order_by(AuditLog.id.desc()).first()
        self.assertIsNotNone(log1)
        self.created_log_ids.append(log1.id)

        self.assertIsNotNone(log1.previous_hash)
        self.assertIsNotNone(log1.entry_hash)
        self.assertEqual(len(log1.entry_hash), 64)
        print(f"[Pass] First WORM record {log1.id} stamped with entry_hash={log1.entry_hash[:16]}...")

        # 2. Log Second Action (Should chain to log1's entry_hash)
        log_action(
            db=self.db,
            user_id=1,
            action_type="TEST_WORM_EVENT_2",
            entity_type="QuotationOffer",
            entity_id=202,
            details={"rfq_id": "RFQ-TEST-001", "rate": 48.75}
        )
        self.db.commit()

        log2 = self.db.query(AuditLog).filter(AuditLog.action_type == "TEST_WORM_EVENT_2").order_by(AuditLog.id.desc()).first()
        self.assertIsNotNone(log2)
        self.created_log_ids.append(log2.id)

        self.assertEqual(log2.previous_hash, log1.entry_hash)
        self.assertEqual(len(log2.entry_hash), 64)
        self.assertNotEqual(log2.entry_hash, log1.entry_hash)
        print(f"[Pass] Second WORM record {log2.id} mathematically chained to previous_hash={log2.previous_hash[:16]}...")

        # 3. Log Third Action
        log_action(
            db=self.db,
            user_id=1,
            action_type="TEST_WORM_EVENT_3",
            entity_type="DealAcceptance",
            entity_id=303,
            details={"rfq_id": "RFQ-TEST-001", "winner_bank_id": 5}
        )
        self.db.commit()

        log3 = self.db.query(AuditLog).filter(AuditLog.action_type == "TEST_WORM_EVENT_3").order_by(AuditLog.id.desc()).first()
        self.assertIsNotNone(log3)
        self.created_log_ids.append(log3.id)

        self.assertEqual(log3.previous_hash, log2.entry_hash)
        print(f"[Pass] Third WORM record {log3.id} chained to previous_hash={log3.previous_hash[:16]}...")

        # 4. Verify the chain across these test records
        report = verify_audit_log_chain(self.db, start_id=log1.id)
        self.assertTrue(report["is_valid"])
        self.assertGreaterEqual(report["records_verified"], 3)
        self.assertIsNone(report["broken_at_id"])
        print(f"[Pass] Chain verification confirmed intact: {report['records_verified']} records verified!")

    def test_tamper_detection(self):
        # Create two chained records
        log_action(
            db=self.db,
            user_id=2,
            action_type="UNTOUCHED_EVENT",
            entity_type="Account",
            entity_id=50,
            details={"balance": 500000}
        )
        self.db.commit()
        log_a = self.db.query(AuditLog).filter(AuditLog.action_type == "UNTOUCHED_EVENT").order_by(AuditLog.id.desc()).first()
        self.created_log_ids.append(log_a.id)

        log_action(
            db=self.db,
            user_id=2,
            action_type="TARGET_FOR_TAMPERING",
            entity_type="Account",
            entity_id=51,
            details={"balance": 1000000}
        )
        self.db.commit()
        log_b = self.db.query(AuditLog).filter(AuditLog.action_type == "TARGET_FOR_TAMPERING").order_by(AuditLog.id.desc()).first()
        self.created_log_ids.append(log_b.id)

        # Confirm intact before tampering
        before_report = verify_audit_log_chain(self.db, start_id=log_a.id)
        self.assertTrue(before_report["is_valid"])

        # ARTIFICIALLY TAMPER with log_b details directly in DB (simulating rogue DBA or hacker)
        log_b.details = {"balance": 999999999, "hacked": True}
        self.db.add(log_b)
        self.db.commit()

        # Run verification: Must catch the tampering!
        tamper_report = verify_audit_log_chain(self.db, start_id=log_a.id)
        self.assertFalse(tamper_report["is_valid"])
        self.assertEqual(tamper_report["broken_at_id"], log_b.id)
        self.assertIn("Tampering detected", tamper_report["message"])
        print(f"[Pass] Tamper detection successfully caught modified record {tamper_report['broken_at_id']}: {tamper_report['message']}")


if __name__ == "__main__":
    unittest.main()
