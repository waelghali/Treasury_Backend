import re
import sys
import os
from decimal import Decimal
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from app.models.models_reconciliation_v2 import InternalLedgerRecord, BankStatement, BankTransaction
from app.models import Customer

def seed_erp_records():
    db = SessionLocal()
    
    # Target companies: Company 1 (Acme Corporation) and Company 18
    target_company_ids = [1, 18]
    customers = db.query(Customer).filter(Customer.id.in_(target_company_ids)).all()
    print(f"Targeting companies: {[c.id for c in customers]}")

    total_created = 0

    for company_id in target_company_ids:
        # 1. First, seed the 120 standard DEMO ERP records
        # Check if already present for this company
        existing_demo_count = db.query(InternalLedgerRecord).filter(
            InternalLedgerRecord.company_id == company_id,
            InternalLedgerRecord.reference_number.like('DEMO-%')
        ).count()

        if existing_demo_count == 0:
            print(f"Seeding standard DEMO ERP records for company {company_id}...")
            # Generate demo invoices/bills for 20 simulated cycles
            for i in range(1, 21):
                m_offset = (i - 1)
                s_year = 2026 + (m_offset // 12)
                s_month = (m_offset % 12) + 1
                base_dt = datetime(s_year, s_month, 15)

                demo_items = [
                    ("AR_INVOICE", f"DEMO-INV-2026-{i:02d}01", "Orange Data Telecom Egypt", Decimal("92400.00"), "1020100 - Trade Receivables"),
                    ("AR_INVOICE", f"DEMO-INV-2026-{i:02d}02", "Vodafone Egypt Corporate", Decimal("48600.00"), "1020100 - Trade Receivables"),
                    ("AR_INVOICE", f"DEMO-INV-2026-{i:02d}03", "Etisalat Misr Enterprise", Decimal("35120.00"), "1020100 - Trade Receivables"),
                    ("AP_BILL", f"DEMO-BILL-2026-{i:02d}10", "Schneider Electric Egypt", Decimal("-145000.00"), "2010100 - Trade Payables"),
                    ("AP_BILL", f"DEMO-BILL-2026-{i:02d}20", "Baker Hughes Egypt", Decimal("-82500.00"), "2010100 - Trade Payables"),
                    ("LG_COMMISSION", f"DEMO-LG-2026-{i:02d}50", "National Contracting Corp", Decimal("-4500.00"), "6020300 - LG Commissions"),
                ]

                for rec_type, ref, entity, amt, gl in demo_items:
                    rec = InternalLedgerRecord(
                        company_id=company_id,
                        record_type=rec_type,
                        reference_number=ref,
                        entity_name=entity,
                        record_date=base_dt,
                        amount=amt,
                        currency="EGP",
                        gl_account=gl,
                        status="OPEN",
                        created_by=1
                    )
                    db.add(rec)
                    total_created += 1

        # 2. Next, seed matching ERP records for EXISTING Bank Transactions for this company
        # We find transactions that do NOT have a matched ERP record yet
        txns = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == company_id
        ).all()
        print(f"Company {company_id} has {len(txns)} bank transactions.")

        # Existing references in InternalLedgerRecord to avoid duplicates
        existing_refs = set(
            r[0] for r in db.query(InternalLedgerRecord.reference_number).filter(
                InternalLedgerRecord.company_id == company_id
            ).all()
        )

        matched_seeded = 0
        for txn in txns:
            # Extract possible reference from description or ref field
            desc = txn.raw_description or ""
            ref_match = re.search(r'(FT\d{5}[A-Z0-9]+|INV[-\w]+|BILL[-\w]+|REF[-\w]+)', desc)
            
            if ref_match:
                extracted_ref = ref_match.group(1)
            else:
                extracted_ref = f"ERP-REF-{txn.id}"

            if extracted_ref in existing_refs:
                continue

            # Determine amount and direction
            debit = Decimal(str(txn.debit_amount or 0))
            credit = Decimal(str(txn.credit_amount or 0))

            if credit > 0:
                rec_type = "AR_INVOICE"
                amt = credit
                gl = "1020100 - Trade Receivables"
                entity = "Inward Collection Client"
            elif debit > 0:
                rec_type = "AP_BILL"
                amt = -debit
                gl = "2010100 - Trade Payables"
                entity = "Trade Vendor / Contractor"
            else:
                continue

            # Only seed ~85% so there is a realistic mix of matched + outstanding items
            if txn.id % 7 == 0:
                continue

            # Approximate booking date (with small +/- 1 day variance)
            rec_date = txn.booking_date if txn.booking_date else datetime.utcnow()
            
            erp_rec = InternalLedgerRecord(
                company_id=company_id,
                record_type=rec_type,
                reference_number=extracted_ref,
                entity_name=entity,
                record_date=rec_date,
                amount=amt,
                currency=txn.currency or "EGP",
                gl_account=gl,
                status="OPEN",
                created_by=1
            )
            db.add(erp_rec)
            existing_refs.add(extracted_ref)
            total_created += 1
            matched_seeded += 1

        print(f"Company {company_id}: seeded {matched_seeded} matching ERP records.")

    db.commit()
    
    final_count_1 = db.query(InternalLedgerRecord).filter(InternalLedgerRecord.company_id == 1).count()
    final_count_18 = db.query(InternalLedgerRecord).filter(InternalLedgerRecord.company_id == 18).count()
    print(f"Done! Company 1 total ERP records: {final_count_1}")
    print(f"Done! Company 18 total ERP records: {final_count_18}")
    print(f"Total new records created: {total_created}")
    db.close()

if __name__ == "__main__":
    seed_erp_records()
