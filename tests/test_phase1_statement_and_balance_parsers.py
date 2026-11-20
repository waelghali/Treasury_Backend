# tests/test_phase1_statement_and_balance_parsers.py
"""
Regression test suite for Phase 1 fixes:
1. PDF/text balance & fallback table parsers (ungrouped >=4-digit numbers, signs, boundaries, currency & account regexes).
2. Corrupted PDF handling & unlock_pdf_bytes encryption-state preservation.
3. ISO 20022 CAMT.052/053/054 parser (forbid_dtd, Decimal InvalidOperation -> ValueError, whitespace stripping,
   CAMT.054 start_date <= end_date, multi-account container isolation, namespace prefixes & UTF-16).
4. _parse_as_of_datetime ISO YYYY-MM-DD month/day preservation vs DD/MM/YYYY.
5. bank_reconcile_service reverse-chronological closing_balance & non-EGP amount_in_egp.
6. reconciliation_endpoints snapshot failure rollback preventing PendingRollbackError.
"""

import io
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import fitz
from fastapi import HTTPException
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
for p in (TESTS_DIR, ROOT_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.services.pdf_statement_parser import (  # noqa: E402
    _extract_account_from_segment,
    _extract_opening_closing_meta_from_text,
    _fallback_extract_lines_table,
    extract_cash_balances_from_text,
    extract_statement_from_pdf,
    unlock_pdf_bytes,
)
from app.services.banking_standards_ontology import banking_standards_ontology  # noqa: E402
from app.services.inbox_action_service import _parse_as_of_datetime  # noqa: E402
from app.services.bank_reconciliation_service import bank_reconcile_service  # noqa: E402


def _make_text_pdf(lines: list) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    c.setFont("Helvetica", 10)
    y = 750
    for ln in lines:
        c.drawString(40, y, ln)
        y -= 16
    c.save()
    return buf.getvalue()


def test_pdf_text_ungrouped_amounts_signs_and_boundaries():
    res1 = extract_cash_balances_from_text("Available Balance: EGP 12500.50")
    assert len(res1) == 1 and res1[0]["available_balance"] == 12500.5 and res1[0]["currency"] == "EGP", res1

    res2 = extract_cash_balances_from_text("Available Balance: 150000.00 USD")
    assert len(res2) == 1 and res2[0]["available_balance"] == 150000.0 and res2[0]["currency"] == "USD", res2

    res_ar = extract_cash_balances_from_text("الرصيد المتاح: ١٢٥٠٠٫٥٠ ج.م")
    assert len(res_ar) == 1 and res_ar[0]["available_balance"] == 12500.5 and res_ar[0]["currency"] == "EGP", res_ar

    res_paren = extract_cash_balances_from_text("Available Balance: (12500.50) EGP")
    assert len(res_paren) == 1 and res_paren[0]["available_balance"] == -12500.5, res_paren

    res_neg = extract_cash_balances_from_text("Available Balance -500.00 EGP")
    assert len(res_neg) == 1 and res_neg[0]["available_balance"] == -500.0, res_neg

    res_sep_dash = extract_cash_balances_from_text("Available Balance - 500.00 EGP")
    assert len(res_sep_dash) == 1 and res_sep_dash[0]["available_balance"] == 500.0, res_sep_dash

    res_draft = extract_cash_balances_from_text("Available Balance: 500.00 draft")
    assert len(res_draft) == 1 and res_draft[0]["available_balance"] == 500.0, res_draft

    res_adj = extract_cash_balances_from_text("Available Balance: 100 2026-11-20")
    assert len(res_adj) == 1 and res_adj[0]["available_balance"] == 100.0, res_adj

    res_left = extract_cash_balances_from_text("Available Balance (USD): 500.00 left in account")
    assert len(res_left) == 1 and res_left[0]["currency"] == "USD" and res_left[0]["available_balance"] == 500.0, res_left

    meta_open = _extract_opening_closing_meta_from_text(
        "Account: 2026-11-20\nOpening Balance: 12500.50 USD\nClosing Balance: 150000.00 USD"
    )
    assert meta_open["opening_balance"] == Decimal("12500.5"), meta_open
    assert meta_open["closing_balance"] == Decimal("150000.0"), meta_open
    assert meta_open["account_number"] is None, meta_open

    assert _extract_account_from_segment("Account: 2026-11-20") is None
    assert _extract_account_from_segment("Account: 2026-11-20\nAccount No: 777-888999-001") == "777-888999-001"


def test_fallback_table_ungrouped_and_parenthesized_amounts():
    pdf_bytes = _make_text_pdf([
        "Date Description Debit Credit Balance",
        "2026-01-15 Transfer 12500.00 50000.00",
        "2026-01-16 Fee (1500.00) 48500.00",
    ])
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        rows = _fallback_extract_lines_table(doc)
    finally:
        doc.close()
    assert len(rows) == 3, rows
    assert rows[1][-2:] == ["12500.00", "50000.00"], rows[1]
    assert rows[2][-2:] == ["(1500.00)", "48500.00"], rows[2]


def test_corrupted_pdf_and_preserved_encryption_state():
    out_bytes, was_enc, is_unl = unlock_pdf_bytes(b"NOT A PDF FILE", ["1234"])
    assert out_bytes == b"NOT A PDF FILE"
    assert was_enc is False
    assert is_unl is False

    try:
        extract_statement_from_pdf(b"NOT A PDF FILE")
        raise AssertionError("Expected ValueError for corrupted PDF")
    except ValueError:
        pass

    try:
        bank_reconcile_service.parse_statement_content(b"NOT A PDF FILE", "pdf", company_id=1)
        raise AssertionError("Expected HTTPException(400) for corrupted PDF")
    except HTTPException as exc:
        assert exc.status_code == 400, exc

    # Simulate fitz detecting needs_pass=True while pypdf fallback raises
    mock_doc = MagicMock()
    mock_doc.needs_pass = True
    mock_doc.authenticate.return_value = 0
    with patch("app.services.pdf_statement_parser.fitz.open", return_value=mock_doc), \
         patch("app.services.pdf_statement_parser.PdfReader", side_effect=RuntimeError("unsupported crypt")):
        _, enc2, unl2 = unlock_pdf_bytes(b"%PDF-1.7 dummy", ["wrong"])
        assert enc2 is True and unl2 is False, (enc2, unl2)


def test_camt052_053_054_hardening_and_multi_account():
    # 1. Namespace-prefixed & UTF-16 detection
    ns_xml = b'<?xml version="1.0"?><Document xmlns:camt="urn:iso:std:iso:20022:tech:xsd:camt.053.001.08"><camt:BkToCstmrStmt/></Document>'
    assert banking_standards_ontology.is_camt053_content(ns_xml)
    utf16_xml = "<Document><BkToCstmrAcctRpt/></Document>".encode("utf-16")
    assert banking_standards_ontology.is_camt053_content(utf16_xml)

    # 2. DTD blocked (forbid_dtd=True)
    dtd_xml = b'<?xml version="1.0"?><!DOCTYPE foo [<!ENTITY x "y">]><Document><BkToCstmrStmt/></Document>'
    try:
        banking_standards_ontology.parse_camt053_statement(dtd_xml)
        raise AssertionError("Expected ValueError for XML with DTD")
    except ValueError:
        pass

    # 3. Malformed decimal raises ValueError (not decimal.InvalidOperation)
    bad_amt_xml = b"""<Document><BkToCstmrStmt><Stmt>
        <Acct><Id><IBAN>EG123</IBAN></Id><Ccy>EGP</Ccy></Acct>
        <Ntry><Amt Ccy="EGP">1,000.00</Amt><CdtDbtInd>CRDT</CdtDbtInd></Ntry>
    </Stmt></BkToCstmrStmt></Document>"""
    try:
        banking_standards_ontology.parse_camt053_statement(bad_amt_xml)
        raise AssertionError("Expected ValueError for comma-formatted XML Amt")
    except ValueError:
        pass

    # 4. Indented / whitespace XML + whitespace-only <Amt> + CAMT.054 start_date <= end_date + v8 Pty/Nm
    camt054_xml = b"""<Document><BkToCstmrDbtCdtNtfctn><Ntfctn>
        <CreDtTm>\n  2026-11-20T10:00:00Z\n</CreDtTm>
        <Acct><Id><IBAN>EG99001</IBAN></Id><Ccy> EGP </Ccy></Acct>
        <Ntry><Amt Ccy="EGP">   </Amt><CdtDbtInd>CRDT</CdtDbtInd></Ntry>
        <Ntry>
            <Amt Ccy="EGP"> 2500.00 </Amt>
            <CdtDbtInd>\n  CRDT\n</CdtDbtInd>
            <BookgDt><Dt>\n  2026-11-18\n</Dt></BookgDt>
            <ValDt><Dt> 2026-11-19 </Dt></ValDt>
            <NtryDtls><TxDtls><RltdPties><Dbtr><Pty><Nm>Acme Buyer</Nm></Pty></Dbtr></RltdPties></TxDtls></NtryDtls>
        </Ntry>
    </Ntfctn></BkToCstmrDbtCdtNtfctn></Document>"""
    txns54, meta54 = banking_standards_ontology.parse_camt053_statement(camt054_xml)
    assert len(txns54) == 1, txns54
    assert txns54[0]["credit_amount"] == Decimal("2500.00") and txns54[0]["debit_amount"] == Decimal("0.00")
    assert txns54[0]["booking_date"].strftime("%Y-%m-%d") == "2026-11-18"
    assert txns54[0]["counterparty_name"] == "Acme Buyer"
    assert meta54["start_date"] <= meta54["end_date"]
    assert meta54["start_date"].strftime("%Y-%m-%d") == "2026-11-18"

    # 5. Multi-container cross-account isolation
    multi_xml = b"""<Document><BkToCstmrStmt>
        <Stmt>
            <Acct><Id><IBAN>EG_ACCT_1</IBAN></Id><Ccy>EGP</Ccy></Acct>
            <Bal><Tp><CdOrPrtry><Cd>OPBD</Cd></CdOrPrtry></Tp><Amt Ccy="EGP">1000.00</Amt><CdtDbtInd>CRDT</CdtDbtInd><Dt><Dt>2026-11-01</Dt></Dt></Bal>
            <Ntry><Amt Ccy="EGP">200.00</Amt><CdtDbtInd>CRDT</CdtDbtInd><BookgDt><Dt>2026-11-02</Dt></BookgDt></Ntry>
        </Stmt>
        <Stmt>
            <Acct><Id><IBAN>EG_ACCT_2</IBAN></Id><Ccy>USD</Ccy></Acct>
            <Bal><Tp><CdOrPrtry><Cd>OPBD</Cd></CdOrPrtry></Tp><Amt Ccy="USD">5000.00</Amt><CdtDbtInd>CRDT</CdtDbtInd><Dt><Dt>2026-11-01</Dt></Dt></Bal>
            <Bal><Tp><CdOrPrtry><Cd>CLBD</Cd></CdOrPrtry></Tp><Amt Ccy="USD">4500.00</Amt><CdtDbtInd>CRDT</CdtDbtInd><Dt><Dt>2026-11-05</Dt></Dt></Bal>
            <Ntry><Amt Ccy="USD">500.00</Amt><CdtDbtInd>DBIT</CdtDbtInd><BookgDt><Dt>2026-11-03</Dt></BookgDt></Ntry>
        </Stmt>
    </BkToCstmrStmt></Document>"""
    m_txns, m_meta = banking_standards_ontology.parse_camt053_statement(multi_xml)
    assert len(m_txns) == 2
    assert m_txns[0]["account_number"] == "EG_ACCT_1" and m_txns[0]["currency"] == "EGP"
    assert m_txns[0]["running_balance"] == Decimal("1200.00")
    assert m_txns[1]["account_number"] == "EG_ACCT_2" and m_txns[1]["currency"] == "USD"
    assert m_txns[1]["running_balance"] == Decimal("4500.00")
    assert m_meta["account_number"] == "EG_ACCT_1"
    assert m_meta["opening_balance"] == Decimal("1000.00")
    assert m_meta["closing_balance"] == Decimal("1200.00")
    assert any("Multi-account CAMT" in w for w in m_meta["continuity_warnings"])


def test_parse_as_of_datetime_iso_vs_dmy():
    dt1 = _parse_as_of_datetime("2026-11-10")
    assert (dt1.year, dt1.month, dt1.day) == (2026, 11, 10), dt1
    dt2 = _parse_as_of_datetime("2026-03-11")
    assert (dt2.year, dt2.month, dt2.day) == (2026, 3, 11), dt2
    dt3 = _parse_as_of_datetime("10/11/2026")
    assert (dt3.year, dt3.month, dt3.day) == (2026, 11, 10), dt3
    dt4 = _parse_as_of_datetime("2026-3-9")
    assert (dt4.year, dt4.month, dt4.day) == (2026, 3, 9), dt4


def test_reconciliation_reverse_order_and_foreign_currency():
    # Reverse-chronological CSV (newest row first, no explicit Closing Balance header)
    csv_rev = (
        "Date,Description,Debit,Credit,Balance\n"
        "2026-11-20,NEWEST TXN,0.00,500.00,2000.00\n"
        "2026-11-01,OLDEST TXN,0.00,1000.00,1500.00\n"
    ).encode("utf-8")
    txns_rev, meta_rev = bank_reconcile_service.parse_statement_content(csv_rev, "csv", company_id=1)
    assert len(txns_rev) == 2
    assert meta_rev["closing_balance"] == Decimal("2000.00"), meta_rev

    # Foreign currency PDF statement with no explicit currency column
    usd_pdf = _make_text_pdf([
        "Account No: 999-111222-001  Currency: USD",
        "Opening Balance: 1,000.00 USD",
        "Date          Description                 Debit      Credit     Balance",
        "2026-11-10    USD WIRE RECEIPT            0.00       500.00     1,500.00",
        "Closing Balance: 1,500.00 USD",
    ])
    txns_usd, meta_usd = bank_reconcile_service.parse_statement_content(usd_pdf, "pdf", company_id=1)
    assert len(txns_usd) == 1
    assert txns_usd[0]["currency"] == "USD", txns_usd[0]
    assert txns_usd[0]["amount_in_egp"] == Decimal("0.00"), txns_usd[0]
    assert meta_usd["currency"] == "USD"


def test_reconciliation_upload_snapshot_failure_rolls_back():
    import asyncio
    from app.api.v1.endpoints.reconciliation_endpoints import upload_bank_statement

    fake_stmt = MagicMock()
    fake_stmt.id = 777
    fake_stmt.bank_id = 1
    fake_stmt.closing_balance = Decimal("100.00")
    fake_stmt.account_number = "12345"
    fake_stmt.statement_end_date = datetime(2026, 11, 20, tzinfo=timezone.utc)
    fake_stmt.balance_continuity_status = "VALIDATED"

    mock_db = MagicMock()
    mock_db.query.return_value.filter.return_value.first.side_effect = [None, fake_stmt]

    fake_user = MagicMock(customer_id=1, user_id=2)
    fake_file = MagicMock(filename="stmt.csv")

    async def _fake_read():
        return b"Date,Description,Amount\n2026-11-20,Test,100\n"

    fake_file.read = _fake_read

    with patch("app.core.trial_guard.enforce_trial_quota"), \
         patch("app.api.v1.endpoints.reconciliation_endpoints.bank_reconcile_service.process_ingestion", return_value=fake_stmt), \
         patch("app.services.inbox_action_service.inbox_action_service.record_cash_balance_snapshot", side_effect=RuntimeError("db fail")):
        res = asyncio.run(
            upload_bank_statement(
                bank_id=1,
                opening_balance=None,
                closing_balance=None,
                start_date=None,
                end_date=None,
                pdf_password=None,
                file=fake_file,
                db=mock_db,
                current_user=fake_user,
            )
        )
    assert res is fake_stmt
    mock_db.rollback.assert_called_once()


def run_suite() -> None:
    test_pdf_text_ungrouped_amounts_signs_and_boundaries()
    test_fallback_table_ungrouped_and_parenthesized_amounts()
    test_corrupted_pdf_and_preserved_encryption_state()
    test_camt052_053_054_hardening_and_multi_account()
    test_parse_as_of_datetime_iso_vs_dmy()
    test_reconciliation_reverse_order_and_foreign_currency()
    test_reconciliation_upload_snapshot_failure_rolls_back()
    print("ALL PHASE 1 PARSER & RECONCILIATION TESTS PASSED!")


if __name__ == "__main__":
    run_suite()
