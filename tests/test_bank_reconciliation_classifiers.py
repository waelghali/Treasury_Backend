from decimal import Decimal
import io
from app.services.bank_reconciliation_service import bank_reconcile_service
from app.services.reconciliation_confidentiality_service import (
    reconciliation_confidentiality,
    classify_amount_tier,
)
from app.core.security_crypto import generate_tenant_dek

def test_builtin_interest_credit():
    txn = {
        "raw_description": "MONTHLY INT CREDIT ON SAVINGS",
        "description_line2": "",
        "debit_amount": Decimal("0.00"),
        "credit_amount": Decimal("1500.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_INTEREST"
    assert result["sub_category"] == "INTEREST_EARNED"
    assert result["confidence"] == 90

def test_builtin_interest_arabic():
    txn = {
        "raw_description": "فائدة دائنة عن شهر أغسطس",
        "description_line2": None,
        "debit_amount": Decimal("0.00"),
        "credit_amount": Decimal("850.50"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_INTEREST"
    assert result["sub_category"] == "INTEREST_EARNED"

def test_builtin_interest_debit():
    txn = {
        "raw_description": "OVERDRAFT INTEREST CHARGE",
        "description_line2": None,
        "debit_amount": Decimal("320.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_INTEREST"
    assert result["sub_category"] == "INTEREST_CHARGED"

def test_builtin_bank_charges():
    txn = {
        "raw_description": "COMMISSION ON SWIFT TRANSFER REF 12345",
        "description_line2": "SERVICE CHARGE",
        "debit_amount": Decimal("45.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_CHARGES"
    assert result["confidence"] == 85

def test_builtin_bank_charges_arabic():
    txn = {
        "raw_description": "عمولة تحويل ومصاريف بنكية",
        "description_line2": "",
        "debit_amount": Decimal("120.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_CHARGES"

def test_builtin_tax_deduction():
    txn = {
        "raw_description": "WHT DEDUCTION ON ACCRUED INTEREST",
        "description_line2": "",
        "debit_amount": Decimal("150.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "TAX_DEDUCTION"

def test_builtin_tax_arabic():
    txn = {
        "raw_description": "ضريبة استقطاع / دمغة نسبية",
        "description_line2": "",
        "debit_amount": Decimal("75.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "TAX_DEDUCTION"

def test_builtin_salary_payroll():
    txn = {
        "raw_description": "MONTHLY SALARY BATCH 2026-08",
        "description_line2": "",
        "debit_amount": Decimal("450000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "SALARY_PAYROLL"

def test_builtin_salary_arabic():
    txn = {
        "raw_description": "صرف مرتبات وأجور العاملين",
        "description_line2": "",
        "debit_amount": Decimal("320000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "SALARY_PAYROLL"

def test_builtin_loan_repayment():
    txn = {
        "raw_description": "LOAN REPAYMENT INSTALLMENT #12",
        "description_line2": "",
        "debit_amount": Decimal("50000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "LOAN_REPAYMENT"

def test_builtin_unrelated_not_classified():
    txn = {
        "raw_description": "SUPPLIER PAYMENT INVOICE 99482 AL MOASSRON",
        "description_line2": "",
        "debit_amount": Decimal("25000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is None

def test_reversed_collection_bounced_cheque():
    txn = {
        "raw_description": "RETURNED CHEQUE NO 884122 FROM CUSTOMER",
        "description_line2": "شيك مرتد",
        "debit_amount": Decimal("35000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "REVERSED_COLLECTION"
    assert result["confidence"] >= 90

def test_accounting_sanity_guardrail_blocks_huge_bank_charge():
    # A 250,000 EGP payment mentioning 'commission' should NOT be classified as a minor BANK_CHARGES line
    txn = {
        "raw_description": "BROKERAGE COMMISSION SETTLEMENT CONTRACT 44",
        "description_line2": "",
        "debit_amount": Decimal("250000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is None

def test_concept_collection_rule():
    from app.models.models_reconciliation_v2 import BankTransaction
    txn = BankTransaction(
        raw_description="TRANSFER FROM ACME HOLDINGS CORP REF 9921",
        credit_amount=Decimal("75000.00"),
        debit_amount=Decimal("0.00"),
        counterparty_name="ACME HOLDINGS CORP"
    )
    rule_condition = {"concept": "COLLECTION_FROM_CUSTOMER"}
    assert bank_reconcile_service._evaluate_group(txn, rule_condition) is True

    cp_condition = {"conditions": [
        {"field": "counterparty_name", "operator": "contains", "value": "ACME"},
        {"field": "credit_amount", "operator": "gt", "value": 0, "joiner": "AND"}
    ]}
    assert bank_reconcile_service._evaluate_group(txn, cp_condition) is True

def test_concept_supplier_payment_rule():
    from app.models.models_reconciliation_v2 import BankTransaction
    txn = BankTransaction(
        raw_description="PAYMENT TO VENDOR ORASCOM CONSTRUCTION",
        credit_amount=Decimal("0.00"),
        debit_amount=Decimal("120000.00"),
        counterparty_name="ORASCOM CONSTRUCTION"
    )
    rule_condition = {"concept": "PAYMENT_TO_SUPPLIER"}
    assert bank_reconcile_service._evaluate_group(txn, rule_condition) is True
    assert bank_reconcile_service._evaluate_group(txn, {"concept": "COLLECTION_FROM_CUSTOMER"}) is False

def test_online_self_healing_rule_exclusions():
    from app.models.models_reconciliation_v2 import BankTransaction
    # Rule has self-healed by adding exclude_tokens=["LEGAL"] and amount_max=5000
    self_healed_conditions = {
        "conditions": [
            {"field": "raw_description", "operator": "contains", "value": "FEES", "joiner": "AND"}
        ],
        "exclude_tokens": ["LEGAL"],
        "amount_max": 5000.0
    }
    small_fee_txn = BankTransaction(
        raw_description="MONTHLY ACCOUNT MAINTENANCE FEES",
        debit_amount=Decimal("150.00"),
        credit_amount=Decimal("0.00")
    )
    large_legal_txn = BankTransaction(
        raw_description="LEGAL FEES CONSULTANCY PAYMENT",
        debit_amount=Decimal("45000.00"),
        credit_amount=Decimal("0.00")
    )
    assert bank_reconcile_service._evaluate_group(small_fee_txn, self_healed_conditions) is True
    assert bank_reconcile_service._evaluate_group(large_legal_txn, self_healed_conditions) is False

def test_zero_knowledge_tokenization_and_at_rest_encryption():
    from app.models.models_reconciliation_v2 import BankTransaction
    dek = generate_tenant_dek()
    raw_narrative = "SWIFT TRF B/O ACME TRADING LLC IBAN EG380010000500000000123456789 ACC 100098765432"
    txn = BankTransaction(
        raw_description=raw_narrative,
        counterparty_name="ACME TRADING LLC",
        debit_amount=Decimal("85000.00"),
        credit_amount=Decimal("0.00"),
    )
    reconciliation_confidentiality.seal_transaction_fields(
        txn,
        dek=dek,
        known_entities=["ACME TRADING LLC"],
        custom_fields={"Cost_Center": "CC-901", "Project": "Alexandria Port"}
    )
    # 1. Verify tokenized_description does NOT contain raw company name or raw account tail
    assert "ACME TRADING" not in txn.tokenized_description
    assert "100098765432" not in txn.tokenized_description
    assert "<IBAN_EG_0010:ACCT_TOK_" in txn.tokenized_description
    assert "<ENT_TOK_" in txn.tokenized_description

    # 2. Verify at-rest ciphertext is AES-256-GCM sealed (`enc:v1:...`)
    assert txn.encrypted_raw_description.startswith("enc:v1:")
    assert txn.encrypted_counterparty_name.startswith("enc:v1:")
    assert txn.encrypted_custom_fields.startswith("enc:v1:")

    # 3. Verify authorized decryption in memory restores exact values
    resolved = reconciliation_confidentiality.resolve_transaction_fields(txn, dek)
    assert resolved["raw_description"] == raw_narrative
    assert resolved["counterparty_name"] == "ACME TRADING LLC"
    assert resolved["custom_fields"]["Cost_Center"] == "CC-901"

    # 4. Verify collaborative signature scrubber removes all PII
    sig_info = reconciliation_confidentiality.extract_privacy_safe_rule_signature(
        raw_description=raw_narrative,
        known_entities=["ACME TRADING LLC"],
        counterparty_name="ACME TRADING LLC"
    )
    assert "ACME" not in (sig_info["collaborative_signature"] or "")
    assert "ACME" not in (sig_info["tenant_signature"] or "")
    assert sig_info["iban_bank_code"] == "0010"

def test_schema_agnostic_csv_parsing_with_custom_columns():
    csv_content = (
        "Branch_Code,Custom_Cost_Center,Post_Date,Movement_Value,D/C,Running_Bal,Transaction_Narrative\n"
        "BR-01,CC-FIN-10,2026-08-01,150.00,D,99850.00,SWIFT TRANSFER COMMISSION FEE\n"
        "BR-01,CC-OPS-20,2026-08-02,25000.00,C,124850.00,CUSTOMER COLLECTION INV-901\n"
    ).encode("utf-8")

    txns, meta = bank_reconcile_service.parse_statement_content(csv_content, "csv")
    assert len(txns) == 2
    assert txns[0]["debit_amount"] == Decimal("150.00")
    assert txns[0]["credit_amount"] == Decimal("0.00")
    assert txns[0]["classification_category"] == "BANK_CHARGES"
    # Verify custom columns were preserved automatically
    assert txns[0]["custom_fields_json"]["Branch_Code"] == "BR-01"
    assert txns[0]["custom_fields_json"]["Custom_Cost_Center"] == "CC-FIN-10"
    assert txns[1]["credit_amount"] == Decimal("25000.00")

def test_7_factor_confidence_calibrator_and_account_purity():
    from app.models.models_reconciliation_v2 import BankTransaction, BankAccountProfile
    txn = BankTransaction(
        raw_description="MONTHLY PAYROLL DISBURSEMENT BATCH",
        debit_amount=Decimal("350000.00"),
        credit_amount=Decimal("0.00"),
    )
    dedicated_payroll_profile = BankAccountProfile(
        company_id=1,
        account_token="ACCT_TOK_123456",
        total_txns_observed=20,
        dominant_category="SALARY_PAYROLL",
        purity_ratio=Decimal("95.00"),
    )
    candidates = [
        {"category": "SALARY_PAYROLL", "gl_account": "5010010", "source": "LEARNED_RULE", "confidence": 85, "lifecycle_state": "AUTO_CONFIRMED", "reason": "Matched Learned Payroll Rule"}
    ]
    winner, top3 = bank_reconcile_service._calibrate_confidence_and_explain(txn, candidates, account_profile=dedicated_payroll_profile)
    assert winner is not None
    # Confidence should be boosted by +7 due to 95% dedicated payroll account profile
    assert winner["confidence"] == 92
    assert "dedicated account profile" in winner["reasoning_text"]
    assert len(top3) == 1

def test_posting_date_vs_value_date_and_historical_import():
    from datetime import datetime
    csv_content = (
        "Value Date,Posting Date,Debit,Credit,Balance,Transaction Details,Category,SubCategory\n"
        "2026-08-08,2026-08-05,50000.00,0.00,150000.00,SUPPLIER WIRE PAYMENT ALPHA CORP,PAYMENT_TO_SUPPLIER,WIRE_OUT\n"
    ).encode("utf-8")

    txns, meta = bank_reconcile_service.parse_statement_content(csv_content, "csv")
    assert len(txns) == 1
    t = txns[0]
    # Verify Value Date and Posting Date were not swapped even though Value Date came first!
    assert t["booking_date"].strftime("%Y-%m-%d") == "2026-08-05"
    assert t["value_date"].strftime("%Y-%m-%d") == "2026-08-08"
    assert t["custom_fields_json"]["clearing_float_days"] == "3"
    # Verify effective date distance bridges the 3-day clearing float when ERP or counterpart leg is on 2026-08-08
    dist = bank_reconcile_service._effective_date_distance_days(
        t["booking_date"], t["value_date"], datetime(2026, 8, 8)
    )
    assert dist == 0
    # Verify pre-labeled historical row is marked as confirmed HISTORICAL_IMPORT
    assert t["classification_category"] == "PAYMENT_TO_SUPPLIER"
    assert t["sub_category"] == "WIRE_OUT"
    assert t["classification_source"] == "HISTORICAL_IMPORT"
    assert t["is_classified"] is True

def test_mt940_and_iso20022_standards_ontology_and_native_parsers():
    from app.services.banking_standards_ontology import banking_standards_ontology

    # 1. Test Structured :86: Subfield & SWIFT Code Decoding inside Excel/CSV narrative
    decoded = banking_standards_ontology.decode_narrative_standards(
        "NTRF /BENM/DELTA INDUSTRIAL SUPPLIES /EREF/INV-2026-991 /CHGS/EGP75.00 /REMI/AUGUST RAW MATERIALS",
        direction="DEBIT",
        amount=Decimal("125000.00"),
    )
    assert decoded["extracted_counterparty"] == "DELTA INDUSTRIAL SUPPLIES"
    assert decoded["extracted_e2e_ref"] == "INV-2026-991"
    assert decoded["embedded_fee_amount"] == Decimal("75.00")
    assert decoded["remittance_info"] == "AUGUST RAW MATERIALS"

    # 2. Test Corporate Treasury LG Cash Margin vs LG Commission & E-Finance
    lg_margin = bank_reconcile_service._run_builtin_classifiers({
        "raw_description": "تعلية غطاء خطاب ضمان ابتدائي رقم 4451",
        "debit_amount": Decimal("250000.00"),
        "credit_amount": Decimal("0.00"),
    })
    assert lg_margin is not None
    assert lg_margin["category"] == "LETTER_OF_GUARANTEE"
    assert lg_margin["sub_category"] == "LG_CASH_MARGIN"

    efin = bank_reconcile_service._run_builtin_classifiers({
        "raw_description": "CPS E-FINANCE CUSTOMS AND TAX AUTHORITY SETTLEMENT",
        "debit_amount": Decimal("85000.00"),
        "credit_amount": Decimal("0.00"),
    })
    assert efin is not None
    assert efin["category"] == "GOVERNMENT_PAYMENT"

    # 3. Test Native SWIFT MT940 Statement Parsing (.mt940) with Value Date vs Posting Date & NRTI/NCHG
    sample_mt940 = (
        ":20:STMT20260810\n"
        ":25:EG380010012300000012345678901\n"
        ":28C:00012/001\n"
        ":60F:C260801EGP500000,00\n"
        ":61:2608050803D100000,00NTRFE2E-SUP-88//BNKREF01\n"
        ":86:/BENM/OMEGA STEEL CO /EREF/E2E-SUP-88 /CHGS/EGP60.00\n"
        ":61:2608030803D60,00NCHGNONREF//BNKREF02\n"
        ":86:SWIFT TRANSFER COMMISSION FEE\n"
        ":61:2608070806D25000,00NRTICHQ-7712//BNKREF03\n"
        ":86:RETURNED BOUNCED CHEQUE NO 7712\n"
        ":62F:C260807EGP374940,00\n"
    ).encode("utf-8")

    mt_txns, mt_meta = bank_reconcile_service.parse_statement_content(sample_mt940, "mt940")
    assert len(mt_txns) == 3
    assert mt_meta["opening_balance"] == Decimal("500000.00")
    assert mt_meta["closing_balance"] == Decimal("374940.00")
    # Line 1: Posting Date 08-03, Value Date 08-05 (2-day clearing float), Beneficiary OMEGA STEEL CO
    assert mt_txns[0]["booking_date"].strftime("%Y-%m-%d") == "2026-08-03"
    assert mt_txns[0]["value_date"].strftime("%Y-%m-%d") == "2026-08-05"
    assert mt_txns[0]["custom_fields_json"]["clearing_float_days"] == "2"
    assert mt_txns[0]["counterparty_name"] == "OMEGA STEEL CO"
    # Line 2: Companion fee linked to Line 1 (same booking date 08-03) + NCHG classified
    assert mt_txns[1]["classification_category"] == "BANK_CHARGES"
    assert mt_txns[1]["_has_companion_parent"] is True
    # Line 3: NRTI -> REVERSED_COLLECTION + is_reversal=True
    assert mt_txns[2]["classification_category"] == "REVERSED_COLLECTION"
    assert mt_txns[2]["is_reversal"] is True

    # 4. Test Native ISO 20022 CAMT.053 XML Statement Parsing (.xml)
    sample_camt053 = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02">'
        '  <BkToCstmrStmt>'
        '    <Stmt>'
        '      <Acct><Id><IBAN>EG120003000123456789012345678</IBAN></Id><Ccy>EGP</Ccy></Acct>'
        '      <Bal><Tp><CdOrPrtry><Cd>OPBD</Cd></CdOrPrtry></Tp><Amt Ccy="EGP">200000.00</Amt><CdtDbtInd>CRDT</CdtDbtInd><Dt><Dt>2026-08-01</Dt></Dt></Bal>'
        '      <Ntry>'
        '        <Amt Ccy="EGP">45000.00</Amt>'
        '        <CdtDbtInd>CRDT</CdtDbtInd>'
        '        <BookgDt><Dt>2026-08-04</Dt></BookgDt>'
        '        <ValDt><Dt>2026-08-06</Dt></ValDt>'
        '        <BkTxCd><Domn><Cd>PMNT</Cd><Fmly><Cd>MCOP</Cd><SubFmlyCd>POSD</SubFmlyCd></Fmly></Domn></BkTxCd>'
        '        <NtryDtls><TxDtls>'
        '          <Refs><EndToEndId>POS-BATCH-882</EndToEndId></Refs>'
        '          <RltdPties><Dbtr><Nm>VISA MERCHANT ACQUIRING</Nm></Dbtr></RltdPties>'
        '          <RmtInf><Ustrd>POS BATCH SETTLEMENT</Ustrd></RmtInf>'
        '        </TxDtls></NtryDtls>'
        '      </Ntry>'
        '    </Stmt>'
        '  </BkToCstmrStmt>'
        '</Document>'
    ).encode("utf-8")

    camt_txns, camt_meta = bank_reconcile_service.parse_statement_content(sample_camt053, "xml")
    assert len(camt_txns) == 1
    assert camt_txns[0]["booking_date"].strftime("%Y-%m-%d") == "2026-08-04"
    assert camt_txns[0]["value_date"].strftime("%Y-%m-%d") == "2026-08-06"
    assert camt_txns[0]["classification_category"] == "POS_MERCHANT_SETTLEMENT"
    assert camt_txns[0]["counterparty_name"] == "VISA MERCHANT ACQUIRING"

def test_full_end_to_end_database_lifecycle_audit():
    """
    Comprehensive End-to-End Audit using an in-memory SQLAlchemy database:
    Tests Ingestion -> At-Rest AES-256-GCM Encryption & Blind Tokenization ->
    Unsupervised Upload Learning -> Foundational Rulebook Seeding ->
    User Confirmation & Online Rule Induction (SUGGESTED -> AUTO_CONFIRMED) ->
    User Override & Online Rule Self-Healing (SYSTEM_ADAPTED) ->
    Cross-Tenant Zero-PII Collaborative Consensus ->
    4-Pass ERP Matching with Posting vs. Value Date Clearing Float ->
    Customizable Export with Double-Entry Journal Legs.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.dialects.postgresql import JSONB
    compiles(JSONB, "sqlite")(lambda element, compiler, **kw: "JSON")
    from app.database import Base
    from app.models.models_reconciliation_v2 import (
        BankStatement, BankTransaction, ClassificationRule, Counterparty,
        CollaborativePattern, CollaborativeTenantVote, InternalLedgerRecord,
        ReconciliationMatch, BankAccountProfile, MatchingToleranceRule,
        StatementInputMappingProfile, ReconciliationExportProfile
    )
    from app.schemas.schemas_reconciliation_v2 import CustomExportExecuteRequest

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    # ── 1. Ingest a Corporate Bank Statement with Custom Columns & Posting/Value Date Float ──
    csv_stmt = (
        "Posting Date,Value Date,Debit,Credit,Running Balance,Transaction Details,Cost_Center\n"
        "2026-09-01,2026-09-04,50000.00,0.00,450000.00,ACH WIRE TRANSFER /BENM/OMEGA INDUSTRIAL SUPPLIES /EREF/BILL-901,CC-PROD\n"
        "2026-09-01,2026-09-01,75.00,0.00,449925.00,SWIFT TRANSFER COMMISSION FEE,CC-FIN\n"
        "2026-09-02,2026-09-02,55000.00,0.00,394925.00,ACH WIRE TRANSFER /BENM/OMEGA INDUSTRIAL SUPPLIES /EREF/BILL-902,CC-PROD\n"
        "2026-09-03,2026-09-05,0.00,30000.00,424925.00,CUSTOMER COLLECTION /ORDP/APEX RETAIL GROUP,CC-SALES\n"
    ).encode("utf-8")

    stmt = bank_reconcile_service.process_ingestion(
        db=db,
        content=csv_stmt,
        file_type="csv",
        company_id=1,
        user_id=10,
        overrides={"bank_id": 1, "account_number": "EG380010012300000012345678901", "file_name": "sep_stmt.csv"},
    )
    assert stmt.id is not None

    txns = db.query(BankTransaction).filter(BankTransaction.statement_id == stmt.id).order_by(BankTransaction.id.asc()).all()
    assert len(txns) == 4

    # Verify at-rest encryption & tokenization
    assert txns[0].encrypted_raw_description.startswith("enc:v1:")
    assert "<ENT_TOK_" in txns[0].tokenized_description
    # Verify companion fee linking (row 1 is companion fee of row 0)
    assert txns[1].companion_parent_id == txns[0].id
    # Verify Unsupervised Upload Learning auto-discovered OMEGA INDUSTRIAL SUPPLIES (appeared 2x on Debit)
    omega_cp = db.query(Counterparty).filter(Counterparty.company_id == 1, Counterparty.name == "OMEGA INDUSTRIAL SUPPLIES").first()
    assert omega_cp is not None
    assert omega_cp.entity_type == "SUPPLIER"
    # Verify Unsupervised Upload Learning auto-profiled bank tolerance rule (75.00 fee & 3-day float)
    tol_rule = db.query(MatchingToleranceRule).filter(MatchingToleranceRule.company_id == 1).first()
    assert tol_rule is not None
    assert tol_rule.max_fixed_fee_variance >= Decimal("75.00")

    # ── 2. User Confirmation & Online Rule Induction (SUGGESTED -> AUTO_CONFIRMED) ──
    t1_confirmed = bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=txns[0].id,
        customer_id=1,
        user_id=10,
        counterparty_name="OMEGA INDUSTRIAL SUPPLIES",
        entity_type="SUPPLIER",
        category="RAW_MATERIAL_PURCHASE",
        gl_account="5010200 - Raw Materials",
    )
    induced_rule_id = t1_confirmed.applied_rule_id
    assert induced_rule_id is not None
    induced_rule = db.query(ClassificationRule).filter(ClassificationRule.id == induced_rule_id).first()
    assert induced_rule.rule_origin == "LEARNED_ONLINE"
    assert induced_rule.lifecycle_state == "SUGGESTED"

    # Confirm second similar transaction -> rule should auto-promote to AUTO_CONFIRMED!
    t2_confirmed = bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=txns[2].id,
        customer_id=1,
        user_id=10,
        counterparty_name="OMEGA INDUSTRIAL SUPPLIES",
        entity_type="SUPPLIER",
        category="RAW_MATERIAL_PURCHASE",
        gl_account="5010200 - Raw Materials",
    )
    db.refresh(induced_rule)
    assert induced_rule.confirmations_count >= 2
    assert induced_rule.lifecycle_state == "AUTO_CONFIRMED"

    # ── 3. User Override & Online Self-Healing (SYSTEM_ADAPTED) ──
    # Suppose user overrides txns[2] to a different category ("CAPEX_MACHINERY")
    bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=txns[2].id,
        customer_id=1,
        user_id=10,
        category="CAPEX_MACHINERY",
        gl_account="1510000 - Machinery",
    )
    db.refresh(induced_rule)
    assert induced_rule.overrides_count >= 1
    assert induced_rule.rule_origin == "SYSTEM_ADAPTED"

    # ── 4. Cross-Tenant Zero-PII Collaborative Consensus ──
    from app.services.collaborative_learning_service import collaborative_service
    for tenant_id, private_vendor in [(1, "ALPHA CORP"), (2, "BETA LLC"), (3, "GAMMA INC")]:
        collaborative_service.submit_confirmation(
            db=db,
            company_id=tenant_id,
            bank_id=1,
            raw_description=f"INSTAPAY OUTGOING SETTLEMENT TO {private_vendor}",
            direction="DEBIT",
            category="SUPPLIER_INSTAPAY",
            gl_account="2010100 - AP InstaPay",
            amount=Decimal("12000.00"),
            counterparty_name=private_vendor,
        )
    collab_match = collaborative_service.match_collaborative(
        db=db,
        bank_id=1,
        raw_description="INSTAPAY OUTGOING SETTLEMENT TO ZETA TRADING",
        direction="DEBIT",
        amount=Decimal("14000.00"),
    )
    assert collab_match is not None
    assert collab_match["category"] == "SUPPLIER_INSTAPAY"
    assert collab_match["distinct_tenants"] == 3
    for secret_name in ("ALPHA", "BETA", "GAMMA", "ZETA"):
        assert secret_name not in collab_match["signature"]

    # ── 5. 4-Pass Mathematical & Tolerance ERP Matching with Posting vs. Value Date Float ──
    # Add ERP records:
    # - BILL-901 dated 2026-09-04 (matches txns[0] Value Date 2026-09-04 with 0-day effective distance!)
    # - Two AR invoices (18,000 + 12,000 = 30,000) dated 2026-09-05 that batch-match txns[3] (30,000 Credit on Value Date 2026-09-05)
    bank_reconcile_service.ingest_erp_records_bulk(
        db=db,
        records_data=[
            {"reference_number": "BILL-901", "entity_name": "OMEGA INDUSTRIAL SUPPLIES", "amount": -50000.00, "record_date": "2026-09-04", "record_type": "AP_BILL"},
            {"reference_number": "INV-501", "entity_name": "APEX RETAIL GROUP", "amount": 18000.00, "record_date": "2026-09-05", "record_type": "AR_INVOICE"},
            {"reference_number": "INV-502", "entity_name": "APEX RETAIL GROUP", "amount": 12000.00, "record_date": "2026-09-05", "record_type": "AR_INVOICE"},
        ],
        company_id=1,
        user_id=10,
    )
    match_res = bank_reconcile_service.run_matching_engine(db=db, customer_id=1, user_id=10, statement_id=stmt.id)
    assert match_res["pass1_optimal_1to1"] >= 1
    assert match_res["pass3_batch_grouped"] >= 1

    # ── 6. User-Customizable Export with Custom Columns & Double-Entry GL Journal Expansion ──
    exp_req = CustomExportExecuteRequest(
        statement_id=stmt.id,
        columns_config=[
            {"source_field": "booking_date", "custom_header": "Post_Date"},
            {"source_field": "value_date", "custom_header": "Val_Date"},
            {"source_field": "internal_category", "custom_header": "GL_Code"},
            {"source_field": "debit_amount", "custom_header": "DR"},
            {"source_field": "credit_amount", "custom_header": "CR"},
            {"source_field": "custom_fields.Cost_Center", "custom_header": "Cost_Center"},
        ],
        expand_journal_legs=True,
        bank_gl_account="1010010 - CIB Current Account",
    )
    headers, rows, _, fmt = bank_reconcile_service.build_custom_export_rows(db, customer_id=1, req=exp_req)
    assert headers == ["Post_Date", "Val_Date", "GL_Code", "DR", "CR", "Cost_Center"]
    # 4 transactions * 2 double-entry journal legs = 8 rows
    assert len(rows) == 8
    assert rows[0]["Cost_Center"] == "CC-PROD"
    db.close()

if __name__ == "__main__":
    test_builtin_interest_credit()
    test_builtin_interest_arabic()
    test_builtin_interest_debit()
    test_builtin_bank_charges()
    test_builtin_bank_charges_arabic()
    test_builtin_tax_deduction()
    test_builtin_tax_arabic()
    test_builtin_salary_payroll()
    test_builtin_salary_arabic()
    test_builtin_loan_repayment()
    test_builtin_unrelated_not_classified()
    test_reversed_collection_bounced_cheque()
    test_accounting_sanity_guardrail_blocks_huge_bank_charge()
    test_concept_collection_rule()
    test_concept_supplier_payment_rule()
    test_online_self_healing_rule_exclusions()
    test_zero_knowledge_tokenization_and_at_rest_encryption()
    test_schema_agnostic_csv_parsing_with_custom_columns()
    test_7_factor_confidence_calibrator_and_account_purity()
    test_posting_date_vs_value_date_and_historical_import()
    test_mt940_and_iso20022_standards_ontology_and_native_parsers()
    test_full_end_to_end_database_lifecycle_audit()
    print("ALL_CLASSIFIER_CONCEPT_SECURITY_AND_SCHEMA_TESTS_PASSED")
