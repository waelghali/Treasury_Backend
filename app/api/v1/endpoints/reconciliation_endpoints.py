from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from decimal import Decimal
import io
import csv
import logging

logger = logging.getLogger(__name__)

from app.database import get_db
from app.models.models_reconciliation_v2 import (
    BankStatement, BankTransaction, InternalLedgerRecord, ClassificationTaxonomy, Counterparty,
    ReconciliationMatch, ClassificationRule, ReconciliationExportProfile, MatchingToleranceRule
)
from app.core.security import (
    TokenData,
    check_subscription_status,
    get_current_corporate_admin_context,
)
from app.constants import UserRole
from app.schemas.all_schemas import BankOut
from app.crud.crud import crud_bank
from app.schemas.schemas_reconciliation_v2 import (
    BankStatementOut, BankTransactionOut, 
    ClassificationRuleCreate, ClassificationRuleUpdate, ClassificationRuleOut,
    CounterpartyCreate, CounterpartyUpdate, CounterpartyOut,
    InternalLedgerRecordOut, TaxonomyNodeCreate, TaxonomyNodeUpdate,
    TaxonomyNodeOut, ClassifyTransactionRequest, ERPUploadResult,
    BulkClassifyRequest, BulkClearClassificationRequest, BulkOperationResult,
    MatchActionRequest, UnmatchActionRequest,
    MatchingToleranceRuleCreate, MatchingToleranceRuleOut,
    ReconciliationExportProfileCreate, ReconciliationExportProfileUpdate,
    ReconciliationExportProfileOut, CustomExportExecuteRequest,
    BRSSignOffRequest,
)

from app.crud.crud_reconciliation_v2 import (
    crud_bank_statement, crud_bank_transaction, crud_classification_rule, crud_counterparty,
    crud_internal_ledger, crud_taxonomy, crud_export_profile, crud_matching_tolerance_rule
)
from app.services.bank_reconciliation_service import bank_reconcile_service


def get_reconciliation_user_context(
    current_user: TokenData = Depends(check_subscription_status),
) -> TokenData:
    """Allows End Users (makers) and Corporate Admins (oversight/governance) to access Bank Reconciliation."""
    if current_user.role not in (UserRole.END_USER, UserRole.CORPORATE_ADMIN):
        raise HTTPException(
            status_code=403,
            detail="Not enough privileges: Requires End User or Corporate Admin role.",
        )
    if current_user.customer_id is None:
        raise HTTPException(
            status_code=400,
            detail="User is not associated with a customer.",
        )
    return current_user


router = APIRouter()


@router.get("/banks", response_model=List[BankOut])
def list_reconciliation_banks(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """Returns active banks for statement ingestion and filtering (accessible to End Users and Corporate Admins)."""
    return crud_bank.get_all(db, skip=0, limit=200)

@router.post("/statements/upload", response_model=BankStatementOut)
async def upload_bank_statement(
    bank_id: int = Form(...),
    opening_balance: Optional[Decimal] = Form(None),
    closing_balance: Optional[Decimal] = Form(None),
    start_date: Optional[str] = Form(None),
    end_date: Optional[str] = Form(None),
    pdf_password: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Ingests a bank statement file with optional metadata.
    Auto-detects values if not provided (supports Excel, CSV, MT940/CAMT.053, and encrypted/plain PDFs).
    """
    from app.core.trial_guard import enforce_trial_quota
    enforce_trial_quota(db, current_user.customer_id, "statement")

    content = await file.read()
    file_type = file.filename.split('.')[-1]
    
    overrides = {
        "bank_id": bank_id,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "start_date": start_date,
        "end_date": end_date,
        "pdf_password": pdf_password,
        "file_name": file.filename
    }
    
    # Use the smart ingestion service
    db_statement = bank_reconcile_service.process_ingestion(
        db, content, file_type, 
        company_id=current_user.customer_id, 
        user_id=current_user.user_id,
        overrides=overrides
    )

    stmt_id = db_statement.id
    try:
        from app.services.inbox_action_service import inbox_action_service
        first_txn = (
            db.query(BankTransaction.currency)
            .filter(BankTransaction.statement_id == db_statement.id)
            .first()
        )
        stmt_ccy = (first_txn[0] if first_txn and first_txn[0] else "EGP").upper()
        inbox_action_service.record_cash_balance_snapshot(
            db=db,
            customer_id=current_user.customer_id,
            bank_id=db_statement.bank_id,
            available_balance=db_statement.closing_balance,
            ledger_balance=db_statement.closing_balance,
            currency_code=stmt_ccy,
            account_number=db_statement.account_number,
            as_of_datetime=db_statement.statement_end_date,
            source_channel="MANUAL_UPLOAD",
            statement_id=db_statement.id,
            continuity_status=db_statement.balance_continuity_status or "VALIDATED",
            notes=f"Manual statement upload '{file.filename}'",
            auto_commit=True,
        )
    except Exception as snap_err:
        logger.warning(f"Failed to record cash balance snapshot for statement {stmt_id}: {snap_err}")
        db.rollback()
        db_statement = db.query(BankStatement).filter(BankStatement.id == stmt_id).first() or db_statement
    
    return db_statement


@router.get("/cash-positions")
def get_reconciliation_cash_positions(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """Returns live aggregated cash positions across all banks and accounts for the customer."""
    from app.services.inbox_action_service import inbox_action_service
    return inbox_action_service.get_live_cash_positions(db, current_user.customer_id)

@router.get("/statements", response_model=List[BankStatementOut])
def get_statements(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    return crud_bank_statement.get_by_customer(db, customer_id=current_user.customer_id)


@router.get("/statements/{statement_id}/transactions", response_model=List[BankTransactionOut])
def get_statement_transactions(
    statement_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Retrieves all transactions for a specific bank statement.
    """
    # Verify ownership
    stmt = crud_bank_statement.get(db, id=statement_id)
    if not stmt or stmt.company_id != current_user.customer_id:
        raise HTTPException(status_code=404, detail="Statement not found")
        
    return crud_bank_transaction.get_by_statement(db, statement_id=statement_id)

@router.delete("/statements/{statement_id}")
def delete_bank_statement(
    statement_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Soft-deletes a bank statement.
    """
    stmt = crud_bank_statement.get(db, id=statement_id)
    if not stmt or stmt.company_id != current_user.customer_id:
        raise HTTPException(status_code=404, detail="Statement not found")
        
    # Soft delete statement, transactions, and linked cash balance snapshots
    from app.models.models_reconciliation_v2 import BankTransaction
    from app.models.models_inbox import CashBalanceSnapshot
    db.query(BankTransaction).filter(BankTransaction.statement_id == statement_id).update({"is_deleted": True})
    db.query(CashBalanceSnapshot).filter(CashBalanceSnapshot.statement_id == statement_id).update({"is_deleted": True})
    
    stmt.is_deleted = True
    db.commit()
    
    return {"message": "Statement deleted successfully"}

@router.post("/auto-match")
def run_global_auto_match(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Runs the auto-matching engine for all unmatched transactions of the customer.
    """
    return bank_reconcile_service.run_matching_engine(
        db, 
        customer_id=current_user.customer_id,
        user_id=current_user.user_id
    )

@router.post("/statements/{statement_id}/auto-match")
def run_auto_match(
    statement_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Runs the auto-matching engine for a specific statement.
    """
    stmt = crud_bank_statement.get(db, id=statement_id)
    if not stmt or stmt.company_id != current_user.customer_id:
        raise HTTPException(status_code=404, detail="Statement not found")

    return bank_reconcile_service.run_matching_engine(
        db, 
        statement_id=statement_id, 
        customer_id=current_user.customer_id,
        user_id=current_user.user_id
    )

@router.post("/match")
def manual_match_records(
    req: MatchActionRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Manually reconciles selected Bank Transactions with selected ERP Ledger Records (1:1, 1:M, M:1, M:N).
    """
    if not req.bank_transaction_ids or not req.erp_record_ids:
        raise HTTPException(status_code=400, detail="Must provide at least one bank transaction and one ERP record.")

    # 1. Fetch bank transactions and verify company ownership
    bank_txns = db.query(BankTransaction).join(BankStatement).filter(
        BankTransaction.id.in_(req.bank_transaction_ids),
        BankStatement.company_id == current_user.customer_id
    ).all()
    if len(bank_txns) != len(req.bank_transaction_ids):
        raise HTTPException(status_code=403, detail="One or more bank transactions are invalid or not owned by your company.")

    # 2. Fetch ERP records and verify company ownership
    erp_records = db.query(InternalLedgerRecord).filter(
        InternalLedgerRecord.id.in_(req.erp_record_ids),
        InternalLedgerRecord.company_id == current_user.customer_id
    ).all()
    if len(erp_records) != len(req.erp_record_ids):
        raise HTTPException(status_code=403, detail="One or more ERP records are invalid or not owned by your company.")

    # 3. Determine match type
    if len(bank_txns) == 1 and len(erp_records) == 1:
        match_type = "1:1"
    elif len(bank_txns) == 1 and len(erp_records) > 1:
        match_type = "1:M"
    elif len(bank_txns) > 1 and len(erp_records) == 1:
        match_type = "M:1"
    else:
        match_type = "M:N"

    primary_bank_id = bank_txns[0].id

    # Calculate total bank vs ERP amounts to detect and learn fee/FX tolerance online
    total_bank_amt = sum(
        max(Decimal(str(b.credit_amount or 0)), Decimal(str(b.debit_amount or 0)))
        for b in bank_txns
    )
    total_erp_amt = sum(abs(Decimal(str(e.amount or 0))) for e in erp_records)
    computed_variance = req.variance_amount if req.variance_amount is not None else abs(total_erp_amt - total_bank_amt)
    disposition = req.variance_disposition or ("BANK_FEE_WRITEOFF" if req.write_off_variance_as_fee or computed_variance > 0 else "NONE")

    # 4. Create reconciliation match records and link
    for b_txn in bank_txns:
        b_txn.is_reconciled = True
        if computed_variance > 0:
            b_txn.variance_amount = computed_variance
        for erp in erp_records:
            m = ReconciliationMatch(
                bank_txn_id=b_txn.id,
                source_type=erp.record_type or "ERP",
                source_record_id=erp.id,
                match_type=match_type,
                match_logic=req.type or "MANUAL",
                variance_amount=computed_variance,
                variance_disposition=disposition,
                created_by=current_user.user_id
            )
            db.add(m)

    for erp in erp_records:
        erp.status = "RECONCILED"
        erp.matched_bank_txn_id = primary_bank_id

    # 5. Online Learning: If 1:1 match had a fee/FX variance or date lag, learn a MatchingToleranceRule online
    if len(bank_txns) == 1 and len(erp_records) == 1 and computed_variance > Decimal("0.00"):
        try:
            b0 = bank_txns[0]
            e0 = erp_records[0]
            direction = "CREDIT" if (b0.credit_amount or 0) > 0 else "DEBIT"
            bank_id = b0.statement.bank_id if b0.statement else None
            b_dt = b0.booking_date.replace(tzinfo=None) if b0.booking_date else None
            e_dt = e0.record_date.replace(tzinfo=None) if e0.record_date else None
            lag_days = abs((b_dt - e_dt).days) if (b_dt and e_dt) else 0
            bank_reconcile_service._learn_matching_tolerance_online(
                db=db,
                customer_id=current_user.customer_id,
                bank_id=bank_id,
                direction=direction,
                record_type=e0.record_type,
                variance_amount=computed_variance,
                gross_amount=total_erp_amt,
                date_lag_days=lag_days,
                disposition=disposition,
                user_id=current_user.user_id,
            )
        except Exception:
            pass

    db.commit()

    return {
        "status": "success",
        "message": f"Successfully matched {len(bank_txns)} bank transactions with {len(erp_records)} ERP records ({match_type}).",
        "match_type": match_type,
        "matched_bank_count": len(bank_txns),
        "matched_erp_count": len(erp_records)
    }


@router.post("/unmatch")
def unmatch_records(
    req: UnmatchActionRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Unlinks / resets reconciled bank transactions and ERP records back to OPEN / unmatched status.
    """
    affected_bank_ids = set(req.bank_transaction_ids or [])
    affected_erp_ids = set(req.erp_record_ids or [])

    match_query = db.query(ReconciliationMatch).join(BankTransaction).join(BankStatement).filter(
        BankStatement.company_id == current_user.customer_id
    )

    if req.match_id:
        match_query = match_query.filter(ReconciliationMatch.id == req.match_id)
    elif req.bank_transaction_ids or req.erp_record_ids:
        conditions = []
        if req.bank_transaction_ids:
            conditions.append(ReconciliationMatch.bank_txn_id.in_(req.bank_transaction_ids))
        if req.erp_record_ids:
            conditions.append(ReconciliationMatch.source_record_id.in_(req.erp_record_ids))
        from sqlalchemy import or_
        match_query = match_query.filter(or_(*conditions))
    else:
        raise HTTPException(status_code=400, detail="Must specify match_id, bank_transaction_ids, or erp_record_ids.")

    matches_to_remove = match_query.all()
    for m in matches_to_remove:
        affected_bank_ids.add(m.bank_txn_id)
        affected_erp_ids.add(m.source_record_id)
        db.delete(m)

    # Reset bank transactions if they have no remaining matches
    if affected_bank_ids:
        for b_id in affected_bank_ids:
            remaining = db.query(ReconciliationMatch).filter(ReconciliationMatch.bank_txn_id == b_id).count()
            if remaining == 0:
                b_txn = db.query(BankTransaction).filter(BankTransaction.id == b_id).first()
                if b_txn:
                    b_txn.is_reconciled = False

    # Reset ERP records if they have no remaining matches
    if affected_erp_ids:
        for e_id in affected_erp_ids:
            remaining = db.query(ReconciliationMatch).filter(ReconciliationMatch.source_record_id == e_id).count()
            if remaining == 0:
                erp = db.query(InternalLedgerRecord).filter(InternalLedgerRecord.id == e_id).first()
                if erp:
                    erp.status = "OPEN"
                    erp.matched_bank_txn_id = None

    db.commit()

    return {
        "status": "success",
        "message": f"Successfully unmatched {len(matches_to_remove)} reconciliation links.",
        "unmatched_links": len(matches_to_remove)
    }


@router.get("/matches")
def get_reconciliation_matches(
    statement_id: Optional[int] = None,
    limit: int = 500,
    skip: int = 0,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Returns all cleared / matched transaction pairs (Bank Transaction <-> ERP Record)
    with side-by-side details for comparative clearing view.
    """
    query = db.query(ReconciliationMatch).join(BankTransaction).join(BankStatement).filter(
        BankStatement.company_id == current_user.customer_id
    )
    if statement_id:
        query = query.filter(BankTransaction.statement_id == statement_id)

    matches = query.order_by(ReconciliationMatch.created_at.desc()).offset(skip).limit(limit).all()
    
    results = []
    for m in matches:
        b_txn = db.query(BankTransaction).filter(BankTransaction.id == m.bank_txn_id).first()
        erp = db.query(InternalLedgerRecord).filter(InternalLedgerRecord.id == m.source_record_id).first()
        
        # Apply search if provided
        if search:
            s = search.lower()
            b_text = f"{b_txn.raw_description or ''} {b_txn.counterparty_name or ''} {b_txn.e2e_id or ''}".lower() if b_txn else ""
            erp_text = f"{erp.reference_number or ''} {erp.entity_name or ''} {erp.gl_account or ''}".lower() if erp else ""
            if s not in b_text and s not in erp_text:
                continue

        results.append({
            "match_id": m.id,
            "match_type": m.match_type,
            "match_logic": m.match_logic,
            "created_at": m.created_at.isoformat() if m.created_at else None,
            "bank_transaction": {
                "id": b_txn.id,
                "statement_id": b_txn.statement_id,
                "booking_date": b_txn.booking_date.isoformat() if b_txn.booking_date else None,
                "value_date": b_txn.value_date.isoformat() if b_txn.value_date else None,
                "debit_amount": float(b_txn.debit_amount or 0),
                "credit_amount": float(b_txn.credit_amount or 0),
                "currency": b_txn.currency,
                "raw_description": b_txn.raw_description,
                "counterparty_name": b_txn.counterparty_name,
                "e2e_id": b_txn.e2e_id,
                "account_number": b_txn.account_number,
                "classification_category": b_txn.classification_category
            } if b_txn else None,
            "erp_record": {
                "id": erp.id,
                "record_type": erp.record_type,
                "reference_number": erp.reference_number,
                "entity_name": erp.entity_name,
                "amount": float(erp.amount or 0),
                "currency": erp.currency,
                "record_date": erp.record_date.isoformat() if erp.record_date else None,
                "gl_account": erp.gl_account,
                "status": erp.status
            } if erp else None
        })

    return results


@router.post("/detect-relationships")
async def run_relationship_detection(

    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Detects reversals and inter-account transfers.
    """
    return await bank_reconcile_service.detect_logical_relationships(
        db, 
        customer_id=current_user.customer_id
    )

@router.post("/transactions/{transaction_id}/unlink")
def unlink_transaction(
    transaction_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Unlinks a paired sweep or reversal transaction.
    """
    txn = db.query(BankTransaction).join(BankStatement).filter(
        BankTransaction.id == transaction_id,
        BankStatement.company_id == current_user.customer_id
    ).first()
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    if txn.linked_txn_id:
        counterpart = db.query(BankTransaction).get(txn.linked_txn_id)
        if counterpart:
            counterpart.linked_txn_id = None
            counterpart.variance_amount = None
        txn.linked_txn_id = None
        txn.variance_amount = None
        db.commit()
        return {"status": "Successfully unlinked transaction pair"}
    
    return {"status": "Transaction has no linked counterpart"}


@router.post("/classify")
def run_global_auto_classification(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Runs the classification rules for all unclassified transactions of the customer.
    """
    return bank_reconcile_service.apply_classification_rules(
        db, 
        customer_id=current_user.customer_id
    )

@router.post("/statements/{statement_id}/classify")
def run_auto_classification(
    statement_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Runs the classification rules for a specific statement.
    """
    stmt = crud_bank_statement.get(db, id=statement_id)
    if not stmt or stmt.company_id != current_user.customer_id:
        raise HTTPException(status_code=404, detail="Statement not found")

    return bank_reconcile_service.apply_classification_rules(
        db, 
        statement_id=statement_id, 
        customer_id=current_user.customer_id
    )

@router.get("/transactions", response_model=List[BankTransactionOut])
def get_all_transactions(
    bank_id: Optional[int] = None,
    account_number: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    is_reconciled: Optional[bool] = None,
    is_classified: Optional[bool] = None,
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 1000,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Unified transaction feed with advanced filtering.
    """
    return crud_bank_transaction.get_filtered(
        db, 
        customer_id=current_user.customer_id,
        bank_id=bank_id,
        account_number=account_number,
        start_date=start_date,
        end_date=end_date,
        is_reconciled=is_reconciled,
        is_classified=is_classified,
        search=search,
        skip=skip,
        limit=limit
    )

@router.post("/learn-from-history")
def learn_from_history(
    statement_id: Optional[int] = None,
    limit: int = 2000,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Bulk Historical Learning:
    Mines ClassificationRules, BankAccountProfiles, Counterparties, and zero-PII
    CollaborativePatterns from historical classified transactions in the database.
    """
    return bank_reconcile_service.learn_from_history(
        db=db,
        customer_id=current_user.customer_id,
        statement_id=statement_id,
        user_id=current_user.user_id,
        limit=limit
    )

@router.post("/seed-foundational-knowledge")
def seed_foundational_knowledge(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Idempotently seeds Foundational Banking Standards Rules (SWIFT MT940, ISO 20022,
    MENA/Egyptian Treasury Rails) and Collaborative Consensus Patterns for this customer.
    """
    from app.services.banking_standards_ontology import banking_standards_ontology
    return banking_standards_ontology.ensure_foundational_knowledge_seeded(
        db=db,
        customer_id=current_user.customer_id
    )

@router.get("/rules", response_model=List[ClassificationRuleOut])
def get_rules(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    return crud_classification_rule.get_by_customer(db, customer_id=current_user.customer_id)

@router.post("/rules", response_model=ClassificationRuleOut)
def create_rule(
    rule_in: ClassificationRuleCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    return crud_classification_rule.create_rule(
        db, 
        obj_in=rule_in, 
        customer_id=current_user.customer_id, 
        user_id=current_user.user_id
    )

@router.put("/rules/{rule_id}", response_model=ClassificationRuleOut)
def update_rule(
    rule_id: int,
    rule_in: ClassificationRuleUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    return crud_classification_rule.update_rule(
        db, 
        rule_id=rule_id, 
        obj_in=rule_in, 
        customer_id=current_user.customer_id
    )

@router.delete("/rules/{rule_id}")
def delete_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    crud_classification_rule.delete_rule(
        db, 
        rule_id=rule_id, 
        customer_id=current_user.customer_id
    )
    return {"status": "success"}


# ═══════════════════════════════════════════════════════════════════
#  COUNTERPARTY REGISTRY & CONCEPT-BASED LEARNING ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@router.get("/counterparties", response_model=List[CounterpartyOut])
def get_counterparties(
    entity_type: Optional[str] = None,
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Returns registered counterparties (both tenant-specific and global system entities).
    """
    return crud_counterparty.get_by_customer(
        db, 
        customer_id=current_user.customer_id,
        entity_type=entity_type,
        search=search,
        skip=skip,
        limit=limit
    )

@router.post("/counterparties", response_model=CounterpartyOut)
def create_counterparty(
    cp_in: CounterpartyCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Creates a new counterparty in the registry.
    """
    return crud_counterparty.create_counterparty(
        db, 
        obj_in=cp_in, 
        customer_id=current_user.customer_id,
        user_id=current_user.user_id
    )

@router.put("/counterparties/{counterparty_id}", response_model=CounterpartyOut)
def update_counterparty(
    counterparty_id: int,
    cp_in: CounterpartyUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Updates a counterparty definition.
    """
    return crud_counterparty.update_counterparty(
        db, 
        counterparty_id=counterparty_id,
        obj_in=cp_in,
        customer_id=current_user.customer_id
    )

@router.delete("/counterparties/{counterparty_id}")
def delete_counterparty(
    counterparty_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Deletes / deactivates a counterparty.
    """
    crud_counterparty.delete_counterparty(
        db,
        counterparty_id=counterparty_id,
        customer_id=current_user.customer_id
    )
    return {"status": "success"}

@router.get("/counterparties/discover")
def discover_counterparties(
    limit: int = 500,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Scans customer historical transactions to discover recurring counterparties,
    inferring whether they are Customers (credits) or Suppliers (debits).
    """
    return bank_reconcile_service.auto_discover_counterparties_from_history(
        db, 
        customer_id=current_user.customer_id, 
        limit=limit
    )

@router.post("/transactions/{transaction_id}/confirm", response_model=BankTransactionOut)
def confirm_and_learn_transaction(
    transaction_id: int,
    counterparty_name: Optional[str] = Form(None),
    entity_type: Optional[str] = Form("CUSTOMER"),
    category: Optional[str] = Form(None),
    gl_account: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Confirms or manually classifies a transaction and automatically reinforces/learns
    the counterparty in the Counterparty Registry for future auto-classification.
    """
    return bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=transaction_id,
        customer_id=current_user.customer_id,
        user_id=current_user.user_id,
        counterparty_name=counterparty_name,
        entity_type=entity_type,
        category=category,
        gl_account=gl_account
    )

@router.post("/concept-match")
def run_concept_matching(
    statement_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Executes concept matching (Counterparties -> Collections / Supplier payments)
    on unclassified transactions.
    """
    return bank_reconcile_service._match_counterparty_and_concept(
        db=db,
        customer_id=current_user.customer_id,
        statement_id=statement_id
    )

@router.get("/collaborative-stats")
def get_collaborative_intelligence_stats(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Returns platform-wide federated collaborative intelligence metrics.
    Zero customer PII or transaction data is exposed.
    """
    from app.services.collaborative_learning_service import collaborative_service
    return collaborative_service.get_platform_stats(db)

@router.post("/collaborative-decay")
def trigger_collaborative_decay(
    inactive_days: int = 180,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Triggers dynamic decay of stale signatures that haven't received confirmations.
    Automatically demotes outdated bank patterns to prevent obsolete formats from lingering.
    """
    from app.services.collaborative_learning_service import collaborative_service
    return collaborative_service.decay_stale_patterns(db, inactive_days=inactive_days)


# ── Classification Taxonomy (Classes & Subclasses) ───────────────────────────

@router.get("/taxonomy", response_model=List[TaxonomyNodeOut])
def get_classification_taxonomy(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Returns full hierarchy of Classes and Subclasses for the company.
    Combines company-specific customizations with global enterprise templates.
    """
    return crud_taxonomy.get_tree(db, customer_id=current_user.customer_id)

@router.post("/taxonomy", response_model=TaxonomyNodeOut)
def create_taxonomy_node(
    node_in: TaxonomyNodeCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Creates a new Class (parent_id=None) or Subclass (parent_id specified).
    """
    return crud_taxonomy.create_node(
        db, node_in=node_in, customer_id=current_user.customer_id, user_id=current_user.user_id
    )

@router.put("/taxonomy/{node_id}", response_model=TaxonomyNodeOut)
def update_taxonomy_node(
    node_id: int,
    node_in: TaxonomyNodeUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Updates a taxonomy node (name, GL account, direction, description).
    """
    db_node = crud_taxonomy.get(db, id=node_id)
    if not db_node or db_node.is_deleted or db_node.company_id != current_user.customer_id:
        raise HTTPException(status_code=404, detail="Taxonomy node not found")
    return crud_taxonomy.update_node(db, db_obj=db_node, node_in=node_in, user_id=current_user.user_id)

@router.delete("/taxonomy/{node_id}")
def delete_taxonomy_node(
    node_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Deactivates/soft-deletes a taxonomy node.
    """
    db_node = crud_taxonomy.get(db, id=node_id)
    if not db_node or db_node.is_deleted or db_node.company_id != current_user.customer_id:
        raise HTTPException(status_code=404, detail="Taxonomy node not found")
    db_node.is_active = False
    db_node.is_deleted = True
    db.commit()
    return {"status": "success", "message": f"Taxonomy node #{node_id} deactivated."}

@router.post("/transactions/{txn_id}/classify", response_model=BankTransactionOut)
def inline_classify_transaction(
    txn_id: int,
    req: ClassifyTransactionRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Inline classification action:
    Classifies a transaction, induces or self-heals online ClassificationRules,
    updates account purity profile, optionally binds to counterparty, and submits
    a zero-PII signature to the collaborative consensus brain.
    """
    txn = crud_bank_transaction.get(db, id=txn_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Bank transaction not found")

    # Call confirm_and_learn first so it captures previous_category & previous_rule_id for online self-healing
    updated_txn = bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=txn.id,
        customer_id=current_user.customer_id,
        user_id=current_user.user_id,
        counterparty_name=req.counterparty_name if req.remember_for_counterparty else None,
        category=req.category,
        gl_account=req.gl_account,
        sub_category=req.sub_category,
    )
    updated_txn.category = req.category
    if req.sub_category:
        updated_txn.sub_category = req.sub_category
    db.commit()
    db.refresh(updated_txn)
    return updated_txn


@router.post("/transactions/bulk-classify", response_model=BulkOperationResult)
def bulk_classify_transactions(
    req: BulkClassifyRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Bulk Classification Action:
    Applies Category, Subclass, and target GL account to 10, 20, or thousands of transactions in a single transaction.
    Optionally reinforces Counterparty Registry and Privacy-Preserving Collaborative Brain.
    """
    if not req.transaction_ids:
        return BulkOperationResult(status="noop", affected_count=0, message="No transactions provided")

    # Chunk into batches of 500 for SQL limits
    chunk_size = 500
    affected_count = 0

    for i in range(0, len(req.transaction_ids), chunk_size):
        chunk = req.transaction_ids[i:i + chunk_size]
        
        # Verify ownership through bank statements
        stmt_q = db.query(BankTransaction.id).join(BankStatement).filter(
            BankTransaction.id.in_(chunk),
            BankStatement.company_id == current_user.customer_id
        )
        valid_ids = [r[0] for r in stmt_q.all()]

        if valid_ids:
            updated = db.query(BankTransaction).filter(
                BankTransaction.id.in_(valid_ids)
            ).update(
                {
                    BankTransaction.classification_category: req.category,
                    BankTransaction.internal_category: req.gl_account or req.category,
                    BankTransaction.category: req.category,
                    BankTransaction.sub_category: req.sub_category,
                    BankTransaction.classification_source: "MANUAL",
                    BankTransaction.classification_confidence: 100,
                    BankTransaction.is_classified: True,
                },
                synchronize_session=False
            )
            affected_count += updated

    # If learn flag is true, trigger learning loop for counterparty
    if req.remember_for_counterparty and req.counterparty_name:
        crud_counterparty.learn_or_increment(
            db=db,
            name=req.counterparty_name,
            customer_id=current_user.customer_id,
            default_category=req.category,
            default_gl=req.gl_account,
            user_id=current_user.user_id
        )

    db.commit()

    return BulkOperationResult(
        status="success",
        affected_count=affected_count,
        message=f"Successfully classified {affected_count} transactions as '{req.category}'."
    )


@router.post("/transactions/bulk-clear-classification", response_model=BulkOperationResult)
def bulk_clear_classification(
    req: BulkClearClassificationRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Resets / un-classifies selected transactions in bulk.
    """
    if not req.transaction_ids:
        return BulkOperationResult(status="noop", affected_count=0, message="No transactions provided")

    chunk_size = 500
    affected_count = 0

    for i in range(0, len(req.transaction_ids), chunk_size):
        chunk = req.transaction_ids[i:i + chunk_size]
        stmt_q = db.query(BankTransaction.id).join(BankStatement).filter(
            BankTransaction.id.in_(chunk),
            BankStatement.company_id == current_user.customer_id
        )
        valid_ids = [r[0] for r in stmt_q.all()]

        if valid_ids:
            updated = db.query(BankTransaction).filter(
                BankTransaction.id.in_(valid_ids)
            ).update(
                {
                    BankTransaction.classification_category: None,
                    BankTransaction.internal_category: None,
                    BankTransaction.category: None,
                    BankTransaction.sub_category: None,
                    BankTransaction.classification_source: None,
                    BankTransaction.classification_confidence: None,
                    BankTransaction.is_classified: False,
                },
                synchronize_session=False
            )
            affected_count += updated

    db.commit()

    return BulkOperationResult(
        status="success",
        affected_count=affected_count,
        message=f"Reset classification for {affected_count} transactions."
    )



# ── Inward ERP Processing Engine ─────────────────────────────────────────────

@router.post("/erp-records/upload", response_model=ERPUploadResult)
async def upload_erp_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Inward ERP File Ingestion:
    Uploads an Excel (.xlsx, .xls) or CSV ledger export.
    Auto-detects columns, validates, and creates internal_ledger_records for dual-pane matching.
    """
    content = await file.read()
    file_type = file.filename.split('.')[-1]
    return bank_reconcile_service.process_erp_file_ingestion(
        db=db,
        file_content=content,
        file_type=file_type,
        company_id=current_user.customer_id,
        user_id=current_user.user_id
    )

@router.post("/erp-records/ingest", response_model=ERPUploadResult)
def ingest_erp_records_bulk(
    records: List[dict],
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    REST API / Webhook Entry Point for direct ERP integration (SAP, Oracle, NetSuite).
    Accepts JSON list of open invoices, bills, payroll batches, or fees.
    """
    return bank_reconcile_service.ingest_erp_records_bulk(
        db=db,
        records_data=records,
        company_id=current_user.customer_id,
        user_id=current_user.user_id
    )

@router.get("/erp-records", response_model=List[InternalLedgerRecordOut])
def get_erp_records(
    limit: int = 500,
    skip: int = 0,
    is_reconciled: Optional[bool] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Retrieves internal ERP ledger records for dual-pane matching.
    """
    query = db.query(InternalLedgerRecord).filter(
        InternalLedgerRecord.company_id == current_user.customer_id
    )
    if is_reconciled is not None:
        target_status = "RECONCILED" if is_reconciled else "OPEN"
        query = query.filter(InternalLedgerRecord.status == target_status)
    if status:
        query = query.filter(InternalLedgerRecord.status == status)
    if search:
        s = f"%{search}%"
        query = query.filter(
            (InternalLedgerRecord.reference_number.ilike(s)) |
            (InternalLedgerRecord.entity_name.ilike(s)) |
            (InternalLedgerRecord.gl_account.ilike(s))
        )
    return query.order_by(InternalLedgerRecord.record_date.desc()).offset(skip).limit(limit).all()


# ═══════════════════════════════════════════════════════════════════
#  ONLINE RULE LIFECYCLE & LEARNED TOLERANCE RULES ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@router.post("/rules/{rule_id}/promote", response_model=ClassificationRuleOut)
def promote_or_toggle_rule_lifecycle(
    rule_id: int,
    lifecycle_state: str = "AUTO_CONFIRMED",
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Promotes an online-learned rule from SUGGESTED to AUTO_CONFIRMED (or demotes to SUGGESTED).
    """
    rule = db.query(ClassificationRule).filter(
        ClassificationRule.id == rule_id,
        ClassificationRule.company_id == current_user.customer_id,
        ClassificationRule.is_deleted == False
    ).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    if lifecycle_state not in ("SUGGESTED", "AUTO_CONFIRMED", "CANDIDATE", "DEMOTED"):
        raise HTTPException(status_code=400, detail="Invalid lifecycle_state")

    rule.lifecycle_state = lifecycle_state
    if lifecycle_state == "AUTO_CONFIRMED":
        rule.calibrated_confidence = max(rule.calibrated_confidence or 85, 90)
    db.commit()
    db.refresh(rule)
    return rule


@router.get("/tolerance-rules", response_model=List[MatchingToleranceRuleOut])
def get_tolerance_rules(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    return crud_matching_tolerance_rule.get_by_customer(db, customer_id=current_user.customer_id)


@router.post("/tolerance-rules", response_model=MatchingToleranceRuleOut)
def create_tolerance_rule(
    rule_in: MatchingToleranceRuleCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    return crud_matching_tolerance_rule.create_rule(
        db, obj_in=rule_in, customer_id=current_user.customer_id, user_id=current_user.user_id
    )


# ═══════════════════════════════════════════════════════════════════
#  USER-CUSTOMIZABLE EXPORT BUILDER & EXECUTION ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@router.get("/export-profiles", response_model=List[ReconciliationExportProfileOut])
def get_export_profiles(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """Returns all saved custom export profiles for the customer."""
    return crud_export_profile.get_by_customer(db, customer_id=current_user.customer_id)


@router.post("/export-profiles", response_model=ReconciliationExportProfileOut)
def create_export_profile(
    profile_in: ReconciliationExportProfileCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """Creates a new customizable export template (custom column selection, order, headers, format)."""
    return crud_export_profile.create_profile(
        db, obj_in=profile_in, customer_id=current_user.customer_id, user_id=current_user.user_id
    )


@router.put("/export-profiles/{profile_id}", response_model=ReconciliationExportProfileOut)
def update_export_profile(
    profile_id: int,
    profile_in: ReconciliationExportProfileUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    return crud_export_profile.update_profile(
        db, profile_id=profile_id, obj_in=profile_in, customer_id=current_user.customer_id
    )


@router.delete("/export-profiles/{profile_id}")
def delete_export_profile(
    profile_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    crud_export_profile.delete_profile(db, profile_id=profile_id, customer_id=current_user.customer_id)
    return {"status": "success"}


@router.get("/account-gl-mappings")
def get_reconciliation_account_gl_mappings(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Returns discovered bank statement account numbers & registered CustomerBankAccounts
    along with their configured ERP Main Bank GL and ERP Clearing Suspense GL codes.
    Accessible to Reconciliation users without requiring the Issuance module.
    """
    from app.models.models import Bank
    cid = current_user.customer_id
    banks_map = {b.id: (b.short_name or b.name) for b in db.query(Bank).all()}

    accounts_out = {}

    # 1. Registered CustomerBankAccount rows
    try:
        from app.models.models_issuance import CustomerBankAccount
        cbas = db.query(CustomerBankAccount).filter(
            CustomerBankAccount.customer_id == cid,
            CustomerBankAccount.is_deleted == False,
        ).all()
        for a in cbas:
            key = (a.account_number or "").strip()
            if not key:
                continue
            accounts_out[key.upper()] = {
                "account_id": a.id,
                "account_number": key,
                "account_name": a.account_name,
                "bank_id": a.bank_id,
                "bank_name": banks_map.get(a.bank_id, f"Bank #{a.bank_id}"),
                "currency": (a.currency_code or "EGP").upper(),
                "main_gl": a.erp_main_gl_account or "",
                "clearing_gl": a.erp_clearing_gl_account or "",
                "source": "REGISTERED_ACCOUNT",
            }
    except Exception:
        pass

    # 2. Accounts discovered on uploaded BankStatements
    stmts = db.query(BankStatement).filter(
        BankStatement.company_id == cid,
        BankStatement.is_deleted == False,
    ).order_by(BankStatement.id.desc()).limit(200).all()

    for s in stmts:
        acct = (s.account_number or "").strip()
        if not acct:
            continue
        if acct.upper() not in accounts_out:
            accounts_out[acct.upper()] = {
                "account_id": None,
                "account_number": acct,
                "account_name": f"{banks_map.get(s.bank_id, 'Bank')} Statement Account",
                "bank_id": s.bank_id,
                "bank_name": banks_map.get(s.bank_id, f"Bank #{s.bank_id}"),
                "currency": (s.currency.iso_code.upper() if getattr(s, "currency", None) and s.currency.iso_code else "EGP"),
                "main_gl": "",
                "clearing_gl": "",
                "source": "DISCOVERED_STATEMENT",
            }

    return {"accounts": list(accounts_out.values())}


@router.put("/account-gl-mappings")
def save_reconciliation_account_gl_mappings(
    body: dict,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Persists ERP Main Bank GL & ERP Clearing Suspense GL codes onto matching CustomerBankAccount
    rows (creating lightweight records if needed for corporate admins) so all export profiles inherit them.
    """
    cid = current_user.customer_id
    mappings = body.get("mappings") if isinstance(body, dict) else None
    if not isinstance(mappings, dict):
        raise HTTPException(status_code=400, detail="Invalid mappings payload")

    updated_count = 0
    try:
        from app.models.models_issuance import CustomerBankAccount
        for acct_no, cfg in mappings.items():
            if not acct_no or not isinstance(cfg, dict):
                continue
            clean_acct = str(acct_no).strip()
            main_gl = (str(cfg.get("main_gl") or "").strip()) or None
            clearing_gl = (str(cfg.get("clearing_gl") or "").strip()) or None
            cba = db.query(CustomerBankAccount).filter(
                CustomerBankAccount.customer_id == cid,
                CustomerBankAccount.account_number == clean_acct,
                CustomerBankAccount.is_deleted == False,
            ).first()
            if cba:
                cba.erp_main_gl_account = main_gl
                cba.erp_clearing_gl_account = clearing_gl
                updated_count += 1
            elif (main_gl or clearing_gl) and cfg.get("bank_id"):
                new_cba = CustomerBankAccount(
                    customer_id=cid,
                    bank_id=int(cfg["bank_id"]),
                    account_name=str(cfg.get("account_name") or f"Account {clean_acct}"),
                    account_number=clean_acct,
                    currency_code=str(cfg.get("currency") or "EGP").upper()[:3],
                    erp_main_gl_account=main_gl,
                    erp_clearing_gl_account=clearing_gl,
                    is_active=True,
                )
                db.add(new_cba)
                updated_count += 1
        db.commit()
    except Exception as err:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to persist account GL mappings: {err}")

    return {"status": "success", "updated_count": updated_count}


@router.get("/export/available-fields")
def get_available_export_fields(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Returns all standard fields, round-trip ERP clearing fields, and dynamic custom columns
    discovered from both uploaded bank statements (`custom_fields.<col>`) and ERP ledgers (`erp_custom_fields.<col>`).
    """
    standard_fields = [
        {"source_field": "booking_date", "default_header": "Booking Date", "group": "Core Bank"},
        {"source_field": "value_date", "default_header": "Value Date", "group": "Core Bank"},
        {"source_field": "account_number", "default_header": "Account Number", "group": "Core Bank"},
        {"source_field": "effective_bank_gl", "default_header": "Resolved Bank / Clearing GL", "group": "Core Bank"},
        {"source_field": "raw_description", "default_header": "Description", "group": "Core Bank"},
        {"source_field": "description_line2", "default_header": "Description Line 2", "group": "Core Bank"},
        {"source_field": "debit_amount", "default_header": "Debit", "group": "Amounts"},
        {"source_field": "credit_amount", "default_header": "Credit", "group": "Amounts"},
        {"source_field": "signed_amount", "default_header": "Signed Net Amount", "group": "Amounts"},
        {"source_field": "absolute_amount", "default_header": "Absolute Amount", "group": "Amounts"},
        {"source_field": "dc_indicator", "default_header": "D/C Indicator", "group": "Amounts"},
        {"source_field": "currency", "default_header": "Currency", "group": "Amounts"},
        {"source_field": "running_balance", "default_header": "Running Balance", "group": "Amounts"},
        {"source_field": "counterparty_name", "default_header": "Counterparty", "group": "Classification & GL"},
        {"source_field": "classification_category", "default_header": "Category", "group": "Classification & GL"},
        {"source_field": "sub_category", "default_header": "Sub-Category", "group": "Classification & GL"},
        {"source_field": "internal_category", "default_header": "GL Account", "group": "Classification & GL"},
        {"source_field": "classification_source", "default_header": "Classification Source", "group": "Classification & GL"},
        {"source_field": "classification_confidence", "default_header": "Confidence %", "group": "Classification & GL"},
        {"source_field": "applied_rule_name", "default_header": "Applied Rule", "group": "Classification & GL"},
        {"source_field": "classification_reasoning", "default_header": "Audit Reasoning", "group": "Classification & GL"},
        {"source_field": "is_reconciled", "default_header": "Reconciled Status", "group": "ERP Clearing & Variance"},
        {"source_field": "matched_erp_reference", "default_header": "Matched ERP Reference", "group": "ERP Clearing & Variance"},
        {"source_field": "payment_or_erp_ref", "default_header": "ERP Ref / Payment ID", "group": "ERP Clearing & Variance"},
        {"source_field": "matched_erp_entity", "default_header": "Matched ERP Entity", "group": "ERP Clearing & Variance"},
        {"source_field": "matched_erp_type", "default_header": "Matched ERP Doc Type", "group": "ERP Clearing & Variance"},
        {"source_field": "matched_erp_gross_amount", "default_header": "Gross ERP Invoice Amount", "group": "ERP Clearing & Variance"},
        {"source_field": "allocated_erp_amount", "default_header": "Allocated Clearing Amount", "group": "ERP Clearing & Variance"},
        {"source_field": "variance_amount", "default_header": "Fee / FX Variance", "group": "ERP Clearing & Variance"},
        {"source_field": "variance_disposition", "default_header": "Variance Disposition", "group": "ERP Clearing & Variance"},
        {"source_field": "variance_gl_account", "default_header": "Variance GL Account", "group": "ERP Clearing & Variance"},
        {"source_field": "back_office_ref", "default_header": "Bank Reference", "group": "References"},
        {"source_field": "e2e_id", "default_header": "E2E / SWIFT ID", "group": "References"},
        {"source_field": "cheque_number", "default_header": "Cheque Number", "group": "References"},
        {"source_field": "counterparty_iban", "default_header": "Counterparty IBAN", "group": "References"},
    ]

    # Discover custom fields preserved from uploaded statements
    sample_txns = db.query(BankTransaction).join(BankStatement).filter(
        BankStatement.company_id == current_user.customer_id,
        BankTransaction.custom_fields_json != None,
        BankTransaction.is_deleted == False
    ).order_by(BankTransaction.id.desc()).limit(100).all()

    custom_keys = set()
    for t in sample_txns:
        if isinstance(t.custom_fields_json, dict):
            custom_keys.update(t.custom_fields_json.keys())

    for ck in sorted(custom_keys):
        standard_fields.append({
            "source_field": f"custom_fields.{ck}",
            "default_header": ck,
            "group": "Custom Uploaded Columns"
        })

    # Discover round-trip custom columns preserved from uploaded ERP Open Items
    sample_erps = db.query(InternalLedgerRecord).filter(
        InternalLedgerRecord.company_id == current_user.customer_id,
        InternalLedgerRecord.custom_fields_json != None,
        InternalLedgerRecord.is_deleted == False,
    ).order_by(InternalLedgerRecord.id.desc()).limit(100).all()

    erp_custom_keys = set()
    for e in sample_erps:
        if isinstance(e.custom_fields_json, dict):
            erp_custom_keys.update(e.custom_fields_json.keys())

    for eck in sorted(erp_custom_keys):
        standard_fields.append({
            "source_field": f"erp_custom_fields.{eck}",
            "default_header": f"ERP {eck}",
            "group": "Round-Trip ERP Columns"
        })

    return {"available_fields": standard_fields}


@router.post("/export/preview")
def preview_custom_export(
    req: CustomExportExecuteRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Returns live export summary stats (including ERP-matched vs Bank-only buckets and double-entry balance proof)
    and the first 50 formatted rows for preview.
    """
    cid = current_user.customer_id
    base_q = db.query(BankTransaction).join(BankStatement).filter(
        BankStatement.company_id == cid,
        BankTransaction.is_deleted == False
    )
    if req.statement_id:
        base_q = base_q.filter(BankTransaction.statement_id == req.statement_id)
    if req.bank_id:
        base_q = base_q.filter(BankStatement.bank_id == req.bank_id)

    scoped_txns = base_q.all()
    total_count = len(scoped_txns)
    finalized_count = sum(1 for t in scoped_txns if t.is_reconciled or t.is_classified)
    unclassified_count = sum(1 for t in scoped_txns if not t.is_classified)
    exported_count = sum(1 for t in scoped_txns if t.is_exported)

    scoped_ids = [t.id for t in scoped_txns]
    erp_matched_ids = set()
    if scoped_ids:
        erp_match_rows = db.query(ReconciliationMatch.bank_txn_id, ReconciliationMatch.source_type).filter(
            ReconciliationMatch.bank_txn_id.in_(scoped_ids),
            ReconciliationMatch.is_deleted == False,
        ).all()
        erp_matched_ids = {
            r[0] for r in erp_match_rows if (r[1] or "").upper() != "TREASURY (LG)"
        }

    erp_matched_count = len(erp_matched_ids)
    paired_reversals_count = sum(1 for t in scoped_txns if t.is_reversal and t.linked_txn_id)
    paired_sweeps_count = sum(
        1 for t in scoped_txns
        if not t.is_reversal and (
            t.linked_txn_id or (t.internal_category or "").upper() in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER")
        )
    )
    bank_only_count = sum(
        1 for t in scoped_txns
        if t.id not in erp_matched_ids and not (t.is_reversal and t.linked_txn_id)
    )

    headers, rows, _, fmt = bank_reconcile_service.build_custom_export_rows(db, cid, req)

    # Compute live Debit / Credit balance proof if Debit & Credit columns exist in columns_config
    effective_cols = req.columns_config
    if not effective_cols and req.profile_id:
        prof = db.query(ReconciliationExportProfile).filter(
            ReconciliationExportProfile.id == req.profile_id,
            ReconciliationExportProfile.company_id == cid,
            ReconciliationExportProfile.is_deleted == False,
        ).first()
        if prof:
            effective_cols = prof.columns_config_json
    effective_cols = effective_cols or bank_reconcile_service.DEFAULT_EXPORT_COLUMNS

    dr_hdr = next((c.get("custom_header") or c.get("source_field") for c in effective_cols if c.get("source_field") == "debit_amount"), None)
    cr_hdr = next((c.get("custom_header") or c.get("source_field") for c in effective_cols if c.get("source_field") == "credit_amount"), None)
    total_dr = round(sum(float(r.get(dr_hdr) or 0) for r in rows), 2) if dr_hdr else 0.0
    total_cr = round(sum(float(r.get(cr_hdr) or 0) for r in rows), 2) if cr_hdr else 0.0
    is_balanced = abs(total_dr - total_cr) < 0.01 if (dr_hdr and cr_hdr) else True

    return {
        "summary": {
            "total_transactions": total_count,
            "finalized_count": finalized_count,
            "unclassified_count": unclassified_count,
            "exported_count": exported_count,
            "matching_export_rows": len(rows),
            "erp_matched_count": erp_matched_count,
            "bank_only_count": bank_only_count,
            "paired_sweeps_count": paired_sweeps_count,
            "paired_reversals_count": paired_reversals_count,
            "total_debit": total_dr,
            "total_credit": total_cr,
            "is_balanced": is_balanced,
        },
        "headers": headers,
        "preview_rows": rows[:50],
        "file_format": fmt,
    }


@router.post("/export/download")
def download_custom_export(
    req: CustomExportExecuteRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context)
):
    """
    Generates and streams the user-customized CSV or Excel (.xlsx) export file.
    Optionally marks transactions as exported & locked.
    """
    cid = current_user.customer_id
    headers, rows, txns, fmt = bank_reconcile_service.build_custom_export_rows(db, cid, req)

    if req.mark_as_exported and txns:
        for t in txns:
            t.is_exported = True
            t.is_locked = True
        db.commit()

    def _sanitize_spreadsheet_cell(val):
        if not isinstance(val, str) or not val:
            return val
        stripped = val.lstrip(" ")
        if not stripped:
            return val
        first_ch = stripped[0]
        if first_ch in ("=", "+", "@") or val[0] in ("\t", "\r"):
            return f"'{val}"
        if first_ch == "-":
            # Allow genuine negative numbers (e.g. "-1250.50" or "-1,250.50") while escaping formulas like "-SUM(...)"
            num_probe = stripped[1:].replace(",", "").strip()
            try:
                float(num_probe)
                return val
            except ValueError:
                return f"'{val}"
        return val

    safe_rows = [
        {k: _sanitize_spreadsheet_cell(v) for k, v in r.items()}
        for r in rows
    ]

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    if fmt == "XLSX":
        import pandas as pd
        df_out = pd.DataFrame(safe_rows, columns=headers)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name="Reconciliation_Export")
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="reconciliation_export_{timestamp}.xlsx"'}
        )
    else:
        str_buf = io.StringIO()
        # Write UTF-8 BOM so Excel opens Arabic characters cleanly
        str_buf.write('\ufeff')
        writer = csv.DictWriter(str_buf, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in safe_rows:
            writer.writerow(r)
        byte_buf = io.BytesIO(str_buf.getvalue().encode("utf-8"))
        return StreamingResponse(
            byte_buf,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="reconciliation_export_{timestamp}.csv"'}
        )


@router.get("/admin-overview")
def get_reconciliation_admin_overview(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Executive Bank Reconciliation Oversight & Progress Telemetry for Corporate Admin.
    Provides real-time visibility into statement ingestion, balance continuity integrity,
    matching & GL classification completion, aged unreconciled backlogs, variance write-offs,
    automation rule health, and team maker productivity strictly from factual DB records.
    """
    from datetime import date as date_cls, timedelta
    from sqlalchemy import func as sqla_func
    from sqlalchemy.orm import joinedload
    from app.models.models import User, Bank

    cid = current_user.customer_id
    today = date_cls.today()
    d7_ago = today - timedelta(days=7)
    d30_ago = today - timedelta(days=30)

    # 1. Load all non-deleted BankStatements for this company
    statements: List[BankStatement] = (
        db.query(BankStatement)
        .options(joinedload(BankStatement.bank), joinedload(BankStatement.currency))
        .filter(BankStatement.company_id == cid, BankStatement.is_deleted == False)
        .order_by(BankStatement.created_at.desc())
        .all()
    )
    stmt_ids = [s.id for s in statements]

    # 2. Load all non-deleted BankTransactions across those statements
    txns: List[BankTransaction] = []
    if stmt_ids:
        txns = (
            db.query(BankTransaction)
            .filter(
                BankTransaction.statement_id.in_(stmt_ids),
                BankTransaction.is_deleted == False,
            )
            .all()
        )

    # 3. Load ERP Ledger records
    erp_records: List[InternalLedgerRecord] = (
        db.query(InternalLedgerRecord)
        .filter(
            InternalLedgerRecord.company_id == cid,
            InternalLedgerRecord.is_deleted == False,
        )
        .all()
    )

    # 4. Load Matches across those transactions
    txn_ids = [t.id for t in txns]
    matches: List[ReconciliationMatch] = []
    if txn_ids:
        matches = (
            db.query(ReconciliationMatch)
            .filter(
                ReconciliationMatch.bank_txn_id.in_(txn_ids),
                ReconciliationMatch.is_deleted == False,
            )
            .all()
        )

    # 5. Rules, Tolerance Rules & Counterparties
    rules: List[ClassificationRule] = (
        db.query(ClassificationRule)
        .filter(ClassificationRule.company_id == cid, ClassificationRule.is_deleted == False)
        .all()
    )
    tolerance_rules_count = (
        db.query(sqla_func.count(MatchingToleranceRule.id))
        .filter(MatchingToleranceRule.company_id == cid, MatchingToleranceRule.is_deleted == False)
        .scalar()
        or 0
    )
    counterparties_count = (
        db.query(sqla_func.count(Counterparty.id))
        .filter(Counterparty.company_id == cid, Counterparty.is_deleted == False)
        .scalar()
        or 0
    )

    # User lookup cache for team activity & uploader attribution
    tenant_users = db.query(User).filter(User.customer_id == cid).all()
    user_map = {u.id: {"email": u.email, "role": u.role.value if hasattr(u.role, "value") else str(u.role)} for u in tenant_users}

    # --- Aggregate Statement & Transaction Metrics ---
    txns_by_stmt: dict = {}
    for t in txns:
        txns_by_stmt.setdefault(t.statement_id, []).append(t)

    total_txns = len(txns)
    reconciled_txns = 0
    classified_txns = 0
    ready_for_export = 0
    exported_txns = 0
    manual_overrides_count = 0
    dup_or_rev_count = 0
    aged_7d_count = 0
    aged_30d_count = 0

    unreconciled_by_ccy: dict = {}
    source_counts: dict = {}

    for t in txns:
        ccy = (t.currency or "EGP").upper()
        if t.is_reconciled:
            reconciled_txns += 1
        else:
            b_date = t.booking_date.date() if isinstance(t.booking_date, datetime) else t.booking_date
            if b_date:
                if b_date <= d7_ago:
                    aged_7d_count += 1
                if b_date <= d30_ago:
                    aged_30d_count += 1
            ub = unreconciled_by_ccy.setdefault(ccy, {"currency": ccy, "unreconciled_count": 0, "debit_total": 0.0, "credit_total": 0.0})
            ub["unreconciled_count"] += 1
            ub["debit_total"] += float(t.debit_amount or 0)
            ub["credit_total"] += float(t.credit_amount or 0)

        if t.is_classified:
            classified_txns += 1
            src = (t.classification_source or "MANUAL").upper()
            source_counts[src] = source_counts.get(src, 0) + 1

        if t.is_exported:
            exported_txns += 1
        elif t.is_reconciled and t.is_classified:
            ready_for_export += 1

        if t.manual_override:
            manual_overrides_count += 1
        if t.is_duplicate or t.is_reversal:
            dup_or_rev_count += 1

    unreconciled_txns = total_txns - reconciled_txns
    unclassified_txns = total_txns - classified_txns
    reconciled_pct = round((reconciled_txns / total_txns) * 100.0, 1) if total_txns > 0 else 0.0
    classified_pct = round((classified_txns / total_txns) * 100.0, 1) if total_txns > 0 else 0.0

    # Continuity Warnings & Recent Statements
    continuity_warnings = []
    recent_statements_out = []
    bank_agg: dict = {}

    for s in statements:
        s_txns = txns_by_stmt.get(s.id, [])
        s_tot = len(s_txns)
        s_rec = sum(1 for x in s_txns if x.is_reconciled)
        s_cls = sum(1 for x in s_txns if x.is_classified)
        b_name = s.bank.name if s.bank else f"Bank #{s.bank_id}"
        b_short = (s.bank.short_name if s.bank and s.bank.short_name else b_name)
        s_ccy = (
            s.currency.iso_code.upper()
            if s.currency and s.currency.iso_code
            else (s_txns[0].currency.upper() if s_txns and s_txns[0].currency else "EGP")
        )
        has_cont_warn = (
            (s.balance_continuity_status or "").upper() == "CONTINUITY_WARNING"
            or (s.status or "").upper() == "CONTINUITY_WARNING"
            or abs(float(s.balance_variance_amount or 0)) > 0.01
        )
        if has_cont_warn:
            continuity_warnings.append({
                "statement_id": s.id,
                "bank_name": b_name,
                "file_name": s.file_name,
                "start_date": s.statement_start_date.isoformat() if s.statement_start_date else None,
                "end_date": s.statement_end_date.isoformat() if s.statement_end_date else None,
                "variance_amount": round(float(s.balance_variance_amount or 0), 2),
                "currency": s_ccy,
            })

        uploader = user_map.get(s.created_by, {}).get("email") if s.created_by else "System"
        if len(recent_statements_out) < 15:
            recent_statements_out.append({
                "id": s.id,
                "file_name": s.file_name,
                "bank_id": s.bank_id,
                "bank_name": b_name,
                "bank_short": b_short,
                "start_date": s.statement_start_date.isoformat() if s.statement_start_date else None,
                "end_date": s.statement_end_date.isoformat() if s.statement_end_date else None,
                "opening_balance": round(float(s.opening_balance or 0), 2),
                "closing_balance": round(float(s.closing_balance or 0), 2),
                "currency": s_ccy,
                "status": s.status,
                "has_continuity_warning": has_cont_warn,
                "uploaded_by": uploader,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "total_txns": s_tot,
                "reconciled_txns": s_rec,
                "reconciled_pct": round((s_rec / s_tot) * 100.0, 1) if s_tot > 0 else 0.0,
                "classified_txns": s_cls,
                "classified_pct": round((s_cls / s_tot) * 100.0, 1) if s_tot > 0 else 0.0,
                "brs_signed_at": s.brs_signed_at.isoformat() if getattr(s, "brs_signed_at", None) else None,
                "brs_worm_hash": getattr(s, "brs_worm_hash", None),
            })

        bg = bank_agg.setdefault(s.bank_id, {
            "bank_id": s.bank_id,
            "bank_name": b_name,
            "bank_short": b_short,
            "statements_count": 0,
            "latest_end_date": None,
            "latest_closing_balance": 0.0,
            "currency": s_ccy,
            "total_txns": 0,
            "reconciled_txns": 0,
            "classified_txns": 0,
            "has_continuity_warning": False,
        })
        bg["statements_count"] += 1
        bg["total_txns"] += s_tot
        bg["reconciled_txns"] += s_rec
        bg["classified_txns"] += s_cls
        if has_cont_warn:
            bg["has_continuity_warning"] = True
        end_iso = s.statement_end_date.isoformat() if s.statement_end_date else None
        if end_iso and (bg["latest_end_date"] is None or end_iso > bg["latest_end_date"]):
            bg["latest_end_date"] = end_iso
            bg["latest_closing_balance"] = round(float(s.closing_balance or 0), 2)
            bg["currency"] = s_ccy

    bank_progress = []
    for bg in sorted(bank_agg.values(), key=lambda x: x["total_txns"], reverse=True):
        btot = bg["total_txns"]
        brec = bg["reconciled_txns"]
        bcls = bg["classified_txns"]
        bg["unreconciled_txns"] = btot - brec
        bg["unclassified_txns"] = btot - bcls
        bg["reconciled_pct"] = round((brec / btot) * 100.0, 1) if btot > 0 else 0.0
        bg["classified_pct"] = round((bcls / btot) * 100.0, 1) if btot > 0 else 0.0
        bank_progress.append(bg)

    # ERP Ledger Open vs Reconciled
    open_erp_count = sum(1 for e in erp_records if (e.status or "OPEN").upper() == "OPEN")
    reconciled_erp_count = len(erp_records) - open_erp_count
    erp_open_by_type: dict = {}
    for e in erp_records:
        if (e.status or "OPEN").upper() == "OPEN":
            rt = e.record_type or "OTHER"
            erp_open_by_type[rt] = erp_open_by_type.get(rt, 0) + 1

    # Variance Write-Offs across Matches
    variance_matches_count = 0
    variance_by_disp: dict = {}
    for m in matches:
        v = float(m.variance_amount or 0)
        if v > 0.005:
            variance_matches_count += 1
            disp = m.variance_disposition or "UNSPECIFIED"
            variance_by_disp[disp] = round(variance_by_disp.get(disp, 0.0) + v, 2)

    # Team Maker Activity
    team_stats: dict = {}

    def _touch_actor(uid, ts, field):
        if not uid:
            return
        uinfo = user_map.get(uid, {"email": f"User #{uid}", "role": "end_user"})
        entry = team_stats.setdefault(uid, {
            "user_id": uid,
            "email": uinfo["email"],
            "role": uinfo["role"],
            "statements_uploaded": 0,
            "matches_created": 0,
            "erp_ingested": 0,
            "last_activity_at": None,
        })
        entry[field] += 1
        ts_iso = ts.isoformat() if isinstance(ts, datetime) else (str(ts) if ts else None)
        if ts_iso and (entry["last_activity_at"] is None or ts_iso > entry["last_activity_at"]):
            entry["last_activity_at"] = ts_iso

    for s in statements:
        _touch_actor(s.created_by, s.created_at, "statements_uploaded")
    for m in matches:
        _touch_actor(m.created_by, m.created_at, "matches_created")
    for e in erp_records:
        _touch_actor(e.created_by, e.created_at, "erp_ingested")

    team_activity = sorted(
        team_stats.values(),
        key=lambda x: (x["matches_created"] + x["statements_uploaded"] + x["erp_ingested"]),
        reverse=True,
    )

    # Automation & Rule Governance
    confirmed_rules_count = sum(1 for r in rules if (r.lifecycle_state or "").upper() == "AUTO_CONFIRMED" and r.is_active)
    suggested_rules_count = sum(1 for r in rules if (r.lifecycle_state or "").upper() in ("CANDIDATE", "SUGGESTED"))

    return {
        "as_of": datetime.utcnow().isoformat(),
        "kpis": {
            "total_statements": len(statements),
            "validated_statements": len(statements) - len(continuity_warnings),
            "continuity_warning_statements": len(continuity_warnings),
            "total_transactions": total_txns,
            "reconciled_transactions": reconciled_txns,
            "unreconciled_transactions": unreconciled_txns,
            "reconciliation_rate_pct": reconciled_pct,
            "classified_transactions": classified_txns,
            "unclassified_transactions": unclassified_txns,
            "classification_rate_pct": classified_pct,
            "ready_for_export_count": ready_for_export,
            "exported_count": exported_txns,
            "open_erp_records_count": open_erp_count,
            "reconciled_erp_records_count": reconciled_erp_count,
            "erp_open_by_type": erp_open_by_type,
            "unreconciled_by_currency": [
                {
                    "currency": v["currency"],
                    "unreconciled_count": v["unreconciled_count"],
                    "debit_total": round(v["debit_total"], 2),
                    "credit_total": round(v["credit_total"], 2),
                }
                for v in unreconciled_by_ccy.values()
            ],
        },
        "exceptions": {
            "continuity_warnings": continuity_warnings,
            "aged_unreconciled_7d_count": aged_7d_count,
            "aged_unreconciled_30d_count": aged_30d_count,
            "variance_matches_count": variance_matches_count,
            "variance_by_disposition": variance_by_disp,
            "manual_overrides_count": manual_overrides_count,
            "duplicates_or_reversals_count": dup_or_rev_count,
            "suggested_rules_pending_count": suggested_rules_count,
        },
        "bank_progress": bank_progress,
        "recent_statements": recent_statements_out,
        "team_activity": team_activity,
        "automation_health": {
            "active_confirmed_rules": confirmed_rules_count,
            "suggested_rules_pending": suggested_rules_count,
            "tolerance_rules_count": tolerance_rules_count,
            "counterparties_count": counterparties_count,
            "classification_sources": source_counts,
        },
    }


# ═══════════════════════════════════════════════════════════════════
#  PHASE 2: MONTH-END BRS AUDIT CERTIFICATE & DEPARTMENTAL ACTION PACKS
# ═══════════════════════════════════════════════════════════════════

@router.get("/statements/{statement_id}/brs")
def get_statement_brs_report(
    statement_id: int,
    erp_book_balance: Optional[Decimal] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Computes the formal Month-End Bank Reconciliation Statement (Proof of Cash) bridge
    for a specific bank statement, including itemized Deposits in Transit, Unpresented Payments,
    and Unbooked Bank Statement Credits/Debits.
    """
    from app.services.reconciliation_brs_and_packs_service import reconciliation_brs_service
    return reconciliation_brs_service.build_statement_brs_report(
        db=db,
        customer_id=current_user.customer_id,
        statement_id=statement_id,
        erp_book_balance_override=erp_book_balance,
    )


@router.post("/statements/{statement_id}/brs/sign-off")
def sign_off_statement_brs(
    statement_id: int,
    req: BRSSignOffRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Formally signs off & locks a Bank Reconciliation Statement, stamping a SHA-256
    WORM Audit Chain hash onto the statement record.
    """
    from app.services.reconciliation_brs_and_packs_service import reconciliation_brs_service
    return reconciliation_brs_service.sign_off_statement_brs(
        db=db,
        customer_id=current_user.customer_id,
        statement_id=statement_id,
        user_id=current_user.user_id,
        erp_book_closing_balance=req.erp_book_closing_balance,
        notes=req.notes,
        lock_transactions=req.lock_transactions,
    )


@router.post("/statements/{statement_id}/brs/reopen")
def reopen_statement_brs(
    statement_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """
    Corporate Admin governance action to reopen a signed-off BRS statement for adjustment.
    """
    from app.services.reconciliation_brs_and_packs_service import reconciliation_brs_service
    return reconciliation_brs_service.reopen_statement_brs(
        db=db,
        customer_id=current_user.customer_id,
        statement_id=statement_id,
        user_id=current_user.user_id,
    )


@router.get("/statements/{statement_id}/brs/export")
async def export_statement_brs_certificate(
    statement_id: int,
    format: str = "PDF",
    erp_book_balance: Optional[Decimal] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Exports the Official Month-End Bank Reconciliation Statement (BRS) as either:
    - `PDF`: Multi-page A4 Audit Certificate with signature blocks & WORM hash
    - `XLSX`: 5-sheet Audit Schedule Workbook
    """
    from app.services.reconciliation_brs_and_packs_service import reconciliation_brs_service
    brs = reconciliation_brs_service.build_statement_brs_report(
        db=db,
        customer_id=current_user.customer_id,
        statement_id=statement_id,
        erp_book_balance_override=erp_book_balance,
    )
    fmt = (format or "PDF").strip().upper()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if fmt == "XLSX":
        xlsx_bytes = reconciliation_brs_service.render_brs_excel_bytes(brs)
        return StreamingResponse(
            io.BytesIO(xlsx_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="BRS_Statement_{statement_id}_{ts}.xlsx"'},
        )

    html_doc = reconciliation_brs_service.render_brs_certificate_html(brs)
    from app.core.document_generator import generate_multipage_pdf_from_html
    from app.core.trial_guard import is_active_trial_context, stamp_trial_pdf_bytes_watermark

    pdf_bytes = await generate_multipage_pdf_from_html(html_doc, filename_hint=f"brs_{statement_id}")
    if not pdf_bytes:
        raise HTTPException(status_code=500, detail="Failed to render BRS PDF certificate")
    if is_active_trial_context():
        pdf_bytes = stamp_trial_pdf_bytes_watermark(pdf_bytes, force_trial=True)

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="BRS_Certificate_{statement_id}_{ts}.pdf"'},
    )


@router.get("/action-packs")
def get_departmental_action_packs(
    pack_key: Optional[str] = None,
    bank_id: Optional[int] = None,
    statement_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Returns summary KPIs across all 4 Departmental Exception & Action Packs
    plus live preview rows for the selected `pack_key`.
    """
    from app.services.reconciliation_brs_and_packs_service import reconciliation_brs_service
    res = reconciliation_brs_service.build_action_packs(
        db=db,
        customer_id=current_user.customer_id,
        selected_pack=pack_key,
        bank_id=bank_id,
        statement_id=statement_id,
    )
    return {
        "packs": res["packs"],
        "selected_pack": res["selected_pack"],
        "selected_meta": res["selected_meta"],
        "headers": res["headers"],
        "preview_rows": res["rows"][:100],
        "total_rows": len(res["rows"]),
    }


@router.get("/action-packs/{pack_key}/export")
async def export_departmental_action_pack(
    pack_key: str,
    format: str = "XLSX",
    bank_id: Optional[int] = None,
    statement_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Streams a ready-to-share Departmental Exception / Action Pack in XLSX, CSV, or PDF format.
    """
    from app.services.reconciliation_brs_and_packs_service import reconciliation_brs_service
    pack_data = reconciliation_brs_service.build_action_packs(
        db=db,
        customer_id=current_user.customer_id,
        selected_pack=pack_key,
        bank_id=bank_id,
        statement_id=statement_id,
    )
    fmt = (format or "XLSX").strip().upper()
    if fmt == "PDF":
        from app.core.document_generator import generate_multipage_pdf_from_html
        from app.core.trial_guard import is_active_trial_context, stamp_trial_pdf_bytes_watermark

        html_doc = reconciliation_brs_service.render_action_pack_html(pack_data)
        pdf_bytes = await generate_multipage_pdf_from_html(html_doc, filename_hint=f"pack_{pack_key.lower()}")
        if not pdf_bytes:
            raise HTTPException(status_code=500, detail="Failed to render Action Pack PDF")
        if is_active_trial_context():
            pdf_bytes = stamp_trial_pdf_bytes_watermark(pdf_bytes, force_trial=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="grow_{pack_key.lower()}_{ts}.pdf"'},
        )

    raw_bytes, media_type, fname = reconciliation_brs_service.export_action_pack_bytes(pack_data, file_format=fmt)
    return StreamingResponse(
        io.BytesIO(raw_bytes),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ==============================================================================
# PHASE 3: CFO DIRECT CASH FLOW, 7/14/30D LIQUIDITY FORECAST & BANK SCORECARD
# ==============================================================================

@router.get("/cash-intelligence")
def get_cfo_cash_intelligence_dashboard(
    currency: Optional[str] = None,
    bank_id: Optional[int] = None,
    days_back: Optional[int] = None,
    minimum_cash_buffer: float = 0.0,
    benchmark_annual_rate_pct: float = 20.0,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Returns Phase 3 CFO Treasury Intelligence:
    1. IAS 7 Direct-Method Cash Flow (isolating cross-bank sweeps & ghost error reversals)
    2. 7 / 14 / 30-Day Rolling Cash Liquidity Forecast with learned counterparty lag intelligence
    3. Bank Fee Leakage (direct + embedded deductions in bps) & Value-Date Clearing Float Scorecard
    """
    from app.services.reconciliation_cash_intelligence_service import (
        reconciliation_cash_intelligence_service,
    )
    return reconciliation_cash_intelligence_service.build_cash_intelligence_report(
        db=db,
        customer_id=current_user.customer_id,
        currency=currency,
        bank_id=bank_id,
        days_back=days_back,
        minimum_cash_buffer=minimum_cash_buffer,
        benchmark_annual_rate_pct=benchmark_annual_rate_pct,
    )


@router.get("/cash-intelligence/export")
async def export_cfo_cash_intelligence_pack(
    format: str = "PDF",
    currency: Optional[str] = None,
    bank_id: Optional[int] = None,
    days_back: Optional[int] = None,
    minimum_cash_buffer: float = 0.0,
    benchmark_annual_rate_pct: float = 20.0,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Streams the CFO Cash Flow, Rolling 30-Day Forecast & Bank Scorecard Pack in:
    - `PDF`: Multi-page A4 Executive Board Briefing (WeasyPrint)
    - `XLSX`: 4-sheet Treasury Analytics Workbook
    """
    from app.services.reconciliation_cash_intelligence_service import (
        reconciliation_cash_intelligence_service,
    )
    report = reconciliation_cash_intelligence_service.build_cash_intelligence_report(
        db=db,
        customer_id=current_user.customer_id,
        currency=currency,
        bank_id=bank_id,
        days_back=days_back,
        minimum_cash_buffer=minimum_cash_buffer,
        benchmark_annual_rate_pct=benchmark_annual_rate_pct,
    )
    fmt = (format or "PDF").strip().upper()
    ccy = report["selected_currency"]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if fmt == "XLSX":
        xlsx_bytes = reconciliation_cash_intelligence_service.render_excel_bytes(report)
        return StreamingResponse(
            io.BytesIO(xlsx_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="CFO_Cash_Intelligence_{ccy}_{ts}.xlsx"'
            },
        )

    from app.core.document_generator import generate_multipage_pdf_from_html
    from app.core.trial_guard import is_active_trial_context, stamp_trial_pdf_bytes_watermark

    html_doc = reconciliation_cash_intelligence_service.render_pdf_html(report)
    pdf_bytes = await generate_multipage_pdf_from_html(
        html_doc, filename_hint=f"cfo_cash_intel_{ccy.lower()}"
    )
    if not pdf_bytes:
        raise HTTPException(status_code=500, detail="Failed to render CFO Cash Intelligence PDF")
    if is_active_trial_context():
        pdf_bytes = stamp_trial_pdf_bytes_watermark(pdf_bytes, force_trial=True)

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="CFO_Cash_Intelligence_{ccy}_{ts}.pdf"'
        },
    )


# ==============================================================================
# PHASE 4: CLOSED-LOOP TREASURY BRIDGE (LG FACILITY TARIFFS + RFQ DEAL SETTLEMENTS)
# ==============================================================================

@router.get("/treasury-bridge")
def get_closed_loop_treasury_bridge(
    currency: Optional[str] = None,
    bank_id: Optional[int] = None,
    minimum_cash_buffer: float = 0.0,
    benchmark_annual_rate_pct: float = 20.0,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Phase 4 Closed-Loop Cross-Module Treasury Audit & Opportunity Engine:
    1. Audits actual bank LG commission & cash-margin debits against agreed Facility tariffs
       and flags unregistered Shadow LGs.
    2. Verifies bank statement settlement of awarded FX Spot & T-Bill RFQ deals.
    3. Generates 1-click pre-filled T-Bill / Money Market RFQ prompts from verified 30d idle cash.
    """
    from app.services.reconciliation_treasury_bridge_service import (
        reconciliation_treasury_bridge_service,
    )
    return reconciliation_treasury_bridge_service.build_treasury_bridge_report(
        db=db,
        customer_id=current_user.customer_id,
        currency=currency,
        bank_id=bank_id,
        minimum_cash_buffer=minimum_cash_buffer,
        benchmark_annual_rate_pct=benchmark_annual_rate_pct,
    )


@router.get("/treasury-bridge/export")
async def export_closed_loop_treasury_bridge(
    format: str = "PDF",
    currency: Optional[str] = None,
    bank_id: Optional[int] = None,
    minimum_cash_buffer: float = 0.0,
    benchmark_annual_rate_pct: float = 20.0,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_reconciliation_user_context),
):
    """
    Streams the Closed-Loop Treasury Audit & Bank Claim Pack in:
    - `PDF`: Multi-page Landscape A4 Bank Claim & Settlement Audit Pack (WeasyPrint)
    - `XLSX`: 3-sheet Workbook (LG Tariff Audit, Shadow LGs, RFQ Deal Settlement Audit)
    """
    from app.services.reconciliation_treasury_bridge_service import (
        reconciliation_treasury_bridge_service,
    )
    report = reconciliation_treasury_bridge_service.build_treasury_bridge_report(
        db=db,
        customer_id=current_user.customer_id,
        currency=currency,
        bank_id=bank_id,
        minimum_cash_buffer=minimum_cash_buffer,
        benchmark_annual_rate_pct=benchmark_annual_rate_pct,
    )
    fmt = (format or "PDF").strip().upper()
    ccy = report["selected_currency"]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if fmt == "XLSX":
        xlsx_bytes = reconciliation_treasury_bridge_service.render_excel_bytes(report)
        return StreamingResponse(
            io.BytesIO(xlsx_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="Treasury_Bridge_Claim_Pack_{ccy}_{ts}.xlsx"'
            },
        )

    from app.core.document_generator import generate_multipage_pdf_from_html
    from app.core.trial_guard import is_active_trial_context, stamp_trial_pdf_bytes_watermark

    html_doc = reconciliation_treasury_bridge_service.render_pdf_html(report)
    pdf_bytes = await generate_multipage_pdf_from_html(
        html_doc, filename_hint=f"treasury_bridge_{ccy.lower()}"
    )
    if not pdf_bytes:
        raise HTTPException(status_code=500, detail="Failed to render Treasury Bridge Claim PDF")
    if is_active_trial_context():
        pdf_bytes = stamp_trial_pdf_bytes_watermark(pdf_bytes, force_trial=True)

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="Treasury_Bridge_Claim_Pack_{ccy}_{ts}.pdf"'
        },
    )





