from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import List, Optional
from decimal import Decimal

from app.database import get_db
from app.models.models_reconciliation_v2 import (
    BankStatement, BankTransaction, InternalLedgerRecord, ClassificationTaxonomy, Counterparty,
    ReconciliationMatch
)
# Fix: get_current_corporate_admin is often a proxy or defined in security
from app.api.v1.endpoints.corporate_admin import get_current_corporate_admin_context as get_current_corporate_admin
from app.schemas.schemas_reconciliation_v2 import (
    BankStatementOut, BankTransactionOut, 
    ClassificationRuleCreate, ClassificationRuleUpdate, ClassificationRuleOut,
    CounterpartyCreate, CounterpartyUpdate, CounterpartyOut,
    InternalLedgerRecordOut, TaxonomyNodeCreate, TaxonomyNodeUpdate,
    TaxonomyNodeOut, ClassifyTransactionRequest, ERPUploadResult,
    BulkClassifyRequest, BulkClearClassificationRequest, BulkOperationResult,
    MatchActionRequest, UnmatchActionRequest
)

from app.crud.crud_reconciliation_v2 import (
    crud_bank_statement, crud_bank_transaction, crud_classification_rule, crud_counterparty,
    crud_internal_ledger, crud_taxonomy
)
from app.services.bank_reconciliation_service import bank_reconcile_service
from app.core.security import TokenData

router = APIRouter()

@router.post("/statements/upload", response_model=BankStatementOut)
async def upload_bank_statement(
    bank_id: int = Form(...),
    opening_balance: Optional[Decimal] = Form(None),
    closing_balance: Optional[Decimal] = Form(None),
    start_date: Optional[str] = Form(None),
    end_date: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Ingests a bank statement file with optional metadata.
    Auto-detects values if not provided.
    """
    content = await file.read()
    file_type = file.filename.split('.')[-1]
    
    overrides = {
        "bank_id": bank_id,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "start_date": start_date,
        "end_date": end_date,
        "file_name": file.filename
    }
    
    # Use the smart ingestion service
    db_statement = bank_reconcile_service.process_ingestion(
        db, content, file_type, 
        company_id=current_user.customer_id, 
        user_id=current_user.user_id,
        overrides=overrides
    )
    
    return db_statement

@router.get("/statements", response_model=List[BankStatementOut])
def get_statements(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    return crud_bank_statement.get_by_customer(db, customer_id=current_user.customer_id)

@router.get("/erp-records", response_model=List[InternalLedgerRecordOut])
def get_erp_records(
    status: Optional[str] = None,
    limit: int = 200,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Returns simulated or integrated internal ERP ledger records (AP Bills, AR Invoices, LG Fees, Payroll).
    """
    return crud_internal_ledger.get_by_customer(db, customer_id=current_user.customer_id, status=status, limit=limit)


@router.get("/statements/{statement_id}/transactions", response_model=List[BankTransactionOut])
def get_statement_transactions(
    statement_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Soft-deletes a bank statement.
    """
    stmt = crud_bank_statement.get(db, id=statement_id)
    if not stmt or stmt.company_id != current_user.customer_id:
        raise HTTPException(status_code=404, detail="Statement not found")
        
    # Soft delete statement and its transactions
    from app.models.models_reconciliation_v2 import BankTransaction
    db.query(BankTransaction).filter(BankTransaction.statement_id == statement_id).update({"is_deleted": True})
    
    stmt.is_deleted = True
    db.commit()
    
    return {"message": "Statement deleted successfully"}

@router.post("/auto-match")
def run_global_auto_match(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Runs the auto-matching engine for a specific statement.
    """
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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

    # 4. Create reconciliation match records and link
    for b_txn in bank_txns:
        b_txn.is_reconciled = True
        for erp in erp_records:
            m = ReconciliationMatch(
                bank_txn_id=b_txn.id,
                source_type=erp.record_type or "ERP",
                source_record_id=erp.id,
                match_type=match_type,
                match_logic=req.type or "MANUAL",
                created_by=current_user.user_id
            )
            db.add(m)

    for erp in erp_records:
        erp.status = "RECONCILED"
        erp.matched_bank_txn_id = primary_bank_id

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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Runs the classification rules for a specific statement.
    """
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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

@router.get("/rules", response_model=List[ClassificationRuleOut])
def get_rules(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    return crud_classification_rule.get_by_customer(db, customer_id=current_user.customer_id)

@router.post("/rules", response_model=ClassificationRuleOut)
def create_rule(
    rule_in: ClassificationRuleCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Updates a taxonomy node (name, GL account, direction, description).
    """
    db_node = crud_taxonomy.get(db, id=node_id)
    if not db_node:
        raise HTTPException(status_code=404, detail="Taxonomy node not found")
    return crud_taxonomy.update_node(db, db_obj=db_node, node_in=node_in, user_id=current_user.user_id)

@router.delete("/taxonomy/{node_id}")
def delete_taxonomy_node(
    node_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Deactivates/soft-deletes a taxonomy node.
    """
    db_node = crud_taxonomy.get(db, id=node_id)
    if not db_node:
        raise HTTPException(status_code=404, detail="Taxonomy node not found")
    db_node.is_active = False
    db_node.is_deleted = True
    db_node.updated_by = current_user.user_id
    db.commit()
    return {"status": "success", "message": f"Taxonomy node #{node_id} deactivated."}

@router.post("/transactions/{txn_id}/classify", response_model=BankTransactionOut)
def inline_classify_transaction(
    txn_id: int,
    req: ClassifyTransactionRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Inline classification action:
    Classifies an unclassified transaction, optionally binds to counterparty,
    and submits anonymized pattern to the collaborative consensus brain.
    """
    txn = crud_bank_transaction.get(db, id=txn_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Bank transaction not found")
    
    # Apply category
    txn.classification_category = req.category
    txn.internal_category = req.gl_account or req.category
    txn.category = req.category
    if req.sub_category:
        txn.sub_category = req.sub_category
    txn.classification_source = "MANUAL"
    txn.classification_confidence = 100
    txn.is_classified = True

    # If learn flag is true, trigger learning loop
    if req.remember_for_counterparty:
        bank_reconcile_service.confirm_and_learn(
            db=db,
            transaction_id=txn.id,
            customer_id=current_user.customer_id,
            user_id=current_user.user_id,
            counterparty_name=req.counterparty_name,
            category=req.category,
            gl_account=req.gl_account
        )

    db.commit()
    db.refresh(txn)
    return txn


@router.post("/transactions/bulk-classify", response_model=BulkOperationResult)
def bulk_classify_transactions(
    req: BulkClassifyRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    current_user: TokenData = Depends(get_current_corporate_admin)
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
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin)
):
    """
    Retrieves internal ERP ledger records for dual-pane matching.
    """
    query = db.query(InternalLedgerRecord).filter(
        InternalLedgerRecord.company_id == current_user.customer_id
    )
    if is_reconciled is not None:
        query = query.filter(InternalLedgerRecord.is_reconciled == is_reconciled)
    if search:
        s = f"%{search}%"
        query = query.filter(
            (InternalLedgerRecord.reference_number.ilike(s)) |
            (InternalLedgerRecord.entity_name.ilike(s)) |
            (InternalLedgerRecord.gl_account.ilike(s))
        )
    return query.order_by(InternalLedgerRecord.record_date.desc()).offset(skip).limit(limit).all()



