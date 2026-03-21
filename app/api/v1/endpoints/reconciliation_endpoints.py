from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import List, Optional
from decimal import Decimal

from app.database import get_db
# Fix: get_current_corporate_admin is often a proxy or defined in security
from app.api.v1.endpoints.corporate_admin import get_current_corporate_admin_context as get_current_corporate_admin
from app.schemas.schemas_reconciliation_v2 import (
    BankStatementOut, BankTransactionOut, 
    ClassificationRuleCreate, ClassificationRuleUpdate, ClassificationRuleOut
)
from app.crud.crud_reconciliation_v2 import (
    crud_bank_statement, crud_bank_transaction, crud_classification_rule
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
