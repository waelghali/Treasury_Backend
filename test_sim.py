import sys
import json
from datetime import date
sys.path.insert(0, 'c:/Grow')

from app.db.session import SessionLocal
from app.services.issuance_service import issuance_service
import app.models.models_issuance as models

db = SessionLocal()
try:
    req = db.query(models.IssuanceRequest).filter_by(id=138).first()
    print(f"Req 138: {req.beneficiary_name}, {req.amount}, {req.lg_type_id}, {req.requested_expiry_date}")
    
    res = issuance_service.get_similarity_matches(
        db, 
        req.customer_id, 
        None, 
        None, 
        req.beneficiary_name, 
        float(req.amount or 0), 
        req.lg_type_id, 
        req.requested_expiry_date, 
        138
    )
    print(json.dumps(res, indent=2, default=str))
finally:
    db.close()
