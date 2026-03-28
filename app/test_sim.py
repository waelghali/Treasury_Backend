import os
import sys
from datetime import date
# Set paths for app.db etc
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal

db = SessionLocal()

print("Testing similarity...")
try:
    from app.services.issuance_service import issuance_service
    # 1. Let's find a Draft request to act as the "user input"
    from app.models.models_issuance import IssuanceRequest
    req = db.query(IssuanceRequest).filter(IssuanceRequest.id == 138).first()
    if not req:
        print("Request 138 not found")
    else:
        print(f"Request 138: Ben={req.beneficiary_name}, Amt={req.amount}, Exp={req.requested_expiry_date}, Type={req.lg_type_id}")
        
        # 2. Let's find all active requests for this customer to see what we are comparing against
        active = db.query(IssuanceRequest).filter(
            IssuanceRequest.customer_id == req.customer_id,
            IssuanceRequest.status.notin_(["ISSUED", "REJECTED_INTERNAL", "REJECTED_BANK", "CANCELLED", "DRAFT"])
        ).all()
        print(f"Found {len(active)} active requests for comparison:")
        for a in active:
            print(f" - {a.id}: Ben={a.beneficiary_name}, Amt={a.amount}, Exp={a.requested_expiry_date}, Type={a.lg_type_id}")

        # 3. Call get_similarity_matches directly using the exact same fields as req 138!
        # If it is missing data in DB, we'll manually specify "suez canal authority".
        print("\n\n-- SIMILARITY MATCH --")
        matches = issuance_service.get_similarity_matches(
            db=db,
            customer_id=req.customer_id,
            reference_type=None,
            reference_number=None,
            beneficiary_name=req.beneficiary_name or "Suez Canal Authority",
            amount=req.amount or 100000.0,
            lg_type_id=req.lg_type_id or 1,
            requested_expiry_date=req.requested_expiry_date or date(2027, 12, 31),
            exclude_request_id=138
        )
        import json
        print(json.dumps(matches, indent=2, default=str))

except Exception as e:
    import traceback
    traceback.print_exc()
finally:
    db.close()
