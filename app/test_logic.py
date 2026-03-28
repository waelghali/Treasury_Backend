from difflib import SequenceMatcher
from datetime import date
import json

def test_sim(reference_type, reference_number, beneficiary_name, amount, lg_type_id, requested_expiry_date):
    
    # Mock issued LG
    item = {
        "type": "issued_lg",
        "linked_req": type("Req", (), {"reference_type": "PO", "reference_number": "PO-123", "lg_type_id": 1})(),
        "ref_num": "LG-999",
        "id": 999,
        "ben_name": "Suez Canal Authority",
        "amt": 100000.0,
        "currency": "USD",
        "issue_date": date(2025,1,1),
        "expiry_date": date(2026,12,31),
        "status": "ISSUED"
    }

    score = 0.0
    breakdown = {}
    total_possible = 100.0
    linked_req = item["linked_req"]

    # 1. Reference
    if reference_type and reference_number:
        if (linked_req and linked_req.reference_type == reference_type and 
                linked_req.reference_number == reference_number):
            score += 30
            breakdown["reference"] = {"matched": True, "score": 30}
        else:
            breakdown["reference"] = {"matched": False, "score": 0}
    else:
        total_possible -= 30
        breakdown["reference"] = {"matched": None, "score": 0, "ignored": True}
        
    # 2. Beneficiary
    if beneficiary_name and item["ben_name"]:
        ratio = SequenceMatcher(
            None, 
            beneficiary_name.lower().strip(),
            item["ben_name"].lower().strip()
        ).ratio()
        if ratio >= 0.8:
            ns = round(ratio * 25, 1)
            score += ns
            breakdown["beneficiary"] = {"matched": True, "score": ns, "similarity": round(ratio * 100)}
        else:
            breakdown["beneficiary"] = {"matched": False, "score": 0, "similarity": round(ratio * 100)}
    else:
        total_possible -= 25
        breakdown["beneficiary"] = {"matched": None, "score": 0, "ignored": True}
        
    # 3. Amount
    if amount and item["amt"]:
        req_amt = float(amount)
        item_amt = item["amt"]
        if req_amt > 0 and item_amt > 0:
            diff_pct = abs(req_amt - item_amt) / max(req_amt, item_amt)
            if diff_pct <= 0.05:
                ams = round((1 - diff_pct / 0.05) * 20, 1)
                score += ams
                breakdown["amount"] = {"matched": True, "score": ams, "lg_amount": str(item_amt)}
            else:
                breakdown["amount"] = {"matched": False, "score": 0, "lg_amount": str(item_amt)}
        else:
            total_possible -= 20
            breakdown["amount"] = {"matched": None, "score": 0, "ignored": True}
    else:
        total_possible -= 20
        breakdown["amount"] = {"matched": None, "score": 0, "ignored": True}
        
    # 4. LG Type
    if lg_type_id and linked_req and hasattr(linked_req, 'lg_type_id') and linked_req.lg_type_id:
        if lg_type_id == linked_req.lg_type_id:
            score += 15
            breakdown["lg_type"] = {"matched": True, "score": 15}
        else:
            breakdown["lg_type"] = {"matched": False, "score": 0}
    else:
        total_possible -= 15
        breakdown["lg_type"] = {"matched": None, "score": 0, "ignored": True}
        
    # 5. Expiry
    if requested_expiry_date:
        if item["expiry_date"]:
            delta_days = abs((requested_expiry_date - item["expiry_date"]).days)
            if delta_days <= 30:
                es = round((1 - delta_days / 30) * 10, 1)
                score += es
                breakdown["expiry"] = {"matched": True, "score": es, "days_diff": delta_days}
            else:
                breakdown["expiry"] = {"matched": False, "score": 0, "days_diff": delta_days}
        else:
            breakdown["expiry"] = {"matched": False, "score": 0}
    else:
        total_possible -= 10
        breakdown["expiry"] = {"matched": None, "score": 0, "ignored": True}

    if total_possible > 0:
        final_score = round((score / total_possible) * 100, 1)
    else:
        final_score = 0

    print(json.dumps({
        "score": final_score,
        "raw_score": score,
        "total_possible": total_possible,
        "breakdown": breakdown
    }, indent=2))

# Fully exact data match except reference is not provided
print("TEST 1: Exact match on Beneficiary, Amount, Type, Expiry (Ref missing)")
test_sim(None, None, "Suez Canal Authority", 100000.0, 1, date(2026,12,31))

# Match where they DID provide a reference, but it DOESN'T match
print("\nTEST 2: Exact match on Ben, Amt, Type, Exp (Ref Mismatched)")
test_sim("PO", "PO-999_MISMATCHED", "Suez Canal Authority", 100000.0, 1, date(2026,12,31))

# Match where only Ben and Amount are provided but no type/expiry
print("\nTEST 3: Partial Data (Ben, Amt) Exact match")
test_sim(None, None, "Suez Canal Authority", 100000.0, None, None)
