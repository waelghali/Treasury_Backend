from app.database import SessionLocal
from app.services.dealer_achievement_service import DealerAchievementService

db = SessionLocal()
# Test individual dealer + bank desk combined
ind_res = DealerAchievementService.get_dealer_achievements(
    db, 
    dealer_email="waelghali79+cibe@gmail.com", 
    bank_id=1, 
    bank_name="Commercial International Bank"
)
print("Mode:", ind_res.get("mode"))
print("Dealer Email:", ind_res.get("dealer_email"))
print("Dealer Tier:", ind_res.get("dealer_tier"))

desk_res = DealerAchievementService.get_dealer_achievements(
    db,
    dealer_email=None,
    bank_id=1,
    bank_name="Commercial International Bank"
)
print("Desk Tier:", desk_res.get("dealer_tier"))
print("Desk Won:", desk_res.get("personal_bests", {}).get("total_deals_won"))

db.close()
