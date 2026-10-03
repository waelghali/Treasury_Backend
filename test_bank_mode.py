from app.database import SessionLocal
from app.services.dealer_achievement_service import DealerAchievementService

db = SessionLocal()
res = DealerAchievementService.get_dealer_achievements(db, bank_id=1, bank_name='Commercial International Bank')
print('Desk Mode:', res.get('mode'))
print('Desk Tier:', res.get('dealer_tier'))
print('Desk Perk:', res.get('dealer_perk'))
print('Desk Deals Won:', res.get('personal_bests', {}).get('total_deals_won'))
print('Desk Volume:', res.get('personal_bests', {}).get('total_volume_won_usd'))
print('Desk Trophies earned:', res.get('earned_trophy_count'))
for t in res.get('trophies', []):
    print(f"  Trophy: {t['id']} -> {t['current_tier']} ({t['current_value']}/{t['target_value']} {t['unit']})")

db.close()
