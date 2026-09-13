# scratch/test_custody_document_ai.py
import sys
import os
from datetime import date

# Ensure root is in path
sys.path.insert(0, r"c:\Grow")

from app.services.document_verification_service import document_verification_service, _check_bank_name_match, _normalize_date

def run_tests():
    print("=== Testing Document Verification Service ===")

    # Test 1: Bank Name Matching
    print("\n--- Test 1: Bank Name Fuzzy Matching ---")
    assert _check_bank_name_match("Commercial International Bank - Egypt", "CIB", "CIB") == True
    assert _check_bank_name_match("البنك التجاري الدولي - فرع مصدق", "Commercial International Bank", "CIB") == True
    assert _check_bank_name_match("National Bank of Egypt", "National Bank of Egypt", "NBE") == True
    assert _check_bank_name_match("بنك مصر", "Banque Misr", "BM") == True
    assert _check_bank_name_match("بنك مصر", "Commercial International Bank", "CIB") == False
    print("✓ Bank matching tests passed!")

    # Test 2: Date Normalization
    print("\n--- Test 2: Date Normalization ---")
    assert _normalize_date("2026-09-12") == "2026-09-12"
    assert _normalize_date("12/09/2026") == "2026-09-12"
    assert _normalize_date("12-09-2026") == "2026-09-12"
    print("✓ Date normalization tests passed!")

    # Test 3: Delivery Evidence - Valid Stamped Corporate Letter
    print("\n--- Test 3: Delivery Evidence (Valid Stamped Letter) ---")
    stamped_letter_text = """
    شركة جرو للتطوير والبرمجيات
    السيد مدير البنك التجاري الدولي (CIB) - فرع المهندسين
    تحية طيبة وبعد،
    الموضوع: طلب مد سريان خطاب ضمان رقم LG-2026-0041
    برجاء التكرم بمد سريان خطاب الضمان المذكور أعلاه حتى 31-12-2026.
    
    [خاتم وارد البنك التجاري الدولي]
    فرع المهندسين
    استلام الفرع بتاريخ: 12-09-2026
    الوقت: 11:30 صباحاً
    """
    res = document_verification_service.verify_delivery_evidence(
        raw_text=stamped_letter_text,
        expected_bank_name="Commercial International Bank",
        expected_bank_short_name="CIB",
        instruction_issue_date=date(2026, 9, 10),
        instruction_serial="INST-EXT-001",
        lg_number="LG-2026-0041"
    )
    print(f"Status: {res['status']}")
    print(f"Matches: {res['matches']}")
    print(f"Mismatches: {res['mismatches']}")
    assert res['status'] == 'verified'
    assert len(res['mismatches']) == 0
    print("✓ Valid delivery evidence verified successfully!")

    # Test 4: Delivery Evidence - Chronology Violation (Stamp date earlier than instruction date)
    print("\n--- Test 4: Delivery Evidence (Chronology Violation) ---")
    early_stamp_text = """
    السيد مدير CIB
    طلب تمديد خطاب الضمان رقم LG-2026-0041
    [خاتم وارد CIB]
    وارد الفرع بتاريخ 05-09-2026
    """
    res_early = document_verification_service.verify_delivery_evidence(
        raw_text=early_stamp_text,
        expected_bank_name="Commercial International Bank",
        expected_bank_short_name="CIB",
        instruction_issue_date=date(2026, 9, 10),
        instruction_serial="INST-EXT-001",
        lg_number="LG-2026-0041"
    )
    print(f"Status: {res_early['status']}")
    print(f"Mismatches: {res_early['mismatches']}")
    assert res_early['status'] == 'mismatch'
    assert any("Chronology conflict" in m for m in res_early['mismatches'])
    print("✓ Chronology violation detected correctly!")

    # Test 5: Delivery Evidence - Bank Mismatch
    print("\n--- Test 5: Delivery Evidence (Bank Mismatch) ---")
    wrong_bank_text = """
    [خاتم وارد بنك مصر - فرع الأوبرا]
    استلام بتاريخ: 12-09-2026
    """
    res_bank_mismatch = document_verification_service.verify_delivery_evidence(
        raw_text=wrong_bank_text,
        expected_bank_name="Commercial International Bank",
        expected_bank_short_name="CIB",
        instruction_issue_date=date(2026, 9, 10),
        instruction_serial="INST-EXT-001",
        lg_number="LG-2026-0041"
    )
    print(f"Status: {res_bank_mismatch['status']}")
    print(f"Mismatches: {res_bank_mismatch['mismatches']}")
    assert res_bank_mismatch['status'] == 'mismatch'
    assert any("Bank mismatch" in m for m in res_bank_mismatch['mismatches'])
    print("✓ Bank mismatch detected correctly!")

    # Test 6: Bank Reply - Increase Amount with Red Security Stamp
    print("\n--- Test 6: Bank Reply (Amount Increase with Red Stamp) ---")
    bank_increase_advice = """
    البنك التجاري الدولي CIB
    ملحق تعديل خطاب ضمان رقم: LG-2026-0041
    نحيطكم علماً بأنه قد تم زيادة قيمة خطاب الضمان ليصبح بمبلغ: 1,500,000.00 جم (مليون وخمسمائة ألف جنيه مصري).
    [الختم البارز / ختم أحمر بالمبلغ: 1,500,000.00 جم]
    خاتم فرع البنك التجاري الدولي
    """
    res_increase = document_verification_service.verify_bank_reply(
        raw_text=bank_increase_advice,
        expected_bank_name="Commercial International Bank",
        expected_bank_short_name="CIB",
        action_type="LG_INCREASE_AMOUNT",
        expected_new_expiry=None,
        expected_new_amount=1500000.00,
        lg_number="LG-2026-0041"
    )
    print(f"Status: {res_increase['status']}")
    print(f"Matches: {res_increase['matches']}")
    print(f"Mismatches: {res_increase['mismatches']}")
    assert res_increase['status'] == 'verified'
    assert any("Red Security Stamp" in m for m in res_increase['matches'])
    print("✓ Amount increase with Red Security Stamp verified successfully!")

    # Test 7: Bank Reply - Increase Amount Missing Red Stamp
    print("\n--- Test 7: Bank Reply (Amount Increase MISSING Red Stamp) ---")
    bank_increase_no_stamp = """
    البنك التجاري الدولي CIB
    ملحق تعديل خطاب ضمان رقم: LG-2026-0041
    نحيطكم علماً بأنه تم تعديل القيمة لمبلغ 1,500,000.00 جم.
    خاتم الفرع
    """
    res_no_red = document_verification_service.verify_bank_reply(
        raw_text=bank_increase_no_stamp,
        expected_bank_name="Commercial International Bank",
        expected_bank_short_name="CIB",
        action_type="LG_INCREASE_AMOUNT",
        expected_new_expiry=None,
        expected_new_amount=1500000.00,
        lg_number="LG-2026-0041"
    )
    print(f"Status: {res_no_red['status']}")
    print(f"Mismatches: {res_no_red['mismatches']}")
    assert res_no_red['status'] == 'mismatch'
    assert any("Red Security Stamp" in m for m in res_no_red['mismatches'])
    print("✓ Missing Red Security Stamp correctly flagged as discrepancy!")

    print("\n========================================================")
    print("🎉 ALL 7 DOCUMENT AI VERIFICATION TESTS PASSED CLEANLY!")
    print("========================================================")

if __name__ == "__main__":
    run_tests()
