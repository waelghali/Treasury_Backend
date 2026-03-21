-- ============================================================================
-- Migration: Steps 5.5 + 5.6 — Post-Issuance Tracking
-- Run against: issued_lg_records table
-- ============================================================================

-- Step 5.5a: Delivery Tracking
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS delivery_date DATE;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS delivery_method VARCHAR;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS delivery_notes TEXT;

-- Step 5.5b: Bank Reply Tracking
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_reply_type VARCHAR;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_reply_date DATE;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_reply_notes TEXT;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_lg_number VARCHAR;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_lg_issue_date DATE;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_lg_expiry_date DATE;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_lg_amount NUMERIC(20, 2);

-- Step 5.6: LG Copy Verification
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS verification_status VARCHAR;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS verification_notes TEXT;
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS verified_by_user_id INTEGER REFERENCES users(id);
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP;

-- (Previous migration — bank form priority)
ALTER TABLE bank_form_templates ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT 0 NOT NULL;
