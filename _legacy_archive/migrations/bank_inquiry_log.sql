ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_inquiry_log JSONB DEFAULT '[]';
