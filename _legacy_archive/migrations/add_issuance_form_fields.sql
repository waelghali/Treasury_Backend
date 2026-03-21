-- Migration: Add new issuance form fields
-- Run this against your database to add the new columns

ALTER TABLE issuance_requests ADD COLUMN IF NOT EXISTS beneficiary_id_number VARCHAR;
ALTER TABLE issuance_requests ADD COLUMN IF NOT EXISTS is_auto_reducing BOOLEAN DEFAULT FALSE;
ALTER TABLE issuance_requests ADD COLUMN IF NOT EXISTS reduction_trigger TEXT;
ALTER TABLE issuance_requests ADD COLUMN IF NOT EXISTS other_conditions TEXT;

-- Index for beneficiary lookup
CREATE INDEX IF NOT EXISTS ix_issuance_requests_beneficiary_id_number 
ON issuance_requests (beneficiary_id_number) WHERE beneficiary_id_number IS NOT NULL;
