-- Migration: Make issuance_requests columns nullable for draft support
-- Date: 2026-03-10
-- Description: Allow saving incomplete drafts by removing NOT NULL constraints
--              from fields that are only required at submission time.

ALTER TABLE issuance_requests ALTER COLUMN issuing_entity_id DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN requestor_name DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN requestor_email DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN lg_type_id DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN lg_purpose DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN amount DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN currency_id DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN requested_issue_date DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN requested_expiry_date DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN beneficiary_name DROP NOT NULL;
ALTER TABLE issuance_requests ALTER COLUMN beneficiary_country DROP NOT NULL;
