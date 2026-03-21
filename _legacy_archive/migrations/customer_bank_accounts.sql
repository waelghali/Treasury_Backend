-- Customer Bank Accounts + Facility Link
-- Run this migration to create the customer_bank_accounts table
-- and add bank_account_id FK to facilities

-- 1. Create customer_bank_accounts table
CREATE TABLE IF NOT EXISTS customer_bank_accounts (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    bank_id INTEGER NOT NULL REFERENCES banks(id),
    entity_id INTEGER REFERENCES customer_entities(id),
    account_name VARCHAR NOT NULL,
    account_number VARCHAR NOT NULL,
    customer_number VARCHAR,
    branch_name VARCHAR,
    iban VARCHAR,
    is_default BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_cba_customer_bank ON customer_bank_accounts(customer_id, bank_id);

-- 2. Add bank_account_id to facilities table
ALTER TABLE facilities ADD COLUMN IF NOT EXISTS bank_account_id INTEGER REFERENCES customer_bank_accounts(id);

COMMENT ON TABLE customer_bank_accounts IS 'How a customer identifies themselves at each bank (account name, number, CIF, branch)';
COMMENT ON COLUMN customer_bank_accounts.entity_id IS 'Optional: entity-specific account. NULL = company-level default';
COMMENT ON COLUMN customer_bank_accounts.customer_number IS 'Bank CIF or customer reference number (optional)';
COMMENT ON COLUMN facilities.bank_account_id IS 'The customer bank account used for this facility';
