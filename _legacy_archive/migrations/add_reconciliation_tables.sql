-- ===========================================================================
-- LG Position Reconciliation — Schema Migration
-- ===========================================================================

-- 1. Reconciliation Sessions
CREATE TABLE IF NOT EXISTS reconciliation_sessions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    bank_id INTEGER NOT NULL REFERENCES banks(id),
    position_date DATE NOT NULL,
    uploaded_file_path VARCHAR,
    original_file_name VARCHAR,
    file_format VARCHAR,
    parsing_method VARCHAR,
    status VARCHAR DEFAULT 'CREATED',
    total_bank_records INTEGER DEFAULT 0,
    bank_reported_total NUMERIC(20,2),
    bank_reported_count INTEGER,
    matched_count INTEGER DEFAULT 0,
    mismatched_count INTEGER DEFAULT 0,
    bank_only_count INTEGER DEFAULT 0,
    system_only_count INTEGER DEFAULT 0,
    ai_usage_log JSONB,
    error_message TEXT,
    reviewed_by_user_id INTEGER REFERENCES users(id),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    notes TEXT,
    created_by_user_id INTEGER REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- 2. Bank Rows (parsed from the position report)
CREATE TABLE IF NOT EXISTS reconciliation_bank_rows (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES reconciliation_sessions(id) ON DELETE CASCADE,
    bank_lg_number VARCHAR,
    beneficiary_name VARCHAR,
    amount NUMERIC(20,2),
    currency_code VARCHAR,
    issue_date DATE,
    expiry_date DATE,
    raw_data JSONB,
    match_status VARCHAR DEFAULT 'UNMATCHED',
    matched_lg_id INTEGER REFERENCES issued_lg_records(id),
    variances JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Reconciliation Results (one per mismatch/flag)
CREATE TABLE IF NOT EXISTS reconciliation_results (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES reconciliation_sessions(id) ON DELETE CASCADE,
    bank_row_id INTEGER REFERENCES reconciliation_bank_rows(id) ON DELETE CASCADE,
    issued_lg_id INTEGER REFERENCES issued_lg_records(id),
    mismatch_type VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    field_name VARCHAR,
    bank_value VARCHAR,
    system_value VARCHAR,
    user_resolution VARCHAR,
    resolution_notes TEXT,
    resolved_by_user_id INTEGER REFERENCES users(id),
    resolved_at TIMESTAMP WITH TIME ZONE,
    approval_status VARCHAR,
    approved_by_user_id INTEGER REFERENCES users(id),
    approved_at TIMESTAMP WITH TIME ZONE,
    record_updated BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Bank Column Mappings (cached per bank+customer for reuse)
CREATE TABLE IF NOT EXISTS bank_column_mappings (
    id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES banks(id),
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    source_column VARCHAR NOT NULL,
    mapped_field VARCHAR NOT NULL,
    mapping_source VARCHAR DEFAULT 'AUTO',
    confidence NUMERIC(3,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_bcm_bank_customer ON bank_column_mappings(bank_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_recon_session_bank ON reconciliation_sessions(bank_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_recon_results_session ON reconciliation_results(session_id);
CREATE INDEX IF NOT EXISTS idx_recon_bank_rows_session ON reconciliation_bank_rows(session_id);
