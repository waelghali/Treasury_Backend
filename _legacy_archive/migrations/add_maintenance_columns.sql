-- Add action_history column to issued_lg_records
ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS action_history JSONB DEFAULT '[]'::jsonb;

-- Create issuance_maintenance_actions table
CREATE TABLE IF NOT EXISTS issuance_maintenance_actions (
    id SERIAL PRIMARY KEY,
    issued_lg_id INTEGER NOT NULL REFERENCES issued_lg_records(id),
    action_type VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'PENDING_APPROVAL',
    action_data JSONB,
    pending_approver_users JSONB DEFAULT '[]'::jsonb,
    current_step_number INTEGER DEFAULT 0,
    approval_history JSONB DEFAULT '[]'::jsonb,
    letter_template_id INTEGER REFERENCES templates(id),
    letter_generated_path VARCHAR,
    letter_serial_number VARCHAR UNIQUE,
    instruction_status VARCHAR,
    is_printed BOOLEAN NOT NULL DEFAULT FALSE,
    delivery_date TIMESTAMPTZ,
    delivery_method VARCHAR,
    delivery_notes TEXT,
    bank_reply_date TIMESTAMPTZ,
    bank_reply_notes TEXT,
    initiated_by_user_id INTEGER NOT NULL REFERENCES users(id),
    executed_by_user_id INTEGER REFERENCES users(id),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_maintenance_issued_lg_id ON issuance_maintenance_actions(issued_lg_id);
CREATE INDEX IF NOT EXISTS idx_maintenance_action_type ON issuance_maintenance_actions(action_type);
CREATE INDEX IF NOT EXISTS idx_maintenance_status ON issuance_maintenance_actions(status);
