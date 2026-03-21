-- Migration: Create bank_form_templates table
-- This stores uploaded bank PDF forms and their AI-generated field mappings

CREATE TABLE IF NOT EXISTS bank_form_templates (
    id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES banks(id),
    name VARCHAR NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    form_type VARCHAR NOT NULL DEFAULT 'FILLABLE_PDF',  -- FILLABLE_PDF | PHYSICAL_OVERLAY
    
    -- Storage
    file_path VARCHAR,
    original_filename VARCHAR,
    
    -- AI-generated mapping (cached)
    field_mapping JSONB,
    ai_analysis JSONB,
    ai_analysis_status VARCHAR DEFAULT 'PENDING',  -- PENDING | ANALYZING | COMPLETED | FAILED
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE,
    
    -- Audit
    uploaded_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_bank_form_templates_bank_id ON bank_form_templates(bank_id);
CREATE INDEX IF NOT EXISTS idx_bank_form_templates_active ON bank_form_templates(is_active, is_deleted);

-- Add lg_type_id for type-specific form matching (NULL = universal, works for any LG type)
ALTER TABLE bank_form_templates ADD COLUMN IF NOT EXISTS lg_type_id INTEGER REFERENCES lg_types(id);
