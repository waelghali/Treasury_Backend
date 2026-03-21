-- Migration: Replace lg_type_id (single FK) with lg_type_ids (JSONB array)
-- This allows bank form templates to cover multiple LG types (e.g., "all except advance payment")

-- Add new column
ALTER TABLE bank_form_templates ADD COLUMN IF NOT EXISTS lg_type_ids JSONB DEFAULT NULL;

-- Migrate existing data: convert single lg_type_id to array
UPDATE bank_form_templates 
SET lg_type_ids = jsonb_build_array(lg_type_id) 
WHERE lg_type_id IS NOT NULL;

-- Drop old column (optional — can keep for backward compat)
-- ALTER TABLE bank_form_templates DROP COLUMN IF EXISTS lg_type_id;
