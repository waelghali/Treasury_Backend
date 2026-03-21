-- Migration: Create form_field_user_values table
-- This stores user-provided values for bank form fields that couldn't be auto-filled.
-- Values persist per customer+form template for reuse on subsequent fills.

CREATE TABLE IF NOT EXISTS form_field_user_values (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    form_template_id INTEGER NOT NULL REFERENCES bank_form_templates(id),
    pdf_field_name VARCHAR NOT NULL,
    saved_value VARCHAR,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ffuv_cust_form ON form_field_user_values(customer_id, form_template_id);
