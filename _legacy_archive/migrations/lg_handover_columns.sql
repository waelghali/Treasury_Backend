-- LG Handover step: recipient + handover tracking columns
-- No handover_method — LG handover is always physical delivery

ALTER TABLE issued_lg_records
    ADD COLUMN IF NOT EXISTS handover_date DATE,
    ADD COLUMN IF NOT EXISTS handover_notes TEXT,
    ADD COLUMN IF NOT EXISTS handover_by_user_id INTEGER REFERENCES users(id),
    ADD COLUMN IF NOT EXISTS recipient_name VARCHAR,
    ADD COLUMN IF NOT EXISTS recipient_email VARCHAR,
    ADD COLUMN IF NOT EXISTS recipient_department VARCHAR,
    ADD COLUMN IF NOT EXISTS recipient_job_title VARCHAR,
    ADD COLUMN IF NOT EXISTS recipient_phone VARCHAR,
    ADD COLUMN IF NOT EXISTS recipient_employee_id VARCHAR,
    ADD COLUMN IF NOT EXISTS recipient_manager_email VARCHAR,
    ADD COLUMN IF NOT EXISTS recipient_second_line_manager_email VARCHAR,
    ADD COLUMN IF NOT EXISTS handover_signed_copy_path VARCHAR;

-- Recipient field configurations on form config
ALTER TABLE customer_form_configurations
    ADD COLUMN IF NOT EXISTS recipient_field_configurations JSONB DEFAULT '{}';
