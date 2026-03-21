-- Migration: Add initial_utilization and dedicated_references to facility sub-limits
-- Also fix existing country data in issuance requests

-- 1. Add initial_utilization column (for onboarding existing facilities)
ALTER TABLE issuance_facility_sub_limits 
    ADD COLUMN IF NOT EXISTS initial_utilization NUMERIC(20,2) NOT NULL DEFAULT 0;

-- 2. Add dedicated_references column (facility-to-contract/PO linkage)
ALTER TABLE issuance_facility_sub_limits 
    ADD COLUMN IF NOT EXISTS dedicated_references JSONB;

-- 3. Fix existing country data: convert full names to ISO codes
UPDATE issuance_requests SET beneficiary_country = 'EG' WHERE UPPER(beneficiary_country) = 'EGYPT';
UPDATE issuance_requests SET beneficiary_country = 'SA' WHERE UPPER(beneficiary_country) IN ('SAUDI ARABIA', 'KSA');
UPDATE issuance_requests SET beneficiary_country = 'AE' WHERE UPPER(beneficiary_country) IN ('UAE', 'UNITED ARAB EMIRATES');
UPDATE issuance_requests SET beneficiary_country = 'QA' WHERE UPPER(beneficiary_country) = 'QATAR';
UPDATE issuance_requests SET beneficiary_country = 'KW' WHERE UPPER(beneficiary_country) = 'KUWAIT';
UPDATE issuance_requests SET beneficiary_country = 'BH' WHERE UPPER(beneficiary_country) = 'BAHRAIN';
UPDATE issuance_requests SET beneficiary_country = 'OM' WHERE UPPER(beneficiary_country) = 'OMAN';
UPDATE issuance_requests SET beneficiary_country = 'JO' WHERE UPPER(beneficiary_country) = 'JORDAN';
UPDATE issuance_requests SET beneficiary_country = 'LB' WHERE UPPER(beneficiary_country) = 'LEBANON';
UPDATE issuance_requests SET beneficiary_country = 'IQ' WHERE UPPER(beneficiary_country) = 'IRAQ';
UPDATE issuance_requests SET beneficiary_country = 'LY' WHERE UPPER(beneficiary_country) = 'LIBYA';
UPDATE issuance_requests SET beneficiary_country = 'TN' WHERE UPPER(beneficiary_country) = 'TUNISIA';
UPDATE issuance_requests SET beneficiary_country = 'MA' WHERE UPPER(beneficiary_country) = 'MOROCCO';
UPDATE issuance_requests SET beneficiary_country = 'DZ' WHERE UPPER(beneficiary_country) = 'ALGERIA';
UPDATE issuance_requests SET beneficiary_country = 'SD' WHERE UPPER(beneficiary_country) = 'SUDAN';
UPDATE issuance_requests SET beneficiary_country = 'TR' WHERE UPPER(beneficiary_country) = 'TURKEY';
UPDATE issuance_requests SET beneficiary_country = 'US' WHERE UPPER(beneficiary_country) IN ('UNITED STATES', 'USA');
UPDATE issuance_requests SET beneficiary_country = 'GB' WHERE UPPER(beneficiary_country) IN ('UNITED KINGDOM', 'UK');
UPDATE issuance_requests SET beneficiary_country = 'DE' WHERE UPPER(beneficiary_country) = 'GERMANY';
UPDATE issuance_requests SET beneficiary_country = 'FR' WHERE UPPER(beneficiary_country) = 'FRANCE';
UPDATE issuance_requests SET beneficiary_country = 'CN' WHERE UPPER(beneficiary_country) = 'CHINA';
UPDATE issuance_requests SET beneficiary_country = 'JP' WHERE UPPER(beneficiary_country) = 'JAPAN';
UPDATE issuance_requests SET beneficiary_country = 'IN' WHERE UPPER(beneficiary_country) = 'INDIA';
UPDATE issuance_requests SET beneficiary_country = 'BR' WHERE UPPER(beneficiary_country) = 'BRAZIL';
UPDATE issuance_requests SET beneficiary_country = 'CA' WHERE UPPER(beneficiary_country) = 'CANADA';
UPDATE issuance_requests SET beneficiary_country = 'AU' WHERE UPPER(beneficiary_country) = 'AUSTRALIA';
UPDATE issuance_requests SET beneficiary_country = 'ZA' WHERE UPPER(beneficiary_country) = 'SOUTH AFRICA';
UPDATE issuance_requests SET beneficiary_country = 'NG' WHERE UPPER(beneficiary_country) = 'NIGERIA';
UPDATE issuance_requests SET beneficiary_country = 'KE' WHERE UPPER(beneficiary_country) = 'KENYA';
UPDATE issuance_requests SET beneficiary_country = 'GH' WHERE UPPER(beneficiary_country) = 'GHANA';
UPDATE issuance_requests SET beneficiary_country = 'SG' WHERE UPPER(beneficiary_country) = 'SINGAPORE';
UPDATE issuance_requests SET beneficiary_country = 'MY' WHERE UPPER(beneficiary_country) = 'MALAYSIA';
UPDATE issuance_requests SET beneficiary_country = 'ID' WHERE UPPER(beneficiary_country) = 'INDONESIA';
UPDATE issuance_requests SET beneficiary_country = 'TH' WHERE UPPER(beneficiary_country) = 'THAILAND';

-- Also fix issuance_country if it exists
UPDATE issuance_requests SET issuance_country = 'EG' WHERE UPPER(issuance_country) = 'EGYPT';
UPDATE issuance_requests SET issuance_country = 'SA' WHERE UPPER(issuance_country) IN ('SAUDI ARABIA', 'KSA');
UPDATE issuance_requests SET issuance_country = 'AE' WHERE UPPER(issuance_country) IN ('UAE', 'UNITED ARAB EMIRATES');
