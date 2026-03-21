-- Default LG Issuance Request Template (Signed Company Letter)
-- Run this once to seed the standard template. System Owner can customize in the UI.
-- If re-running: this will NOT overwrite existing templates (ON CONFLICT DO NOTHING).

-- Delete the old template first if you want to re-seed
DELETE FROM templates WHERE action_type = 'LG_ISSUANCE_REQUEST' AND name = 'Standard LG Issuance Request Letter' AND is_global = TRUE;

INSERT INTO templates (name, template_type, action_type, content, is_global, is_notification_template, is_default, subject, is_deleted)
VALUES (
    'Standard LG Issuance Request Letter',
    'LETTER',
    'LG_ISSUANCE_REQUEST',
    '<!DOCTYPE html>
<html>
<head>
<style>
    body { font-family: ''Times New Roman'', serif; font-size: 12pt; margin: 40px; line-height: 1.6; color: #333; }
    .header { text-align: center; margin-bottom: 30px; border-bottom: 2px solid #1a3c6e; padding-bottom: 15px; }
    .header h1 { font-size: 16pt; color: #1a3c6e; margin: 0; letter-spacing: 1px; }
    .header p { font-size: 10pt; color: #666; margin: 5px 0 0; }
    .meta { margin-bottom: 20px; }
    .meta table { width: 100%; border-collapse: collapse; }
    .meta td { padding: 3px 0; vertical-align: top; }
    .meta td:first-child { width: 120px; font-weight: bold; color: #1a3c6e; }
    .recipient { margin-bottom: 20px; }
    .recipient p { margin: 2px 0; }
    .subject-line { font-weight: bold; font-size: 13pt; text-align: center; margin: 20px 0 15px; color: #1a3c6e; text-decoration: underline; }
    .body-text { text-align: justify; margin-bottom: 12px; }
    .details-table { width: 100%; border-collapse: collapse; margin: 12px 0; }
    .details-table th { background: #1a3c6e; color: white; padding: 8px 12px; text-align: left; font-size: 10pt; }
    .details-table td { padding: 6px 12px; border-bottom: 1px solid #ddd; font-size: 11pt; }
    .details-table tr:nth-child(even) { background: #f8f9fa; }
    .details-table td:first-child { font-weight: bold; width: 200px; color: #555; }
    .conditions { background: #f5f7fb; border-left: 3px solid #1a3c6e; padding: 12px 16px; margin: 15px 0; font-size: 11pt; }
    .instructions { background: #fff8e1; border-left: 3px solid #f9a825; padding: 12px 16px; margin: 15px 0; font-size: 11pt; }
    .signature { margin-top: 40px; }
    .signature-line { border-top: 1px solid #333; width: 250px; margin-top: 50px; padding-top: 5px; }
</style>
</head>
<body>

<div class="header">
    <h1>{{customer_name}}</h1>
    <p>{{entity_name}}</p>
</div>

<div class="meta">
    <table>
        <tr><td>Date:</td><td>{{current_date}}</td></tr>
        <tr><td>Reference:</td><td>{{serial_number}}</td></tr>
    </table>
</div>

<div class="recipient">
    <p><strong>To:</strong></p>
    <p>{{bank_name}}</p>
    <p>{{branch_name}}</p>
</div>

<p class="subject-line">Request for Issuance of Letter of Guarantee</p>

<p class="body-text">
    Dear Sir/Madam,
</p>

<p class="body-text">
    We hereby request the issuance of a Letter of Guarantee as per the details outlined below.
    Kindly debit our account for the applicable charges and issue the guarantee at your earliest convenience.
</p>

<table class="details-table">
    <tr><th colspan="2">Account Details</th></tr>
    <tr><td>Account Name</td><td>{{account_name}}</td></tr>
    <tr><td>Account Number</td><td>{{account_number}}</td></tr>
    <tr><td>Customer Number</td><td>{{customer_number}}</td></tr>
</table>

<table class="details-table">
    <tr><th colspan="2">Guarantee Details</th></tr>
    <tr><td>Type of Guarantee</td><td>{{lg_type}}</td></tr>
    <tr><td>Amount</td><td>{{currency_code}} {{amount}}</td></tr>
    <tr><td>Amount in Words</td><td>{{amount_in_words}} {{currency_name}}</td></tr>
    <tr><td>Issue Date</td><td>{{issue_date}}</td></tr>
    <tr><td>Expiry / Validity Date</td><td>{{expiry_date}}</td></tr>
    <tr><td>Purpose</td><td>{{purpose}}</td></tr>
    <tr><td>LG Format</td><td>{{lg_wording_clause}}</td></tr>
</table>

<table class="details-table">
    <tr><th colspan="2">Beneficiary Information</th></tr>
    <tr><td>In Favor Of</td><td>{{beneficiary_name}}</td></tr>
    <tr><td>Address</td><td>{{beneficiary_address}}</td></tr>
</table>

<table class="details-table">
    <tr><th colspan="2">Reference Details</th></tr>
    <tr><td>Reference Type</td><td>{{reference_type}}</td></tr>
    <tr><td>Reference Number</td><td>{{reference_number}}</td></tr>
</table>

<div class="conditions">
    <p><strong>Terms & Conditions:</strong></p>
    <p>{{conditions_acceptance}}</p>
    <p>We authorize you to debit our account for all applicable fees, commissions, and charges 
    related to this guarantee issuance. This request is binding and irrevocable upon your receipt.</p>
</div>

{{other_instructions_section}}

<p class="body-text">
    Please do not hesitate to contact the undersigned for any clarifications.
</p>

<div class="signature">
    <p>Yours faithfully,</p>
    <p><strong>{{customer_name}}</strong></p>
    <div class="signature-line">
        <p>Authorized Signatory</p>
    </div>
</div>

</body>
</html>',
    TRUE,   -- is_global
    FALSE,  -- is_notification_template
    TRUE,   -- is_default
    NULL,   -- subject (not applicable for letters)
    FALSE   -- is_deleted
)
ON CONFLICT ON CONSTRAINT _template_unique_per_scope_and_purpose_and_name DO NOTHING;
