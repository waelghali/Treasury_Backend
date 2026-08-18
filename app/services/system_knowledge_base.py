# app/services/system_knowledge_base.py
"""
Grow Platform Master Grounding Knowledge Base
Authoritative codebase and database schema grounding for the AI Assistant (Level 4).
"""

SYSTEM_KNOWLEDGE_CONTEXT = """================================================================================
GROW PLATFORM AUTHORITATIVE CAPABILITIES, SETTINGS & WORKFLOW GROUNDING
(Synchronized Directly from Codebase & Database Schemas)
================================================================================

1. USER ROLES & ACCESS CONTROL
- `system_owner`: Super-Admin. Manages customer onboarding, subscription tiers, global system configurations, bank form templates, background scheduler, and system audit logs.
- `corporate_admin`: Organization Administrator. Full organization management: users, approval matrix, bank facilities, bank accounts, settings & module configurations (`/corporate-admin/module-configs`), issuance form configs (`/corporate-admin/issuance/form-config`), LG categories (`/corporate-admin/lg-categories`), migration hub, and organization reports.
- `checker`: Senior Approver. Maker-checker reviews in the Approval Center (approves/rejects LG actions, issuance requests, quotations). Self-Approval is strictly prohibited by system guardrails.
- `end_user`: Operations Specialist. Records new LGs (AI OCR scan / manual entry), initiates lifecycle maintenance actions (Extend, Release, Liquidate, Decrease, Amend), and submits issuance requests.
- `viewer`: Read-only stakeholder. Can view dashboards, LG records, and reports without operational modification privileges.

2. DETAILED NAVIGATION MAP & PAGE DIRECTORY

[CORPORATE ADMIN NAVIGATION]:
- **Dashboard**: `/corporate-admin/dashboard`
- **Approval Center**: `/corporate-admin/approval-requests`
- **Settings & Module Configurations**: `/corporate-admin/module-configs` (accessible via Sidebar -> Configuration -> Settings)
- **Issuance Form Configuration**: `/corporate-admin/issuance/form-config` (accessible via Sidebar -> Issuance -> Issuance Form Config)
- **LG Categories**: `/corporate-admin/lg-categories` (custom business categories and cost centers)
- **User Management**: `/corporate-admin/users`
- **Bank Facilities**: `/corporate-admin/issuance/facilities`
- **Bank Accounts**: `/corporate-admin/issuance/bank-accounts`
- **Requests Inbox (Issuance)**: `/corporate-admin/issuance/requests`
- **Issued LGs Management**: `/corporate-admin/issuance/issued-lgs`
- **Owner Management**: `/corporate-admin/issuance/owner-management`
- **Position Reconciliation**: `/corporate-admin/issuance/reconciliation`
- **Migration Hub (Issuance)**: `/corporate-admin/issuance/migration-hub`
- **All LG Records (Custody)**: `/corporate-admin/lg-records`
- **Action Center**: `/corporate-admin/action-center`
- **Migration Hub (Custody)**: `/corporate-admin/migration-hub`
- **Bank Reconciliation Workspace**: `/corporate-admin/reconciliation`
- **Reconciliation Rules Engine**: `/corporate-admin/reconciliation/rules`
- **Accounting Export**: `/corporate-admin/reconciliation/export`
- **Quotation Control Center**: `/corporate-admin/quotations`
- **Organization Reports**: `/corporate-admin/reports`
- **Audit Logs**: `/corporate-admin/audit-logs`

[END USER NAVIGATION]:
- **Dashboard**: `/end-user/dashboard`
- **Record New LG**: `/end-user/lg-records/new`
- **Manage LG Records**: `/end-user/lg-records`
- **Action Center**: `/end-user/action-center`
- **Pending Approvals / Withdraw**: `/end-user/pending-approvals`
- **New Issuance Request**: `/end-user/issuance/requests/new`
- **Issuance Requests Inbox**: `/end-user/issuance/requests`
- **Issued LGs**: `/end-user/issuance/issued-lgs`
- **Quotation Requests & History**: `/end-user/quotations`

3. EXHAUSTIVE 5-GROUP SYSTEM SETTINGS & CONFIGURATIONS (`/corporate-admin/module-configs`)

--- GROUP 1: OPERATIONAL TIMERS, EXPIRIES & BANK REMINDER WINDOWS ---
* **Bank Reminder Time Window Parameters**:
  - `REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE` (Default: `3` days): Minimum days that must have passed since instruction issuance before a bank reminder can be generated.
  - `REMINDER_TO_BANKS_DAYS_SINCE_DELIVERY` (Default: `7` days): Minimum days after physical delivery proof is recorded before a follow-up reminder can be issued.
  - `REMINDER_TO_BANKS_MAX_DAYS_SINCE_ISSUANCE` (Default: `90` days): The upper cutoff threshold after which the reminder window closes and reminders stop being suggested.
  - `NUMBER_OF_DAYS_FOR_NEXT_REMINDER` (Default: `7` days): Interval cadence for next reminder milestone in the Action Center.
* **Print & Cancellation Timers**:
  - `DAYS_FOR_FIRST_PRINT_REMINDER` (Default: `2` days): Days after approval before the system reminds the Maker to print the approved instruction letter.
  - `DAYS_FOR_PRINT_ESCALATION` (Default: `5` days): Days after approval when an escalation alert is dispatched to both Maker and Checker if the letter is unprinted.
  - `MAX_DAYS_FOR_LAST_INSTRUCTION_CANCELLATION` (Default: `3` days): Grace window in days to cancel/withdraw an unexecuted bank instruction.
* **Undelivered Reporting Window**:
  - `NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED`: Days after issuance when an unconfirmed delivery starts reporting.
  - `NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED`: Days after which unconfirmed deliveries stop reporting.
* **Expiry & Auto-Renewal**:
  - `AUTO_RENEW_REMINDER_START_DAYS_BEFORE_EXPIRY` (Default: `60` days): Days before expiry when maturity warnings start appearing for standard LGs.
  - `AUTO_RENEWAL_DAYS_BEFORE_EXPIRY` (Default: `30` days): Days before expiry when auto-renew LGs are queued for automatic renewal.
  - `FORCED_RENEW_DAYS_BEFORE_EXPIRY`: Mandatory forced renewal cutoff.

--- GROUP 2: DOCUMENT COMPLIANCE & MANDATORY EVIDENCE POLICIES (`DOC_MANDATORY_*`) ---
When set to `true`, End Users are blocked from submitting the action until the required document is attached:
- `DOC_MANDATORY_RELEASE` (Default: `false`): Requires attaching release proof (e.g. Beneficiary Release Letter) before releasing an LG.
- `DOC_MANDATORY_LIQUIDATION` (Default: `false`): Requires attaching bank claim/debit advice document before liquidating an LG.
- `DOC_MANDATORY_DECREASE_AMOUNT` (Default: `false`): Requires attaching supporting documentation when reducing liability.
- `DOC_MANDATORY_ACTIVATE` (Default: `false`): Requires attaching proof of advance payment credit before activating Non-Operative LGs.
- `DOC_MANDATORY_RECORD_DELIVERY` (Default: `false`): Requires attaching stamped courier or bank delivery slip.
- `DOC_MANDATORY_RECORD_BANK_REPLY` (Default: `false`): Requires attaching bank's formal confirmation letter.
- `DOC_MANDATORY_LG_HANDOVER` (Default: `false`): Requires signed receiving voucher when handing over physical paper LGs.

--- GROUP 3: SMART BANK FACILITY RECOMMENDATION & SCORING WEIGHTS ---
Calculates a 0-100 score for each available bank facility when recommending optimal banks for issuance requests:
* **Normal Issuance Scoring Weights**:
  - `FACILITY_SCORE_WEIGHT_COST` (Default: `30%`): Weight for commission and issuance fees.
  - `FACILITY_SCORE_WEIGHT_CAPACITY` (Default: `20%`): Weight for available facility headroom.
  - `FACILITY_SCORE_WEIGHT_CURRENCY_MATCH` (Default: `20%`): Weight for matching currency vs FX conversion.
  - `FACILITY_SCORE_WEIGHT_MARGIN` (Default: `15%`): Weight for cash margin collateral requirement.
  - `FACILITY_SCORE_WEIGHT_SLA` (Default: `15%`): Weight for bank turnaround speed.
* **Urgent Issuance Scoring Weights**:
  - `FACILITY_SCORE_WEIGHT_URGENT_SLA` (Default: `40%`): Drastically prioritizes bank turnaround speed.
  - `FACILITY_SCORE_WEIGHT_URGENT_COST` (Default: `15%`)
  - `FACILITY_SCORE_WEIGHT_URGENT_CAPACITY` (Default: `15%`)
  - `FACILITY_SCORE_WEIGHT_URGENT_CURRENCY_MATCH` (Default: `20%`)
  - `FACILITY_SCORE_WEIGHT_URGENT_MARGIN` (Default: `10%`)
* **Facility Limits & Portal**:
  - `RESERVATION_TTL_DAYS` (Default: `14` days): Facility limit reservation hold duration before automatic release.
  - `PUBLIC_ISSUANCE_SESSION_EXPIRY_MINUTES` (Default: `60` min): OTP session expiry for external requestors on public portal.

--- GROUP 4: OPERATIONAL GOVERNANCE, CONTROLS & POSITION RECONCILIATION ---
- `ALLOW_SIMULTANEOUS_MAINTENANCE` (Default: `true`):
  * If `false`, only ONE pending maintenance request is permitted on an LG at a time.
  * If `true`, different maintenance types (e.g. Extend & Decrease) can be submitted concurrently.
  * Note: Duplicate same-type requests (e.g. two Extensions) are ALWAYS strictly blocked by the system regardless of setting.
- `APPROVAL_REQUEST_MAX_PENDING_DAYS` (Default: `7` days):
  * Maximum days an approval request can stay pending before being automatically rejected/expired.
- `DAYS_FOR_RECONCILIATION_REMINDER` (Default: `60` days):
  * Overdue alert triggered on dashboard if an issuing bank's **LG Position Reconciliation** (`/corporate-admin/issuance/reconciliation`) has not been conducted within this period.
- `QUOTATION_APPROVAL_REQUIRED` (Default: `false`):
  * If `true`, End Users' FX/T-Bill RFQs must be approved by Corporate Admin before broadcasting to bank dealers.
- `COMMON_COMMUNICATION_LIST` (Default: `[]`):
  * Distribution list of corporate admin email addresses for organization alerts and escalations.
- `REFERENCE_EXPIRY_REMINDER_DAYS` (Default: `30` days):
  * Reminder threshold before underlying commercial contracts/references expire.

--- GROUP 5: SECURITY, AUTHENTICATION & PLATFORM POLICIES ---
- `PASSWORD_MIN_LENGTH` (Default: `8`)
- `PASSWORD_REQUIRE_UPPERCASE` (Default: `true`)
- `PASSWORD_REQUIRE_LOWERCASE` (Default: `true`)
- `PASSWORD_REQUIRE_DIGIT` (Default: `true`)
- `PASSWORD_RESET_TOKEN_EXPIRY_MINUTES` (Default: `15` min)
- `GRACE_PERIOD_DAYS` (Default: `14` days)
- `STORAGE_BUCKET_NAME` (Tenant-specific GCS bucket)
- `TC_VERSION` & `PP_VERSION` (Terms & Conditions, Privacy Policy versions)

4. LG CUSTODY LIFECYCLE ACTIONS & WORKFLOWS
Available from LG Details page -> Actions Menu:
- **Extend LG Validity**: Initiates an extension request with new target maturity date and justification. Generates formal bank instruction letter.
- **Release LG Obligation**: Formally discharges the guarantee liability upon contract fulfillment and logs return of physical letter to issuing bank.
- **Liquidate LG Claim**: Records full or partial beneficiary claim payout executed by bank against company credit line.
- **Decrease Liability Amount**: Reduces active guarantee liability amount and automatically restores available headroom in the associated Bank Facility.
- **Amend LG Terms**: Amends operative conditions, beneficiary name, or text amendments.
- **Activate Non-Operative LG**: Transitions guarantee from Non-Operative to Operative once advance payment or condition precedent is satisfied.
- **Cancel Instruction**: Withdraws last unexecuted instruction if the bank has not yet processed it.
- **Change LG Owner**: Reassigns internal custodian/owner for organizational accountability.
- **Record Delivery Proof**: Logs courier or physical delivery receipt of instructions to bank.
- **Record Bank Reply**: Logs bank's formal confirmation, acceptance, or debit advice.
- **Send Bank Reminder**: Generates formal reminder letter (1st, 2nd, final) for pending bank execution within the reminder time window.
"""


def get_system_knowledge() -> str:
    """Returns the full grounding system knowledge base text."""
    return SYSTEM_KNOWLEDGE_CONTEXT.strip()
