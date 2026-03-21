# LG Issuance — Complete Customer Journey

Your thoughts organized into a structured, end-to-end story. **Gaps filled from the codebase** are marked with 🔍. Items completed since the original are marked with ✅ Done. Bug fixes applied during testing are listed in the appendix.

---

## Part 0 — System Architecture (Foundation)

### Modular Subscription Platform

The system is **modular and subscription-based**. Customers subscribe to one or more modules:

- **Module 1 — LG Custody** ✅ (Live/Production) — customer is the **beneficiary** of the LG
- **Module 2 — LG Issuance** ✅ (Live) — customer is the **applicant** of the LG
- **Module 3 — Bank Reconciliation** ✅ (Live) — bank statement import, AI classification, rules engine, accounting export

### Core Roles

| Role | Who | Responsibilities |
|------|-----|-----------------|
| **System Owner** | Platform operator (not a customer) | Manage system configs, common lists (banks, currencies, LG types), customers, subscription plans, bank form templates |
| **Corporate Admin** | Customer's main manager | Customize configurations, manage approval matrix, manage users/entities, approve transactions, oversee everything |
| **End User** | Customer's operational staff | Execute transactions (reserve facilities, issue to bank), generate invite links for external requestors |

🔍 **Additional role from codebase: Checker** — can review and approve requests but cannot initiate them. Checkers are used in the approval matrix alongside Corporate Admins. Login is MFA-gated (requires authenticator app).

🔍 **External Requestor** — company employee without a system account. Submits requests via public portal, views their issued LGs, requests maintenance actions (extend, increase, close, amend).

---

## Part 1 — Setup (Before Any LG Can Be Issued)

### 1.1 Customer Onboarding (System Owner)

1. System Owner creates customer with subscription plan
2. Creates initial **Corporate Admin** user + initial **Entity**
3. `domains` auto-populated from admin email (e.g., `admin@acmecorp.com` → `["acmecorp.com"]`)
4. Additional domains can be added for public portal access

🔍 Domain uniqueness enforced — same domain cannot belong to two different customers

### 1.2 Bank Accounts (Corporate Admin / End User)

🔍 Before creating facilities, customers register their **bank accounts** per bank:

- Account name, account number, CIF/customer number, branch, IBAN
- Can be entity-specific or company-level default
- One default account per customer+bank pair
- 🔍 Bank account data (CIF, IBAN, account number) is automatically mapped into bank form auto-fill

### 1.3 Facilities — Bank Agreements (End User)

Load the bank facility (credit line agreement) into the system:

| Field | Description |
|-------|-------------|
| Bank | Which bank issued the facility |
| Entity(ies) | Which customer entities can use it |
| Currency | Base currency |
| Total Limit | Maximum amount |
| Tenor (months) | Maximum LG tenor allowed under this facility |
| SLA (days) | Bank's agreed service level for issuance |
| Cross-border allowed | Can issue to beneficiaries in other countries |
| Multi-currency allowed | Can issue LGs in different currencies |
| Third-party allowed | Can issue on behalf of other entities |
| Start / Expiry / Review dates | Lifecycle management |

🔍 **Advanced risk controls** found in codebase:

- `fx_breach_auto_suspend` — auto-suspend if FX movement causes limit breach
- `margin_reduces_exposure` — cash margin deducted from utilized limit
- `exposure_start_trigger` — count exposure ON_APPROVAL or ON_ISSUANCE
- `required_cash_margin_days` — days before issuance to deposit margin

### 1.4 Sub-Limits (Within Each Facility)

Each facility must have **at least 1 sub-limit**. Sub-limits:

- Are typed by **LG type** (Bid Bond, Performance Bond, etc.)
- Have their own **limit amount** (cap) — can be up to the full facility amount
- Total of all sub-limits **can exceed** facility total (overlapping allocation)
- **Utilization is calculated at the facility level** — sub-limits consume from the main facility amount

| Sub-Limit Field | Description |
|----------------|-------------|
| LG Type IDs | Which LG types this sub-limit covers |
| Limit Amount | Maximum for this sub-limit |
| Max Amount Per LG | Single LG amount ceiling |
| Max Tenor Days | Maximum tenor per LG |
| Allowed Countries | Country whitelist/blacklist (JSONB) |
| Allows Confirmation | Whether confirmation is available |
| Initial Utilization | For onboarding existing commitments |
| Commission Rate / Cash Margin / Fees | Default pricing per sub-limit |
| **Project Dedication** | Restrict to specific corporate projects |

### 1.5 LG Categories (Corporate Admin)

✅ Done: LG Categories allow grouping of requests for routing and reporting:

| Category Type | Examples | Editable? |
|--------------|----------|-----------|
| Universal (System) | Default Category (DF), Projects (CP), Service (SC) | Status only — cannot be deleted |
| Customer-Specific | HR Department (HR), IT Projects (IT) | Full CRUD by Corporate Admin |

Each category can define:

- An **Extra Field** with configurable name (e.g., "Project Code", "Service ID") — set as mandatory or optional
- **Entity scoping** — applied to "All Entities" or specific entities
- **Communication List** — optional CC recipients for related notifications

🔍 Soft-delete supported — deleted categories can be restored via "Show Deleted Categories" toggle

### 1.6 Projects (Corporate Admin / End User)

Projects provide business context for facility dedication:

- Type: CONTRACT / PROJECT / PURCHASE_ORDER / TENDER / OTHER
- Status: ACTIVE / COMPLETED / CANCELLED
- Facilities can be earmarked for specific projects via sub-limit dedication

### 1.7 Departments & Groups (Corporate Admin)

- **Departments**: Organizational units with a designated **manager** — used as approval triggers
- **Groups**: Collections of users to simplify approver selection in the approval matrix
- ✅ Done: Creating/updating departments and groups goes through **dual-control** (another admin must approve the change)

### 1.8 Request Form Configuration (Corporate Admin)

The issuance request form is **fully customizable**:

- Toggle **visibility** and **mandatory** status of each configurable field
- Core fields (amount, type, beneficiary) are always mandatory
- **Custom fields** (2x) with configurable labels and types (e.g., "Cost Center", "Budget Code")
- **Mandatory document types** (e.g., Formal Request letter always required)
- **Reference types** list is configurable (Contract, PO, Tender, etc.)
- 🔍 **Recipient field configurations**: Same configurability for LG handover recipient fields

### 1.9 Approval Matrix (Corporate Admin)

Sequential approval workflow with configurable triggers:

| Trigger (condition_type) | Description |
|--------------------------|-------------|
| `ALWAYS` | Every request requires this approval |
| `AMOUNT_OVER` | Triggered when LG amount exceeds threshold (with currency) |
| `AMOUNT_RANGE` | Triggered when LG amount falls within a range |
| `DEPT_MATCH` | Triggered when request comes from a specific department |
| `CROSS_BORDER` | Triggered when `is_cross_border = true` |
| `THIRD_PARTY` | Triggered when `is_third_party = true` |

For each trigger:

- **Approver pool** (type: `ROLE` / `USERS` / `DEPT_HEAD` / `GROUP`)
- **Required signatures** count
- **Step sequence** for multi-level approvals

**Key behavior**: If the same approver is required by 2 different triggers, the system **skips the duplicate** — no one approves the same request twice.

🔍 **Additional behaviors from codebase**:

- Self-approval is blocked (requestor cannot approve their own request)
- If no approval policies match, request auto-advances to `APPROVED_INTERNAL`
- Notification sent at every step

### 1.10 Bank Form Templates (System Owner)

Pre-load bank forms for automated filling:

| Form Type | Description |
|-----------|-------------|
| FILLABLE_PDF | Standard fillable PDF — fields populated automatically |
| PHYSICAL_OVERLAY | Transparent text printed over a physical form (overlay only) |
| SCANNED_FILL | Overlay merged onto original scan image |

Each form template:

- Targeted to specific LG types (or universal for all types)
- AI-analyzed once → field mapping cached for subsequent fills
- Has language (AR / EN / BILINGUAL) and priority ranking (0-100)
- ✅ Done: System Owners can search, suspend/reactivate, soft-delete/restore, and edit field mappings with coordinate editing (X, Y, size)
- Users can report issues (missing/outdated/filling problems)

---

## Part 2 — Request Submission

### 2.1 How Requests Enter the System

**Path A — Public Portal (Push)**

1. Requestor visits the public webpage (`/portal/issuance`)
2. Enters their corporate email (e.g., `john@acmecorp.com`)
3. System verifies domain against customer's `domains` JSONB array
4. 6-digit OTP sent to email for verification (10 minute expiry)
5. ✅ Done: Session has configurable expiry time (`PUBLIC_ISSUANCE_SESSION_EXPIRY_MINUTES`, default 60 min)
6. Requestor redirected to their **Requestor Dashboard** — can submit new requests or view existing ones
7. Requestor fills and submits the configured form

**Path B — Tokenized Invite (Pull)**

1. ✅ Done: **Both Corporate Admin and End User** can generate a tokenized link from the Issuance Requests page ("Generate Invite" button)
2. ✅ Done: Link has configurable expiry time (`INVITE_LINK_EXPIRY_HOURS`, default 168 hours / 7 days)
3. Requestor opens link → bypasses OTP, goes straight to form
4. Fills remaining fields and submits

Requestors are **NOT** system users — they're anyone working for the company with a matching email domain.

### 2.2 The Requestor Dashboard

✅ Done: After login/verification, external requestors land on their personal dashboard showing:

- **Stats cards**: Total requests, pending approvals, approved/issued count
- **Request table**: All their requests with serial number, status, beneficiary, amount, dates, actions
- **Draft management**: Edit or delete saved drafts
- ✅ Done: **"My Issued LGs" tab** — view all LGs linked to their requests with status, amount, bank, dates
- ✅ Done: **Maintenance buttons** on active LGs — Extend, Increase Amount, Close, Amend (inline forms)
- ✅ Done: **Maintenance Action History** — track submitted actions with status badges

### 2.3 What the Request Form Captures (4-Step Wizard)

✅ Done: The form was redesigned into a 4-stage stepper:

| Step | Section | Key Fields |
|------|---------|------------|
| 1 | Requestor Info | Name, email, department, job title, employee ID, manager emails. Auto-fills from previous submissions |
| 2 | Reference & Project | Reference type/number, linked project. Duplicate reference check with warning |
| 3 | LG Details | LG type, entity, amount, currency, issue/expiry dates, urgency, cross-border, purpose, special wording, applicable rules |
| 4 | Beneficiary | Name, address, contact, country, ID number. Fuzzy name suggest + ID auto-fill from history |

Additional captured data:

- `applicable_rules` — URDG 758, ISP98, Local Law
- `cross_border_details` — advising bank, counter-guarantee, sanctions screening, governing law
- `treasury_enrichment` — JSONB for treasury team's technical input
- `is_auto_reducing` with `reduction_trigger` — for auto-reducing LGs
- Documents: Upload contracts, formal requests, special wording docs (stored in GCS)

Serial number auto-generated: `REQ-YYYY-XXXX` (scoped per customer per year)

**Draft Mode**: Requestors can save the request as a DRAFT and come back later. Drafts are visible in their dashboard and can be edited or deleted.

---

## Part 3 — Approval Cycle

**Status**: `DRAFT` → `PENDING_APPROVAL` → `APPROVED_INTERNAL`

1. Request submitted → system evaluates all applicable workflow policies
2. For each matching trigger (ordered by `step_sequence`):
   - Resolve approver pool (users by role, specific IDs, department head, or group)
   - Skip if approver already appeared in a previous step (deduplicate)
   - Request pending approvers and send notifications
3. Each approver can **Approve**, **Reject**, or ✅ **Return for Revision**
4. Once required signatures met for a step → advance to next step
5. All steps complete → status becomes `APPROVED_INTERNAL`

### Approval Roadmap (Visual Timeline)

✅ Done: Every request shows a premium **vertical timeline** displaying the full approval journey:

- **Completed** steps — with timestamps and approver names (green)
- **Active** step — pulsing animation, shows who is being waited on (blue)
- **Skipped** steps — conditions didn't apply (gray)
- **Pending** steps — future steps that will be reached (outline)

The roadmap re-evaluates conditions fresh on every load.

### Edge Cases

- If zero policies match → auto-approved (`APPROVED_INTERNAL` immediately)
- Rejected at any step → `REJECTED` (entire request, with reason)
- `AUTO_REJECTED_EXPIRED` — if pending too long (configurable days, default 7)
- ✅ Done: **Return for Revision** — approver sends back to requestor with notes. Requestor edits and resubmits. Approval restarts from Step 1. Return history captured in `approval_chain_audit`.

---

## Part 4 — Treasury Processing

**Status**: `APPROVED_INTERNAL` → `FACILITY_RESERVED`

Once approved, the treasury team (End Users) can:

1. **Edit technical data** (treasury enrichment) — requestors may not be qualified for these:
   - Applicable rules override
   - Advising bank details
   - Margin instructions
   - Internal notes
   - Edits create a version (version N+1) with tracked `changed_fields` and mandatory `change_reason` if post-approval

2. **Facility Recommendation** — System suggests suitable facilities:
   - Checks: bank match, LG type match, currency match, amount headroom, tenor fit
   - Checks: country restrictions, cross-border/third-party allowance
   - Checks: project dedication (if request has a project linked)
   - Ranks by utilization and suitability

3. **Duplicate Detection** — System checks for potential duplicates between the request and already-issued LGs (same beneficiary, similar amount/type)

4. **Reserve Facility** — Lock the amount against a specific sub-limit:
   - Creates an `IssuanceExposureEntry` (type: ISSUANCE)
   - Uses FX rate conversion if facility currency ≠ request currency
   - Status → `FACILITY_RESERVED`
   - Can be released if plans change → back to `APPROVED_INTERNAL`

---

## Part 5 — Bank Issuance (External Request)

**Status**: `FACILITY_RESERVED` → `PENDING_BANK_CONFIRMATION`

> ⚠️ Important distinction: **Internal request** (from requestor to treasury) vs **External request** (from treasury to bank)

### 5.1 Issuing Instructions to Bank (3-Step Wizard)

✅ Done: The issuance is executed through a **3-step wizard**:

**Step 1 — Select Bank / Facility**
- Matched facilities shown with utilization stats (currency, amount, availability)
- Or select any active bank without a facility (for one-off arrangements)

**Step 2 — Choose Method**

| Method | How It Works |
|--------|-------------|
| Company Letter | Generate a formal issuance letter (PDF) with all LG details, serial number, and company branding |
| Bank Form (Fillable PDF) | Auto-fill a pre-loaded fillable PDF from the bank using AI-mapped field data |
| Bank Form (Scanned Fill) | Overlay text onto a scanned copy of a bank form |
| Bank Form (Physical Overlay) | Print transparent text over a physical bank form (AI-analyzed field positions) |

For bank form filling:
- System auto-fills from: request data + customer data + entity data + bank account data
- Fields that can't be auto-filled → user provides values → cached for next time (per customer+form template, stored in `FormFieldUserValue`)
- ✅ Done: **Gap Detection** — after auto-fill, system compares mapped fields against request data. If unmapped critical fields detected (e.g., auto-reduction conditions, special wording), amber warning shows gaps and suggests generating a supplementary letter
- Special wording documents auto-download alongside the bank form

**Step 3 — Confirm & Issue**
- User reviews details, optionally provides custom LG reference
- One-click atomic execution: lock → `IssuedLGRecord` created → facility reserved→utilized → bank instruction generated

### 5.2 Delivery Tracking

Record that the instruction was delivered to the bank:

- Delivery date, method (HAND_DELIVERY / COURIER / EMAIL), notes

### 5.3 Bank Reply Tracking

Track the bank's response:

| Reply Type | Action |
|------------|--------|
| LG_ISSUED | Bank issued the LG — record bank LG number, dates, amount |
| INQUIRY | Bank has questions — log inquiry, respond, re-track |
| REJECTED | Bank rejected — log reason |
| NO_RESPONSE | SLA timer running — system can flag/remind |

Inquiry log: JSONB array tracking back-and-forth `{date, notes, type, logged_by_user_id}`

### 5.4 SLA Monitoring

System tracks against `sla_agreement_days` from the facility:

- If bank hasn't responded within SLA → flag/notification
- ✅ Done: SLA breaches tracked in **Treasury Dashboard** KPI cards

### 5.5 LG Copy Verification

Once bank provides the LG copy:

- Upload scanned copy
- AI verification — scan the LG and compare against the original request to ensure it matches
- Verification status: `PENDING` → `MATCHED` / `DISCREPANCY` → `ACCEPTED`
- If discrepancy → notes captured, user decides whether to accept or request correction

### 5.6 LG Handover

Record physical handover of the LG to the original requestor (or an alternative):

- Recipient: name, email, department, job title, employee ID, phone, manager chain
- Recipient fields follow the same form configuration as requestor fields
- Signed receiving document uploaded
- Handover date and notes

---

## Part 6 — Post-Issuance Lifecycle

### 6.1 Active LG Tracking (External Position)

Once issued, the LG record (`IssuedLGRecord`) includes:

- LG reference number, bank LG number, dates, current amount, status
- 7-tab detail modal:
  1. **LG Details** — amounts, dates, collection info
  2. **Bank & Facility** — bank, branch, facility reference
  3. **Original Request** — full traceability to requestor's intent
  4. **Maintenance History** — all post-issuance actions
  5. **Documents** — uploaded copies and generated letters
  6. **Verification** — AI comparison results
  7. **Activity Log** — audit trail of reprints, custody transfers, status changes
- ✅ Done: **Smart Reprint** — button re-generates original document based on stored `issuance_method` (Company Letter or Bank Form), with dynamic label ("Reprint Letter" or "Reprint Bank Form"). Every reprint logged in audit trail.
- Full custody chain log (JSONB array tracking physical document handovers)

### 6.2 Automated Reminders & Alerts

| Reminder Type | Trigger |
|--------------|---------|
| LG Expiry — 1st | Configurable days before expiry |
| LG Expiry — Interval | Repeated at configurable interval after first |
| LG Expired | After expiry date passes |
| Facility Utilization | At 80%, 90%, 100% thresholds |
| Facility Expiry | Facility nearing its own expiry date |
| Reference Validity | LG outlives the underlying contract |

### 6.3 Maintenance Actions

6 types of post-issuance actions on active LGs:

| Action | Description |
|--------|-------------|
| EXTEND | Extend the LG expiry date |
| INCREASE_AMOUNT | Increase the LG amount |
| CLOSE | Close/return the LG |
| LIQUIDATION | Trigger partial or full liquidation |
| AMENDMENT | Amend LG terms/wording |
| ACTIVATE | Activate a non-operative LG |

Each maintenance action:

- Has its own approval workflow (same matrix pattern as issuance requests)
- Generates a letter/instruction to the bank (with own serial number)
- Tracks delivery and bank reply
- Increases/amendments create new exposure entries in the facility ledger

✅ Done: **Who can initiate maintenance?**
- **Corporate Admins / End Users** — from the Issued LGs page internally
- **External Requestors** — from the Requestor Dashboard → "My Issued LGs" tab, using inline Extend/Increase/Close/Amend buttons with action-specific forms

### 6.4 Bank Reconciliation

Automated comparison between system records and bank position:

1. **Upload**: Upload bank's LG position report (Excel, CSV, PDF, or manual)
2. **Parse**: AI or tabular parsing of the bank file → `ReconciliationBankRow` records
3. **Match**: System matches bank rows to `IssuedLGRecord` entries
4. **Results**: Flag mismatches by type and severity:

| Mismatch Type | Example |
|--------------|---------|
| AMOUNT | Bank shows different amount than system |
| EXPIRY | Different expiry dates |
| INITIAL_DATA | Other field discrepancies |
| BANK_ONLY | Bank has an LG we don't have |
| SYSTEM_ONLY | We have an LG the bank doesn't show |

5. **Resolution**: User resolves each mismatch:
   - `ADJUSTED` — update system record (requires corporate admin approval)
   - `DISPUTE` — flag for follow-up with bank
   - `IGNORE` — acknowledged, no action

Bank Column Mapping: AI/manual column mappings are cached per bank+customer to avoid re-mapping on every upload.

✅ Done: **Header Drift Detection** — on upload, system compares headers against cached mapping. If columns changed → warning: *"Column structure has changed."* User can trigger one-click re-analysis to rebuild the mapping.

---

## Part 7 — Treasury Dashboard

✅ Done: The Corporate Admin now has a real-time **Treasury Dashboard** showing:

- **KPI Cards** (clickable → navigate to relevant page):
  - Pending Requests in pipeline
  - My Pending Approvals awaiting action
  - SLA Breaches (requests stuck >7 days)
  - LGs Expiring in 7 Days

- **Portfolio Overview Banner**: Total active LGs, aggregate exposure amount, pending bank replies

- **Facility Utilization Gauges**: Per-bank horizontal bars showing limit vs. utilized vs. available — color-coded green (healthy), amber (high), red (critical)

- **Expiring LGs Table**: Top 10 LGs expiring within 30 days, sorted by urgency, with countdown badges

- **Recent Activity Timeline**: Combined feed of recent issuances and maintenance actions

- **Quick Actions**: Direct navigation to Approval Inbox, Requests, Issued LGs, Facilities, Manage LG Categories, Reconciliation

---

## Part 8 — Admin Governance

### 8.1 User Management (Corporate Admin)

✅ Done: **Organization & Teams** page with 3 tabs:

- **Users tab** — list of all organization users with Email, Role, Status (Active/Deleted), and Actions (Edit, Delete, Restore). "Add User" button for onboarding
- **Departments tab** — department management (create, edit, delete with dual-control approval)
- **Approval Groups tab** — group management for building approval matrix pools

Quick access to **Approval Matrix** configuration from the page header.

### 8.2 Audit Logs (Corporate Admin)

✅ Done: Full audit trail accessible from the sidebar:

| Column | Example |
|--------|---------|
| Timestamp | 2026-03-14 07:29:47 |
| User Name | corp.admin@acmecorp.com |
| Action Type | REPORT_ACCESS_AVG_BANK_PROCESSING_TIME |
| Entity Type | Report |
| Entity Name | Report |

Features:
- Expandable **filter panel** for date range, user, action type
- **"Export to CSV"** button for compliance reporting
- Captures: logins, report access, configuration changes, approval actions, request status transitions

### 8.3 Reports Dashboard (Corporate Admin)

✅ Done: Four operational reports accessible from the sidebar:

1. **Treasury Ops Health** — Net Operational Volume with breakdown by action type (New LGs, Extensions, Reductions, Amendments, Activations, Releases, Liquidations, Reminders)
2. **Activity Trend** — 4-month stacked bar chart by action type (`ACTV`, `AMND`, `EXTN`, `LIQD`, `New`, `REDU`, `RELS`, `RMND`)
3. **Portfolio Status Composition** — doughnut chart showing Valid, Expired, Liquidated, Released proportions
4. **Period Activity Flow** — KPI cards for each action type in the selected period

Date range selector with Refresh button. All report access captured in audit logs.

### 8.4 Dual-Control (Maker-Checker for Admin Changes)

✅ Done: Configuration changes by one Corporate Admin require approval from another:

- Types: `DEPARTMENT_CREATE`, `DEPARTMENT_UPDATE`, `GROUP_CREATE`, `GROUP_UPDATE`
- Status: `PENDING` → `APPROVED` / `REJECTED`
- Change payload captured with old/new values
- Initiator cannot approve their own change (same-user protection)

🔍 Dual-control is active for department and group changes. Other configuration changes (approval matrix, form config) are direct for now.

---

## Complete Status Lifecycle Summary

```
DRAFT → PENDING_APPROVAL → APPROVED_INTERNAL → FACILITY_RESERVED → PENDING_BANK_CONFIRMATION → [LG Issued]
  ↓           ↓                    ↓                   ↓
CANCELLED   REJECTED          CANCELLED           CANCELLED (releases reservation)
            RETURNED_FOR_REVISION → Requestor edits → Resubmit → PENDING_APPROVAL
```

Post-issuance LG statuses:

```
PENDING_CONFIRMATION → ACTIVE → EXPIRED / CANCELLED
```

Maintenance action statuses:

```
PENDING_APPROVAL → APPROVED → EXECUTED (or REJECTED / CANCELLED)
```

---

## ✅ Resolved — Original Open Questions

| # | Original Question | Resolution |
|---|-------------------|------------|
| 1 | Is there a "return for revision" flow in approvals? | ✅ Yes — implemented in Phase 3.1. Approver returns with notes, requestor revises and resubmits, approval restarts from Step 1 |
| 2 | Is dual-control active for all config changes or just specific categories? | ✅ Active for department and group create/update. Other config changes are direct |
| 3 | Should tokenized invite pre-populate entity/department or just bypass OTP? | ✅ Just bypasses OTP. Department collected during form entry |
| 4 | Are maintenance actions initiated only by End Users? | ✅ Both: Internal users from the Issued LGs page, AND external requestors from their portal dashboard |
| 5 | Does reconciliation approval use the same matrix or simpler sign-off? | ✅ Simple corporate admin sign-off, except amount increase which uses approval matrix |
| 6 | Is there a treasury dashboard? | ✅ Yes — full Treasury Dashboard with KPIs, facility gauges, expiry table, activity timeline, and quick actions |
| 7 | Can End Users generate invite links? | ✅ Yes — both Corporate Admin and End User can generate tokenized invite links from the Issuance Requests page |

---

## 🔧 Bug Fixes Applied During Testing

| # | Issue | Root Cause | Fix |
|---|-------|------------|-----|
| 1 | Public Portal `verify-domain` returning 500 | `Customer.domains` is `JSON` type, not `JSONB`. `.contains()` generated a `LIKE` query instead of `@>` | Cast to JSONB: `cast(Customer.domains, JSONB).contains([domain])` |
| 2 | `generate-invite` endpoint same crash | Identical root cause — `.contains()` on generic JSON column | Same JSONB cast fix applied |
| 3 | Dashboard stats returning 500 | `can't subtract offset-naive and offset-aware datetimes` | Timezone-aware datetime handling |
