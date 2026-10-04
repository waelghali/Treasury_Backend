# Grow Treasury — Quotation Module Architectural Roadmap

*Last Updated: September 2026*  
*Status: Living Strategic & Technical Roadmap*

---

## 📌 Table of Contents
1. [Executive Summary & Vision](#1-executive-summary--vision)
2. [Production Database Migration Ledger (Manual Production DDL)](#-production-database-migration-ledger-manual-production-ddl)
3. [Phase 1: Counterparty Integrity & Security Controls (Completed)](#2-phase-1-counterparty-integrity--security-controls-completed)
4. [Phase 2: Bank Protection & Cryptographic Security Hardening (Completed)](#3-phase-2-bank-protection--cryptographic-security-hardening-completed--verified-)
5. [Phase 3: Multi-Leg "Invisible" Legs — Selective Counterparty Exclusion](#4-phase-3-multi-leg-invisible-legs--selective-counterparty-exclusion)
6. [Phase 4: Smart Counterparty Intelligence & Dynamic Recommendation Engine](#5-phase-4-smart-counterparty-intelligence--dynamic-recommendation-engine)
7. [Phase 5: Institutional Banking Cyber Security & Compliance Readiness](#6-phase-5-institutional-banking-cyber-security--compliance-readiness-enterprise-onboarding-track)
8. [Phase 6: Selective Leg Quoting, Uncontested Deal Governance & Unified "Skipped Bank" Architecture](#7-phase-6-selective-leg-quoting-uncontested-deal-governance--unified-skipped-bank-architecture)
9. [Phase 7: Platform Owner Diagnostics Console & Dealer Voice System](#8-phase-7-platform-owner-diagnostics-console--dealer-voice-system)
10. [Phase 8: Zero-Knowledge Architecture & Privacy-Preserving Collaborative Analytics](#9-phase-8-zero-knowledge-architecture--privacy-preserving-collaborative-analytics)

---

## 1. Executive Summary & Vision

The Grow Quotation Module is built to give Corporate Treasuries institutional-grade control, confidentiality, and data-driven counterparty allocation during FX Spot, Multi-Leg Portfolios, and T-Bill competitive tenders.

This roadmap consolidates all architectural designs, threat models, and feature specifications agreed upon to ensure continuity across development cycles.

---

## 🔒 Engineering Discipline & Cross-Session Continuity Protocol

To prevent context loss across chat sessions, avoid hallucinated assumptions, and eliminate regression bugs, all development on this platform strictly adheres to the following four rules:

### Rule 1: Ground Truth in Existing Code (Zero Assumptions)
- **Never guess or assume**: Variable names, database models, status enums, API request/response keys, and UI state properties must be inspected in the codebase using file viewers/ripgrep before writing any logic.
- Verify both backend and frontend counterparts (e.g. if checking `is_passed`, inspect both `QuotationBankLegConfig` in Python and the offer iteration loop in React).

### Rule 2: Deep Pre-Flight Analysis Before Every Sub-Phase
Before writing code for any sub-phase, perform a systematic 5-point inspection:
1. **Existing Touchpoints**: Which exact files, endpoints, and components are affected?
2. **Data Flow & Types**: What exact types are sent, received, and stored?
3. **Edge Cases & Failure Modes**: What happens on network drops, timeouts, partial submissions, or single quotes?
4. **Impact on Related Roles**: How does a dealer change affect corporate admin, approver, and audit views?
5. **No Visual or Performance Regressions**: Ensure styling adheres strictly to platform light corporate palette and adds zero latency.

### Rule 3: Session Resumption & Context Preservation
- At the end of every active milestone, code must be verified and committed locally with clear semantic Git messages.
- The roadmap file (`QUOTATION_SYSTEM_ROADMAP.md`) serves as the single source of truth (SSOT). Any new chat session starts by reading this file and checking `git status` / `git log` to resume immediately without missing a beat.

### Rule 4: Git Push Exclusivity
- **NEVER execute `git push` without explicit, unambiguous user confirmation**. Local commits are made continuously to protect progress, but pushing to remote `origin/main` remains strictly under the user's manual command.

### Rule 5: Sub-Phase Pre-Flight Briefing
- **Before touching any code**: Present a structured Pre-Flight Brief to the user:
  1. What is going to happen in this sub-phase.
  2. Which exact files, database columns, endpoints, and UI components will change.
  3. Ground-truth code findings confirming variable/status alignment.
  4. Explicit verification criteria.

### Rule 6: Sub-Phase Post-Flight Living Roadmap Update
- **Immediately upon completing each sub-phase**: Update `QUOTATION_SYSTEM_ROADMAP.md` with:
  1. **Work Actually Done**: Concrete list of files modified and logic implemented.
  2. **Technical Findings & Gotchas**: Uncovered realities or system nuances discovered during implementation.
  3. **Plan Deviations**: Any divergence from the original conceptual design with explicit architectural justification.
  4. **Verification Proof**: Exact test steps and database/UI checks confirming success.

---

## 🗄️ Production Database Migration Ledger (Manual Production DDL)

To maintain a clean codebase without single-use migration scripts, all manual DDL modifications applied to local development databases are logged in this centralized ledger with exact SQL statements, target tables, and verification queries. 

**Run these exact statements when deploying updates to Production PostgreSQL:**

| Phase | Target Table | Action | Production SQL Statement | Verification Query |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 6.1** | `quotation_bank_leg_configs` | Add column `is_passed` | ```sql<br>ALTER TABLE quotation_bank_leg_configs<br>ADD COLUMN IF NOT EXISTS is_passed BOOLEAN NOT NULL DEFAULT FALSE;<br>``` | ```sql<br>SELECT column_name, data_type, column_default<br>FROM information_schema.columns<br>WHERE table_name = 'quotation_bank_leg_configs' AND column_name = 'is_passed';<br>``` |

---

## 2. Phase 1: Counterparty Integrity & Security Controls (Completed)

| Feature | Description | Implementation Status |
| :--- | :--- | :--- |
| **Official Bank Domain Matching** | Enforces that contacts added to a bank must match that bank's registered domain in `Bank.email_domain` (e.g. `@cibeg.com`, `@nbe.com.eg`) with full subdomain support (`@fx.cibeg.com`). | ✅ **Live & Verified** (`bank_validation.py`, `crud_quotation.py`, `QuotationBanksModal.js`) |
| **Negative List Enforcement** | Blocks free webmail and disposable email providers (`@gmail.com`, `@yahoo.com`, `@hotmail.com`, etc.) for counterparty desks. | ✅ **Live & Verified** |
| **Anti-Collusion Shield** | Prohibits bank contacts from using the customer organization's own corporate domain (preventing self-quoting). | ✅ **Live & Verified** |
| **Authorized Testing Whitelist** | Unconditional bypass for authorized testing counterparties (`waelghali79+*@gmail.com`) across local and production environments. | ✅ **Live & Verified** |
| **Corporate Admin RBAC Guard** | Restricts counterparty desk configuration endpoints strictly to `corporate_admin` and `super_admin`. | ✅ **Live & Verified** |
| **Structured Role Audit Trail** | Logs changes to dealer permissions (`APPROVER`, `EXECUTION`, `VIEW_ONLY`) in the audit log. | ✅ **Live & Verified** |

---

## 3. Phase 2: Bank Protection & Cryptographic Security Hardening (Completed & Verified ✅)

### 2.1 Cryptographic OTP Salted Hashing (Zero-Plaintext Storage) ✅
- **Objective**: Ensure that 2FA OTP codes are never stored in plaintext in the database (aligned with NIST SP 800-63B & OWASP).
- **Implemented**:
  - `app/core/otp_security.py` stores `HMAC-SHA256(otp_code, server_secret_salt)`.
  - Constant-time verification (`hmac.compare_digest`) with backwards-compatible plaintext fallback.
  - Complete protection against database dumps, backups, or internal DB inspection.

### 2.2 Automated OTP Throttling & Self-Service Unlock (Zero Manual Intervention) ✅
- **Objective**: Eliminate brute-force enumeration attacks against 6-digit OTP codes without requiring any administrative tickets or manual intervention.
- **Implemented**:
  - Max **3 consecutive incorrect OTP entries** tracked via `failed_attempts` column in `quotation_access_otps`.
  - On the 3rd failed attempt: The specific OTP code is burned (`is_used = True`) and permanently invalidated.
  - **Self-Service Instant Unlock**: The trader's account is NOT locked. The UI immediately displays remaining attempts countdown and provides a 1-click `"Request New Verification Code"` self-service unlock button. Zero admin tickets, zero friction.

### 2.3 Public Endpoint Rate Limiting (OWASP API4:2023 Abuse Protection) ✅
- **Objective**: Prevent automated bots and scripted abuse against public OTP and token endpoints.
- **Implemented**:
  - `app/core/rate_limiter.py` with sliding window rate limiting.
  - `POST /api/v1/public-quotation/request-otp`: Max **3 requests per 5 minutes** per client IP + assignment token.
  - `POST /api/v1/public-quotation/verify-otp`: Max **5 verification attempts per minute**.
  - Returns `429 Too Many Requests` with standard `Retry-After` header.

### 2.4 Bank-Specific Scoped Deal Execution Receipt ✅
- **Objective**: Provide the winning bank with a cryptographically verified Deal Confirmation Slip attached to their confirmation email and visible in the portal.
- **Implemented**:
  - `generate_scoped_deal_receipt` calculates an HMAC-SHA256 digital signature over canonical deal data.
  - **Strict Privacy Scope**: Scoped **strictly to the specific legs won by that bank counterparty**, guaranteeing 0% information leakage of other portfolio legs.
  - Displayed prominently in the portal outcome view and included in trade execution confirmation emails.

### 2.5 Dynamic 4-Eyes Dual Approval (Maker-Checker) (Queued for Phase 4 Governance)
- **Objective**: Prevent a rogue corporate officer from adding an unvetted bank contact and granting them trading execution rights without secondary approval.

### 2.6 Automated Domain Consistency & Enrichment (Queued)
- Auto-discovering and linking bank domains from official MX / reverse DNS records when new institutions or foreign banks are registered.

---

## 4. Phase 3: Multi-Leg "Invisible" Legs — Selective Counterparty Exclusion

### 4.1 Business Need
In multi-leg transactions (e.g. FX Swaps, multi-currency portfolios, concurrent Spot and Forward legs), a Corporate Treasury frequently needs to **hide specific legs from specific banks** to:
1. Protect market confidentiality (prevent banks from discovering the full hedging package and widening spreads).
2. Direct specific currency pairs only to specialist market makers (e.g. Bank A for EUR/EGP, Bank B for USD/EGP).
3. Respect bank credit limits and product authorization boundaries.

### 4.2 Architectural Impact & System Touchpoints

```
                       ┌────────────────────────────────────────────────────────┐
                       │           "Invisible" Leg Selected in Wizard           │
                       └───────────────────────────┬────────────────────────────┘
                                                   │
         ┌───────────────────┬─────────────────────┼─────────────────────┬───────────────────┐
         ▼                   ▼                     ▼                     ▼                   ▼
  1. Sent Emails      2. Bank Portal        3. Submission         4. Ranking &        5. Results View
   & Reminders        Session Payload         Validation           Benchmarks          & Execution
  (Leaked Details)   (Single vs Multi)      (Malicious API)      (Biased Stats)       (Grid Display)
```

1. **Email Builder (`app/services/unified_email_builder.py`)**:
   - *Confidentiality Risk*: Default emails summarize all RFQ legs in the subject line, header, and breakdown table.
   - *Requirement*: Filter legs by `visible_legs` per assignment. If only 1 leg is visible to Bank B, the email subject and body must render as a single-currency pair RFQ without revealing that other legs exist.
2. **Scheduled Reminders (`app/services/quotation_reminder_service.py`)**:
   - Must evaluate pending quote completion against *visible legs only*, preventing false reminder alerts if Bank B already quoted their assigned legs.
3. **Public Bank Portal (`app/api/v1/endpoints/public_quotations.py` & `QuotationBankOfferPage.js`)**:
   - `GET /public-quotation/{token}` must filter out invisible legs from the JSON payload completely so DevTools inspection cannot reveal hidden legs.
   - Bank portal UI seamlessly displays single-pair mode if only 1 leg is visible.
4. **Submission Integrity Guard (`POST /offers-batch`)**:
   - Server-side validation rejecting any submitted quote targeting an invisible leg ID (`403 Forbidden`).
5. **Live Ranking & SLA Benchmark Analytics (`live_ranking_service.py`, `quotation_benchmark_service.py`)**:
   - Exclude hidden banks from the rank denominator for legs they were not invited to.
   - Response rate SLA calculations must use `invited_visible_legs` as the denominator to prevent penalizing bank response ratings.
6. **Corporate Results View (`ResultsView.js`)**:
   - Comparison grid renders a neutral `🚫 Excluded` / `👁️ Hidden` badge instead of `No Quote` or `Declined`.
   - Supports split-leg execution (e.g. awarding Leg 1 to Bank B and Leg 2 to Bank A).
7. **Creation Wizard Guardrails (`QuotationRequestWizardModal.js`)**:
   - Enforce: No "Ghost Legs" (every leg must have at least 1 visible bank).
   - Enforce: No "Ghost Banks" (every selected bank must have at least 1 visible leg).

---

## 5. Phase 4: Smart Counterparty Intelligence & Dynamic Recommendation Engine

### 5.1 Objective
Enhance `@router.get("/recommendations")` from a simple volume counter into a **predictive, multi-dimensional Counterparty Recommendation Engine** that guides the corporate user to invite the highest-probability, best-pricing banks for every specific deal.

### 5.2 Key Evaluation Dimensions

#### 1. Indicative vs. Firm Execution Divergence ("Bait-and-Switch" Detection)
- **The Problem**: Some bank desks quote aggressive, tight spreads on Indicative benchmarks (to look good on market intelligence dashboards), but widen their spreads significantly or decline to quote when invited to binding Firm Execution RFQs.
- **The Intelligence Metric**:
  $$\text{Execution Degradation Spread} = \text{Spread}_{\text{Execution}} - \text{Spread}_{\text{Indicative}}$$
- **Behavior**:
  - Banks that maintain tight pricing on firm execution receive an **Execution Consistency Premium**.
  - Banks with chronic spread widening on firm execution are flagged and penalized in execution recommendations.

#### 2. Currency Pair Affinity & Desk Specialization
- **The Problem**: A bank with a modest 25% overall win rate across all currencies might actually win **75% of EUR/EGP** transactions because they manage large corporate export flows in Euros.
- **The Intelligence Metric**:
  - Calculate pair-specific win rates and spread competitive percentiles:
    $$\text{Pair Affinity Score} = \text{Win Rate}_{\text{Pair}} \times 0.6 + \text{Response Rate}_{\text{Pair}} \times 0.4$$
- **Behavior**:
  - When the user selects `EUR / EGP` in the wizard, the recommendation engine elevates the "EUR Specialist" bank with an explicit badge: `⭐ EUR/EGP Specialist (75% Win Rate in EUR)`.

#### 3. Response Reliability & Price Freshness (Window Completion vs. Ghosting)
- **Market Reality (The Trader's Advantage)**: In institutional FX, quoting in the final seconds before window close is **an indicator of high efficiency and sophistication**. Desks that wait until the final seconds do so to price against the live interbank order book without needing wide defensive buffers, delivering the freshest, most aggressive rates to the corporate.
- **The Intelligent Metric**:
  - **Window Completion Rate**: Measures whether the bank delivered a valid quote before the cutoff (penalizes true ghosting / non-response, without penalizing strategic late-window quoting).
  - **Price Freshness Index**: Recognizes active desks that refresh quotes close to the deadline.
  - **Quote Refinement Rate**: Tracks whether a desk actively tightens its quote during live market movement.

#### 4. Ticket Size & Volume Sweet Spots
- Recognizes bank balance sheet appetite (e.g. Bank X is #1 for tickets > $5M, while Bank Y is most competitive for tickets < $500k).

---

## 6. Phase 5: Institutional Banking Cyber Security & Compliance Readiness (Enterprise Onboarding Track)

### 6.1 Edge WAF & Layer 7 DDoS Mitigation
- **Scope**: Deploy Cloudflare Enterprise / Google Cloud Armor proxy in front of FastAPI origin servers.
- **Controls**:
  - OWASP Core Rule Set (CRS) inspection: automated filtering of SQLi, XSS, and command injection at the cloud edge.
  - Rate limiting & automated bot management shielding login and quoting endpoints from credential stuffing and volumetric floods.
  - Geo-blocking capabilities for jurisdictions outside authorized trading perimeters.

### 6.2 Immutable / WORM Audit Log Forwarding (Write Once, Read Many)
- **Scope**: Stream application and trade audit logs asynchronously to an immutable cloud sink (e.g. AWS S3 with Object Lock in Compliance Mode, or GCP Cloud Logging with locked buckets).
- **Controls**:
  - Guarantees that neither a compromised database user nor a rogue system administrator can alter or erase historical deal logs.
  - Satisfies Central Bank and Basel III / MIFID II regulatory requirements for 5–7 year tamper-evident trade record retention.

### 6.3 Third-Party Penetration Testing & Vulnerability Assessment
- **Scope**: Commission an annual grey-box / black-box penetration test by a CREST-accredited / ISO 27001 cybersecurity auditing firm.
- **Deliverable**: Executive Summary and Clean Letter of Attestation for Tier-1 Bank Vendor Risk Management (VRM) Committees.

### 6.4 Key Management & Envelope Encryption (KMS / HSM)
- **Scope**: Transition sensitive database credentials and encryption keys from environment files to a dedicated Hardware Security Module (HSM) or Cloud KMS (AWS KMS / HashiCorp Vault / GCP KMS) with automated 90-day key rotation.

---

## 7. Phase 6: Selective Leg Quoting, Uncontested Deal Governance & Unified "Skipped Bank" Architecture

### 7.1 Strategic Objective & Executive Context
In multi-currency portfolios, concurrent swap legs, and single-pair spot RFQs, bank counterparties do not always possess appetite, credit lines, or currency inventory to quote on every individual leg. 

Concurrently, corporate treasuries require institutional-grade protections:
1. Counterparties must be empowered to quote selectively without being forced into an all-or-nothing submission or accidental draft errors.
2. Corporate treasuries must be protected against **uncontested monopoly pricing** where only a single counterparty provides a quote.
3. Automated deal execution (`AUTO_ACCEPT`) must never bypass human review on uncontested quotes without explicit, audited corporate consent.
4. Corporate users require full transparency on **why** any counterparty in the invited pool did not quote on any specific leg.

---

### 7.2 Core Architectural Principles & Key Design Notes

#### 1. The Unified "Skipped Bank" Matrix (Intersection with Phase 3 Invisible Legs)
- **The Conceptual Insight**: In any tender session, the invited counterparty pool represents the starting universe. When a bank does not have a quote on a specific leg, that bank was **skipped on that leg**.
- **The Two Sources of Skipping**:
  - **Corporate-Initiated Skipping (Phase 3 "Invisible Legs")**: The Corporate Client intentionally hides/excludes Bank B from Leg 2 prior to RFQ dispatch (e.g. for confidentiality, credit line ceiling, or currency specialization). Bank B does not see or know Leg 2 exists.
  - **Dealer-Initiated Skipping ("Pass Leg")**: The Corporate invited Bank B to Leg 2, but the Bank Dealer actively chooses not to quote that currency pair at runtime.
- **The Architectural Bridge**: Both actions feed into the unified per-leg counterparty configuration model (`QuotationBankLegConfig`). Rather than managing disconnected exceptions, the engine evaluates each `(Bank, Leg)` pair through an explicit **Participation Status**:
  - `EXCLUDED_BY_CORPORATE`: Leg was hidden/invisible to this bank at creation.
  - `PASSED_BY_DEALER`: Dealer explicitly opted out using `[ Pass Leg ]`.
  - `DECLINED_BY_BANK`: Bank internal approver declined the entire tender.
  - `TIMED_OUT_NO_QUOTE`: Bank was invited and approved, but the window expired with no quote submitted.

#### 2. Status Taxonomy & Strict Integrity (Zero Duplication / Zero Conflict)
To prevent confusion across dealers, corporate admins, and audit logs, the system maintains strict status naming integrity without altering existing database enums:

| Scenario | Trigger / Actor | Corporate View Status Badge | Quoting Console State | Timestamp Column | Internal Representation |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Normal Quote** | Dealer entered valid rate | `🏆 Awarded` / `Competitive` | Active rate card | Exact time (e.g. `10:14:22 AM`) | `QuotationOffer.price > 0` |
| **Explicit Pass** | Dealer clicked `[ Pass Leg ]` | `Passed on this leg` *(Slate neutral)* | `Passed (Declined to Quote)` | `Passed by Dealer` | `QuotationBankLegConfig.is_passed = True` |
| **Passive Non-Response** | Trader did not submit | `No Quote Submitted` *(Muted gray)* | `⏳ Awaiting Quote` | `No Submission` | `offer.price IS NULL` and `is_passed = False` |
| **Corporate Excluded** | Hidden in wizard (Phase 3) | `Excluded / Not Invited` *(Subtle tag)* | Leg card hidden from DOM | `Excluded` | `QuotationBankLegConfig.is_invited = False` |
| **Approver Declined** | Bank approver declined RFQ | `Participation Declined` *(Rose badge)* | Terminal locked / declined | `Declined by Bank` | `QuotationBankAssignment.approval_status = 'DECLINED'` |

#### 3. Institutional Legal Principle: Administrative Execution Delegation vs. Commercial Decision
- **The Core Distinction**: Enabling `AUTO_ACCEPT` or `AUTO_ACCEPT_SINGLE_QUOTE` is strictly a **procedural delegation of executing the acceptance action upon timeout** under pre-configured parameters.
- **Decision Ownership**: It is **NOT** a delegation of the financial, commercial, or trading decision. 
- **Platform Boundary**: The Grow platform acts strictly as an automated execution assistant. The corporate organization and its authorized officers retain 100% sole legal, commercial, and financial responsibility for counterparty selection, accepted rates, and market spread exposure.
- **Enforcement Mechanism**: The settings can never be changed via a casual checkbox or toggle click. Enabling requires completing a **High-Importance Dual-Confirmation Consent Modal** with a mandatory, non-prechecked legal acknowledgment checkbox, recorded immutably in the `AuditLog` with user ID, email, IP, and full consent text.

#### 4. Live Market Thermometer vs. CBE Benchmark
- **The Reality of Central Bank Rates**: Official CBE benchmarks publish once daily **after market close** (~4:00 PM CLT). Intraday spot bidding sessions cannot be fairly measured against yesterday's closing reference.
- **Live Interbank Mid Transition**: The system introduces a live interbank spot mid reference feed (via FXStreet, XE, or dedicated financial market data APIs).
- **Commercial Markup Awareness**: Retail/aggregator mid-market feeds reflect zero-margin interbank mid. Bank commercial execution rates will naturally reflect the bank's operational markup. The platform therefore displays:
  - **Live Interbank Mid**: The unbiased real-time market thermometer.
  - **Spread / Pip Delta**: The transparent difference between the bank's firm quote and live mid (e.g. `Quote: 48.6500 | Live Mid: 48.5800 | Spread: +14.4 pips (+0.14%)`).

#### 5. Symmetrical Single-Leg & Multi-Leg Architecture
All controls operate identically whether an RFQ has 1 leg or 10 legs:
- **Single-Leg Quotation**: Clicking `[ Pass Leg ]` allows a dealer to formally decline quoting the single pair without abandoning the session. If only 1 bank quotes, the leg is flagged as an Uncontested Single Quote.
- **Multi-Leg Quotation**: Dealers can selectively quote Leg 1 and pass Leg 2. Uncontested quote detection and auto-accept governance evaluate independently **per leg**.

---

### 7.3 Phased Implementation & Verification Plan

```
  Phase 6.1: Dealer Quoting Terminal
  [Pass Leg] Button & Batch Submit Validation
                    │
                    ▼
  Phase 6.2: Corporate Results View
  "Skipped Bank" Matrix (Passed vs. No Quote vs. Excluded)
                    │
                    ▼
  Phase 6.3: Uncontested / Single-Quote Detection
  Per-Leg Monopoly Warning in Corporate Evaluation UI
                    │
                    ▼
  Phase 6.4: Governance & Consent Modal
  Auto-Accept & Single-Quote Policy with Legal Delegation Consent
                    │
                    ▼
  Phase 6.5: Live Market Benchmark
  Interbank Mid Reference (FXStreet / XE Feed) & Spread Tracker
```

---

#### 📌 Phase 6.1: Dealer Quoting Terminal — Explicit `[ Pass Leg ]` Interaction (Completed & Verified ✅)
*Scope: Public bank portal quoting console, batch submission payload, and database persistence.*

- **Work Actually Done**:
  - **Database & Model**: Added `is_passed = Column(Boolean, default=False, nullable=False)` to `QuotationBankLegConfig` in [`app/models/models_quotation.py`](file:///c:/Grow/app/models/models_quotation.py). Added column to PostgreSQL schema via `ALTER TABLE quotation_bank_leg_configs ADD COLUMN IF NOT EXISTS is_passed BOOLEAN NOT NULL DEFAULT FALSE`.
  - **Schema**: Updated `FXSpotMultiOfferCreate` in [`app/schemas/schemas_quotation.py`](file:///c:/Grow/app/schemas/schemas_quotation.py) to accept `passed_legs: Optional[List[str]] = []`.
  - **Backend API**: In `POST /offers-batch` ([`app/api/v1/endpoints/public_quotations.py`](file:///c:/Grow/app/api/v1/endpoints/public_quotations.py)), implemented automatic upsert for `QuotationBankLegConfig` persisting `is_passed = True` for all passed leg IDs.
  - **Backend API**: In `GET /public-quotation/{token}`, exposed `is_passed: bool` on each leg object in `portal_legs`.
  - **Frontend UI & State**: In [`QuotationBankOfferPage.js`](file:///c:/Grow/frontend/src/pages/Public/QuotationBankOfferPage.js):
    - Added `passedLegs` state with auto-hydration from `fetchRfq`.
    - Implemented `[ Pass Leg ✕ ]` and reversible `[ ↩ Quote this Leg ]` action toggles on currency pair cards.
    - Added clean neutral state styling: crossed-out pair title, slate badge `Passed (Declined to Quote)`, and italicized disabled input placeholder `Leg Passed — No Quote`.
    - Updated `handleBatchSubmit` to bypass rate validation on passed legs while strictly enforcing that at least 1 leg is quoted.
    - Updated `executeBatchSubmit` to transmit `passed_legs: passedLegIds` to the backend.
    - Dynamic quoting counter in console header: displays both `X Passed` and `Y / Z Quoted`.
    - Dynamic submit button label: `Submit Quotes (X of Y Pairs)` and disabled guardrail when all legs are passed.

- **Technical Findings & Gotchas**:
  - **Database Column Prerequisite**: SQLAlchemy ORM lazy loading immediately raises `UndefinedColumn: column quotation_bank_leg_configs.is_passed does not exist` when accessing relationships if the database column is missing. Executed clean direct `ALTER TABLE` without throwaway migration files.
  - **Fat-Finger Guard Nuance**: The cross-leg synthetic cross-rate swap detection and individual 10x deviation checks in `handleBatchSubmit` must evaluate only active quoted legs (`quotesToSubmit`), ignoring passed legs so dealers can pass legs without triggering false anomaly modals.

- **Verification Proof**:
  - `python -m py_compile` passed on all backend models and schemas with zero errors.
  - Frontend production build (`craco build`) passed cleanly (`main.44c4e966.js`, code 0).
  - Live API verification: `GET /api/v1/public-quotation/9ecd319f-2f81-4c6d-8f08-25afa39b4733` returned `HTTP 200 OK` with `is_passed` exposed on all legs.

---

#### 📌 Phase 6.2: Corporate Results View — "Skipped Bank" Audit Matrix
*Scope: Corporate evaluation dashboard, leg comparison tables, and audit logs.*

- **Backend Aggregation (`compute_rfq_standings` in `quotations_endpoints.py`)**:
  - Expose `is_passed: bool` in each bank's leg result payload.
- **Frontend Presentation (`ResultsView.js` & `QuotationRequestDashboard.js`)**:
  - In each leg's counterparty table, render the exact audit reason for any unquoted bank:
    - `res.is_passed === true` $\rightarrow$ **`Passed on this leg`** (Slate neutral badge; timestamp: `Passed by Dealer`).
    - `res.submitted_at === null` $\rightarrow$ **`No Quote Submitted`** (Muted gray badge; timestamp: `No Submission`).
    - `res.approval_status === 'DECLINED'` $\rightarrow$ **`Participation Declined`** (Rose badge).
    - `res.is_invited === false` (Phase 3 Invisible) $\rightarrow$ **`Excluded / Not Invited`** (Subtle outline badge).
- **Verification & Testing Criteria**:
  - Open corporate dashboard for the Phase 6.1 test tender.
  - Verify Leg 1 displays the bank's active competitive rate.
  - Verify Leg 2 displays the bank with `Passed on this leg` and rate `—`.
  - Verify uninvited or timed-out banks display their respective badges cleanly without layout shifting.

---

#### 📌 Phase 6.3: Uncontested / Single-Quote Monopoly Detection & UI Warning
*Scope: Algorithmic detection of sole-counterparty legs and visual risk advisories.*

- **Per-Leg Uncontested Detection**:
  - Triggered whenever `valid_execution_quotes.length === 1` on any specific leg (whether caused by dealer passes, approver declines, or only 1 invited bank).
- **Corporate UI Warning**:
  - On the leg card and inside the Deal Acceptance modal:
    - Display prominent warning banner: ⚠️ **Uncontested Rate (Single Quote)**
    - Advisory Text: *"Only 1 bank counterparty provided a quote on this leg. No competing offers were received to establish market spread."*
- **Verification & Testing Criteria**:
  - Run an RFQ where 2 banks are invited; Bank B passes Leg 2.
  - Leg 1 displays standard competitive multi-bank comparison.
  - Leg 2 displays Bank A's rate accompanied by the ⚠️ Uncontested Single Quote warning banner.

---

#### 📌 Phase 6.4: Corporate Governance & Legal Delegation Consent Modal
*Scope: Configuration engine, high-importance consent dialog, and auto-accept execution engine.*

- **Configuration Settings (Customer Configuration)**:
  - `QUOTATION_ACCEPTANCE_DEFAULT_ACTION`: `AUTO_ACCEPT` vs `AUTO_REJECT`.
  - `AUTO_ACCEPT_SINGLE_QUOTE`: `True` vs `False` (Sub-configuration).
- **High-Importance Dual-Confirmation Consent Modal**:
  - Triggers when enabling either setting in Corporate Admin Settings.
  - Displays the mandatory legal responsibility text:
    > *"Notice of Administrative Delegation & Sole Responsibility:*  
    > *Enabling automated execution constitutes an administrative delegation of executing the acceptance action upon timeout according to your pre-configured parameters. This is NOT a delegation of commercial, financial, or trading decision-making.*  
    > *The user and [Company] acknowledge that the Grow platform acts solely as an automated processing assistant, and that all trading decisions, counterparty selections, pricing acceptance, and financial risks remain solely the responsibility of the corporate organization."*
  - Requires explicit checkbox acknowledgment before saving.
  - Logs full details to `AuditLog` (admin ID, email, timestamp, IP, consent text).
- **Timeout Execution Engine (`quotations_endpoints.py`)**:
  - If `AUTO_ACCEPT = True` and quotes $\ge 2$: auto-accepts normally.
  - If `AUTO_ACCEPT = True` but quotes $== 1$ and `AUTO_ACCEPT_SINGLE_QUOTE = False`:
    - The engine **halts automated execution** for that leg.
    - Leaves the leg in `PENDING_ACCEPTANCE` and logs: *"Uncontested quote requires manual corporate sign-off."*
  - If `AUTO_ACCEPT_SINGLE_QUOTE = True` (with recorded consent): proceeds to auto-accept.
- **Verification & Testing Criteria**:
  - Attempt toggling `AUTO_ACCEPT` without checkbox $\rightarrow$ confirm save button is disabled.
  - Enable with checkbox $\rightarrow$ verify audit log entry with legal text.
  - Allow window to expire on an uncontested leg with `AUTO_ACCEPT_SINGLE_QUOTE = False` $\rightarrow$ confirm system halts auto-accept and marks leg as requiring manual corporate approval.

---

#### 📌 Phase 6.5: Live Market Benchmark Integration (Interbank Mid Reference)
*Scope: Replacing static CBE post-close benchmark with live intraday reference rates.*

- **Live Data Feed Integration**:
  - Connect live interbank mid rates (via FXStreet, XE, or institutional market API).
- **UI Integration**:
  - Display **`Live Interbank Mid`** alongside bank firm quotes.
  - Display real-time **Spread / Pip Delta** relative to live mid.
- **Verification & Testing Criteria**:
  - Verify live mid rates update in real-time during market hours.
  - Verify pip/spread calculation against submitted bank quotes is mathematically accurate.
## 8. Phase 7: Platform Owner Diagnostics Console & Dealer Voice System

---

#### 📌 Phase 7.1: Internal Trophy & Liquidity Diagnostics Console (Super Admin Portal)
*Scope: Internal platform diagnostics, mathematical verification, counterparty oversight, and trophy calculation audit.*

- **Context & Operational Rationale**:
  - The Platform Owner / Super Admin requires central visibility into how bank desks and individual dealers perform, which trophies have been unlocked, and whether gamification algorithms are calculating accurately.
  - **Zero Public Disclaimer Requirement**: Because this is strictly internal administrative telemetry and QA diagnostics for the platform creator—and is never exposed to competing banks or corporate clients—no external legal disclaimers or public terms-of-use changes are required.
- **Access Control & Security Isolation**:
  - Strictly restricted to `SYSTEM_ADMIN` / `SUPER_ADMIN` roles.
  - Completely invisible to corporate admins, branch users, and participating bank dealers.
  - Zero interbank data leakage: Bank A can never access Bank B's metrics.
- **Console Features & UI Architecture**:
  - **Bank Desk Selector**: Dropdown to inspect any onboarded institution (Banque Misr, CIB, AlexBank, NBE, QNB, HSBC, etc.).
  - **Dealer Roster & Activity**: Drill down by specific dealer email/name or view aggregate bank desk achievements.
  - **Live 8-Trophy Mathematical Audit Showcase**:
    1. **Deal Closer (`DEAL_CLOSER`)**: Total firm deals won; click to view list of awarded RFQs and execution legs.
    2. **Unbroken Victor (`TRIPLE_CROWN`)**: 
       - Active Streak vs. Personal Best Record.
       - **Streak Audit Trail**: Chronological timeline of evaluated RFQs showing exact state transitions:
         - Clean Sweep ($\ge 1$ legs won, 100% of awarded legs won) $\rightarrow +1$ streak.
         - Competitor Win ($\ge 1$ leg won by rival bank) $\rightarrow$ streak reset to 0 with explanation.
         - Wash / Neutral (client-rejected, auto-rejected, or aborted RFQ with 0 awards) $\rightarrow$ streak preserved.
    3. **Liquidity Titan (`VOLUME_TITAN`)**: Total volume awarded converted to USD ($M/B), showing currency breakdown (USD, EUR, GBP, EGP converted at 50.0).
    4. **Swift Quoting (`PRECISION_SPEED`)**: Total firm quotation submissions.
    5. **Market Intelligence (`MARKET_INTELLIGENCE`)**: Total indicative pricing quotes submitted.
    6. **The Active Desk (`THE_RELIABLE_DESK`)**: Total corporate tenders entered and quoted; verification of the **"Active Participant"** tier at 5 tenders.
    7. **Market Versatility (`CURRENCY_EXPLORER`)**: Distinct interbank currency pairs traded (e.g., USD/EGP, EUR/EGP, EUR/USD).
    8. **Ready at the Bell (`READY_AT_THE_BELL`)**: Punctual logins authenticated within 15 minutes before the opening bell.
  - **Instant Calculation Verification**:
    - Re-run calculation button to instantly verify and trace live database state against cached achievement payloads.
- **Verification & Testing Criteria**:
  - Open console as Super Admin $\rightarrow$ select Banque Misr.
  - Confirm all 8 trophies reflect authentic database records.
  - Verify that clicking on `Unbroken Victor` shows the streak audit history explaining why the streak is at its current number.
  - Verify unauthorized roles (dealers, corporate clients) receive HTTP 403 Forbidden.

---

#### 📌 Phase 7.2: Dealer Voice & Feedback Mechanism (Quotation Terminal)
*Scope: Institutional feedback widget, trader experience rating, and non-blocking toast prompt on the public quotation interface.*

- **Context & Operational Rationale**:
  - Giving bank dealers a direct channel to provide feedback, report system issues, or rate the terminal builds relationship goodwill and provides early warning for latency or usability bugs.
- **Critical Trading Desk Design Constraints**:
  - **Non-Interference with Live Trading**: FX and Treasury dealers operate under high stress during active quotation windows. The feedback mechanism must **never** block, modal-lock, or delay the quotation submission flow or countdown timers.
  - **100% Optional & Low Friction**: Must take under 5 seconds to complete.
- **Frontend Components (`QuotationBankOfferPage.js`)**:
  - **1. Discreet Header Button**:
    - A subtle, sleek button placed alongside the trophy showcase and session badge:
      `[ 💬 Feedback ]`
    - Clicking opens a compact, floating feedback modal.
  - **2. Optional Post-Quote Experience Toast**:
    - Triggers only *after* quotes are successfully submitted or when the quotation window has closed.
    - Displays a lightweight, elegant toast in the lower corner:
      > *"How was your quotation experience today? [ ★ ★ ★ ★ ★ ] (Optional)"*
    - Automatically dismisses after 8 seconds if ignored, without modal backdrop or interaction lock.
  - **3. Compact Feedback Modal**:
    - **Star Rating**: 1 to 5 stars.
    - **Category Pills**: `Execution Speed & Latency`, `Rate Triangulation / Calculations`, `Terminal UI & Usability`, `Feature Request`, `Other`.
    - **Short Comment**: Multi-line textarea (max 300 characters, optional).
    - **Privacy Toggle**: `Include my name & bank` vs. `Submit Anonymously`.
- **Backend API (`public_quotations.py` / `dealer_feedback.py`)**:
  - `POST /api/v1/public-quotation/feedback`
    - Validates quotation token / OTP assignment.
    - Records bank ID, dealer email (if not anonymous), star rating, category, message, tender ID, and timestamp.
    - Rate-limited to prevent spam (max 2 submissions per tender session).

---

#### 📌 Phase 7.3: Admin Feedback Inbox & Satisfaction Stream (Super Admin Portal)
*Scope: Platform owner feedback feed, sentiment tracking, and issue triaging.*

- **Console Features**:
  - **Feedback Stream**: Reverse-chronological table of all received dealer submissions:
    - Timestamp, Bank Name, Dealer Email (or "Anonymous Dealer"), Star Rating, Category, and Comment.
  - **Filter & Search**:
    - Filter by Bank, Star Rating (e.g. show 1-2 star alerts first), Category, or Date Range.
  - **Status Triaging**:
    - Status badges: `New` $\rightarrow$ `Under Review` $\rightarrow$ `Resolved / Addressed`.
    - Internal admin notes (e.g. *"Discussed with AlexBank Head of FX on Oct 4"*).
  - **CSAT / NPS Summary Card**:
    - Average quotation satisfaction score (e.g. 4.8 / 5.0).
    - Breakdown by category (speed, UI, rate triangulation).
- **Verification & Testing Criteria**:
  - Submit a feedback test from the dealer quotation page with 5 stars and category `Rate Triangulation`.
  - Verify the entry appears instantly in the Super Admin Feedback Inbox.
  - Test the anonymous toggle and confirm the dealer email is masked while retaining the bank association.

---

## 9. Phase 8: Zero-Knowledge Architecture & Privacy-Preserving Collaborative Analytics

### 9.1 Strategic Vision & Executive Objective
To build an institutional-grade security perimeter where **the database contains zero plaintext confidential financial terms** (bank quotes, profit margins, deal spreads, pricing models), isolating the System Owner, database hosting providers, and compromised backups from customer data.

Concurrently, the platform must preserve:
1. **Uninterrupted UX**: Zero key-management friction for non-technical corporate treasurers.
2. **Self-Contained Maintenance**: 100% automated user onboarding and self-service email password recovery with rate-limiting cooldown timers, requiring **zero manual intervention from the System Owner**.
3. **Collaborative Intelligence**: Platform-wide market benchmarks (interbank spread averages, percentile rankings, liquidity depth, and dealer gamification trophies) computed securely without ever decrypting or exposing individual corporate deal specifics.

---

### 9.2 The Phased Cryptographic Model: Seamless $0.00 to Cloud KMS Evolution

To balance operational cost during early growth with Tier-1 bank procurement readiness, the system employs **Envelope Encryption with an Abstracted Key Management Provider**:

```
                       ┌──────────────────────────────────────────────┐
                       │           Sensitive Quotation Data           │
                       │    (Quotes, Spreads, Margins, Volume)        │
                       └──────────────────────┬───────────────────────┘
                                              │
                              Encrypted via AES-256-GCM
                                              │
                                              ▼
                       ┌──────────────────────────────────────────────┐
                       │     Tenant Data Encryption Key (DEK)         │
                       │          (Unique Per Customer)               │
                       └──────────────────────┬───────────────────────┘
                                              │
                               Sealed via Master KEK
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
         [ Stage 1: $0.00 / Month ]                      [ Stage 2: $5–$15 / Month ]
       Self-Contained Server Isolation                    Cloud KMS Hardware Security
   (Local Master KEK + Tenant Envelopes)             (AWS KMS / GCP Cloud KMS HSM Provider)
   - Zero infrastructure cost                        - FIPS 140-2 Level 2/3 hardware isolation
   - Ideal for pilot customers & dev                 - Hardware-enforced separation of duties
   - Code swap: exactly 1 file / 20 lines            - Meets institutional CISO bank checklists
```

#### Why Upgrading Requires Zero Rewriting:
* **The Database Schema NEVER Changes**: In both Stage 1 and Stage 2, ciphertext columns in PostgreSQL (`encrypted_offer_price`, `encrypted_spread`, `dek_envelope`) remain 100% identical.
* **Zero Data Re-Encryption Needed**: The customer's Tenant DEK encrypts the deals. Upgrading from $0 to Cloud KMS only wraps the DEK with a Cloud KEK instead of a local Server KEK. No historical deal rows ever need to be decrypted or re-encrypted.
* **Unified Code Interface**:
  ```python
  # app/core/security_crypto.py
  class KeyProvider(ABC):
      @abstractmethod
      def unwrap_dek(self, encrypted_dek: bytes, tenant_id: str) -> bytes: pass

  class LocalServerKeyProvider(KeyProvider):  # Stage 1: $0.00
      ...
  class CloudKmsKeyProvider(KeyProvider):     # Stage 2: $5-$15 (Plug-and-play)
      ...
  ```

---

### 9.3 Self-Contained Lifecycle & Zero System Owner Maintenance

#### 1. Zero-Touch Onboarding
1. System Owner invites Customer and creates the first Corporate Admin with their email address.
2. The platform automatically dispatches an encrypted, single-use activation link to the user's email.
3. The Corporate Admin opens the link in their browser and sets their own private password.
4. The backend initializes a cryptographically random Tenant DEK (256-bit AES), encrypts it inside the user's envelope, and commits it.
5. **System Owner Visibility**: Zero knowledge of the password, zero plaintext exposure.

#### 2. Self-Contained Password Recovery (Forgot Password)
1. **Initiation**: User clicks "Forgot Password" on the login screen.
2. **Abuse Prevention**: Platform enforces a **Cooldown Timer & Rate Limiter** (e.g., max 3 recovery attempts per 1 hour; 15-minute token TTL).
3. **Signed Token**: An HMAC-SHA256 time-bounded recovery token is dispatched directly to the user's registered corporate email.
4. **Automated Re-Enveloping**: 
   - Upon clicking the verified email link, the user enters their new password.
   - The platform unseals the tenant recovery envelope and re-encrypts the Tenant DEK with the user's new password derivative.
5. **Operational Result**: Zero data loss, zero customer lockout, and **zero support tickets or manual tasks for the System Owner**.

---

### 9.4 Privacy-Preserving Collaborative Analytics & Trophies

The platform achieves a mathematical dual-state: **Absolute Private Isolation vs. Collective Market Analytics**:

| System Domain | Privacy Mechanism | System Owner Visibility | Counterparty Visibility |
| :--- | :--- | :--- | :--- |
| **Corporate Deal Terms** | Envelope AES-256-GCM | ❌ Scrambled Ciphertext | Only authorized invited banks on that specific leg |
| **Bank Relative Ranking** | Zero-Knowledge Blinded Percentiles | ❌ No raw spreads revealed | Bank sees relative rank (`#1 of 4`, `Top Quartile`) without seeing competitor rates |
| **Market Spread Trend** | Aggregate Differential Privacy ($\epsilon$-Noise) | ✅ Trend & median curve | Corporate views market spread vs. interbank mid |
| **Dealer Achievements** | One-Way Tokenized Telemetry (Win counts, speed ms, streaks) | ✅ Trophies & gamification metrics calculated accurately | Dealer sees unlocked personal trophies; rivals see zero deal data |

---

### 9.5 Harmonized Execution Sequence Across All Roadmap Phases

To ensure development proceeds in strict logical order without circular dependencies, the implementation roadmap is harmonized into the following unified milestones:

```
[ Phase 1 & 2 ] Counterparty Security, Domain Integrity & OTP Hardening (Completed ✅)
      │
      ▼
[ Phase 6.1 - 6.4 ] Selective Leg Quoting, Pass Leg & Uncontested Deal Governance (In Progress)
      │
      ▼
[ Phase 3 ] Multi-Leg "Invisible" Legs (Selective Counterparty Exclusion)
      │
      ▼
[ Phase 7 ] Platform Owner Diagnostics Console & Dealer Voice System
      │
      ▼
[ Phase 4 ] Smart Counterparty Intelligence & Dynamic Recommendation Engine
      │
      ▼
[ Phase 6.5 ] Live Market Benchmark (FXStreet / XE Interbank Mid Feed)
      │
      ▼
[ Phase 8 ] Zero-Knowledge Architecture & Privacy-Preserving Collaborative Analytics
      │   ├─ Stage 1: $0.00 Self-Contained Envelope Encryption & Automated Email Recovery
      │   └─ Stage 2: Seamless Multi-Tenant Cloud KMS Plug-in ($5–$15/mo)
      │
      ▼
[ Phase 5 ] Institutional Cyber Security, WORM Auditing & Bank Compliance Attestation
```

---

### 9.6 The 10-Dimension Complete Systemic Audit & Safety Matrix

To guarantee that **nothing** is overlooked across the entire platform ecosystem, Phase 8 is governed by a strict **10-Dimension Operational Checklist**. This expands beyond frontend and database to include asynchronous cron tasks, email templates, audit trails, and search indexing:

| Dimension | System Touchpoints | Architectural Mitigation & Safety Rule | Verification Check |
| :--- | :--- | :--- | :--- |
| **1. Authentication & Session Lifecycles** | `POST /login`, `POST /refresh`, JWT tokens | - Session tokens embed unsealed session key in memory-only context.<br>- Zero key writing to localStorage or browser cookies.<br>- Sliding token expiration handles active session renewals cleanly. | Verify login works seamlessly with 0 added user clicks or friction. |
| **2. Role-Based Access Control (RBAC)** | `corporate_admin`, `approver`, `execution`, `view_only` | - Permission trees are maintained independently of data encryption.<br>- Envelope access mirrors existing company organization tree.<br>- Revoking a user's role instantly revokes their envelope access. | Verify `view_only` users cannot decrypt deal execution approval actions. |
| **3. Database Queries & Historical Data** | PostgreSQL tables: `quotations`, `quotation_offers`, `quotation_legs` | - **Dual-Read Compatibility Layer**: Application reads both unencrypted legacy rows and newly encrypted rows without error.<br>- **Idempotent Background Worker**: Backfills and encrypts historical data in 50-row chunks during live operations with zero downtime.<br>- Legacy plaintext columns are dropped only after 100% verification. | Run query comparing legacy rows vs newly encrypted rows; outputs must match to 6 decimal places. |
| **4. Aggregations, Reports & Dashboards** | PDF export, Excel summaries, monthly savings reports | - **Compute-on-Decryption Pattern**: Treasury reports fetch encrypted rows, decrypt in application memory using the tenant session key, and generate reports on-the-fly.<br>- The database never needs to run plaintext math over confidential fields. | Export a 6-month historical Treasury report; confirm exact totals and currency conversions match. |
| **5. Frontend UI/UX & Live Quotation Flows** | `ResultsView.js`, `QuotationRequestDashboard.js`, `QuotationBankOfferPage.js` | - Zero visual changes: screens continue rendering identical typography, currency cards, and charts.<br>- Bank dealers continue using single-use OTP links without needing customer keys.<br>- Instant auto-transition upon deal resolution preserved. | Execute a live 2-minute competitive tender; verify zero UI lag or layout shift. |
| **6. Asynchronous Background Jobs & Cron Tasks** | `quotation_reminder_service.py`, auto-timeout tasks, deal expiration | - **System Service Context**: Background daemons operate under a scoped System Context. They evaluate non-confidential operational columns (`status`, `expires_at`, `is_accepted`, `leg_id`) to manage timeouts and reminders without needing to decrypt financial rates or spreads. | Verify auto-expiration and reminder dispatch function perfectly without decrypting prices. |
| **7. Outbound Notifications, Emails & Webhooks** | `unified_email_builder.py`, SMTP dispatch, deal confirmation slips | - Emails are formatted in-memory at execution time when the transaction is processed.<br>- Encrypted deal receipts are signed with HMAC-SHA256 tokens.<br>- Email logs stored in DB sanitize raw rates so server logs never retain plaintext deal terms. | Dispatch deal award email; verify recipient sees clean confirmation and database email log stores sanitized payload. |
| **8. Search, Filtering, Sorting & Pagination** | Search by Currency, Bank, Status, Date, Deal ID | - **Blind Indexing**: Searchable non-confidential metadata (`currency_pair`, `status`, `created_at`, `counterparty_id`) remains indexed in plaintext/hashes for ultra-fast SQL sorting.<br>- Numeric price range filtering is executed in-memory after tenant retrieval. | Search past deals by "EUR/EGP" and filter by "Awarded"; verify sub-50ms query response. |
| **9. Audit Trails & Forensic Compliance** | `AuditLog` table, WORM forwarding, Super Admin views | - Audit logs record *who* performed *what action* (e.g. `ACCEPTED_OFFER`, `MODIFIED_BANK_DESK`) without logging raw confidential price spreads.<br>- State transitions are fully auditable while maintaining zero-knowledge data isolation. | Review audit trail for executed deal; confirm complete forensic traceability with zero rate exposure. |
| **10. Disaster Recovery & Self-Contained Maintenance** | Password resets, account recovery, DB backups | - Rate-limited self-service email recovery links (with cooldown timers).<br>- Automated envelope re-sealing without data loss.<br>- Encrypted DB backups are completely useless if stolen, protecting customer confidentiality. | Perform an end-to-end "Forgot Password" flow; verify user logs in with new password and historical deals remain 100% accessible. |

