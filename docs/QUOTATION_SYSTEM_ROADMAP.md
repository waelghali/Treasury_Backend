# Grow Treasury — Quotation Module Architectural Roadmap

*Last Updated: September 2026*  
*Status: Living Strategic & Technical Roadmap*

---

## 📌 Table of Contents
1. [Executive Summary & Vision](#1-executive-summary--vision)
2. [Production Database Migration Ledger (Manual Production DDL)](#-production-database-migration-ledger-manual-production-ddl)
3. [Phase 1: Counterparty Integrity & Security Controls (Completed)](#2-phase-1-counterparty-integrity--security-controls-completed)
4. [Phase 2: Bank Protection & Cryptographic Security Hardening (Completed)](#3-phase-2-bank-protection--cryptographic-security-hardening-completed--verified-)
5. [Phase 3: Multi-Leg "Invisible" Legs — Selective Counterparty Exclusion (Completed)](#4-phase-3-multi-leg-invisible-legs--selective-counterparty-exclusion-completed--verified-)
6. [Phase 4: Smart Counterparty Intelligence & Dynamic Recommendation Engine (Completed)](#5-phase-4-smart-counterparty-intelligence--dynamic-recommendation-engine-completed--verified-)
7. [Phase 5: Institutional Banking Cyber Security & Compliance Readiness](#6-phase-5-institutional-banking-cyber-security--compliance-readiness-enterprise-onboarding-track)
8. [Phase 6: Selective Leg Quoting, Uncontested Deal Governance & Unified "Skipped Bank" Architecture (Completed)](#7-phase-6-selective-leg-quoting-uncontested-deal-governance--unified-skipped-bank-architecture)
9. [Phase 7: Platform Owner Diagnostics Console & Dealer Voice System (Completed)](#8-phase-7-platform-owner-diagnostics-console--dealer-voice-system)
10. [Workflow Governance, Dual Notifications & Performance Hardening (Completed)](#8-workflow-governance-maker-checker-dual-channel-notifications--real-time-performance-hardening-completed--verified-)
11. [Phase 8: Zero-Knowledge Architecture & Privacy-Preserving Collaborative Analytics (Current Core Focus)](#9-phase-8-zero-knowledge-architecture--privacy-preserving-collaborative-analytics)

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
| **Phase 6.6** | `quotation_rfqs`, `quotation_legs` | Add `market_benchmark_snapshot` JSONB | ```sql<br>ALTER TABLE quotation_rfqs<br>ADD COLUMN IF NOT EXISTS market_benchmark_snapshot JSONB;<br>ALTER TABLE quotation_legs<br>ADD COLUMN IF NOT EXISTS market_benchmark_snapshot JSONB;<br>``` | ```sql<br>SELECT column_name, data_type<br>FROM information_schema.columns<br>WHERE table_name = 'quotation_rfqs' AND column_name = 'market_benchmark_snapshot';<br>``` |
| **Phase 6.6** | `quotation_market_rate_history` | Create Time-Series Archive Table | ```sql<br>CREATE TABLE IF NOT EXISTS quotation_market_rate_history (<br>&nbsp;&nbsp;id SERIAL PRIMARY KEY,<br>&nbsp;&nbsp;currency_pair VARCHAR(10) NOT NULL,<br>&nbsp;&nbsp;base_currency VARCHAR(5) NOT NULL,<br>&nbsp;&nbsp;quote_currency VARCHAR(5) NOT NULL,<br>&nbsp;&nbsp;rate DOUBLE PRECISION NOT NULL,<br>&nbsp;&nbsp;source VARCHAR(50) NOT NULL,<br>&nbsp;&nbsp;cbe_official_mid DOUBLE PRECISION,<br>&nbsp;&nbsp;cbe_gap_bps DOUBLE PRECISION,<br>&nbsp;&nbsp;created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL<br>);<br>CREATE INDEX IF NOT EXISTS idx_quotation_mkt_rate_pair_created<br>ON quotation_market_rate_history (currency_pair, created_at DESC);<br>``` | ```sql<br>SELECT count(*)<br>FROM quotation_market_rate_history;<br>``` |

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

## 4. Phase 3: Multi-Leg "Invisible" Legs — Selective Counterparty Exclusion (Completed & Verified ✅)

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

## 5. Phase 4: Smart Counterparty Intelligence & Dynamic Recommendation Engine (Completed & Verified ✅)

### 5.1 Objective
Enhanced `@router.get("/recommendations")` from a simple volume counter into a **predictive, multi-dimensional Counterparty Recommendation Engine** that guides the corporate user to invite the highest-probability, best-pricing banks for every specific deal.

### 5.2 Implemented Evaluation Dimensions & Features
- **Direct Currency Pair Win Rate & Affinity**: Evaluates historical wins per currency pair; dynamically awards `⭐ {Pair} Specialist ({X} of {Y} won)` or `⭐ Proven in {Pair}`.
- **Competitive Proximity & Runner-Up Index (Metric A)**: Computes when a bank finished in the Top 2 or quoted within tight spread tolerances, awarding `🎯 Tight Competitor (Top 2 in {X}% of quotes)`.
- **Ticket Size Appetite & Volume Sweet Spots (Metric B)**: Analyzes institutional balance sheet performance on tickets $\ge \$1\text{M}$, awarding `🏛️ Mega-Ticket Dominance` or `🏛️ Large-Ticket Proven`.
- **Multi-Leg Basket Coverage**: Recognizes counterparties capable of pricing entire multi-pair portfolios (`📦 Full Basket Quoter`).
- **Pure Participation Rate**: Tracks commitments delivered without penalizing strategic late-window quoting (`⚡ Highly Active ({X}% participation)` vs `⚠️ Low Response Rate`).
- **Dynamic 100-Point Scoring Model**: Automatically factors pair affinity (40%), top-2 rate (35%), participation (25%), mega-ticket bonus (+10), and non-response penalty (-15).
- **Frontend Wizard Integration ([`QuotationRequestDashboard.js`](file:///c:/Grow/frontend/src/pages/EndUser/Quotations/QuotationRequestDashboard.js))**:
  - Dynamically recalculates recommendations as pairs, amounts, and settlement dates change.
  - Highlights `Top Pick` with spark icons (`Sparkles`).
  - Renders color-coded badges for Specialist (`⭐`, emerald), Competitor (`🎯`, indigo), Mega-Ticket (`🏛️`, purple), and Low Response (`⚠️`, amber).

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
    - **Dealer Full Pass (Option 1 Adopted)**: Allowed dealers to pass on all legs and transmit a complete pass. Dynamic button styles to slate `✕ Submit Pass on All Legs` (or `✕ Update Pass on All Legs`), prompts with safety confirmation modal before transmitting, and dispatches backend audit trail and notifications with zero rates.

- **Technical Findings & Gotchas**:
  - **Database Column Prerequisite**: SQLAlchemy ORM lazy loading immediately raises `UndefinedColumn: column quotation_bank_leg_configs.is_passed does not exist` when accessing relationships if the database column is missing. Executed clean direct `ALTER TABLE` without throwaway migration files.
  - **Fat-Finger Guard Nuance**: The cross-leg synthetic cross-rate swap detection and individual 10x deviation checks in `handleBatchSubmit` must evaluate only active quoted legs (`quotesToSubmit`), ignoring passed legs so dealers can pass legs without triggering false anomaly modals.
  - **Dealer Decline Scope**: The "Decline Participation" button is strictly scoped to internal bank `APPROVER` roles during pre-trade reviews. Permitting execution dealers to submit a full pass via `[ Submit Pass on All Legs ]` provides a unified, intuitive UX without requiring separate decline workflows.

- **Verification Proof**:
  - `python -m py_compile` passed on all backend models and schemas with zero errors.
  - Frontend production build (`craco build`) passed cleanly (`main.01500639.js`, code 0).
  - Local commits: Backend `4d32ad9`, `dd4ce11`, `ffa6271`; Frontend `09d194c`, `81366fe`. Live API and public portal verified.

---

#### 📌 Phase 6.2: Corporate Results View — "Skipped Bank" Audit Matrix (Completed & Verified ✅)
*Scope: Corporate evaluation dashboard, leg comparison tables, and audit logs.*

- **Work Actually Done**:
  - **Backend Aggregation (`compute_rfq_standings` in [`app/api/v1/endpoints/quotations_endpoints.py`](file:///c:/Grow/app/api/v1/endpoints/quotations_endpoints.py))**:
    - Injected `"is_passed": bool(getattr(cfg, 'is_passed', False)) if cfg else False` in unquoted bank leg results.
    - Set `"is_passed": False` for active quote submissions.
  - **Frontend Counterparty Cards ([`ResultsView.js`](file:///c:/Grow/frontend/src/pages/EndUser/Quotations/ResultsView.js))**:
    - Subtitle Header: Displays `Passed by Dealer • Declined to quote this pair` when `result.is_passed === true`.
    - Status Badge: Displays a distinct, elevated slate pill:
      - `Passed on this leg` with a neutral slate dot indicator.
      - Accompanying caption: `Declined to quote by dealer`.
    - Retains full backward compatibility with `Declined by Bank` (Approver decline), `No Offer Received` (Window closed without response), and `Awaiting Submission` (Active bidding in progress).
  - **Trophy & Accolade Safety Audit**:
    - Performed a mathematical audit on the Dealer Accolade Engine ([`dealer_achievement_service.py`](file:///c:/Grow/app/services/dealer_achievement_service.py)).
    - Confirmed that passed legs do not inject dummy rows into `QuotationOffer`, preventing false inflation of pricing velocity trophies (`Swift Quoting`) and ensuring 100% accurate win streaks (`Unbroken Victor`) and volume metrics (`Liquidity Titan`).

- **Technical Findings & Gotchas**:
  - `compute_rfq_standings` is invoked in both live evaluation and deal acceptance workflows; maintaining `is_passed` as an explicit boolean dictionary key ensures downstream consumers never encounter `KeyError` or falsy misclassifications.

- **Verification Proof**:
  - `python -m py_compile` passed on `quotations_endpoints.py` with zero errors.
  - Frontend production build (`craco build`) compiled cleanly (`main.ca0d065c.js`, code 0).
  - Local commits: Backend `994a77c`; Frontend `0e81270`.

---

#### 📌 Phase 6.3: Uncontested / Single-Quote Monopoly Detection & UI Warning (Completed & Verified ✅)
*Scope: Algorithmic detection of sole-counterparty legs across single-leg, multi-leg, and T-Bill tenders, and visual risk advisories.*

- **Work Actually Done**:
  - **Backend Detection Engine (`compute_rfq_standings` in [`quotations_endpoints.py`](file:///c:/Grow/app/api/v1/endpoints/quotations_endpoints.py))**:
    - **Single-Leg & Multi-Leg FX Baskets**: Evaluates active execution bids per leg (`len(execution_bids) == 1`). If exactly one bank provided a valid execution quote, sets `leg_is_uncontested = True` and populates `leg_uncontested_reason` explaining sole-source status and absence of competing market spread.
    - **T-Bill Auctions**: Evaluates valid execution offers (`len(valid_exec_tbills) == 1`), setting `is_uncontested = True` and detailed reason.
    - **Root Standings Response**: Exposes `is_uncontested` and `uncontested_reason` at both the tender root level and inside each individual leg object in `legs_data`.
  - **Deal Acceptance Engine (`poll_pending_deal_acceptance` in [`deal_acceptance_service.py`](file:///c:/Grow/app/services/deal_acceptance_service.py))**:
    - Propagates `is_uncontested` and `uncontested_reason` into each leg summary and the top-level deal dictionary of `urgent_deals`.
  - **Frontend Corporate Evaluation View ([`ResultsView.js`](file:///c:/Grow/frontend/src/pages/EndUser/Quotations/ResultsView.js))**:
    - State hydration: Stores `isUncontested` and `uncontestedReason` in `resultsMeta`.
    - **Deal Acceptance Banner**: Displays an amber Sole-Source Advisory callout if any awarded leg received only a single quote, warning treasury before execution.
    - **Deal Acceptance Leg Checklist**: Renders a dedicated `⚠️ Single Quote` warning badge alongside the awarded rate and winner name on uncontested legs.
    - **Multi-Leg "ALL" View**: Renders prominent `Sole-Source Advisory (Single Quote Received)` warning callout banner between the leg header and counterparty rows.
    - **Multi-Leg Tabbed View**: Renders `Sole-Source Advisory` banner for the currently selected leg.
    - **Single-Leg Tender View**: Renders `Sole-Source Advisory` banner across the counterparty list when uncontested.
  - **Global Deal Acceptance Modal ([`GlobalDealAcceptanceModal.js`](file:///c:/Grow/frontend/src/components/Quotations/GlobalDealAcceptanceModal.js))**:
    - Top-Level Advisory Banner: Renders sole-source advisory warning if the deal or any leg is uncontested.
    - Single-Leg Rate Card: Displays `⚠️ Single Quote Received` badge.
    - Multi-Leg Basket Checklist: Renders `⚠️ Single Quote` badge beside currency pair info on uncontested legs.

- **Technical Findings & Gotchas**:
  - Uncontested monopoly detection operates comprehensively regardless of the underlying cause: whether due to dealer explicit pass, competitor timeout, approver decline, or initial single-bank invitation.
  - Filtering strictly by `(quotation_base or 'Execution').lower() == 'execution'` ensures that indicative or market-intelligence benchmarks do not falsely mask sole-source firm execution monopolies.

- **Verification Proof**:
  - `python -m py_compile` passed on `quotations_endpoints.py` and `deal_acceptance_service.py` with zero errors.
  - Frontend production build (`craco build`) compiled cleanly (`main.9b9adf15.js`, code 0).
  - Local commits: Backend `79de5aa`; Frontend `d3ee69f`. Live API and UI verified.

---

#### 📌 Phase 6.4: Corporate Governance & Legal Delegation Consent Modal (Completed & Verified ✅)
*Scope: Customer configuration governance, high-importance legal consent dialog, invariant business rules, cascade resets, and auto-accept execution engine.*

- **Work Actually Done**:
  - **Database & Enum Schema**:
    - Added `AUTO_ACCEPT_SINGLE_QUOTE` to PostgreSQL `globalconfigkey` enum via `ALTER TYPE globalconfigkey ADD VALUE 'AUTO_ACCEPT_SINGLE_QUOTE'`.
    - Seeded `GlobalConfiguration` record (id: 112, key: `AUTO_ACCEPT_SINGLE_QUOTE`, default: `'false'`, unit: `'boolean'`, module_tags: `['quotation', 'quotations']`).
    - Added `AUTO_ACCEPT_SINGLE_QUOTE = "AUTO_ACCEPT_SINGLE_QUOTE"` to `GlobalConfigKey` in [`app/constants.py`](file:///c:/Grow/app/constants.py).
  - **Strict Business Rule Invariant (`crud_config.py` & `corporate_admin.py`)**:
    - **Invariant Enforced**: `if QUOTATION_ACCEPTANCE_DEFAULT_ACTION = AUTO_REJECT then AUTO_ACCEPT_SINGLE_QUOTE cannot be set to true ever`.
    - Attempting to set `AUTO_ACCEPT_SINGLE_QUOTE = true` when default action is `AUTO_REJECT` raises an immediate `HTTPException(400, "AUTO_ACCEPT_SINGLE_QUOTE cannot be set to true when QUOTATION_ACCEPTANCE_DEFAULT_ACTION is set to AUTO_REJECT.")`.
    - **Cascade Reset**: When `QUOTATION_ACCEPTANCE_DEFAULT_ACTION` is switched to `AUTO_REJECT`, `AUTO_ACCEPT_SINGLE_QUOTE` is automatically reset to `'false'` in both backend database and frontend client state, accompanied by an immutable audit log entry.
  - **Quotation Window Expiry Execution Engine (`quotations_endpoints.py`)**:
    - Placed timeout evaluation after full standings computation so uncontested monopoly status is known with 100% precision.
    - If `acceptance_timeout_action == "AUTO_ACCEPT"`:
      - Checks `AUTO_ACCEPT_SINGLE_QUOTE` configuration.
      - If uncontested single quote AND `AUTO_ACCEPT_SINGLE_QUOTE == False`:
        - **Halts automated execution!** Leaves deal pending corporate review, sets revision note: *"Automated execution halted: sole-source uncontested quote received. Corporate treasury manual approval required by governance policy."*
        - Logs audit trail: `QUOTATION_AUTO_ACCEPT_HALTED_SINGLE_QUOTE`.
      - If competitive $\ge 2$ quotes OR `AUTO_ACCEPT_SINGLE_QUOTE == True`:
        - Executes `AUTO_ACCEPTED` normally.
    - If `acceptance_timeout_action == "AUTO_REJECT"`:
      - Auto-rejects on timeout as configured.
  - **Deal Acceptance Alert Polling (`deal_acceptance_service.py` & `GlobalDealAcceptanceModal.js`)**:
    - Identifies halted deals and passes `is_auto_accept_halted = True`.
    - In `GlobalDealAcceptanceModal.js`, displays distinct amber badge: `Auto-Accept Halted (Manual Sign-Off Required)`.
  - **High-Importance Dual-Confirmation Consent Modal ([`QuotationAutoAcceptConsentModal.js`](file:///c:/Grow/frontend/src/components/Modals/QuotationAutoAcceptConsentModal.js))**:
    - Prompts whenever turning ON `AUTO_ACCEPT` or `AUTO_ACCEPT_SINGLE_QUOTE`.
    - Displays institutional legal notice: administrative execution delegation vs. commercial decision. Organization retains 100% sole responsibility for price risk and counterparty selection.
    - Mandates non-prechecked confirmation checkbox before enabling action button.
    - Records immutable `QUOTATION_LEGAL_CONSENT_ACKNOWLEDGED` entry in `AuditLog` capturing user ID, corporate email, IP address, and legal text.
  - **Corporate Admin Settings UI ([`CustomerConfigurationManagementPage.js`](file:///c:/Grow/frontend/src/pages/CorporateAdmin/CustomerConfigurationManagementPage.js))**:
    - Dynamically evaluates `QUOTATION_ACCEPTANCE_DEFAULT_ACTION`.
    - When default action is `AUTO_REJECT`, the `AUTO_ACCEPT_SINGLE_QUOTE` switch is visually locked and disabled with tooltip: *"Locked: Cannot be enabled when Quotation Acceptance Default Action is set to Auto-Reject"*.
    - Value column displays `Disabled (Auto-Reject Active)`.
    - Switching to `AUTO_ACCEPT` unlocks the toggle for configuration via the Legal Consent Modal.

- **Verification Proof**:
  - `python -m py_compile` passed on all modified backend files with zero errors.
  - Dedicated Python invariant test suite executed cleanly:
    - `AUTO_REJECT` $\rightarrow$ setting `AUTO_ACCEPT_SINGLE_QUOTE = true` threw 400 Bad Request ✅.
    - `AUTO_ACCEPT` $\rightarrow$ setting `AUTO_ACCEPT_SINGLE_QUOTE = true` succeeded ✅.
    - Switching back to `AUTO_REJECT` $\rightarrow$ cascade reset `AUTO_ACCEPT_SINGLE_QUOTE` to `false` ✅.
  - Frontend production build (`craco build`) compiled cleanly (`main.d5777097.js`, code 0).
  - Local commits: Backend `14d240d`; Frontend `7b9072a`. Live API and UI verified.

---

#### 📌 Phase 6.5: Live Market Benchmark Integration (Interbank Mid Reference & Empirical Spread Engine) (Completed & Verified ✅)
*Scope: Replacing static CBE post-close benchmark with live intraday reference rates and customer-specific historical empirical fair-value models.*

- **Live Data Feed Integration**:
  - Connected real-time interbank spot feed via resilient multi-tier architecture (`LiveMarketService` with 60s memory cache and CBE database fallback).
  - Implemented **Intraday CBE Fixing Drift Tracker**: dynamically calculates gap between Live Mid and official CBE daily fixing (`cbe_gap`, `cbe_gap_bps`, `cbe_gap_pips`).
- **Dynamic Empirical Historical Spread Engine**:
  - Implemented 3-level hierarchical historical lookup:
    1. **Level 1 (Highest Fidelity)**: Customer ID + Currency Pair + Volume Tier (`TIER_1` <$250k, `TIER_2` $250k–$1M, `TIER_3` >$1M).
    2. **Level 2 (Customer Aggregate)**: Customer ID + Currency Pair across all historical tickets.
    3. **Level 3 (Platform Benchmark)**: Platform-wide anonymized historical tenders.
  - Automatically derives **Suggested Reference Rate** (`Live Mid + Historical Mean Spread`) when sample size $N \ge 3$, with graceful cold-start mode for new pairs.
  - Compares incoming bank quotes or winning bids against the empirical suggested reference to evaluate market tightness (`Tighter than Historical Norm`, `Consistent with Historical Norm`, `Wider than Historical Norm`).
- **Mandatory Governance & Legal Disclaimer**:
  - Automatically attached to all API payloads and prominently displayed across the UI:
    > *"⚠️ Historical Empirical Model: Benchmarks & suggested reference rates are derived mathematically from live feeds & historical platform executions. Indicative only — does not replace customer verification or internal compliance policies."*
- **UI & Quoting Terminal Integration**:
  - **Dealer Quoting Terminal (`QuotationBankOfferPage.js`)**: Real-time Live Mid ticker badge with pulsing status dot, CBE drift badge, and live Spread / Pip Delta tracker calculating dynamically as the dealer enters their quote rate.
  - **Deal Acceptance Modal (`GlobalDealAcceptanceModal.js`)**: Executive HUD Box 3 and leg rows display Live Mid, CBE Drift, Suggested Reference Rate, and the historical tightness evaluation.
  - **Corporate Results View (`ResultsView.js`)**: Multi-leg and single-leg results cards display complete benchmark breakdown and governance disclaimer.
- **Verification & Testing Criteria**:
  - Verified live USD/EGP mid rates pull in real-time (`52.2297`) with automated CBE fallback (`52.2944`).
  - Verified Intraday CBE drift calculation (`-12.37 bps`).
  - Verified historical sample aggregation across 14+ customer tenders yielding accurate empirical suggested reference rates (`50.9186`).
  - Verified frontend build passes with zero errors (`main.8b1b1f11.js`).

---

#### 📌 Phase 6.6: Market Benchmark Governance Hardening, Dealer Blind Quoting & Trade Execution Freeze (Completed & Verified ✅)
*Scope: Remove internal customer expectations and suggested reference rates from bank dealer portals, preserve universal benchmark on corporate side, permanently freeze market benchmarks on deal acceptance, and archive live spot rates into a dedicated historical database.*

- **Context & Operational Rationale**:
  - Exposing the customer's Suggested Reference Rate or spread delta calculations to bank counterparties creates an anti-competitive leak ("showing cards" to the bidding bank). Bank desks must quote blindly and independently without seeing internal customer benchmarks.
  - Comparing a sealed deal to moving market rates weeks or months after acceptance invalidates historical audit trails. The exact market mid, CBE drift, and spread must be snapshotted and frozen at the second of trade execution.
  - Archiving every fetched spot rate builds a proprietary time-series market database for the platform owner to power future analytical and AI models.
- **Architectural & Security Controls**:
  - **Dealer Blind Quoting**: Bank portals (`QuotationBankOfferPage.js`) only show the official CBE Mid benchmark if available. The Suggested Reference Rate, Delta Tracker, and customer expectation indicators were removed entirely.
  - **Public API Sanitization**: `GET /api/v1/public-quotation/{token}` explicitly returns `market_benchmark: null` to prevent any backend leak of customer reference models to quoting desks.
  - **Universal Corporate Benchmark**: Corporate acceptance modals and results views maintain the Live Interbank Mid, CBE Drift, and Historical Spread across **ALL deals** (both competitive multi-bank and sole-source single-bank tenders).
  - **Trade Execution Snapshot Freeze**:
    - Added `market_benchmark_snapshot` (`JSONB`) to `quotation_rfqs` and `quotation_legs`.
    - When `execute_shared_deal_acceptance` is triggered, the exact live mid, suggested reference, and winning quote spread are snapshotted into `market_benchmark_snapshot` before database commit.
    - Subsequent inquiries in `compute_rfq_standings` and the acceptance modal load the frozen snapshot with a `🔒 Locked at Acceptance` indicator, preventing post-trade drift.
    - **Permanent Historical Rate Audit in RFQ Details Modal**: Embedded a dedicated **"Market Reference & Regulatory Rate Audit"** 4-pillar card in [`ResultsView.js`](file:///c:/Grow/frontend/src/pages/EndUser/Quotations/ResultsView.js), allowing corporate treasurers, makers, and auditors to inspect the frozen Central Bank of Egypt (CBE) fixing, live market mid at execution, calculated reference expectation, and executed quoted rate with green/red status anytime they refer back to past tenders.
  - **Proprietary Spot Rate Archive**:
    - Created `quotation_market_rate_history` table (`QuotationMarketRateHistory` model) recording `currency_pair`, `base_currency`, `quote_currency`, `rate`, `source`, `cbe_official_mid`, and `cbe_gap_bps`.
    - Continuous archiving in `live_market_service.py` safely throttled to 60-second intervals per currency pair.
- **Database Ledger (Production DDL)**:
  - `ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS market_benchmark_snapshot JSONB;`
  - `ALTER TABLE quotation_legs ADD COLUMN IF NOT EXISTS market_benchmark_snapshot JSONB;`
  - `CREATE TABLE IF NOT EXISTS quotation_market_rate_history (...);`
- **Verification Proof**:
  - `verify_live_api.py` confirmed `market_benchmark: null` across public bank portal API.
  - `verify_archive.py` verified live spot rate insertion into `quotation_market_rate_history`.
  - Frontend production build clean (`main.2b6ad714.js`).

---

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
  - Open console as Super Admin $\rightarrow$ select Banque Misr or CIB.
  - Confirm all 8 trophies reflect authentic database records.
  - Verify that clicking on `Unbroken Victor` shows the streak audit history explaining why the streak is at its current number.
  - Verify unauthorized roles (dealers, corporate clients) receive HTTP 403 Forbidden.
- **Implementation Status (Completed & Verified ✅)**:
  - **Backend**:
    - [`dealer_achievement_service.py`](file:///c:/Grow/app/services/dealer_achievement_service.py): Implemented `get_streak_audit_trail(db, bank_id)` providing chronological RFQ breakdown with state transitions (`+1 Clean Sweep`, `Reset to 0 Competitor Win`, or `Preserved Wash`).
    - [`system_owner.py`](file:///c:/Grow/app/api/v1/endpoints/system_owner.py): Enhanced `GET /api/v1/system-owner/quotation-diagnostics/bank-trophies` to aggregate and return comparative metrics for all 37 onboarded banks (`deals`, `vol_usd`, `quotes`, `streak`, `tenders`, `pairs`, `unlocked`, `tier`, `has_execution`), sort active desks by volume, compute `macro_summary` (7 active desks, $6.45M USD volume, 49 deals, 467 quotes), and default selection to market liquidity leader (Bank of Alexandria).
  - **Frontend**:
    - [`QuotationTelemetryDashboard.js`](file:///c:/Grow/frontend/src/pages/SystemOwner/QuotationTelemetryDashboard.js): 
      - Added dual-mode sub-navigation: **Counterparty Comparison Matrix** vs. **Single Desk 8-Trophies Deep Dive**.
      - Implemented **Macro Network Diagnostics Banner**: 4 KPI cards for Active Desks (7/37), Executed Volume ($6.45M USD), Deals Won (49), and Live Quotes Logged (467).
      - Added **Zero-Execution Counterparty Filtering**: Default `[✓] Hide 0-Execution Desks` filter automatically isolating the 7 active institutions with a toggle to inspect all 37 banks, plus live text search and multi-column sorting (Volume, Deals, Quotes, Trophies).
      - Built high-density **Comparative Leaderboard Table**: Side-by-side ranks (#1-#3 podium badges), bank names, dealer count, status pills, desk tiers, 8-trophy progress bars, awarded USD volume with market share %, deals won with win rate %, quotes, tenders, streaks, and 1-click `Inspect ➔` buttons.
      - Seamless deep-dive drilldown: Clicking any bank in the comparison matrix navigates directly to that institution's 8-trophy breakdown, with a prominent `‹ Return to Comparison Matrix` button and cleaned dropdowns grouping active vs. zero-execution desks.
  - **Verification Proof**:
    - Python API test verified macro summary and instant multi-bank metrics calculation in 0.83s across all 37 institutions.
    - Verified filtering: correctly isolates 7 active banks (Bank of Alexandria, CIB, HSBC, Banque Misr, The United Bank, NBE, SAIB) with $6.45M total volume, while cleanly hiding the 30 zero-execution institutions.
    - Frontend production build (`craco build`) compiled cleanly (`main.4494659f.js`, exit code 0).
    - Local commits: Backend `51de5aa`; Frontend `ffd231e`. Live UI and API verified.

---

#### 📌 Phase 7.2: Dealer Voice & Feedback Mechanism (Quotation Terminal) (Completed & Verified ✅)
*Scope: Institutional feedback widget, trader experience rating, and non-blocking toast prompt on the public quotation interface.*

- **Context & Operational Rationale**:
  - Giving bank dealers a direct channel to provide feedback, report system issues, or rate the terminal builds relationship goodwill and provides early warning for latency or usability bugs.
- **Critical Trading Desk Design Constraints**:
  - **Non-Interference with Live Trading**: FX and Treasury dealers operate under high stress during active quotation windows. The feedback mechanism must **never** block, modal-lock, or delay the quotation submission flow or countdown timers.
  - **100% Optional & Low Friction**: Must take under 5 seconds to complete.
- **Frontend Components ([`QuotationBankOfferPage.js`](file:///c:/Grow/frontend/src/pages/Public/QuotationBankOfferPage.js))**:
  - **1. Discreet Header Button**:
    - Sleek, high-contrast button placed alongside the trophy showcase and session badge:
      `[ 💬 Feedback ]`
    - Clicking opens a compact, floating feedback modal without affecting quoting timers.
  - **2. Optional Post-Quote Experience Toast**:
    - Triggers only *after* quotes are successfully submitted or when the quotation window has closed.
    - Displays a lightweight, elegant toast in the lower corner:
      > *"How was your quotation experience today? [ ★ ★ ★ ★ ★ ] (Optional)"*
    - Automatically dismisses after 8 seconds if ignored, without modal backdrop or interaction lock.
  - **3. Compact Feedback Modal**:
    - **Star Rating**: 1 to 5 interactive stars with amber glow.
    - **Category Pills**: `Rate Triangulation`, `Speed & Latency`, `UI Usability`, `Feature Request`, `Other`.
    - **Short Comment**: Multi-line textarea (max 300 characters, optional with live counter).
    - **Privacy Toggle**: `Include my name & bank` vs. `Submit Anonymously` (masks dealer name/email while preserving institutional counterparty association).
- **Backend API ([`public_quotations.py`](file:///c:/Grow/app/api/v1/endpoints/public_quotations.py) & [`models_quotation.py`](file:///c:/Grow/app/models/models_quotation.py))**:
  - `POST /api/v1/public-quotation/feedback`:
    - Validates quotation token session.
    - Persists `QuotationDealerFeedback` record (`quotation_dealer_feedbacks` table) storing `rfq_id`, `quotation_bank_id`, `bank_id`, `bank_name`, `dealer_email`, `dealer_name`, `is_anonymous`, `star_rating`, `category`, `comment`, `status`, `admin_notes`.
    - Session-level rate limiting (enforces max 3 submissions per tender session to prevent abuse).

---

#### 📌 Phase 7.3: Admin Feedback Inbox & Satisfaction Stream (Super Admin Portal) (Completed & Verified ✅)
*Scope: Platform owner feedback feed, sentiment tracking, and issue triaging in the Telemetry Dashboard.*

- **Console Features ([`QuotationTelemetryDashboard.js`](file:///c:/Grow/frontend/src/pages/SystemOwner/QuotationTelemetryDashboard.js))**:
  - **Tab 3 Sub-Navigation**: `💬 Dealer Voice & Satisfaction Stream` with live new-feedback counter badge.
  - **Macro CSAT KPI Banner**:
    - Platform CSAT (Average rating / 5.0 with star visualization).
    - Total Feedback Submissions.
    - Pending Triage Count (`NEW` status).
    - Resolved / Addressed Count (`RESOLVED` status).
  - **Multi-Filter & Search Toolbar**:
    - Real-time text search across dealer comments, bank names, and email addresses.
    - Status Filter (`ALL`, `NEW`, `IN_REVIEW`, `RESOLVED`, `ARCHIVED`).
    - Rating Filter (`ALL`, `5 Stars`, `4 Stars`, `3 Stars`, `1-2 Stars Alert`).
    - Category Filter (`ALL`, `Rate Triangulation`, `Speed & Latency`, `UI Usability`, `Feature Request`, `Other`).
    - Instant Refresh button.
  - **Interactive Satisfaction Stream Cards**:
    - Reverse-chronological cards displaying star ratings, category badges, anonymous masks vs. dealer email, and relative timestamps.
    - Interactive Status dropdown (`NEW` -> `IN_REVIEW` -> `RESOLVED` -> `ARCHIVED`) triggering instant status mutations.
    - Inline editable Operations Notes (`admin_notes`) allowing Super Admins to record follow-up logs with bank treasury heads.
- **Backend API ([`system_owner.py`](file:///c:/Grow/app/api/v1/endpoints/system_owner.py))**:
  - `GET /api/v1/system-owner/dealer-feedback`: Returns filtered reverse-chronological feedback stream and comprehensive `macro_csat` analytics (average rating, distribution 1-5, category breakdown, status breakdown).
  - `PATCH /api/v1/system-owner/dealer-feedback/{feedback_id}`: Updates feedback status (`NEW`, `IN_REVIEW`, `RESOLVED`, `ARCHIVED`), records resolver admin ID and timestamp upon resolution, and updates administrative notes.
- **Verification Proof**:
  - Backend compilation passed with `python -m py_compile` with zero errors.
  - Direct database test verified table creation, feedback insertion with anonymous toggle, and macro CSAT aggregation (5.0 average on initial test feedback).
  - Frontend production build (`craco build`) compiled cleanly (`main.ca148b6d.js`, exit code 0).
  - Local commits: Backend `17b01e6`; Frontend `ad6c031`. Live UI, APIs, and real-time dashboard verified.

---

## 8. Workflow Governance, Maker-Checker Dual-Channel Notifications & Real-Time Performance Hardening (Completed & Verified ✅)

### 8.1 Dual-Channel Approval Workflow Notification Engine
To guarantee institutional compliance and eliminate operational delays between corporate makers (treasury analysts) and checkers (corporate admins), the platform implements an automated dual-channel notification service (`app/services/quotation_approval_notifications.py`):

1. **Maker Submits Quotation for Approval (`QUOTATION_RFQ_PENDING_APPROVAL`)**:
   - Corporate Admins receive an in-app system notification **and** an immediate high-priority email alerting them to review and approve the deal, containing deal details and a direct 1-click review link.
2. **Admin Approves Quotation (`QUOTATION_RFQ_APPROVED`)**:
   - The creator/maker receives an in-app notification **and** a green success email with live or scheduled release confirmation and direct deal link.
3. **Admin Requests Revision (`QUOTATION_RFQ_NEEDS_REVISION`)**:
   - The maker receives an in-app notification **and** an amber alert email containing the administrator's exact feedback/notes and a direct link to edit and resubmit.
4. **Admin Rejects Quotation (`QUOTATION_RFQ_REJECTED`)**:
   - Mandates or records the admin's **Rejection Reason** in database records (`rejection_reason` / `admin_notes`) and audit trails.
   - The maker receives an in-app notification **and** an alert email detailing the exact Rejection Reason.
5. **Maker Resubmits Revised Quotation (`QUOTATION_RFQ_RESUBMITTED`)**:
   - Corporate Admins receive an in-app notification **and** an email showing the maker's updated package for re-review.

### 8.2 Maker Pre-Approval RFQ Editing & In-Place Resubmission
- **Pre-Approval In-Place Editing**: Makers are empowered to modify and update quotations that are in `PENDING_APPROVAL` status prior to admin review (previously restricted to `NEEDS_REVISION`).
- Resubmitting updates parameters in place, preserves reference numbers, and dispatches a fresh re-review alert to Corporate Admins.
- **Frontend Action Buttons**: Added direct **Edit Quotation** buttons in both [`QuotationHistoryDashboard.js`](file:///c:/Grow/frontend/src/pages/EndUser/Quotations/QuotationHistoryDashboard.js) and [`ResultsView.js`](file:///c:/Grow/frontend/src/pages/EndUser/Quotations/ResultsView.js).

### 8.3 Quotation Cloning & Parameter Unlocking Overhaul
- **100% Unlocked Parameter Editing**: Clicking "⚡ Clone as New Quotation" previously locked parameters like Direction, Amount, Legal Entity, Currencies, and Leg additions. Cloned templates are now 100% unlocked, allowing users to modify:
  - Trade Type (FX Spot vs T-Bills)
  - Requesting Legal Entity
  - Direction, Currencies, and Ticket Amounts
  - Settlement / Value Dates
  - Adding or removing multi-currency legs freely
- **Streamlined Results UX**: Eliminated duplicate header buttons when quotations conclude, standardize labeling to `⚡ Clone as New Quotation`.

### 8.4 Live Market Benchmark Performance Gating & Instant History Loading
- **Strict Benchmark Gating**:
  - Defined `can_fetch_live_market = bool(is_live_bidding or is_acceptance_open)` in `quotations_endpoints.py`.
  - Both leg-level and root-level market benchmark calculations now **only run** if the deal is currently live (`PENDING`, `OPEN`, `EVALUATING`) or the customer acceptance window is open.
  - Concluded, rejected, cancelled, and past deals **never trigger live rate fetches or empirical models** (they only display their preserved frozen snapshot if one exists).
- **In-Memory Cache & Network Resilience**:
  - Increased in-memory cache TTL in `live_market_service.py` from 60 seconds to **300 seconds (5 minutes)**.
  - Lowered external request timeout from 4s to 2s to guarantee worker threads are never held up.
- **Instant History Table Rendering**:
  - In `QuotationHistoryDashboard.js`, decoupled primary history loading from secondary statistics (`setLoading(false)` as soon as RFQ list returns). Table renders in milliseconds.
  - Restricted `MarketSpreadTicker` on the history page: only mounts when active live tenders are running.
- **Optimized Terminal State Polling**:
  - In `ResultsView.js`, background polling immediately idles once deals reach terminal states (`COMPLETED`, `CANCELLED`, `REJECTED`, `INCONCLUSIVE`, `EXPIRED`, or finalized deal acceptance).
  - Polling interval during active bidding or open acceptance relaxed from 1.5s to **3.0s**.

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
[ Phase 6.1 - 6.5 ] Multi-Pair FX Tender Advanced Quoting, Governance & Live Market Benchmark (Completed ✅)
      │
      ▼
[ Phase 4 ] Smart Counterparty Intelligence & Dynamic Recommendation Engine (Completed ✅)
      │
      ▼
[ Phase 7 ] Platform Owner Diagnostics Console & Dealer Voice System (Completed ✅)
      │
      ▼
[ Phase 3 ] Multi-Leg "Invisible" Legs (Selective Counterparty Exclusion) (Completed & Verified ✅)
      │
      ▼
[ Workflow & Performance Hardening ] Dual Approval Notifications, Unlocked Clone & Live Market Gating (Completed & Verified ✅)
      │
      ▼
[ Phase 8 ] Zero-Knowledge Architecture & Privacy-Preserving Collaborative Analytics ◄── (CURRENT UPCOMING CORE PHASE)
      │   ├─ Stage 1: $0.00 Self-Contained Envelope Encryption & Automated Email Recovery
      │   └─ Stage 2: Seamless Multi-Tenant Cloud KMS Plug-in ($5–$15/mo)
      │
      ▼
[ Phase 5 ] Institutional Cyber Security, WORM Auditing & Bank Compliance Attestation (Enterprise Onboarding Track)
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

---

## 10. Production Database Migration Ledger (Audit & Compliance Log)

This ledger tracks all production database schema states, DDL operations, and migration audits across the roadmap implementation:

| Timestamp (UTC) | Phase | Target Table(s) | Column(s) / Constraints | Migration Type | Status & Operational Impact |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **2026-10-05** | **Phase 3** | `quotation_bank_leg_configs` | `is_invited` (`BOOLEAN NOT NULL DEFAULT True`) | **Schema Audit / Verification** | **Verified Pre-Existing ✅**<br>Audited live PostgreSQL schema. `is_invited` was already present from initial model creation. **Zero DDL alterations / Zero migration scripts required.** |
| **2026-10-04** | **Phase 7** | `dealer_feedbacks` | `id`, `rfq_id`, `assignment_id`, `rating`, `comment`, `created_at` | DDL Migration | Active ✅ |
| **2026-10-03** | **Phase 2** | `quotation_access_otps` | `failed_attempts` (`INTEGER DEFAULT 0`), `hashed_otp` (`VARCHAR(64)`) | DDL Migration | Active ✅ |
| **2026-10-02** | **Phase 6** | `quotation_bank_leg_configs` | Multi-leg tariff overrides (`cost_min`, `cost_percent`, `cost_max`, `cost_flat`, `value_date`, `quotation_base`, `allow_alternative_value_date`) | DDL Migration | Active ✅ |

