# Grow Treasury — Quotation Module Architectural Roadmap

*Last Updated: September 2026*  
*Status: Living Strategic & Technical Roadmap*

---

## 📌 Table of Contents
1. [Executive Summary & Vision](#1-executive-summary--vision)
2. [Phase 1: Counterparty Integrity & Security Controls (Completed)](#2-phase-1-counterparty-integrity--security-controls-completed)
3. [Phase 2: Governance & Institutional Controls (Queued)](#3-phase-2-governance--institutional-controls-queued)
4. [Phase 3: Multi-Leg "Invisible" Legs — Selective Counterparty Exclusion](#4-phase-3-multi-leg-invisible-legs--selective-counterparty-exclusion)
5. [Phase 4: Smart Counterparty Intelligence & Dynamic Recommendation Engine](#5-phase-4-smart-counterparty-intelligence--dynamic-recommendation-engine)

---

## 1. Executive Summary & Vision

The Grow Quotation Module is built to give Corporate Treasuries institutional-grade control, confidentiality, and data-driven counterparty allocation during FX Spot, Multi-Leg Portfolios, and T-Bill competitive tenders.

This roadmap consolidates all architectural designs, threat models, and feature specifications agreed upon to ensure continuity across development cycles.

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

## 3. Phase 2: Governance & Institutional Controls (Queued)

### 2.1 Dynamic 4-Eyes Dual Approval (Maker-Checker)
- **Objective**: Prevent a rogue corporate officer from adding an unvetted bank contact and granting them trading execution rights without secondary approval.
- **Smart Fallback**:
  - If the organization has **$\ge$ 2 Corporate Admins**: Modifying bank counterparty contacts requires dual approval (Admin A proposes, Admin B confirms).
  - If the organization has **only 1 Corporate Admin**: Automatically auto-approves to eliminate administrative deadlock, but records an elevated risk flag in the audit logs and dispatches an alert email to the organization owner.

### 2.2 Cryptographic OTP Salted Hashing (Zero-Plaintext Storage)
- **Objective**: Ensure that 2FA OTP codes are never stored in plaintext in the database (aligned with NIST SP 800-63B & OWASP).
- **Mechanism**:
  - Store `HMAC-SHA256(otp_code, server_secret_salt)`.
  - Constant-time verification (`hmac.compare_digest`).
  - Total protection against database dumps, backups, or internal DB snooping.

### 2.3 Automated OTP Throttling & Self-Service Unlock (Zero Manual Intervention)
- **Objective**: Eliminate brute-force enumeration attacks against 6-digit OTP codes without requiring any administrative tickets or manual intervention.
- **Mechanism**:
  - Allow **max 3 consecutive incorrect OTP entries**.
  - On the 3rd failed attempt: The specific OTP code is burned (`is_used = True`) and permanently invalidated.
  - **Self-Service Instant Unlock**: The trader's account is NOT locked. The UI immediately displays:  
    `"Too many failed attempts. This code is no longer valid. [Request New Verification Code]"`.
  - A 30-second cooldown prevents spamming. Clicking the button immediately emails a brand new code to the trader's verified banking email. Zero admin tickets, zero friction.

### 2.4 Public Endpoint Rate Limiting (OWASP API4:2023 Abuse Protection)
- **Objective**: Prevent automated bots and scripted abuse against public OTP and token endpoints.
- **Mechanism**:
  - `POST /api/v1/public-quotation/request-otp`: Max **3 requests per 5 minutes** per assignment/IP (prevents inbox flooding and SMS/email API cost inflation).
  - `POST /api/v1/public-quotation/verify-otp`: Max **5 verification attempts per minute**.
  - Returns `429 Too Many Requests` with standard `Retry-After` header.

### 2.5 Bank-Specific Scoped Deal Execution Receipt
- **Objective**: Provide the winning bank with a downloadable, cryptographically verified Deal Confirmation Slip attached to their confirmation email and visible in the portal.
- **Strict Privacy Scope**: Must be dynamically tailored so that in multi-leg portfolios, the bank's receipt **strictly includes only the specific leg(s) they won**, maintaining 100% confidentiality of other package legs.

### 2.6 Automated Domain Consistency & Enrichment
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
