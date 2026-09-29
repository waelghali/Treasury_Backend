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

### 2.2 Automated Domain Consistency & Enrichment
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
