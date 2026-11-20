# app/core/legal_seed.py
"""
Canonical Legal Artifacts Baseline (v1.1 In-Place Multi-Module + Trial Sandbox Edition).

Guarantees:
1. Fresh databases (such as an isolated `grow_trial` PostgreSQL instance) automatically
   seed `terms_and_conditions` and `privacy_policy` at v1.1 on startup so public
   registration and authentication never 404.
2. Existing v1.1 databases carrying earlier v1.1 drafts are upgraded IN-PLACE at
   `version = 1.1` — preserving `id` and `version` so existing paying customers experience
   ZERO login popup disruption until a formal version bump (e.g. v1.2) is intentionally published.
"""

import logging
from sqlalchemy.orm import Session

from app.constants import LegalArtifactType

logger = logging.getLogger("app.legal_seed")

_LEGAL_REV_MARKER = 'data-legal-rev="v1.1-2026-11-multimodule"'

_GROW_LEAF_PATH = (
    "m 35.940217,38.500167 c -0.0102,-1.94922 -0.12121,-4.321 -0.24676,-5.27062 "
    "-0.53779,-4.06762 -0.55018,-3.90205 0.42854,-5.72498 1.42388,-2.6521 3.9536,-5.33517 "
    "6.57854,-6.97734 1.02194,-0.63932 0.45123,-0.73376 -0.80684,-0.13351 "
    "-2.28239,1.089 -5.1891,3.7869 -6.41699,5.95601 -0.33125,0.58518 -0.63363,1.06396 "
    "-0.67196,1.06396 -0.0383,0 -0.21906,-0.42937 -0.40164,-0.95416 "
    "-0.66852,-1.92156 -1.89713,-3.98345 -3.10208,-5.20601 -1.1931,-1.21054 "
    "-4.18646,-3.11616 -4.4663,-2.84332 -0.0777,0.0757 0.0759,0.26954 0.34133,0.43069 "
    "0.84423,0.51259 3.47557,2.99199 4.06585,3.83108 0.67504,0.95956 1.95503,4.56758 "
    "2.39143,6.74092 0.47178,2.34953 0.5769,5.56945 0.29933,9.16883 -0.28246,3.66278 "
    "-0.30658,3.58963 1.15743,3.50983 l 0.86861,-0.0473 z m -8.18508,-15.61123 "
    "c -1.95988,-0.20231 -3.05786,-1.05757 -5.15718,-4.01712 -1.26619,-1.78504 "
    "-2.02842,-2.47758 -3.41693,-3.10453 -1.08938,-0.49188 -1.12026,-0.52394 "
    "-0.66242,-0.68784 1.19655,-0.42834 4.34471,-0.93301 5.82019,-0.93301 2.17519,0 "
    "3.34261,0.35476 4.5607,1.38593 1.1149,0.94382 1.76073,1.89432 2.24296,3.30108 "
    "0.42958,1.25318 0.56452,1.12142 0.1617,-0.1579 -0.44703,-1.41969 -0.95823,-2.28173 "
    "-1.93787,-3.26785 -1.27225,-1.28065 -2.27163,-1.66214 -4.6262,-1.76593 "
    "-2.26402,-0.0998 -3.82854,0.11508 -6.58795,0.90482 -1.49881,0.42897 -1.97851,0.64126 "
    "-1.97851,0.87559 0,0.23662 0.2321,0.33522 0.98734,0.41944 1.99692,0.22269 "
    "3.56151,1.26336 5.05906,3.36499 2.07789,2.91607 3.42889,3.86462 5.4386,3.8185 "
    "l 1.15815,-0.0266 z m 15.57234,4.97727 c 1.83993,-0.77319 3.42321,-2.43328 "
    "5.71815,-5.99558 1.70556,-2.64745 2.7356,-3.84961 4.16575,-4.86185 1.383,-0.97886 "
    "2.93567,-1.57201 4.55918,-1.74169 1.126,-0.11769 1.15816,-0.136 1.15816,-0.65953 "
    "0,-0.51167 -0.0865,-0.56892 -1.73723,-1.14953 -4.87344,-1.71417 -7.66012,-2.28607 "
    "-11.09895,-2.27781 -2.64044,0.006 -4.0282,0.27845 -5.65937,1.10969 "
    "-1.19065,0.60675 -3.00117,2.42843 -3.87019,3.89406 -0.83792,1.41315 "
    "-1.79843,4.08086 -1.94933,5.41404 l -0.11315,0.9996 0.37262,-1.27222 "
    "c 1.0436,-3.56304 2.71139,-6.1418 4.93333,-7.62796 1.89712,-1.26891 2.6465,-1.44834 "
    "5.99655,-1.43583 2.45565,0.009 3.21784,0.0847 5.01866,0.49711 2.10333,0.48174 "
    "5.11517,1.35572 5.11517,1.48433 0,0.0367 -0.69548,0.40743 -1.54552,0.82382 "
    "-2.55861,1.25335 -3.7578,2.57959 -6.75457,7.47022 -1.51064,2.46532 -3.29949,4.2937 "
    "-4.84725,4.95439 -0.59091,0.25224 -1.66518,0.50618 -2.48771,0.58806 "
    "-1.41628,0.14098 -1.42885,0.1466 -0.57908,0.259 1.25416,0.16589 2.46295,0.008 "
    "3.60478,-0.47232 z"
)

_LEGAL_SCOPED_CSS = """
<style>
  .grow-legal-doc {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    color: #334155;
    font-size: 13px;
    line-height: 1.6;
  }
  .grow-legal-doc .grow-legal-masthead {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-top: 4px solid #0f172a;
    border-radius: 8px;
    padding: 16px 18px;
    margin-bottom: 18px;
  }
  .grow-legal-doc .grow-legal-brand-table {
    width: 100%;
    border-collapse: collapse;
    margin: 0 0 10px 0;
  }
  .grow-legal-doc .grow-legal-brand-table td {
    border: none;
    padding: 0;
    vertical-align: middle;
  }
  .grow-legal-doc .grow-legal-wordmark {
    font-size: 18px;
    font-weight: 800;
    color: #0f172a;
    letter-spacing: -0.02em;
    line-height: 1.1;
  }
  .grow-legal-doc .grow-legal-subbrand {
    font-size: 9px;
    font-weight: 700;
    color: #1d4ed8;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    margin-top: 2px;
  }
  .grow-legal-doc .grow-legal-doc-badge {
    display: inline-block;
    background: #eff6ff;
    color: #1e40af;
    border: 1px solid #bfdbfe;
    border-radius: 999px;
    padding: 3px 10px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .grow-legal-doc h1.grow-legal-title {
    margin: 6px 0 8px 0;
    padding: 0;
    font-size: 19px;
    font-weight: 800;
    color: #0f172a;
    line-height: 1.25;
  }
  .grow-legal-doc .grow-legal-meta-row {
    margin-top: 6px;
  }
  .grow-legal-doc .grow-legal-pill {
    display: inline-block;
    background: #ffffff;
    border: 1px solid #cbd5e1;
    border-radius: 999px;
    padding: 2px 9px;
    font-size: 11px;
    font-weight: 600;
    color: #334155;
    margin: 2px 6px 2px 0;
  }
  .grow-legal-doc h2 {
    margin: 18px 0 7px 0;
    padding: 2px 0 2px 10px;
    border-left: 3px solid #1e40af;
    font-size: 14px;
    font-weight: 700;
    color: #0f172a;
    line-height: 1.3;
  }
  .grow-legal-doc p {
    margin: 0 0 9px 0;
    color: #334155;
  }
  .grow-legal-doc ul {
    margin: 4px 0 11px 0;
    padding-left: 18px;
  }
  .grow-legal-doc li {
    margin-bottom: 5px;
    color: #334155;
  }
  .grow-legal-doc strong {
    color: #0f172a;
  }
  .grow-legal-doc a {
    color: #1d4ed8;
    text-decoration: underline;
  }
  .grow-legal-doc .grow-legal-callout {
    margin: 16px 0 4px 0;
    padding: 14px 16px;
    background: #f8fafc;
    border: 1px solid #cbd5e1;
    border-left: 4px solid #1e40af;
    border-radius: 6px;
  }
  .grow-legal-doc .grow-legal-callout h2 {
    margin-top: 0;
    border-left: none;
    padding-left: 0;
    color: #1e3a8a;
  }
</style>
""".strip()


def _build_legal_masthead_html(badge_text: str, title_text: str) -> str:
    return f"""
<div class="grow-legal-masthead">
  <table class="grow-legal-brand-table" role="presentation">
    <tr>
      <td style="width:46px;">
        <svg viewBox="15.4 10.4 44.3 32.5" width="38" height="28" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
          <path d="{_GROW_LEAF_PATH}" fill="#44aa00" />
        </svg>
      </td>
      <td>
        <div class="grow-legal-wordmark">Grow</div>
        <div class="grow-legal-subbrand">Business Development</div>
      </td>
      <td style="text-align:right;">
        <span class="grow-legal-doc-badge">{badge_text}</span>
      </td>
    </tr>
  </table>
  <h1 class="grow-legal-title">{title_text}</h1>
  <div class="grow-legal-meta-row">
    <span class="grow-legal-pill">Version 1.1</span>
    <span class="grow-legal-pill">Effective: November 2026</span>
    <span class="grow-legal-pill">Jurisdiction: Cairo, Egypt</span>
  </div>
</div>
""".strip()


TERMS_AND_CONDITIONS_V1_1_HTML = f"""
<div class="grow-legal-doc" {_LEGAL_REV_MARKER}>
{_LEGAL_SCOPED_CSS}
{_build_legal_masthead_html("Legal Agreement", "Terms of Service — Grow Corporate Treasury Platform")}

<h2>1. Introduction &amp; Acceptance</h2>
<p>
These Terms of Service ("Terms") govern access to and use of the Grow Corporate Treasury Platform ("Platform"), operated by <strong>Grow Business Development</strong> ("Grow", "Company", "we", or "us"). By registering for, accessing, or using the Platform (including any Guided Free Trial or Sandbox environment), the subscribing business entity and its authorized users ("Customer" or "you") agree to be bound by these Terms.
</p>

<h2>2. Platform Scope &amp; Modular Architecture</h2>
<ul>
  <li><strong>Platform</strong>: Grow's cloud-based corporate treasury software suite, comprising modular workflows including (a) <em>Letter of Guarantee (LG) Custody &amp; Lifecycle Governance</em>, (b) <em>Outbound LG Issuance &amp; Multi-Bank Credit Facility Scoring</em>, (c) <em>Competitive FX &amp; Sovereign Treasury Bill (T-Bill) Sealed-Bid Quotations</em>, (d) <em>Cash &amp; Bank Statement Auto-Reconciliation</em>, and (e) <em>Advisory AI Document, OCR &amp; Copilot Tools</em>, according to Customer's active subscription or trial selection.</li>
  <li><strong>Customer Data</strong>: All corporate records, scanned guarantees, contracts, bank statements, ERP open-item ledgers, facility parameters, counterparty rosters, RFQ instructions, and exported journals uploaded, generated, or entered by or on behalf of Customer.</li>
  <li><strong>Authorized Users</strong>: Customer's designated Corporate Administrators, End Users (Makers), Checkers (Approvers), Department Requestors, Viewers, and external Bank Counterparty Representatives invited by Customer.</li>
</ul>

<h2>3. Customer Governance, Dual Control &amp; Corporate Mail Relay</h2>
<p>
Customer is solely responsible for (a) the accuracy, legality, and completeness of all Customer Data, (b) configuring internal Maker-Checker approval matrices, entity permissions, and signatory authorities appropriate to its corporate governance policies, (c) safeguarding login credentials, MFA/OTP verification codes, and paired devices, (d) verifying all physical or electronic bank instructions, beneficiary details, IBANs, amounts, and expiry dates prior to execution with any financial institution, and (e) authorizing and maintaining any corporate SMTP/Exchange mail server credentials and designated bank dealer rosters configured by Customer for outbound communications.
</p>

<h2>4. FX &amp; Sovereign T-Bill Quotations — Non-Exchange &amp; Execution Terms</h2>
<ul>
  <li><strong>Private Communication Conduit &amp; Non-Exchange Status</strong>: The Quotations module operates strictly as a private, Customer-directed Request-for-Quote (RFQ) workflow and audit-trail record between Customer and its own banking counterparties. Grow is an independent software provider — not a bank, broker-dealer, multilateral trading facility, regulated exchange, clearinghouse, custodian, or settlement agent under Central Bank of Egypt (CBE), Financial Regulatory Authority (FRA), or other financial regulations — and charges zero transaction fees or spread commissions.</li>
  <li><strong>RFQ Configuration, Timers, Auto-Actions &amp; Delegation</strong>: Customer is solely responsible for configuring <em>Execution</em> versus <em>Indicative</em> RFQ modes, currency legs, value/maturity dates, deviation tolerances, sealed bidding windows, acceptance countdown timers, automated timeout rules (such as auto-accept or auto-reject actions), multi-leg selections, and colleague acceptance delegations.</li>
  <li><strong>Bilateral Trade Formation &amp; Transmission Risk</strong>: Whether a submitted bank quote, acceptance action, or cryptographic Deal Receipt constitutes a non-binding indication or a binding bilateral transaction is governed exclusively by Customer's bilateral master agreements and confirmations with the participating financial institution. Grow assumes no responsibility or liability for corporate mail-server routing delays, network or ISP latency near window close, unreceived bids, re-tenders, or bilateral trade settlement or non-settlement.</li>
</ul>

<h2>5. Cash &amp; Bank Statement Reconciliation — Accounting &amp; Verification Terms</h2>
<ul>
  <li><strong>Statement Formats &amp; Automated Parsing</strong>: Bank statement files (including Excel, CSV, SWIFT MT940, ISO 20022, or structured bank exports) vary across financial institutions and may change format without notice. Automated column mapping, balance-continuity flags, inter-bank sweep or reversal pairing, tolerance matching, and rule-based classifications are operational workflow aids and may contain omissions or variances.</li>
  <li><strong>Mandatory Human Accounting Sign-Off</strong>: Reconciliation views, matched ledger pairs, and exported double-entry ERP/GL journal files are preparation tools only — not an official accounting system of record, statutory audit, bank balance confirmation, or fraud-detection guarantee. Customer's qualified treasury and accounting personnel must independently review and validate all balances, tax/withholding treatments, fee allocations, and exported entries before posting to any ERP, general ledger, financial statement, or tax/regulatory filing.</li>
</ul>

<h2>6. Advisory Nature of AI, Scoring &amp; Market Reference Rates</h2>
<ul>
  <li><strong>Operational Decision-Support Only</strong>: Automated OCR/AI field extraction, Guarantee Wording Risk indicators, suggested protective clauses, facility scoring rankings, statement matching suggestions, AI Copilot responses, CBE/market reference rate displays, and "saved vs. average" spread calculations are provided strictly as operational decision-support aids.</li>
  <li><strong>Not Legal, Financial, or Investment Advice</strong>: Nothing generated by the Platform constitutes formal legal counsel, banking representation, audit certification, or regulated investment advice. Market reference rates and valuation benchmarks are informational snapshots that may lag or differ from live executable interbank rates. Customer remains solely responsible for independent legal, accounting, and financial verification before executing binding instruments or treasury trades.</li>
</ul>

<h2>7. Prohibited Uses</h2>
<p>
Customer shall not (a) reverse engineer, copy, benchmark-scrape, or attempt to extract source code or underlying models of the Platform, (b) probe, bypass, or disrupt Platform security, rate limits, or tenant isolation controls, (c) upload unlawful or unauthorized third-party data, or (d) misrepresent simulated or trial-watermarked outputs as official corporate or banking instruments.
</p>

<h2>8. Intellectual Property &amp; Customer Data Ownership</h2>
<p>
Grow retains all intellectual property rights in the Platform, templates, workflows, algorithms, and documentation. Customer retains all ownership rights in its Customer Data and grants Grow a limited license solely to host, process, and display such Customer Data to provide the subscribed services.
</p>

<h2>9. Confidentiality, Tenant Encryption &amp; Zero-PII Pattern Learning</h2>
<p>
Customer Data is logically isolated per tenant, protected using per-tenant AES-256 cryptographic key envelopes and TLS encryption in transit, and handled strictly in accordance with our Privacy Policy. Grow never sells Customer Data and never uses Customer's confidential financial documents to train public third-party AI models. Strictly de-identified, zero-PII banking syntax patterns (stripped of company names, counterparties, IBANs, account numbers, amounts, and dates, and promoted only upon meeting multi-tenant <em>k</em>-anonymity thresholds) may be used to improve automated transaction classification rules across commercial workspaces.
</p>

<h2>10. Subscription Fees, Availability &amp; Support</h2>
<p>
Commercial subscriptions are governed by the applicable order form or subscription plan. While Grow targets high availability and applies automated backups and security safeguards, uninterrupted or error-free operation is not guaranteed during scheduled maintenance or external network, mail-server, or cloud-provider outages. For onboarding or technical support, contact <a href="mailto:support@growbusinessdevelopment.com">support@growbusinessdevelopment.com</a>.
</p>

<h2>11. Limitation of Liability &amp; Indemnity</h2>
<p>
To the maximum extent permitted by applicable law, Grow shall not be liable for indirect, consequential, punitive, or special damages, missed guarantee renewal or claim deadlines, bank or corporate mail-relay delays, foreign exchange or interest-rate movements, unexecuted or disputed RFQ bids, reconciliation or ERP/GL posting variances, or reliance on automated extraction outputs. Grow's aggregate liability under any paid commercial subscription shall not exceed the subscription fees actually paid by Customer during the twelve (12) months preceding the claim. Customer agrees to indemnify and hold harmless Grow against third-party claims arising from Customer's breach of these Terms or unauthorized submission of data, counterparty contacts, or documents.
</p>

<h2>12. Term, Suspension &amp; Termination</h2>
<p>
Either party may terminate a subscription in accordance with the agreed commercial plan. Grow may suspend access immediately where reasonably necessary to prevent security threats, unlawful activity, or material breach of these Terms.
</p>

<h2>13. Governing Law &amp; Jurisdiction</h2>
<p>
These Terms are governed by the laws of the Arab Republic of Egypt. Any dispute arising out of or relating to these Terms or the Platform shall be subject to the exclusive jurisdiction of the competent courts of Cairo, Egypt.
</p>

<div class="grow-legal-callout">
  <h2>14. Special Terms for Guided Free Trial &amp; Sandbox Evaluation</h2>
  <p>
  When accessing the Platform under a Free Trial or Guided Trial Sandbox registration, the following terms apply and prevail over any conflicting provision above:
  </p>
  <ul>
    <li><strong>Evaluation Purpose Only</strong>: The Trial Sandbox is provided strictly for internal software evaluation and testing. It is not authorized for live commercial treasury execution or regulatory record-keeping.</li>
    <li><strong>Strict Prohibition on Official Submission of Watermarked Outputs</strong>: All letters, auto-filled bank PDF forms, QR verification records, and Deal Receipts generated in Trial Mode carry prominent <em>"TRIAL SANDBOX — EVALUATION ONLY · NOT VALID FOR OFFICIAL BANK SUBMISSION"</em> markings. Customer expressly agrees <strong>never</strong> to submit, transmit, or represent any trial-generated document or receipt to any bank, beneficiary, auditor, or third party as an official instrument.</li>
    <li><strong>Synthetic Facilities &amp; Simulated Dealer Quotes</strong>: Pre-seeded bank credit lines, demo OTP credentials, sample bank statements, and simulated FX/T-Bill bank quotes inside the Trial Sandbox are synthetic demonstration figures and do not represent offers, pricing, or commitments of any real financial institution.</li>
    <li><strong>Sandbox Isolation &amp; Quotas</strong>: Trial workspaces are subject to evaluation duration and transaction volume caps, suppress external bank email dispatch, and are quarantined from cross-customer analytics pools.</li>
    <li><strong>"As-Is" Provision, Zero Liability &amp; Data Purge</strong>: The Trial Sandbox is provided strictly <em>"AS IS"</em> without warranties of any kind, and Grow bears zero liability for any loss or claim arising from trial use. Grow may delete or purge Trial Sandbox accounts and uploaded evaluation data upon trial expiration without obligation of retention unless converted to a paid commercial agreement.</li>
  </ul>
</div>
</div>
""".strip()


PRIVACY_POLICY_V1_1_HTML = f"""
<div class="grow-legal-doc" {_LEGAL_REV_MARKER}>
{_LEGAL_SCOPED_CSS}
{_build_legal_masthead_html("Privacy &amp; Data Protection", "Privacy Policy — Grow Corporate Treasury Platform")}

<p>
<strong>Grow Business Development</strong> ("Grow", "we", "our", or "us") respects corporate confidentiality and personal privacy. This Privacy Policy explains how we collect, process, store, and safeguard information across the Grow Corporate Treasury Platform, website, and Guided Trial Sandbox ("Platform").
</p>

<h2>1. Information We Collect</h2>
<ul>
  <li><strong>Business Contact &amp; Account Information</strong>: Full name, corporate work email address, business phone/WhatsApp number, organization name, entity structure, and assigned platform role (e.g., Corporate Admin, End User/Maker, Checker, Requestor, Viewer, or invited Bank Dealer).</li>
  <li><strong>Treasury Operational &amp; Document Data</strong>: Guarantees, contracts, bank forms, bank statements, ERP open-item files, facility parameters, and FX/T-Bill quotation records uploaded or created within Customer's isolated workspace.</li>
  <li><strong>Security &amp; Audit Telemetry</strong>: IP addresses, browser/device identifiers, MFA/OTP verification events, sequential hash-chained audit logs, and legal acceptance timestamps required for institutional security and non-repudiation.</li>
</ul>

<h2>2. How We Use Information</h2>
<ul>
  <li>Provisioning, operating, and securing Customer's subscribed treasury modules and Maker-Checker workflows.</li>
  <li>Performing requested automated document OCR extraction, wording risk checks, statement reconciliation, and sealed-bid quotation execution.</li>
  <li>Delivering transactional notifications, expiry/SLA alerts, OTP codes, and requested onboarding or technical support.</li>
  <li>Protecting Platform integrity against unauthorized access, automated bot abuse, and tampering.</li>
</ul>

<h2>3. Confidentiality, Encryption &amp; AI Privacy</h2>
<ul>
  <li><strong>Tenant Isolation &amp; Encryption</strong>: Sensitive corporate records, counterparty registries, and quotation rates are protected via per-tenant cryptographic key envelopes (AES-256), TLS encryption in transit, and strict role-based access controls.</li>
  <li><strong>No Public AI Model Training</strong>: Uploaded corporate guarantees, contracts, bank statements, and financial records are processed solely to deliver Platform features to Customer and are <strong>never</strong> used to train public third-party AI models.</li>
  <li><strong>Zero-PII Classification Syntax</strong>: Only strictly de-identified, zero-PII structural banking syntax patterns (stripped of company names, counterparties, IBANs, account numbers, amounts, and dates, and gated by multi-tenant <em>k</em>-anonymity thresholds) may be retained to improve automated bank statement categorization across commercial tenants.</li>
  <li><strong>Trial Sandbox Quarantine</strong>: Data entered in the Guided Trial Sandbox is strictly isolated from production customer benchmarks and suppressed from external bank email delivery.</li>
</ul>

<h2>4. Information Sharing</h2>
<p>
We never sell or rent Customer or personal data. Information is disclosed only:
</p>
<ul>
  <li>To counterparties explicitly designated and authorized by Customer within a live production workflow (such as invited bank dealers in a live quotation).</li>
  <li>To vetted cloud infrastructure and security sub-processors bound by confidentiality and data-protection obligations.</li>
  <li>Where strictly required by applicable law, court order, or regulatory authority.</li>
</ul>

<h2>5. Data Retention &amp; Deletion</h2>
<ul>
  <li><strong>Commercial Accounts</strong>: Retained for the duration of Customer's active agreement and applicable statutory audit periods, or exported/deleted upon written corporate instruction at contract end.</li>
  <li><strong>Trial Sandbox Accounts</strong>: Evaluation workspaces and uploaded test files may be automatically purged following trial expiration unless transitioned to an active subscription, or earlier upon Customer's written request to <a href="mailto:support@growbusinessdevelopment.com">support@growbusinessdevelopment.com</a> or <a href="mailto:info@growbusinessdevelopment.com">info@growbusinessdevelopment.com</a>.</li>
</ul>

<h2>6. Your Rights &amp; Contact</h2>
<p>
Authorized corporate representatives may request access to, correction of, or deletion of their account details or evaluation data by contacting Grow Support &amp; Data Protection at <a href="mailto:support@growbusinessdevelopment.com">support@growbusinessdevelopment.com</a> or <a href="mailto:info@growbusinessdevelopment.com">info@growbusinessdevelopment.com</a>.
</p>
</div>
""".strip()


def ensure_legal_artifacts_seeded_in_place(db: Session) -> dict:
    """
    Ensures `terms_and_conditions` and `privacy_policy` exist at v1.1 and upgrades
    earlier v1.1 drafts IN-PLACE without bumping version.
    """
    from app.models.models import LegalArtifact

    result = {"tc": "unchanged", "pp": "unchanged"}

    # 1. Terms & Conditions
    tc = (
        db.query(LegalArtifact)
        .filter(
            LegalArtifact.artifact_type == LegalArtifactType.TERMS_AND_CONDITIONS.value,
            LegalArtifact.is_deleted == False,
        )
        .order_by(LegalArtifact.version.desc())
        .first()
    )
    if not tc:
        tc = LegalArtifact(
            artifact_type=LegalArtifactType.TERMS_AND_CONDITIONS.value,
            version=1.1,
            content=TERMS_AND_CONDITIONS_V1_1_HTML,
        )
        db.add(tc)
        result["tc"] = "seeded_v1.1"
    elif float(tc.version) == 1.1 and _LEGAL_REV_MARKER not in (tc.content or ""):
        tc.content = TERMS_AND_CONDITIONS_V1_1_HTML
        db.add(tc)
        result["tc"] = "updated_in_place_v1.1"

    # 2. Privacy Policy
    pp = (
        db.query(LegalArtifact)
        .filter(
            LegalArtifact.artifact_type == LegalArtifactType.PRIVACY_POLICY.value,
            LegalArtifact.is_deleted == False,
        )
        .order_by(LegalArtifact.version.desc())
        .first()
    )
    if not pp:
        pp = LegalArtifact(
            artifact_type=LegalArtifactType.PRIVACY_POLICY.value,
            version=1.1,
            content=PRIVACY_POLICY_V1_1_HTML,
        )
        db.add(pp)
        result["pp"] = "seeded_v1.1"
    elif float(pp.version) == 1.1 and _LEGAL_REV_MARKER not in (pp.content or ""):
        pp.content = PRIVACY_POLICY_V1_1_HTML
        db.add(pp)
        result["pp"] = "updated_in_place_v1.1"

    if result["tc"] != "unchanged" or result["pp"] != "unchanged":
        db.commit()
        logger.info(f"Legal artifacts v1.1 baseline synchronized: {result}")

    return result
