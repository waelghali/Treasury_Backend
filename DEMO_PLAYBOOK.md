# Executive Customer Demo Playbook: Bank Auto-Reconciliation & Intelligence

> **How to use this playbook**: Keep this open on your tablet or second screen during a client meeting with a CFO, VP of Finance, or Corporate Treasurer. Each statement in `c:\Grow\demo_statements\` is pre-configured with complete treasury data and matching ERP records.

---

## 1. Executive Pitch & The Hook (What to say in the first 2 minutes)

> *"Mr./Ms. CFO, corporate treasurers spend hundreds of hours every month manually matching bank statements line-by-line, tracking inter-account sweeps, calculating bank fee variances, and chasing invoices.*
>
> *Traditional systems fail because they rely on exact keyword matches and fall apart when dates or narrative formats change. Today, I’m going to show you Grow's Auto-Reconciliation Engine: zero-config banking AI, automatic liquidity sweep pairing, and live ERP auto-matching."*

---

## 2. Choosing Your Demo Statement

All 20 statements in `c:\Grow\demo_statements\` share the **same comprehensive treasury scenarios**, but each has a unique bank, account number, and sequential period so you can demo repeatedly without duplicate collisions:

| Demo # | Bank Name | File Name | Account # |
|---|:---|:---|:---|
| **01** | HSBC Bank Egypt S.A.E. | `Customer_Demo_01_HSBC.xlsx` | `001-987654-001` |
| **02** | Commercial International Bank (CIB) | `Customer_Demo_02_CIB.xlsx` | `001-987654-002` |
| **03** | National Bank of Egypt (NBE) | `Customer_Demo_03_NBE.xlsx` | `001-987654-003` |
| **04** | QNB Alahli | `Customer_Demo_04_QNB.xlsx` | `001-987654-004` |
| **05** | Banque Misr | `Customer_Demo_05_MISR.xlsx` | `001-987654-005` |
| **06** | First Abu Dhabi Bank (FAB Misr) | `Customer_Demo_06_FAB.xlsx` | `001-987654-006` |
| **07** | Citibank Egypt | `Customer_Demo_07_CITI.xlsx` | `001-987654-007` |
| **08** | Arab African International Bank (AAIB) | `Customer_Demo_08_AAIB.xlsx` | `001-987654-008` |
| **...** | *(Statements 09 to 20 covering Alexandria, ADCB, Attijariwafa, ENBD, Standard Chartered, Credit Agricole, Faisal, ABK, Midbank, Suez Canal, Arab Bank, HDB)* |

👉 **Tip**: Match the statement to the client’s actual house bank! If you're pitching to a company banking with CIB, upload `Customer_Demo_02_CIB.xlsx`.

---

## 3. The 4-Step Live Demo Flow

### Step 1: Upload & Zero-Config Instant Classification
1. Go to **Bank Reconciliation ➔ Upload Statement**.
2. Select your client's statement (e.g. `Customer_Demo_01_HSBC.xlsx`).
3. Click **Upload & Process**:
   - Point out to the client:
     > *"Notice that we didn't define any rules yet. Immediately upon upload, the system recognized over 50% of bank overhead lines automatically."*
   - Highlight the green **`⚡ BUILTIN`** badges:
     - `MONTHLY CORPORATE ACCOUNT MAINTENANCE FEE` $\rightarrow$ `BANK_CHARGES (85%)`
     - `STATUTORY FISCAL STAMP DUTY CHARGE TAX LAW` $\rightarrow$ `TAX_DEDUCTION (85%)`
     - `CREDIT INTEREST EARNED ON OVERNIGHT DEPOSIT` $\rightarrow$ `BANK_INTEREST (90%)`
     - `TAX DEDUCTION 20% WITHHOLDING TAX` $\rightarrow$ `TAX_DEDUCTION (85%)`
     - `MONTHLY NET SALARY BULK DISBURSEMENT` $\rightarrow$ `SALARY_PAYROLL (90%)`

---

### Step 2: Showcase Automatic Liquidity Sweeps & Fee Variance
1. Click the **`🔗 Sweeps`** filter chip in the toolbar:
   - The table isolates paired sweeps and error reversals.
2. **Showcase Exact Cash Pooling**:
   - Point to the paired `SWEEP TO` and `SWEEP FROM` lines.
   - Click the purple **`Sweep ↔ #...`** badge:
   - The **Sweep Pair Verification Modal** opens:
     > *"The engine matched the outflow (-400,000 EGP) and inflow (+400,000 EGP) legs using the shared tracking reference code across accounts."*
3. **Showcase Sweep with Bank Transfer Fee**:
   - Point to the next pair: Outflow `-250,075.00 EGP` vs Inflow `+250,000.00 EGP`.
   - Point to the amber badge: **`⚡ Fee: 75.00 EGP`**:
     > *"Notice this: the debit was 250,075 while the credit was 250,000. Traditional matching fails here. Our engine recognized the 75 EGP difference as the bank's wire transfer fee, paired them together, and flagged the fee variance for review."*
4. **Showcase Same-Day Bank Error & Reversal**:
   - Point to `DUPLICATE DEBIT BANK ERROR` (-14,250) and `CORRECTION REVERSAL` (+14,250):
     > *"The bank posted an erroneous charge and reversed it the same day. Our system paired them as a Reversal pair, clearing both so your team doesn't waste time reconciling ghost entries."*

---

### Step 3: Switch to Dual-Pane Split View & Live Auto-Matching
1. In the top bar, toggle the view mode to **Split Match**:
   - The left pane shows the **Bank Statement Lines**.
   - The right pane shows the **Internal ERP Ledger (AR Invoices, AP Bills, LG Fees)**.
2. Click the **`⚡ Auto-Match`** button in the header:
   - Watch both sides simultaneously update!
   - All customer collections and supplier disbursements turn green with **`✓ Matched`**:
     - `INWARD ACH PYMT ORANGE DATA TELECOM` $\rightarrow$ Matched with `AR Invoice DEMO-INV-...01` (+92,400.00 EGP)
     - `INSTAPAY IPN RECEIPT VODAFONE EGYPT` $\rightarrow$ Matched with `AR Invoice DEMO-INV-...02` (+48,600.00 EGP)
     - `FAWRY PAY MERCHANDISE SETTLEMENT` $\rightarrow$ Matched with `AR Invoice DEMO-INV-...03` (+35,120.00 EGP)
     - `OUTWARD WIRE SCHNEIDER ELECTRIC` $\rightarrow$ Matched with `AP Bill DEMO-BILL-...10` (-145,000.00 EGP)
     - `VENDOR DISBURSEMENT BAKER HUGHES` $\rightarrow$ Matched with `AP Bill DEMO-BILL-...20` (-82,500.00 EGP)
   - Talking point:
     > *"With one click, our engine reconciled the cash receipts and vendor payments against your ERP ledger using invoice numbers, entity names, and signed cash flows."*

---

### Step 4: System Honesty & The Boundary Limit ("What It Cannot Do")
1. Point to the last transaction: `MISC TRF BNK MEMO 90124809214...`
2. Point out that it is flagged as **`Unclassified`**:
   > *"We don't pretend AI is magic. When a bank sends a completely cryptic memo with no vendor, no invoice, and no context, our system honestly flags it for human review with 0% confidence, rather than making a dangerous guess. You click 'Confirm', teach the engine once, and it remembers forever."*

---

## 4. Answers to Tough CFO Questions

| CFO Question | Your Winning Answer |
|:---|:---|
| *"What if our bank uses UK dates (`05/02`) and another uses US (`02/05`)?"* | *"Our Multi-Signal Date Engine inspects narrative month tokens (`05FEB2026`), statement period headers, and column monotonic sequences so it never inverts days and months."* |
| *"Can our accounting categories connect to SAP or Oracle?"* | *"Yes. Every classification maps to an exact G/L Account Code and Cost Center, and our matched batches can export directly into SAP, Oracle, NetSuite, or Odoo journal format."* |
| *"Is our company's financial data shared with other clients?"* | *"Never. Our Privacy Shield strips all account numbers, amounts, dates, and names before any signature is submitted to the collaborative consensus engine. Only zero-PII structural tokens (like utility codes) participate in consensus."* |
