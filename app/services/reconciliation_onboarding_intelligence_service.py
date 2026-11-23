"""
Day-1 Intelligence Onboarding Service for Bank Reconciliation:
1. Train-Only Historical Sheet Learning (`train_from_historical_file`):
   - Parses historical Excel/CSV sheets where the customer previously classified transactions
     (`Description`, `Debit`/`Credit` or `Amount`, `Category`, `GL Account`, optional `Counterparty`).
   - Induces `ClassificationRule`s, `Counterparty` records, `BankAccountProfile`s, and optional
     `ClassificationTaxonomy` nodes WITHOUT polluting live `bank_statements`, `bank_transactions`,
     or `CashBalanceSnapshot` balances!
   - Supports interactive `dry_run=True` preview and selective signature approval.
2. Bulk Rulebook & Knowledge Importer, Template & Exporter:
   - Generates a ready-to-fill 3-sheet Excel template (`1_Rules_Import`, `2_Counterparties_Import`, `3_Taxonomy_Import`).
   - Imports rules, counterparties, and taxonomy in bulk from `.xlsx`, `.csv`, or `.json` with row-level
     validation, `dry_run` preview, and configurable duplicate strategy (`UPDATE`, `SKIP`, `APPEND`).
   - Exports a tenant's full rulebook + counterparties + custom taxonomy to `.xlsx` or `.json`.
"""

import hashlib
import io
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import HTTPException
from sqlalchemy import inspect as sa_inspect, or_
from sqlalchemy.orm import Session

from app.core.security_crypto import encrypt_field
from app.models.models_reconciliation_v2 import (
    BankAccountProfile,
    ClassificationRule,
    ClassificationTaxonomy,
    Counterparty,
)
from app.services.reconciliation_brs_and_packs_service import sanitize_spreadsheet_cell
from app.services.reconciliation_confidentiality_service import (
    classify_amount_tier,
    reconciliation_confidentiality,
)

logger = logging.getLogger(__name__)


def _has_table(db: Session, table_name: str) -> bool:
    try:
        return bool(sa_inspect(db.connection()).has_table(table_name))
    except Exception:
        return False


def _clean_str(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val).strip()
    return "" if s.lower() in ("nan", "none", "null") else s


def _clean_dec(val: Any) -> Decimal:
    s = _clean_str(val)
    if not s:
        return Decimal("0.00")
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    cleaned = re.sub(r"[^\d.\-]", "", s.replace(",", ""))
    if not cleaned or cleaned in ("-", ".", "-."):
        return Decimal("0.00")
    try:
        d = Decimal(cleaned)
        return -abs(d) if neg else d
    except Exception:
        return Decimal("0.00")


def _norm_header(col: Any) -> str:
    return re.sub(r"[^a-z0-9\u0600-\u06FF]", "", str(col or "").lower())


def _split_keywords(raw: Any) -> List[str]:
    s = _clean_str(raw)
    if not s:
        return []
    parts = re.split(r"[,;|]+", s)
    return [p.strip() for p in parts if p.strip()]


class ReconciliationOnboardingIntelligenceService:
    """Train-only historical statement learning & bulk rulebook import/export."""

    # ==================================================================================
    # 1. TRAIN-ONLY HISTORICAL LABELED SHEET LEARNING ENGINE
    # ==================================================================================

    HIST_COLUMN_ALIASES = {
        "desc": ["description", "rawdescription", "narrative", "details", "particulars", "memo", "بيان", "تفاصيل", "البيان", "الوصف"],
        "desc2": ["description2", "descriptionline2", "additionaldetails", "remarks", "ملاحظات"],
        "debit": ["debit", "debitamount", "withdrawal", "withdrawals", "outflow", "dr", "مدين", "سحب"],
        "credit": ["credit", "creditamount", "deposit", "deposits", "inflow", "cr", "دائن", "ايداع", "إيداع"],
        "amount": ["amount", "netamount", "signedamount", "transactionamount", "value", "المبلغ", "مبلغ", "القيمة", "صافي"],
        "dc_flag": ["dc", "drcr", "type", "direction", "indicator", "نوعالحركة"],
        "category": ["category", "classification", "classificationcategory", "class", "تصنيف", "فئة", "الفئة"],
        "sub_category": ["subcategory", "subclass", "فئةفرعية", "تصنيففرعي"],
        "gl_account": ["glaccount", "gl", "glcode", "accountcode", "assignedglaccount", "ledgeraccount", "internalcategory", "حسابالأستاذ", "رقمحسابالأستاذ", "كودالحساب"],
        "counterparty": ["counterparty", "counterpartyname", "beneficiary", "beneficiaryname", "vendor", "customer", "party", "payee", "payer", "المستفيد", "الطرفالثاني", "العميل", "المورد"],
        "account_no": ["accountno", "accountnumber", "bankaccount", "iban", "رقمالساب", "رقمحسابالبنك"],
    }

    def _read_tabular_bytes(self, content: bytes, filename: str, sheet_name: Any = 0) -> pd.DataFrame:
        ext = (filename or "").lower().rsplit(".", 1)[-1] if "." in (filename or "") else ""
        if ext == "csv":
            for enc in ("utf-8-sig", "utf-8", "cp1256", "latin-1"):
                try:
                    return pd.read_csv(io.BytesIO(content), encoding=enc)
                except Exception:
                    continue
            raise HTTPException(status_code=400, detail="Unable to decode CSV file.")
        elif ext in ("xlsx", "xls", "xlsb"):
            try:
                return pd.read_excel(io.BytesIO(content), sheet_name=sheet_name)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Unable to read Excel file: {e}")
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Upload .xlsx, .xls, or .csv.")

    def _detect_historical_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """Finds header row (even below bank banner rows) and maps historical training columns."""
        df = df.dropna(how="all").reset_index(drop=True)
        if df.empty:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")

        def _score_headers(cols: List[Any]) -> Dict[str, str]:
            mapped: Dict[str, str] = {}
            used_cols = set()
            # Exact match pass first, then substring pass
            for pass_exact in (True, False):
                for key, aliases in self.HIST_COLUMN_ALIASES.items():
                    if key in mapped:
                        continue
                    for c in cols:
                        if c in used_cols:
                            continue
                        cn = _norm_header(c)
                        if not cn or cn.startswith("unnamed"):
                            continue
                        if pass_exact:
                            if cn in aliases:
                                mapped[key] = c
                                used_cols.add(c)
                                break
                        else:
                            if any(a in cn for a in aliases):
                                mapped[key] = c
                                used_cols.add(c)
                                break
            return mapped

        best_map = _score_headers(list(df.columns))
        if not (best_map.get("desc") and (best_map.get("category") or best_map.get("gl_account"))):
            # Scan top 25 rows in case bank metadata precedes header row
            for r_idx in range(min(25, len(df))):
                row_vals = [_clean_str(v) or f"Col_{j}" for j, v in enumerate(df.iloc[r_idx].values)]
                probe = _score_headers(row_vals)
                if probe.get("desc") and (probe.get("category") or probe.get("gl_account")):
                    df.columns = row_vals
                    df = df.iloc[r_idx + 1 :].reset_index(drop=True)
                    best_map = probe
                    break

        if not best_map.get("desc"):
            raise HTTPException(
                status_code=400,
                detail="Could not find a 'Description' / 'Narrative' column in the historical sheet.",
            )
        if not best_map.get("category") and not best_map.get("gl_account"):
            raise HTTPException(
                status_code=400,
                detail="Historical training sheet must include at least a 'Category' or 'GL Account' column with past classifications.",
            )
        return df, best_map

    def _fallback_distinctive_tokens(self, desc: str) -> List[str]:
        """Fallback token extractor when a description consists solely of vendor/brand names stripped by PII scrubber."""
        cleaned = re.sub(r"[^A-Z\u0600-\u06FF\s]", " ", (desc or "").upper())
        stop = {
            "TRANSFER", "TRF", "PAYMENT", "PMT", "DEBIT", "CREDIT", "REF", "DATE", "VALUE",
            "FROM", "FOR", "THE", "AND", "EGP", "USD", "EUR", "LLC", "SAE", "LTD", "INC", "CORP",
            "تحويل", "سداد", "حساب", "من", "إلى",
        }
        words = []
        for w in cleaned.split():
            if len(w) >= 3 and w not in stop and w not in words:
                words.append(w)
        return words[:3]

    def train_from_historical_file(
        self,
        db: Session,
        customer_id: int,
        user_id: Optional[int],
        content: bytes,
        filename: str,
        bank_id: Optional[int] = None,
        dry_run: bool = True,
        auto_promote_min_hits: int = 2,
        auto_create_taxonomy: bool = True,
        selected_signatures: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        df, col_map = self._detect_historical_columns(self._read_tabular_bytes(content, filename))
        min_hits = max(1, int(auto_promote_min_hits or 2))

        dek = (
            reconciliation_confidentiality.get_tenant_dek(db, customer_id)
            if _has_table(db, "quotation_tenant_keys")
            else None
        )
        known_entities = [
            c[0]
            for c in db.query(Counterparty.name)
            .filter(
                or_(Counterparty.company_id == customer_id, Counterparty.company_id == None),
                Counterparty.is_deleted == False,
            )
            .limit(500)
            .all()
            if c and c[0]
        ]

        # Track grouped rule clusters by `(sig_key, direction)`
        clusters: Dict[Tuple[str, str], Dict[str, Any]] = {}
        cp_clusters: Dict[str, Dict[str, Any]] = {}
        acct_cat_counts: Dict[str, Counter] = defaultdict(Counter)
        taxonomy_candidates: Dict[str, Dict[str, Any]] = {}

        rows_scanned = 0
        labeled_rows = 0
        unlabeled_skipped = 0

        for _, row in df.iterrows():
            desc = _clean_str(row.get(col_map["desc"]))
            if not desc:
                continue
            desc_low = desc.lower()
            if any(b in desc_low for b in ("opening balance", "closing balance", "balance b/f", "رصيد سابق", "رصيد حالي")):
                continue

            rows_scanned += 1
            cat = _clean_str(row.get(col_map["category"])) if col_map.get("category") else ""
            sub_cat = _clean_str(row.get(col_map["sub_category"])) if col_map.get("sub_category") else ""
            gl_acc = _clean_str(row.get(col_map["gl_account"])) if col_map.get("gl_account") else ""

            if not cat and not gl_acc:
                unlabeled_skipped += 1
                continue

            eff_cat = cat or gl_acc
            eff_gl = gl_acc or cat
            labeled_rows += 1

            desc2 = _clean_str(row.get(col_map["desc2"])) if col_map.get("desc2") else ""
            cp_name = _clean_str(row.get(col_map["counterparty"])) if col_map.get("counterparty") else ""
            acct_no = _clean_str(row.get(col_map["account_no"])) if col_map.get("account_no") else ""

            # Resolve direction & magnitude
            dr = abs(_clean_dec(row.get(col_map["debit"]))) if col_map.get("debit") else Decimal("0.00")
            cr = abs(_clean_dec(row.get(col_map["credit"]))) if col_map.get("credit") else Decimal("0.00")
            amt_signed = _clean_dec(row.get(col_map["amount"])) if col_map.get("amount") else Decimal("0.00")
            dc_raw = _clean_str(row.get(col_map["dc_flag"])).upper() if col_map.get("dc_flag") else ""

            if cr > 0 and dr == 0:
                direction = "CREDIT"
                abs_amt = cr
            elif dr > 0 and cr == 0:
                direction = "DEBIT"
                abs_amt = dr
            elif dc_raw in ("C", "CR", "CREDIT", "INFLOW", "دائن", "إيداع", "ايداع"):
                direction = "CREDIT"
                abs_amt = abs(amt_signed)
            elif dc_raw in ("D", "DR", "DEBIT", "OUTFLOW", "مدين", "سحب"):
                direction = "DEBIT"
                abs_amt = abs(amt_signed)
            elif amt_signed > 0:
                direction = "CREDIT"
                abs_amt = amt_signed
            elif amt_signed < 0:
                direction = "DEBIT"
                abs_amt = abs(amt_signed)
            else:
                direction = "EITHER"
                abs_amt = Decimal("0.00")

            amt_tier = classify_amount_tier(abs_amt)

            # 1) Counterparty cluster
            if cp_name and len(cp_name) >= 3:
                cp_key = cp_name.upper()
                cp_rec = cp_clusters.setdefault(
                    cp_key,
                    {
                        "name": cp_name,
                        "entity_type": "CUSTOMER" if direction == "CREDIT" else "SUPPLIER",
                        "cat_votes": Counter(),
                        "gl_votes": Counter(),
                        "occurrences": 0,
                    },
                )
                cp_rec["occurrences"] += 1
                cp_rec["cat_votes"][eff_cat] += 1
                cp_rec["gl_votes"][eff_gl] += 1

            # 2) Account purity tracking
            if acct_no and eff_cat.upper() not in ("BANK_CHARGES", "TAX_DEDUCTION"):
                acct_cat_counts[acct_no][eff_cat] += 1

            # 3) Taxonomy candidate
            cat_code = re.sub(r"[^A-Z0-9]+", "_", eff_cat.upper()).strip("_")[:80]
            if cat_code:
                taxonomy_candidates.setdefault(
                    cat_code,
                    {
                        "code": cat_code,
                        "name": eff_cat,
                        "direction": direction,
                        "default_gl_account": eff_gl if eff_gl != eff_cat else "",
                    },
                )

            # 4) Rule signature extraction
            sig_info = reconciliation_confidentiality.extract_privacy_safe_rule_signature(
                raw_description=desc,
                description_line2=desc2 or None,
                known_entities=known_entities,
                counterparty_name=cp_name or None,
            )
            tokens = sig_info.get("signature_tokens") or []
            if not tokens:
                tokens = self._fallback_distinctive_tokens(f"{desc} {desc2}")
            if not tokens:
                continue

            anchor_tokens = tokens[:3]
            sig_str = f"{' '.join(anchor_tokens)}|{direction}|{sig_info.get('iban_bank_code') or ''}"
            sig_hash = hashlib.sha256(sig_str.encode("utf-8")).hexdigest()[:24]

            cl = clusters.setdefault(
                (sig_hash, direction),
                {
                    "signature_hash": sig_hash,
                    "direction": direction,
                    "tokens": anchor_tokens,
                    "iban_bank_code": sig_info.get("iban_bank_code"),
                    "label_votes": Counter(),
                    "sub_votes": Counter(),
                    "tiers": set(),
                    "samples": [],
                    "occurrences": 0,
                },
            )
            cl["occurrences"] += 1
            cl["label_votes"][(eff_cat, eff_gl)] += 1
            if sub_cat:
                cl["sub_votes"][sub_cat] += 1
            cl["tiers"].add(amt_tier)
            if len(cl["samples"]) < 2 and desc not in cl["samples"]:
                cl["samples"].append(desc[:90])

        # Build induced rules list sorted by occurrences desc
        existing_rules_by_sig = {
            r.signature_hash: r
            for r in db.query(ClassificationRule)
            .filter(
                ClassificationRule.company_id == customer_id,
                ClassificationRule.signature_hash != None,
                ClassificationRule.is_deleted == False,
            )
            .all()
        }

        induced_rules_preview: List[Dict[str, Any]] = []
        selected_set = set(selected_signatures) if selected_signatures is not None else None

        for (sig_hash, direction), cl in sorted(
            clusters.items(), key=lambda kv: (-kv[1]["occurrences"], kv[0][0])
        ):
            (win_cat, win_gl), win_votes = cl["label_votes"].most_common(1)[0]
            total_votes = cl["occurrences"]
            precision = round((win_votes / max(1, total_votes)) * 100.0, 2)
            win_sub = cl["sub_votes"].most_common(1)[0][0] if cl["sub_votes"] else None

            lifecycle = (
                "AUTO_CONFIRMED"
                if (win_votes >= min_hits and precision >= 85.0)
                else "SUGGESTED"
            )
            confidence = min(98, int((78 + min(win_votes * 4, 20)) * (precision / 100.0)))
            rule_name = f"Historical: {' '.join(cl['tokens'])} -> {win_cat}"[:120]

            ui_conds = [
                {"field": "raw_description", "operator": "contains", "value": tok, "joiner": "AND"}
                for tok in cl["tokens"]
            ]
            if direction == "CREDIT":
                ui_conds.append({"field": "credit_amount", "operator": "gt", "value": 0, "joiner": "AND"})
            elif direction == "DEBIT":
                ui_conds.append({"field": "debit_amount", "operator": "gt", "value": 0, "joiner": "AND"})

            cond_json: Dict[str, Any] = {
                "conditions": ui_conds,
                "direction": direction,
                "allowed_tiers": sorted(cl["tiers"]),
                "exclude_tokens": [],
                "iban_bank_code": cl["iban_bank_code"],
            }
            if win_cat.upper() == "BANK_CHARGES":
                cond_json["amount_max"] = 10000.0

            already_exists = sig_hash in existing_rules_by_sig
            induced_rules_preview.append({
                "signature_hash": sig_hash,
                "rule_name": rule_name,
                "direction": direction,
                "tokens": cl["tokens"],
                "assigned_category": win_cat,
                "assigned_sub_category": win_sub,
                "assigned_gl_account": win_gl,
                "occurrences": total_votes,
                "empirical_precision": precision,
                "calibrated_confidence": confidence,
                "lifecycle_state": lifecycle,
                "action": "REINFORCE_EXISTING" if already_exists else "CREATE_NEW",
                "sample_narrative": cl["samples"][0] if cl["samples"] else "",
                "_conditions_json": cond_json,
            })

        counterparties_preview = [
            {
                "name": cp["name"],
                "entity_type": cp["entity_type"],
                "default_category": cp["cat_votes"].most_common(1)[0][0],
                "default_gl_account": cp["gl_votes"].most_common(1)[0][0],
                "occurrences": cp["occurrences"],
            }
            for cp in sorted(cp_clusters.values(), key=lambda x: -x["occurrences"])
        ]

        existing_tax_codes = {
            (t.code or "").upper()
            for t in db.query(ClassificationTaxonomy)
            .filter(
                or_(ClassificationTaxonomy.company_id == customer_id, ClassificationTaxonomy.company_id == None),
                ClassificationTaxonomy.is_deleted == False,
            )
            .all()
        }
        new_taxonomy_preview = [
            t for code, t in taxonomy_candidates.items() if code.upper() not in existing_tax_codes
        ]

        if dry_run:
            return {
                "dry_run": True,
                "detected_columns": {k: str(v) for k, v in col_map.items()},
                "rows_scanned": rows_scanned,
                "labeled_rows_found": labeled_rows,
                "unlabeled_rows_skipped": unlabeled_skipped,
                "rules_count": len(induced_rules_preview),
                "auto_confirmed_count": sum(1 for r in induced_rules_preview if r["lifecycle_state"] == "AUTO_CONFIRMED"),
                "suggested_count": sum(1 for r in induced_rules_preview if r["lifecycle_state"] == "SUGGESTED"),
                "counterparties_count": len(counterparties_preview),
                "new_taxonomy_count": len(new_taxonomy_preview),
                "rules_preview": [
                    {k: v for k, v in r.items() if not k.startswith("_")}
                    for r in induced_rules_preview
                ],
                "counterparties_preview": counterparties_preview[:50],
                "new_taxonomy_preview": new_taxonomy_preview[:50],
            }

        # ── Commit Learned Rules, Counterparties, Account Profiles & Optional Taxonomy ──
        from app.crud.crud_reconciliation_v2 import crud_counterparty

        rules_created = 0
        rules_reinforced = 0
        for r_item in induced_rules_preview:
            sig_h = r_item["signature_hash"]
            if selected_set is not None and sig_h not in selected_set:
                continue

            if sig_h in existing_rules_by_sig:
                ex = existing_rules_by_sig[sig_h]
                ex.confirmations_count = (ex.confirmations_count or 1) + r_item["occurrences"]
                ex.assigned_gl_account = r_item["assigned_gl_account"]
                ex.assigned_category = r_item["assigned_category"]
                if r_item["assigned_sub_category"]:
                    ex.assigned_sub_category = r_item["assigned_sub_category"]
                if r_item["lifecycle_state"] == "AUTO_CONFIRMED":
                    ex.lifecycle_state = "AUTO_CONFIRMED"
                ex.calibrated_confidence = max(ex.calibrated_confidence or 80, r_item["calibrated_confidence"])
                rules_reinforced += 1
            else:
                db.add(
                    ClassificationRule(
                        company_id=customer_id,
                        bank_id=bank_id,
                        rule_name=r_item["rule_name"],
                        rule_type="LEARNED_COMPOSITE",
                        rule_origin="LEARNED_ONLINE",
                        lifecycle_state=r_item["lifecycle_state"],
                        signature_hash=sig_h,
                        priority=50,
                        stop_after_match=True,
                        conditions_json=r_item["_conditions_json"],
                        assigned_gl_account=r_item["assigned_gl_account"],
                        assigned_category=r_item["assigned_category"],
                        assigned_sub_category=r_item["assigned_sub_category"],
                        usage_count=r_item["occurrences"],
                        confirmations_count=r_item["occurrences"],
                        overrides_count=0,
                        empirical_precision=Decimal(str(r_item["empirical_precision"])),
                        calibrated_confidence=r_item["calibrated_confidence"],
                        is_active=True,
                        created_by=user_id,
                    )
                )
                rules_created += 1

        cps_saved = 0
        for cp in counterparties_preview:
            crud_counterparty.learn_or_increment(
                db=db,
                name=cp["name"],
                customer_id=customer_id,
                entity_type=cp["entity_type"],
                default_category=cp["default_category"],
                default_gl=cp["default_gl_account"],
                user_id=user_id,
            )
            cps_saved += 1

        # Update BankAccountProfile purity from historical sheet without creating BankStatements!
        accts_updated = 0
        for acct_no, cat_counter in acct_cat_counts.items():
            acct_tok = reconciliation_confidentiality.compute_blind_token(acct_no, dek, prefix="ACCT_TOK")
            prof = (
                db.query(BankAccountProfile)
                .filter(
                    BankAccountProfile.company_id == customer_id,
                    BankAccountProfile.account_token == acct_tok,
                )
                .first()
            )
            if not prof:
                enc_acct = encrypt_field(acct_no, dek, field_context="account_profile:account_number") if dek else None
                counts = dict(cat_counter)
                tot = sum(counts.values())
                dom_cat, dom_cnt = max(counts.items(), key=lambda x: x[1])
                db.add(
                    BankAccountProfile(
                        company_id=customer_id,
                        bank_id=bank_id,
                        account_token=acct_tok,
                        encrypted_account_number=enc_acct,
                        total_txns_observed=tot,
                        category_counts_json=counts,
                        dominant_category=dom_cat,
                        purity_ratio=Decimal(str(round((dom_cnt / max(1, tot)) * 100.0, 2))),
                    )
                )
            else:
                merged = Counter(prof.category_counts_json or {})
                merged.update(cat_counter)
                counts = dict(merged)
                tot = sum(counts.values())
                dom_cat, dom_cnt = max(counts.items(), key=lambda x: x[1])
                prof.category_counts_json = counts
                prof.total_txns_observed = tot
                prof.dominant_category = dom_cat
                prof.purity_ratio = Decimal(str(round((dom_cnt / max(1, tot)) * 100.0, 2)))
            accts_updated += 1

        tax_created = 0
        if auto_create_taxonomy and new_taxonomy_preview:
            parent_cls = (
                db.query(ClassificationTaxonomy)
                .filter(
                    ClassificationTaxonomy.company_id == customer_id,
                    ClassificationTaxonomy.parent_id == None,
                    ClassificationTaxonomy.code == "HISTORICAL_IMPORTED_CLASS",
                    ClassificationTaxonomy.is_deleted == False,
                )
                .first()
            )
            if not parent_cls:
                parent_cls = ClassificationTaxonomy(
                    company_id=customer_id,
                    parent_id=None,
                    code="HISTORICAL_IMPORTED_CLASS",
                    name="Imported Historical Categories",
                    name_ar="تصنيفات مستوردة من البيانات التاريخية",
                    direction="EITHER",
                    is_active=True,
                )
                db.add(parent_cls)
                db.flush()

            for t_item in new_taxonomy_preview:
                db.add(
                    ClassificationTaxonomy(
                        company_id=customer_id,
                        parent_id=parent_cls.id,
                        code=t_item["code"],
                        name=t_item["name"],
                        direction=t_item["direction"],
                        default_gl_account=t_item["default_gl_account"] or None,
                        is_active=True,
                    )
                )
                tax_created += 1

        db.commit()
        return {
            "dry_run": False,
            "rows_scanned": rows_scanned,
            "labeled_rows_found": labeled_rows,
            "rules_created": rules_created,
            "rules_reinforced": rules_reinforced,
            "counterparties_learned": cps_saved,
            "account_profiles_updated": accts_updated,
            "taxonomy_nodes_created": tax_created,
            "status": (
                f"Trained from {labeled_rows} historical rows (zero live ledger pollution): "
                f"{rules_created} new rules created, {rules_reinforced} existing rules reinforced, "
                f"{cps_saved} counterparties learned."
            ),
        }

    # ==================================================================================
    # 2. BULK RULEBOOK & KNOWLEDGE TEMPLATE, IMPORTER & EXPORTER
    # ==================================================================================

    def generate_bulk_template_xlsx(self) -> bytes:
        rules_examples = [
            {
                "Rule_Name": "Telecom & Fiber Internet Charges",
                "Priority": 80,
                "Assigned_GL_Account": "5040200 - Telecom & Internet Expense",
                "Assigned_Category": "TELECOM_INTERNET",
                "Assigned_Sub_Category": "Corporate Fiber",
                "Direction": "DEBIT",
                "Match_Field": "raw_description",
                "Must_Contain_All_AND": "",
                "Match_Any_OR": "VODAFONE BUSINESS, WE INTERNET, ORANGE DATA, اتصالات انترنت",
                "Exclude_Keywords": "REFUND, REVERSAL",
                "Min_Amount": "",
                "Max_Amount": 100000,
                "Bank_ID": "",
                "Lifecycle_State": "AUTO_CONFIRMED",
            },
            {
                "Rule_Name": "Monthly Head Office Rent Outflow",
                "Priority": 70,
                "Assigned_GL_Account": "5030100 - Office Lease & Rent",
                "Assigned_Category": "OFFICE_RENT",
                "Assigned_Sub_Category": "",
                "Direction": "DEBIT",
                "Match_Field": "raw_description",
                "Must_Contain_All_AND": "RENT, CAIRO FESTIVAL",
                "Match_Any_OR": "",
                "Exclude_Keywords": "",
                "Min_Amount": 10000,
                "Max_Amount": "",
                "Bank_ID": "",
                "Lifecycle_State": "AUTO_CONFIRMED",
            },
            {
                "Rule_Name": "POS Merchant Card Settlement Inflow",
                "Priority": 75,
                "Assigned_GL_Account": "1020100 - Trade Receivables (POS)",
                "Assigned_Category": "COLLECTION_FROM_CUSTOMER",
                "Assigned_Sub_Category": "POS Card Batch",
                "Direction": "CREDIT",
                "Match_Field": "raw_description",
                "Must_Contain_All_AND": "",
                "Match_Any_OR": "POS SETTLEMENT, PAYMOB, FAWRY SETTLEMENT, تحصيل نقاط البيع",
                "Exclude_Keywords": "CHARGEBACK, مرتد",
                "Min_Amount": "",
                "Max_Amount": "",
                "Bank_ID": "",
                "Lifecycle_State": "AUTO_CONFIRMED",
            },
        ]

        cp_examples = [
            {
                "Counterparty_Name": "Elsewedy Electric SAE",
                "Entity_Type": "SUPPLIER",
                "Aliases_Comma_Separated": "ELSEWEDY, EL SEWEDY CABLES, السويدي للكابلات",
                "Default_Category": "PAYMENT_TO_SUPPLIER",
                "Default_GL_Account": "2010100 - Trade Payables",
            },
            {
                "Counterparty_Name": "Modern Food Distribution Co",
                "Entity_Type": "CUSTOMER",
                "Aliases_Comma_Separated": "MODERN FOOD, شركة التوزيع الحديثة",
                "Default_Category": "COLLECTION_FROM_CUSTOMER",
                "Default_GL_Account": "1020100 - Trade Receivables",
            },
        ]

        tax_examples = [
            {
                "Class_Name": "Facilities & Utilities",
                "Class_Code": "FACILITIES_UTILITIES",
                "Subclass_Name": "Electricity & Water Bills",
                "Subclass_Code": "UTILITY_ELEC_WATER",
                "Name_Arabic": "فواتير الكهرباء والمياه",
                "Direction": "DEBIT",
                "Default_GL_Account": "5030400 - Utilities Expense",
                "Description": "Monthly electricity, water, and gas bills",
            },
        ]

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            pd.DataFrame(rules_examples).to_excel(writer, index=False, sheet_name="1_Rules_Import")
            pd.DataFrame(cp_examples).to_excel(writer, index=False, sheet_name="2_Counterparties_Import")
            pd.DataFrame(tax_examples).to_excel(writer, index=False, sheet_name="3_Taxonomy_Import")
        buf.seek(0)
        return buf.getvalue()

    def export_rulebook(
        self,
        db: Session,
        customer_id: int,
        file_format: str = "XLSX",
    ) -> Tuple[bytes, str, str]:
        fmt = (file_format or "XLSX").strip().upper()
        rules = (
            db.query(ClassificationRule)
            .filter(
                ClassificationRule.company_id == customer_id,
                ClassificationRule.is_deleted == False,
            )
            .order_by(ClassificationRule.priority.asc(), ClassificationRule.id.asc())
            .all()
        )
        cps = (
            db.query(Counterparty)
            .filter(
                Counterparty.company_id == customer_id,
                Counterparty.is_deleted == False,
            )
            .order_by(Counterparty.name.asc())
            .all()
        )
        tax_nodes = (
            db.query(ClassificationTaxonomy)
            .filter(
                ClassificationTaxonomy.company_id == customer_id,
                ClassificationTaxonomy.is_deleted == False,
            )
            .order_by(ClassificationTaxonomy.parent_id.asc().nullsfirst(), ClassificationTaxonomy.id.asc())
            .all()
        )
        tax_by_id = {t.id: t for t in tax_nodes}
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        if fmt == "JSON":
            payload = {
                "schema_version": "grow_reconciliation_rulebook_v1",
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "rules": [
                    {
                        "rule_name": r.rule_name,
                        "priority": r.priority,
                        "rule_type": r.rule_type,
                        "rule_origin": r.rule_origin,
                        "lifecycle_state": r.lifecycle_state,
                        "bank_id": r.bank_id,
                        "assigned_gl_account": r.assigned_gl_account,
                        "assigned_category": r.assigned_category,
                        "assigned_sub_category": r.assigned_sub_category,
                        "conditions_json": r.conditions_json,
                        "stop_after_match": r.stop_after_match,
                        "is_active": r.is_active,
                    }
                    for r in rules
                ],
                "counterparties": [
                    {
                        "name": c.name,
                        "aliases": c.aliases or [c.name],
                        "entity_type": c.entity_type,
                        "default_category": c.default_category,
                        "default_gl_account": c.default_gl_account,
                    }
                    for c in cps
                ],
                "taxonomy": [
                    {
                        "parent_code": tax_by_id[t.parent_id].code if (t.parent_id and t.parent_id in tax_by_id) else None,
                        "parent_name": tax_by_id[t.parent_id].name if (t.parent_id and t.parent_id in tax_by_id) else None,
                        "code": t.code,
                        "name": t.name,
                        "name_ar": t.name_ar,
                        "direction": t.direction,
                        "default_gl_account": t.default_gl_account,
                        "description": t.description,
                    }
                    for t in tax_nodes
                ],
            }
            return (
                json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"),
                "application/json; charset=utf-8",
                f"grow_rulebook_export_{ts}.json",
            )

        # Export to 3-sheet Excel (round-trip re-importable!)
        rule_rows = []
        for r in rules:
            cj = r.conditions_json or {}
            conds = cj.get("conditions") or []
            and_kws = [
                str(c.get("value"))
                for c in conds
                if c.get("operator") == "contains" and (c.get("joiner") or "AND").upper() == "AND"
            ]
            or_kws = [
                str(c.get("value"))
                for c in conds
                if c.get("operator") == "contains" and (c.get("joiner") or "").upper() == "OR"
            ]
            direction = cj.get("direction") or (
                "CREDIT"
                if any(c.get("field") == "credit_amount" for c in conds)
                else ("DEBIT" if any(c.get("field") == "debit_amount" for c in conds) else "EITHER")
            )
            first_field = next(
                (c.get("field") for c in conds if c.get("field") not in ("credit_amount", "debit_amount")),
                "raw_description",
            )
            rule_rows.append(
                {
                    k: sanitize_spreadsheet_cell(v)
                    for k, v in {
                        "Rule_Name": r.rule_name or "",
                        "Priority": r.priority,
                        "Assigned_GL_Account": r.assigned_gl_account,
                        "Assigned_Category": r.assigned_category or "",
                        "Assigned_Sub_Category": r.assigned_sub_category or "",
                        "Direction": direction,
                        "Match_Field": first_field,
                        "Must_Contain_All_AND": ", ".join(and_kws),
                        "Match_Any_OR": ", ".join(or_kws),
                        "Exclude_Keywords": ", ".join(cj.get("exclude_tokens") or []),
                        "Min_Amount": cj.get("amount_min") if cj.get("amount_min") is not None else "",
                        "Max_Amount": cj.get("amount_max") if cj.get("amount_max") is not None else "",
                        "Bank_ID": r.bank_id or "",
                        "Lifecycle_State": r.lifecycle_state or "AUTO_CONFIRMED",
                        "Raw_Conditions_JSON": json.dumps(cj, ensure_ascii=False),
                    }.items()
                }
            )

        cp_rows = [
            {
                k: sanitize_spreadsheet_cell(v)
                for k, v in {
                    "Counterparty_Name": c.name,
                    "Entity_Type": c.entity_type,
                    "Aliases_Comma_Separated": ", ".join(c.aliases or [c.name]),
                    "Default_Category": c.default_category or "",
                    "Default_GL_Account": c.default_gl_account or "",
                }.items()
            }
            for c in cps
        ]

        tax_rows = []
        for t in tax_nodes:
            if t.parent_id is None:
                # Only emit standalone class row if it has no children
                has_children = any(ch.parent_id == t.id for ch in tax_nodes)
                if not has_children:
                    tax_rows.append({
                        "Class_Name": sanitize_spreadsheet_cell(t.name),
                        "Class_Code": sanitize_spreadsheet_cell(t.code),
                        "Subclass_Name": "",
                        "Subclass_Code": "",
                        "Name_Arabic": sanitize_spreadsheet_cell(t.name_ar or ""),
                        "Direction": t.direction or "EITHER",
                        "Default_GL_Account": sanitize_spreadsheet_cell(t.default_gl_account or ""),
                        "Description": sanitize_spreadsheet_cell(t.description or ""),
                    })
            else:
                p = tax_by_id.get(t.parent_id)
                tax_rows.append({
                    "Class_Name": sanitize_spreadsheet_cell(p.name if p else "Custom Class"),
                    "Class_Code": sanitize_spreadsheet_cell(p.code if p else "CUSTOM_CLASS"),
                    "Subclass_Name": sanitize_spreadsheet_cell(t.name),
                    "Subclass_Code": sanitize_spreadsheet_cell(t.code),
                    "Name_Arabic": sanitize_spreadsheet_cell(t.name_ar or ""),
                    "Direction": t.direction or "EITHER",
                    "Default_GL_Account": sanitize_spreadsheet_cell(t.default_gl_account or ""),
                    "Description": sanitize_spreadsheet_cell(t.description or ""),
                })

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            pd.DataFrame(rule_rows or [{"Info": "No custom rules"}]).to_excel(writer, index=False, sheet_name="1_Rules_Import")
            pd.DataFrame(cp_rows or [{"Info": "No custom counterparties"}]).to_excel(writer, index=False, sheet_name="2_Counterparties_Import")
            pd.DataFrame(tax_rows or [{"Info": "No custom taxonomy"}]).to_excel(writer, index=False, sheet_name="3_Taxonomy_Import")
        buf.seek(0)
        return (
            buf.getvalue(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            f"grow_rulebook_export_{ts}.xlsx",
        )

    def _parse_rule_row_dict(self, row: Dict[str, Any], row_num: int) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        norm_row = {_norm_header(k): v for k, v in row.items()}

        def _g(*keys: str) -> str:
            for k in keys:
                if k in norm_row:
                    val = _clean_str(norm_row[k])
                    if val:
                        return val
            return ""

        if _g("info"):
            return None, None  # Skip empty-placeholder info row

        rule_name = _g("rulename", "name", "title", "اسمالقاعدة")
        gl_acc = _g("assignedglaccount", "glaccount", "gl", "glcode", "defaultglaccount", "حسابالأستاذ")
        cat = _g("assignedcategory", "category", "class", "تصنيف") or gl_acc
        sub_cat = _g("assignedsubcategory", "subcategory", "subclass") or None

        if not gl_acc and not cat:
            return None, f"Row {row_num}: Missing 'Assigned_GL_Account' (or 'Assigned_Category')."
        eff_gl = gl_acc or cat

        raw_json_str = _g("rawconditionsjson", "conditionsjson")
        direction = (_g("direction", "اتجاه") or "EITHER").upper()
        if direction not in ("DEBIT", "CREDIT", "EITHER"):
            direction = "EITHER"

        field = _g("matchfield", "field") or "raw_description"
        and_kws = _split_keywords(_g("mustcontainalland", "andkeywords", "contains", "keywordsand", "mustcontain"))
        or_kws = _split_keywords(_g("matchanyor", "orkeywords", "anykeywords", "keywords", "matchany"))
        excl_kws = _split_keywords(_g("excludekeywords", "exclude", "notcontains"))

        min_amt_s = _g("minamount", "amountmin")
        max_amt_s = _g("maxamount", "amountmax")
        min_amt = float(_clean_dec(min_amt_s)) if min_amt_s else None
        max_amt = float(_clean_dec(max_amt_s)) if max_amt_s else None

        cond_json: Optional[Dict[str, Any]] = None
        # Only use Raw_Conditions_JSON if user didn't supply spreadsheet keyword columns or if identical
        if raw_json_str and not and_kws and not or_kws:
            try:
                parsed_cj = json.loads(raw_json_str)
                if isinstance(parsed_cj, dict) and parsed_cj.get("conditions"):
                    cond_json = parsed_cj
            except Exception:
                pass

        if cond_json is None:
            if not and_kws and not or_kws and min_amt is None and max_amt is None:
                return None, f"Row {row_num}: Provide at least one keyword in 'Must_Contain_All_AND' or 'Match_Any_OR'."

            conds: List[Dict[str, Any]] = []
            for kw in and_kws:
                conds.append({"field": field, "operator": "contains", "value": kw, "joiner": "AND"})
            for idx_or, kw in enumerate(or_kws):
                # First OR keyword joins with AND if there are no prior ORs, subsequent join with OR!
                joiner = "OR" if (idx_or > 0 or not and_kws and idx_or > 0) else ("AND" if and_kws else "OR")
                if idx_or == 0 and not and_kws:
                    joiner = "AND"
                conds.append({"field": field, "operator": "contains", "value": kw, "joiner": joiner})

            if direction == "CREDIT":
                conds.append({"field": "credit_amount", "operator": "gt", "value": 0, "joiner": "AND"})
            elif direction == "DEBIT":
                conds.append({"field": "debit_amount", "operator": "gt", "value": 0, "joiner": "AND"})

            cond_json = {
                "conditions": conds,
                "direction": direction,
                "exclude_tokens": [k.upper() for k in excl_kws],
            }
            if min_amt is not None and min_amt > 0:
                cond_json["amount_min"] = min_amt
            if max_amt is not None and max_amt > 0:
                cond_json["amount_max"] = max_amt

        prio_s = _g("priority", "أولوية")
        try:
            priority = int(float(prio_s)) if prio_s else 100
        except Exception:
            priority = 100

        bank_s = _g("bankid")
        try:
            bank_id = int(float(bank_s)) if bank_s else None
        except Exception:
            bank_id = None

        lifecycle = (_g("lifecyclestate", "status") or "AUTO_CONFIRMED").upper()
        if lifecycle not in ("AUTO_CONFIRMED", "SUGGESTED"):
            lifecycle = "AUTO_CONFIRMED"

        if not rule_name:
            sample_kw = (and_kws + or_kws)[:2]
            rule_name = f"Bulk: {', '.join(sample_kw)} -> {cat}"[:120]

        return {
            "rule_name": rule_name[:120],
            "priority": priority,
            "bank_id": bank_id,
            "assigned_gl_account": eff_gl,
            "assigned_category": cat,
            "assigned_sub_category": sub_cat,
            "lifecycle_state": lifecycle,
            "conditions_json": cond_json,
        }, None

    def bulk_import_rules_and_knowledge(
        self,
        db: Session,
        customer_id: int,
        user_id: int,
        content: bytes,
        filename: str,
        dry_run: bool = True,
        duplicate_strategy: str = "UPDATE",  # UPDATE | SKIP | APPEND
    ) -> Dict[str, Any]:
        strategy = (duplicate_strategy or "UPDATE").strip().upper()
        if strategy not in ("UPDATE", "SKIP", "APPEND"):
            strategy = "UPDATE"

        ext = (filename or "").lower().rsplit(".", 1)[-1] if "." in (filename or "") else ""
        parsed_rules: List[Dict[str, Any]] = []
        parsed_cps: List[Dict[str, Any]] = []
        parsed_tax: List[Dict[str, Any]] = []
        row_errors: List[str] = []

        if ext == "json":
            try:
                doc = json.loads(content.decode("utf-8-sig"))
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid JSON file: {e}")

            raw_rules = doc.get("rules", doc if isinstance(doc, list) else [])
            for idx, r in enumerate(raw_rules, start=1):
                if not isinstance(r, dict) or not r.get("assigned_gl_account") or not r.get("conditions_json"):
                    row_errors.append(f"JSON Rule #{idx}: Missing assigned_gl_account or conditions_json")
                    continue
                parsed_rules.append({
                    "rule_name": (r.get("rule_name") or f"Imported Rule #{idx}")[:120],
                    "priority": int(r.get("priority") or 100),
                    "bank_id": r.get("bank_id"),
                    "assigned_gl_account": str(r["assigned_gl_account"]),
                    "assigned_category": str(r.get("assigned_category") or r["assigned_gl_account"]),
                    "assigned_sub_category": r.get("assigned_sub_category"),
                    "lifecycle_state": r.get("lifecycle_state") or "AUTO_CONFIRMED",
                    "conditions_json": r["conditions_json"],
                })
            for cp in doc.get("counterparties", []):
                if isinstance(cp, dict) and cp.get("name"):
                    parsed_cps.append({
                        "name": str(cp["name"]).strip(),
                        "entity_type": str(cp.get("entity_type") or "CUSTOMER").upper(),
                        "aliases": cp.get("aliases") or [str(cp["name"]).strip()],
                        "default_category": cp.get("default_category"),
                        "default_gl_account": cp.get("default_gl_account"),
                    })
            for tx in doc.get("taxonomy", []):
                if isinstance(tx, dict) and tx.get("name"):
                    parsed_tax.append(tx)
        elif ext in ("xlsx", "xls", "xlsb"):
            try:
                xl = pd.ExcelFile(io.BytesIO(content))
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid Excel workbook: {e}")

            for s_name in xl.sheet_names:
                sn_low = s_name.lower()
                df_s = xl.parse(s_name).dropna(how="all")
                if df_s.empty:
                    continue
                if "counterpart" in sn_low:
                    for _, r in df_s.iterrows():
                        nr = {_norm_header(k): v for k, v in r.to_dict().items()}
                        nm = _clean_str(nr.get("counterpartyname") or nr.get("name"))
                        if not nm or _clean_str(nr.get("info")):
                            continue
                        et = (_clean_str(nr.get("entitytype") or nr.get("type")) or "CUSTOMER").upper()
                        aliases = _split_keywords(nr.get("aliasescommaseparated") or nr.get("aliases")) or [nm]
                        parsed_cps.append({
                            "name": nm,
                            "entity_type": et if et in ("CUSTOMER", "SUPPLIER", "BANK", "GOVERNMENT", "OTHER") else "CUSTOMER",
                            "aliases": aliases,
                            "default_category": _clean_str(nr.get("defaultcategory")) or None,
                            "default_gl_account": _clean_str(nr.get("defaultglaccount")) or None,
                        })
                elif "taxonom" in sn_low:
                    for _, r in df_s.iterrows():
                        nr = {_norm_header(k): v for k, v in r.to_dict().items()}
                        cls_name = _clean_str(nr.get("classname"))
                        sub_name = _clean_str(nr.get("subclassname"))
                        if (not cls_name and not sub_name) or _clean_str(nr.get("info")):
                            continue
                        parsed_tax.append({
                            "parent_name": cls_name or "Custom Imported Class",
                            "parent_code": _clean_str(nr.get("classcode")) or re.sub(r"[^A-Z0-9]+", "_", (cls_name or "CUSTOM").upper()).strip("_"),
                            "name": sub_name or cls_name,
                            "code": _clean_str(nr.get("subclasscode")) or re.sub(r"[^A-Z0-9]+", "_", (sub_name or cls_name).upper()).strip("_"),
                            "is_subclass": bool(sub_name),
                            "name_ar": _clean_str(nr.get("namearabic")) or None,
                            "direction": (_clean_str(nr.get("direction")) or "EITHER").upper(),
                            "default_gl_account": _clean_str(nr.get("defaultglaccount")) or None,
                            "description": _clean_str(nr.get("description")) or None,
                        })
                else:
                    # Rules sheet
                    for r_idx, r in enumerate(df_s.to_dict(orient="records"), start=2):
                        parsed_r, err = self._parse_rule_row_dict(r, r_idx)
                        if err:
                            row_errors.append(err)
                        elif parsed_r:
                            parsed_rules.append(parsed_r)
        else:
            df_csv = self._read_tabular_bytes(content, filename)
            for r_idx, r in enumerate(df_csv.to_dict(orient="records"), start=2):
                parsed_r, err = self._parse_rule_row_dict(r, r_idx)
                if err:
                    row_errors.append(err)
                elif parsed_r:
                    parsed_rules.append(parsed_r)

        # Match against existing tenant rules by normalized `rule_name`
        existing_by_name = {
            (r.rule_name or "").strip().lower(): r
            for r in db.query(ClassificationRule)
            .filter(
                ClassificationRule.company_id == customer_id,
                ClassificationRule.is_deleted == False,
            )
            .all()
            if r.rule_name
        }

        duplicates_found = sum(
            1 for r in parsed_rules if r["rule_name"].strip().lower() in existing_by_name
        )

        if dry_run:
            return {
                "dry_run": True,
                "duplicate_strategy": strategy,
                "valid_rules_count": len(parsed_rules),
                "duplicates_detected": duplicates_found,
                "valid_counterparties_count": len(parsed_cps),
                "valid_taxonomy_count": len(parsed_tax),
                "errors_count": len(row_errors),
                "row_errors": row_errors[:50],
                "rules_preview": [
                    {
                        "rule_name": r["rule_name"],
                        "priority": r["priority"],
                        "assigned_gl_account": r["assigned_gl_account"],
                        "assigned_category": r["assigned_category"],
                        "lifecycle_state": r["lifecycle_state"],
                        "conditions_count": len(r["conditions_json"].get("conditions", [])),
                        "is_duplicate": r["rule_name"].strip().lower() in existing_by_name,
                    }
                    for r in parsed_rules[:100]
                ],
                "counterparties_preview": parsed_cps[:50],
                "taxonomy_preview": parsed_tax[:50],
            }

        # ── Execute Atomic Bulk Commit ──
        from app.crud.crud_reconciliation_v2 import crud_counterparty

        created_rules = 0
        updated_rules = 0
        skipped_rules = 0

        for r in parsed_rules:
            nm_key = r["rule_name"].strip().lower()
            ex = existing_by_name.get(nm_key)
            if ex and strategy == "SKIP":
                skipped_rules += 1
                continue
            elif ex and strategy == "UPDATE":
                ex.priority = r["priority"]
                ex.bank_id = r["bank_id"]
                ex.assigned_gl_account = r["assigned_gl_account"]
                ex.assigned_category = r["assigned_category"]
                ex.assigned_sub_category = r["assigned_sub_category"]
                ex.lifecycle_state = r["lifecycle_state"]
                ex.conditions_json = r["conditions_json"]
                ex.is_active = True
                updated_rules += 1
            else:
                new_r = ClassificationRule(
                    company_id=customer_id,
                    bank_id=r["bank_id"],
                    rule_name=r["rule_name"],
                    rule_type="LITERAL",
                    rule_origin="MANUAL",
                    lifecycle_state=r["lifecycle_state"],
                    priority=r["priority"],
                    stop_after_match=True,
                    conditions_json=r["conditions_json"],
                    assigned_gl_account=r["assigned_gl_account"],
                    assigned_category=r["assigned_category"],
                    assigned_sub_category=r["assigned_sub_category"],
                    is_active=True,
                    created_by=user_id,
                )
                db.add(new_r)
                created_rules += 1

        cps_imported = 0
        for cp in parsed_cps:
            obj = crud_counterparty.learn_or_increment(
                db=db,
                name=cp["name"],
                customer_id=customer_id,
                entity_type=cp["entity_type"],
                default_category=cp["default_category"],
                default_gl=cp["default_gl_account"],
                user_id=user_id,
            )
            if obj and cp.get("aliases"):
                merged_aliases = list(dict.fromkeys((obj.aliases or []) + cp["aliases"]))
                obj.aliases = merged_aliases
                obj.is_verified = True
            cps_imported += 1

        tax_imported = 0
        if parsed_tax:
            existing_classes = {
                (c.code or "").upper(): c
                for c in db.query(ClassificationTaxonomy)
                .filter(
                    or_(ClassificationTaxonomy.company_id == customer_id, ClassificationTaxonomy.company_id == None),
                    ClassificationTaxonomy.parent_id == None,
                    ClassificationTaxonomy.is_deleted == False,
                )
                .all()
            }
            existing_all_codes = {
                (c.code or "").upper()
                for c in db.query(ClassificationTaxonomy)
                .filter(
                    or_(ClassificationTaxonomy.company_id == customer_id, ClassificationTaxonomy.company_id == None),
                    ClassificationTaxonomy.is_deleted == False,
                )
                .all()
            }

            for tx in parsed_tax:
                p_code = (tx.get("parent_code") or "").upper()
                t_code = (tx.get("code") or "").upper()
                if not t_code or t_code in existing_all_codes:
                    continue

                parent_id = None
                if p_code and p_code != t_code:
                    p_obj = existing_classes.get(p_code)
                    if not p_obj:
                        p_obj = ClassificationTaxonomy(
                            company_id=customer_id,
                            parent_id=None,
                            code=p_code,
                            name=tx.get("parent_name") or p_code,
                            direction="EITHER",
                            is_active=True,
                        )
                        db.add(p_obj)
                        db.flush()
                        existing_classes[p_code] = p_obj
                        existing_all_codes.add(p_code)
                    parent_id = p_obj.id

                new_node = ClassificationTaxonomy(
                    company_id=customer_id,
                    parent_id=parent_id,
                    code=t_code,
                    name=tx["name"],
                    name_ar=tx.get("name_ar"),
                    direction=tx.get("direction") or "EITHER",
                    default_gl_account=tx.get("default_gl_account"),
                    description=tx.get("description"),
                    is_active=True,
                )
                db.add(new_node)
                db.flush()
                if parent_id is None:
                    existing_classes[t_code] = new_node
                existing_all_codes.add(t_code)
                tax_imported += 1

        db.commit()
        return {
            "dry_run": False,
            "rules_created": created_rules,
            "rules_updated": updated_rules,
            "rules_skipped": skipped_rules,
            "counterparties_imported": cps_imported,
            "taxonomy_nodes_imported": tax_imported,
            "errors_count": len(row_errors),
            "row_errors": row_errors[:50],
            "status": (
                f"Bulk import completed: {created_rules} rules created, {updated_rules} updated, "
                f"{skipped_rules} skipped, {cps_imported} counterparties & {tax_imported} taxonomy items imported."
            ),
        }


reconciliation_onboarding_service = ReconciliationOnboardingIntelligenceService()
