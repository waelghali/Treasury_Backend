# app/services/inbox_outbound_service.py
"""
Outbound Email Service for Smart Inbox
Handles sending data request emails (LG Positions, Bank Statements, Cash Balances) to banks.
Attaches custom Message-ID and reference headers ([Ref: POS-...], [Ref: STM-...], [Ref: BAL-...])
for deterministic thread matching upon bank reply.
"""

import email
import email.utils
import hashlib
import logging
import smtplib
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape as html_escape
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.constants import SubscriptionStatus
from app.core.email_service import get_customer_email_settings, process_recipients
from app.models.models import Bank, Customer, SubscriptionPlan
from app.models.models_inbox import InboxOutboundRequest, InboxScheduleConfig
from app.models.models_issuance import CustomerBankAccount
from app.models.models_quotation import QuotationBank

logger = logging.getLogger("app.inbox_outbound")

REQUEST_REF_PREFIXES = {
    "LG_POSITION": "POS",
    "BANK_STATEMENT": "STM",
    "CASH_POSITION": "BAL",
}


class InboxOutboundService:
    """
    Manages sending system-initiated data request emails to banks.
    """

    def _get_bank_accounts_html(self, accounts: List[CustomerBankAccount]) -> Tuple[str, str]:
        """Builds clean EN/AR HTML summary lists of the customer's bank accounts at the target bank."""
        if not accounts:
            return "", ""
        items_en = "".join(
            f"<li><strong>{html_escape(a.account_name)}</strong>: "
            f"<span style='font-family:monospace;'>{html_escape(a.account_number)}</span>"
            f"{f' ({html_escape(a.currency_code)})' if a.currency_code else ''}"
            f"{f' · IBAN: {html_escape(a.iban)}' if a.iban else ''}</li>"
            for a in accounts
        )
        items_ar = "".join(
            f"<li><strong>{html_escape(a.account_name)}</strong>: "
            f"<span style='font-family:monospace; direction:ltr; display:inline-block;'>{html_escape(a.account_number)}</span>"
            f"{f' ({html_escape(a.currency_code)})' if a.currency_code else ''}</li>"
            for a in accounts
        )
        en_block = (
            f"<div style='margin:12px 0;'><strong>Accounts Included:</strong>"
            f"<ul style='margin:6px 0; padding-left:20px;'>{items_en}</ul></div>"
        )
        ar_block = (
            f"<div style='margin:12px 0;'><strong>الحسابات المشمولة:</strong>"
            f"<ul style='margin:6px 0; padding-right:20px;'>{items_ar}</ul></div>"
        )
        return en_block, ar_block

    def _compose_lg_position_email(
        self,
        customer_name: str,
        bank_name: str,
        position_date: date,
        request_reference: str,
        custom_notes: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Generates subject and bilingual HTML body for LG position request with unique reference number."""
        date_str = position_date.strftime("%d/%m/%Y")
        subject = f"[Ref: {request_reference}] LG Position Request · {date_str}"
        notes_html = f"<p style='margin-top:10px; color:#4b5563;'><strong>Notes:</strong> {html_escape(custom_notes)}</p>" if custom_notes else ""

        html_body = f"""
        <!DOCTYPE html>
        <html dir="ltr" lang="en">
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #1f2937; margin: 0; padding: 20px; }}
                .card {{ max-width: 600px; margin: auto; background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 24px; }}
                .header {{ border-bottom: 2px solid #3b82f6; padding-bottom: 12px; margin-bottom: 16px; }}
                .header h2 {{ margin: 0; color: #1e3a8a; font-size: 20px; }}
                .highlight {{ background: #eff6ff; border-left: 4px solid #3b82f6; padding: 12px; border-radius: 4px; margin: 16px 0; }}
                .ar {{ direction: rtl; text-align: right; margin-top: 24px; border-top: 1px dashed #d1d5db; padding-top: 16px; }}
                .footer {{ margin-top: 24px; font-size: 12px; color: #6b7280; border-top: 1px solid #f3f4f6; padding-top: 12px; }}
            </style>
        </head>
        <body>
            <div class="card">
                <div class="header">
                    <h2>Letter of Guarantee Position Request</h2>
                </div>
                <p>Dear <strong>{html_escape(bank_name)}</strong> Corporate Treasury &amp; Trade Finance Team,</p>
                <p>On behalf of <strong>{html_escape(customer_name)}</strong>, we kindly request the official Letter of Guarantee (LG) position report representing all active guarantees issued through your esteemed bank as of the following snapshot date:</p>
                
                <div class="highlight">
                    <p style="margin:0;"><strong>Reference No. / الرقم المرجعي:</strong> <span style="font-family: monospace; font-size: 15px; font-weight: bold; color: #1e3a8a;">{request_reference}</span></p>
                    <p style="margin:4px 0 0 0;"><strong>Company:</strong> {html_escape(customer_name)}</p>
                    <p style="margin:4px 0 0 0;"><strong>Requested Position Date:</strong> {date_str}</p>
                </div>

                <p>Please reply directly to this email with the position report in Excel (preferred) or PDF format.</p>
                <p style="font-size: 13px; color: #4b5563;"><em>Note: If there are no active guarantees under our entity as of this date, kindly reply stating "NIL / Zero Position".</em></p>
                {notes_html}

                <div class="ar">
                    <p>السادة / فريق تمويل التجارة والضمانات بـ <strong>{html_escape(bank_name)}</strong>،</p>
                    <p>بالإشارة إلى التسهيلات الائتمانية الخاصة بشركة <strong>{html_escape(customer_name)}</strong>، برجاء التكرم بموافاتنا بموقف خطابات الضمان السارية والصادرة من طرفكم لصالح شركتنا حتى تاريخ <strong>{date_str}</strong>.</p>
                    <p>يرجى التكرم بالرد على هذا البريد الإلكتروني مباشرة وإرفاق بيان الضمانات (Excel / PDF)، أو الإفادة كتابياً في حال عدم وجود خطابات ضمان قائمة (موقف صفر).</p>
                </div>

                <div class="footer">
                    <p>This automated message was generated by Grow Treasury Platform for {html_escape(customer_name)} [Ref: {request_reference}]. Please reply directly to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        return subject, html_body

    def _compose_bank_statement_email(
        self,
        customer_name: str,
        bank_name: str,
        period_start: date,
        period_end: date,
        request_reference: str,
        accounts: List[CustomerBankAccount],
        custom_notes: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Generates subject and bilingual HTML body requesting account statement & closing balances."""
        start_str = period_start.strftime("%d/%m/%Y")
        end_str = period_end.strftime("%d/%m/%Y")
        subject = f"[Ref: {request_reference}] Account Statement Request · {start_str} - {end_str}"
        notes_html = f"<p style='margin-top:10px; color:#4b5563;'><strong>Notes:</strong> {html_escape(custom_notes)}</p>" if custom_notes else ""
        acct_en, acct_ar = self._get_bank_accounts_html(accounts)

        html_body = f"""
        <!DOCTYPE html>
        <html dir="ltr" lang="en">
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #1f2937; margin: 0; padding: 20px; }}
                .card {{ max-width: 620px; margin: auto; background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 24px; }}
                .header {{ border-bottom: 2px solid #059669; padding-bottom: 12px; margin-bottom: 16px; }}
                .header h2 {{ margin: 0; color: #065f46; font-size: 20px; }}
                .highlight {{ background: #ecfdf5; border-left: 4px solid #059669; padding: 12px; border-radius: 4px; margin: 16px 0; }}
                .ar {{ direction: rtl; text-align: right; margin-top: 24px; border-top: 1px dashed #d1d5db; padding-top: 16px; }}
                .footer {{ margin-top: 24px; font-size: 12px; color: #6b7280; border-top: 1px solid #f3f4f6; padding-top: 12px; }}
            </style>
        </head>
        <body>
            <div class="card">
                <div class="header">
                    <h2>Corporate Bank Statement &amp; Balance Request</h2>
                </div>
                <p>Dear <strong>{html_escape(bank_name)}</strong> Corporate Cash Management Team,</p>
                <p>On behalf of <strong>{html_escape(customer_name)}</strong>, kindly send us the official account statement(s) and closing balances for the period below:</p>
                <div class="highlight">
                    <p style="margin:0;"><strong>Reference No. / الرقم المرجعي:</strong> <span style="font-family: monospace; font-size: 15px; font-weight: bold; color: #065f46;">{request_reference}</span></p>
                    <p style="margin:4px 0 0 0;"><strong>Company:</strong> {html_escape(customer_name)}</p>
                    <p style="margin:4px 0 0 0;"><strong>Statement Period:</strong> {start_str} to {end_str}</p>
                </div>
                {acct_en}
                <p>Please reply directly to this email with the statement in <strong>Excel (.xlsx / .csv)</strong>, <strong>MT940 / CAMT.053</strong>, or <strong>PDF</strong> format.</p>
                {notes_html}

                <div class="ar">
                    <p>السادة / إدارة حسابات الشركات وإدارة السيولة بـ <strong>{html_escape(bank_name)}</strong>،</p>
                    <p>بالإشارة إلى حسابات شركة <strong>{html_escape(customer_name)}</strong> لدى مصرفكم الموقر، برجاء التكرم بموافاتنا بكشف الحساب الرسمي والأرصدة الختامية عن الفترة من <strong>{start_str}</strong> إلى <strong>{end_str}</strong>.</p>
                    {acct_ar}
                    <p>يرجى الرد على هذا البريد الإلكتروني مباشرة وإرفاق كشف الحساب بصيغة (Excel / CSV / MT940 / PDF).</p>
                </div>

                <div class="footer">
                    <p>Generated by Grow Treasury Platform for {html_escape(customer_name)} [Ref: {request_reference}]. Please reply directly to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        return subject, html_body

    def _compose_cash_position_email(
        self,
        customer_name: str,
        bank_name: str,
        position_date: date,
        request_reference: str,
        accounts: List[CustomerBankAccount],
        custom_notes: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Generates subject and bilingual HTML body requesting daily available & ledger cash balances."""
        date_str = position_date.strftime("%d/%m/%Y")
        subject = f"[Ref: {request_reference}] Daily Cash Balance Request · {date_str}"
        notes_html = f"<p style='margin-top:10px; color:#4b5563;'><strong>Notes:</strong> {html_escape(custom_notes)}</p>" if custom_notes else ""
        acct_en, acct_ar = self._get_bank_accounts_html(accounts)

        html_body = f"""
        <!DOCTYPE html>
        <html dir="ltr" lang="en">
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #1f2937; margin: 0; padding: 20px; }}
                .card {{ max-width: 620px; margin: auto; background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 24px; }}
                .header {{ border-bottom: 2px solid #0d9488; padding-bottom: 12px; margin-bottom: 16px; }}
                .header h2 {{ margin: 0; color: #115e59; font-size: 20px; }}
                .highlight {{ background: #f0fdfa; border-left: 4px solid #0d9488; padding: 12px; border-radius: 4px; margin: 16px 0; }}
                .ar {{ direction: rtl; text-align: right; margin-top: 24px; border-top: 1px dashed #d1d5db; padding-top: 16px; }}
                .footer {{ margin-top: 24px; font-size: 12px; color: #6b7280; border-top: 1px solid #f3f4f6; padding-top: 12px; }}
            </style>
        </head>
        <body>
            <div class="card">
                <div class="header">
                    <h2>Daily Cash Position &amp; Account Balance Request</h2>
                </div>
                <p>Dear <strong>{html_escape(bank_name)}</strong> Corporate Relationship &amp; Operations Team,</p>
                <p>On behalf of <strong>{html_escape(customer_name)}</strong>, please share our current <strong>Available Balance</strong> and <strong>Ledger Balance</strong> across all active currencies as of <strong>{date_str}</strong>:</p>
                <div class="highlight">
                    <p style="margin:0;"><strong>Reference No. / الرقم المرجعي:</strong> <span style="font-family: monospace; font-size: 15px; font-weight: bold; color: #115e59;">{request_reference}</span></p>
                    <p style="margin:4px 0 0 0;"><strong>Company:</strong> {html_escape(customer_name)}</p>
                    <p style="margin:4px 0 0 0;"><strong>Balance As-Of Date:</strong> {date_str}</p>
                </div>
                {acct_en}
                <p>You may reply directly in the email body (stating Available Balance &amp; Ledger Balance per account/currency) or attach a balance summary sheet.</p>
                {notes_html}

                <div class="ar">
                    <p>السادة / فريق خدمة عملاء الشركات بـ <strong>{html_escape(bank_name)}</strong>،</p>
                    <p>بالإشارة إلى حسابات شركة <strong>{html_escape(customer_name)}</strong>، برجاء التكرم بموافاتنا بالرصيد المتاح والرصيد الدفتري الحالي لكافة الحسابات والعملات حتى تاريخ <strong>{date_str}</strong>.</p>
                    {acct_ar}
                    <p>يمكنكم الرد مباشرة في نص البريد الإلكتروني بذكر (الرصيد المتاح / الرصيد الدفتري) أو إرفاق ملف الأرصدة.</p>
                </div>

                <div class="footer">
                    <p>Generated by Grow Treasury Platform for {html_escape(customer_name)} [Ref: {request_reference}]. Please reply directly to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        return subject, html_body

    def send_position_request(
        self,
        db: Session,
        customer_id: int,
        bank_id: int,
        position_date: Optional[date] = None,
        custom_recipients: Optional[List[str]] = None,
        custom_subject: Optional[str] = None,
        custom_notes: Optional[str] = None,
        user_id: Optional[int] = None,
        request_type: str = "LG_POSITION",
        statement_period_start: Optional[date] = None,
        statement_period_end: Optional[date] = None,
    ) -> InboxOutboundRequest:
        """
        Sends an LG position, bank statement, or cash balance request email to a specific bank
        and logs the outbound request for thread matching.
        """
        customer = db.query(Customer).filter(Customer.id == customer_id, Customer.is_deleted == False).first()
        if not customer:
            raise ValueError(f"Customer {customer_id} not found")

        bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == False).first()
        if not bank:
            raise ValueError(f"Bank {bank_id} not found")

        req_type = (request_type or "LG_POSITION").strip().upper()
        if req_type not in REQUEST_REF_PREFIXES:
            req_type = "LG_POSITION"

        pos_date = position_date or date.today()
        period_end = statement_period_end or pos_date
        period_start = statement_period_start or (period_end - timedelta(days=1))

        # Generate unique human-readable Reference Number: POS-..., STM-..., or BAL-...
        ref_prefix = REQUEST_REF_PREFIXES[req_type]
        hash_seed = f"{customer_id}-{bank_id}-{req_type}-{pos_date}-{datetime.now(timezone.utc).isoformat()}".encode()
        suffix = hashlib.md5(hash_seed).hexdigest()[:4].upper()
        req_ref = f"{ref_prefix}-{pos_date.strftime('%Y%m%d')}-{suffix}"

        # Determine recipient emails (explicit -> schedule config -> quotation bank -> domain default)
        recipients: List[str] = []
        if custom_recipients:
            recipients = [r.strip() for r in custom_recipients if r and r.strip()]

        if not recipients:
            sched_cfg = db.query(InboxScheduleConfig).filter(
                InboxScheduleConfig.customer_id == customer_id,
                InboxScheduleConfig.bank_id == bank_id,
                InboxScheduleConfig.request_type == req_type,
                InboxScheduleConfig.is_deleted == False,
            ).first()
            if sched_cfg and isinstance(sched_cfg.recipient_emails, list) and sched_cfg.recipient_emails:
                recipients = [str(r).strip() for r in sched_cfg.recipient_emails if str(r).strip()]

        if not recipients:
            q_bank = db.query(QuotationBank).filter(
                QuotationBank.customer_id == customer_id,
                QuotationBank.bank_id == bank_id,
                QuotationBank.is_deleted == False,
            ).first()
            if q_bank and q_bank.emails:
                recipients = q_bank.emails if isinstance(q_bank.emails, list) else [q_bank.emails]

        if not recipients and bank.email_domain:
            default_alias = "trade-finance" if req_type == "LG_POSITION" else "cash-management"
            recipients = [f"{default_alias}@{bank.email_domain}"]

        if not recipients:
            raise ValueError(f"No recipient email addresses found for bank '{bank.name}'. Please specify recipient emails.")

        accounts = (
            db.query(CustomerBankAccount)
            .filter(
                CustomerBankAccount.customer_id == customer_id,
                CustomerBankAccount.bank_id == bank_id,
                CustomerBankAccount.is_active == True,
                CustomerBankAccount.is_deleted == False,
            )
            .all()
        )

        # Compose Subject & Body per request_type
        if req_type == "BANK_STATEMENT":
            default_subject, html_body = self._compose_bank_statement_email(
                customer_name=customer.name,
                bank_name=bank.name,
                period_start=period_start,
                period_end=period_end,
                request_reference=req_ref,
                accounts=accounts,
                custom_notes=custom_notes,
            )
        elif req_type == "CASH_POSITION":
            default_subject, html_body = self._compose_cash_position_email(
                customer_name=customer.name,
                bank_name=bank.name,
                position_date=pos_date,
                request_reference=req_ref,
                accounts=accounts,
                custom_notes=custom_notes,
            )
        else:
            default_subject, html_body = self._compose_lg_position_email(
                customer_name=customer.name,
                bank_name=bank.name,
                position_date=pos_date,
                request_reference=req_ref,
                custom_notes=custom_notes,
            )
        subject = custom_subject or default_subject

        # Get customer email settings
        email_settings, _source = get_customer_email_settings(db, customer_id)
        if not email_settings or not email_settings.smtp_host or not email_settings.sender_email:
            raise ValueError(f"Customer {customer_id} does not have valid SMTP settings configured.")

        # Generate unique Message-ID
        timestamp = int(datetime.now(timezone.utc).timestamp())
        msg_id_domain = email_settings.sender_email.split("@")[-1] if "@" in email_settings.sender_email else "grow.com"
        message_id_header = f"req-{customer_id}-{bank_id}-{req_type.lower()}-{pos_date.strftime('%Y%m%d')}-{timestamp}@{msg_id_domain}"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{email_settings.sender_display_name or customer.name} <{email_settings.sender_email}>"
        msg["To"] = ", ".join(recipients)
        msg["Message-ID"] = f"<{message_id_header}>"
        msg["Date"] = email.utils.formatdate(localtime=True)
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        deliverable_recipients = process_recipients(recipients)

        if not deliverable_recipients:
            logger.info(f"Outbound email delivery simulated: All recipients ({recipients}) belong to test/dummy domains.")
        elif not email_settings.smtp_host or email_settings.smtp_host in ("smtp.example.com", "localhost", "dummy"):
            logger.info("Outbound email delivery simulated: SMTP host not configured for live delivery.")
        else:
            try:
                if int(email_settings.smtp_port) == 443:
                    from requests_ntlm import HttpNtlmAuth
                    import requests
                    from xml.sax.saxutils import escape

                    auth = HttpNtlmAuth(email_settings.smtp_username, email_settings.smtp_password)
                    url = f"https://{email_settings.smtp_host}/ews/exchange.asmx"
                    to_recipients_xml = "".join([f"<t:Mailbox><t:EmailAddress>{escape(addr)}</t:EmailAddress></t:Mailbox>" for addr in deliverable_recipients])
                    soap_payload = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
               xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" 
               xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types" 
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <t:RequestServerVersion Version="Exchange2016" />
  </soap:Header>
  <soap:Body>
    <m:CreateItem MessageDisposition="SendAndSaveCopy">
      <m:SavedItemFolderId>
        <t:DistinguishedFolderId Id="sentitems" />
      </m:SavedItemFolderId>
      <m:Items>
        <t:Message>
          <t:ItemClass>IPM.Note</t:ItemClass>
          <t:Subject>{escape(subject)}</t:Subject>
          <t:Body BodyType="HTML">{escape(html_body)}</t:Body>
          <t:ToRecipients>{to_recipients_xml}</t:ToRecipients>
        </t:Message>
      </m:Items>
    </m:CreateItem>
  </soap:Body>
</soap:Envelope>"""
                    resp = requests.post(url, data=soap_payload.encode("utf-8"), auth=auth, headers={"Content-Type": "text/xml; charset=utf-8"}, timeout=20)
                    if resp.status_code != 200 or "NoError" not in resp.text:
                        raise RuntimeError(f"Exchange EWS dispatch failed (HTTP {resp.status_code}): {resp.text[:250]}")
                elif int(email_settings.smtp_port) == 465:
                    server = smtplib.SMTP_SSL(email_settings.smtp_host, email_settings.smtp_port, timeout=15)
                    if email_settings.smtp_username and email_settings.smtp_password:
                        server.login(email_settings.smtp_username, email_settings.smtp_password)
                    server.sendmail(email_settings.sender_email, deliverable_recipients, msg.as_string())
                    server.quit()
                else:
                    server = smtplib.SMTP(email_settings.smtp_host, email_settings.smtp_port, timeout=15)
                    server.starttls()
                    if email_settings.smtp_username and email_settings.smtp_password:
                        server.login(email_settings.smtp_username, email_settings.smtp_password)
                    server.sendmail(email_settings.sender_email, deliverable_recipients, msg.as_string())
                    server.quit()

                logger.info(f"Sent {req_type} request [{req_ref}] to {deliverable_recipients} for bank {bank.name}")

            except Exception as e:
                logger.error(f"Failed sending outbound email to {deliverable_recipients}: {e}", exc_info=True)
                raise ValueError(f"SMTP error connecting to '{email_settings.smtp_host}:{email_settings.smtp_port}'. Please verify your SMTP settings. Details: {str(e)}")

        outbound_rec = InboxOutboundRequest(
            customer_id=customer_id,
            bank_id=bank_id,
            request_type=req_type,
            request_reference=req_ref,
            sent_to_emails=recipients,
            subject=subject,
            message_id=message_id_header,
            position_date=pos_date,
            statement_period_start=period_start if req_type == "BANK_STATEMENT" else None,
            statement_period_end=period_end if req_type == "BANK_STATEMENT" else None,
            sent_by_user_id=user_id,
            is_scheduled=(user_id is None),
        )
        db.add(outbound_rec)
        db.commit()
        db.refresh(outbound_rec)
        return outbound_rec

    def send_scheduled_requests(self, db: Session) -> List[InboxOutboundRequest]:
        """
        Scans active schedule configs and sends scheduled requests due today.
        Supports LG_POSITION, BANK_STATEMENT, and CASH_POSITION without duplicating
        dispatches on the same day.
        """
        today = date.today()
        configs = (
            db.query(InboxScheduleConfig)
            .join(Customer, Customer.id == InboxScheduleConfig.customer_id)
            .join(SubscriptionPlan, SubscriptionPlan.id == Customer.subscription_plan_id)
            .filter(
                InboxScheduleConfig.is_active == True,
                InboxScheduleConfig.is_deleted == False,
                Customer.is_deleted == False,
                Customer.status == SubscriptionStatus.ACTIVE,
                SubscriptionPlan.can_email_inbox == True,
            )
            .all()
        )

        sent_requests = []
        for cfg in configs:
            # Skip if already sent today
            if cfg.last_sent_at and cfg.last_sent_at.date() == today:
                continue

            is_due = False
            if cfg.frequency == "MONTHLY" and cfg.day_of_month == today.day:
                is_due = True
            elif cfg.frequency == "WEEKLY" and cfg.day_of_week == today.weekday():
                is_due = True
            elif cfg.frequency == "DAILY":
                is_due = True

            if is_due:
                try:
                    p_start = None
                    p_end = today
                    if cfg.request_type == "BANK_STATEMENT":
                        if cfg.frequency == "DAILY":
                            p_start = today - timedelta(days=1)
                        elif cfg.frequency == "WEEKLY":
                            p_start = today - timedelta(days=7)
                        else:
                            first_of_this_month = today.replace(day=1)
                            p_end = first_of_this_month - timedelta(days=1)
                            p_start = p_end.replace(day=1)

                    req = self.send_position_request(
                        db=db,
                        customer_id=cfg.customer_id,
                        bank_id=cfg.bank_id,
                        position_date=today,
                        custom_recipients=cfg.recipient_emails if isinstance(cfg.recipient_emails, list) else None,
                        custom_subject=cfg.custom_subject,
                        custom_notes=cfg.custom_body,
                        user_id=None,
                        request_type=cfg.request_type or "LG_POSITION",
                        statement_period_start=p_start,
                        statement_period_end=p_end,
                    )
                    cfg.last_sent_at = datetime.now(timezone.utc)
                    req.schedule_config_id = cfg.id
                    db.commit()
                    db.refresh(req)
                    sent_requests.append(req)
                except Exception as e:
                    db.rollback()
                    logger.error(f"Scheduled request failed for customer {cfg.customer_id}, bank {cfg.bank_id}: {e}")

        return sent_requests


inbox_outbound_service = InboxOutboundService()
