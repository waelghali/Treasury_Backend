# core/email_service.py

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Tuple, Dict, Any
from sqlalchemy.orm import Session, selectinload
import logging

# Import encryption/decryption utilities
from app.core.encryption import decrypt_data

# NEW: Import Customer and CustomerEmailSetting models
from app.models import Customer, CustomerEmailSetting

logger = logging.getLogger(__name__) # Initialize logger for this module

# We'll define a simple structure for email settings to pass around
class EmailSettings:
    def __init__(self, smtp_host: str, smtp_port: int, smtp_username: str, smtp_password: str, sender_email: str, sender_display_name: Optional[str] = None):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password # This would be decrypted
        self.sender_email = sender_email
        self.sender_display_name = sender_display_name

def get_global_email_settings() -> EmailSettings:
    """
    Retrieves global email settings from environment variables.
    """
    _sender_email = os.getenv("EMAIL_SENDER_EMAIL")
    _smtp_host = os.getenv("EMAIL_SMTP_HOST")
    _smtp_port = int(os.getenv("EMAIL_SMTP_PORT", 587)) # Default to 587 if not set
    _smtp_username = os.getenv("EMAIL_SMTP_USERNAME")
    _smtp_password = os.getenv("EMAIL_SMTP_PASSWORD") # This would be plain text from env
    _global_sender_display_name = os.getenv("GLOBAL_SENDER_DISPLAY_NAME", "Treasury Platform Notifications") # NEW env var for global display name

    if not all([_sender_email, _smtp_host, _smtp_username, _smtp_password]):
        logger.warning("Missing one or more global email configuration environment variables. Using fallback/empty settings.")
        return EmailSettings(
            smtp_host="", smtp_port=587, smtp_username="", smtp_password="",
            sender_email="no-reply@example.com", sender_display_name="System Notifications"
        )

    return EmailSettings(
        smtp_host=_smtp_host,
        smtp_port=_smtp_port,
        smtp_username=_smtp_username,
        smtp_password=_smtp_password,
        sender_email=_sender_email,
        sender_display_name=_global_sender_display_name
    )

# NEW FUNCTION: get_customer_email_settings
# MODIFIED: Now returns Tuple[EmailSettings, str]
def get_customer_email_settings(db: Session, customer_id: int) -> Tuple[EmailSettings, str]:
    """
    Retrieves customer-specific email settings from the database.
    If not found or inactive, falls back to global settings.
    Returns a tuple of (EmailSettings object, string indicating method used).
    """
    customer = db.query(Customer).options(
        selectinload(Customer.customer_email_settings)
    ).filter(Customer.id == customer_id).first()

    if (
        customer
        and customer.customer_email_settings
        and customer.customer_email_settings.is_active
    ):
        try:
            decrypted_password = decrypt_data(
                customer.customer_email_settings.smtp_password_encrypted
            )
            logger.info(f"Using customer-specific email settings for customer ID: {customer_id}")
            return (
                EmailSettings(
                    smtp_host=customer.customer_email_settings.smtp_host,
                    smtp_port=customer.customer_email_settings.smtp_port,
                    smtp_username=customer.customer_email_settings.smtp_username,
                    smtp_password=decrypted_password,
                    sender_email=customer.customer_email_settings.sender_email,
                    sender_display_name=customer.customer_email_settings.sender_display_name,
                ),
                "customer_specific" # Indicate method used
            )
        except Exception as e:
            logger.error(f"Failed to decrypt/load customer-specific email settings for customer ID {customer_id}: {e}. Falling back to global settings.", exc_info=True)
            # Fallback returns tuple as well
            return get_global_email_settings(), "global_fallback_due_to_error"
    else:
        logger.info(f"Customer-specific email settings not found or inactive for customer ID: {customer_id}. Falling back to global settings.")
        # Fallback returns tuple as well
        return get_global_email_settings(), "global"


# app/core/email_service.py
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional, Tuple, Dict, Any, Union
from sqlalchemy.orm import Session, selectinload
import logging
from io import BytesIO

# Import encryption/decryption utilities
from app.core.encryption import decrypt_data

# NEW: Import Customer and CustomerEmailSetting models
from app.models import Customer, CustomerEmailSetting

logger = logging.getLogger(__name__) # Initialize logger for this module

# We'll define a simple structure for email settings to pass around
class EmailSettings:
    def __init__(self, smtp_host: str, smtp_port: int, smtp_username: str, smtp_password: str, sender_email: str, sender_display_name: Optional[str] = None):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password # This would be decrypted
        self.sender_email = sender_email
        self.sender_display_name = sender_display_name
        
# NEW: Data structure for an attachment
class EmailAttachment:
    def __init__(self, filename: str, content: bytes, mime_type: str):
        self.filename = filename
        self.content = content
        self.mime_type = mime_type

def get_global_email_settings() -> EmailSettings:
    """
    Retrieves global email settings from environment variables.
    """
    _sender_email = os.getenv("EMAIL_SENDER_EMAIL")
    _smtp_host = os.getenv("EMAIL_SMTP_HOST")
    _smtp_port = int(os.getenv("EMAIL_SMTP_PORT", 587)) # Default to 587 if not set
    _smtp_username = os.getenv("EMAIL_SMTP_USERNAME")
    _smtp_password = os.getenv("EMAIL_SMTP_PASSWORD") # This would be plain text from env
    _global_sender_display_name = os.getenv("GLOBAL_SENDER_DISPLAY_NAME", "Treasury Platform Notifications") # NEW env var for global display name

    if not all([_sender_email, _smtp_host, _smtp_username, _smtp_password]):
        logger.warning("Missing one or more global email configuration environment variables. Using fallback/empty settings.")
        return EmailSettings(
            smtp_host="", smtp_port=587, smtp_username="", smtp_password="",
            sender_email="no-reply@example.com", sender_display_name="System Notifications"
        )

    return EmailSettings(
        smtp_host=_smtp_host,
        smtp_port=_smtp_port,
        smtp_username=_smtp_username,
        smtp_password=_smtp_password,
        sender_email=_sender_email,
        sender_display_name=_global_sender_display_name
    )

# NEW FUNCTION: get_customer_email_settings
def get_customer_email_settings(db: Session, customer_id: int) -> Tuple[EmailSettings, str]:
    """
    Retrieves customer-specific email settings from the database.
    If not found or inactive or invalid, falls back to global settings.
    Returns a tuple of (EmailSettings object, string indicating method used).
    """
    customer = db.query(Customer).options(
        selectinload(Customer.customer_email_settings)
    ).filter(Customer.id == customer_id).first()

    if (
        customer
        and customer.customer_email_settings
        and customer.customer_email_settings.is_active
    ):
        # NEW: Check for missing or blank essential fields before attempting to use them
        settings = customer.customer_email_settings
        if not all([settings.smtp_host, settings.smtp_username, settings.smtp_password_encrypted, settings.sender_email]):
            logger.warning(f"Incomplete customer-specific email settings found for customer ID {customer_id}. Falling back to global.")
            return get_global_email_settings(), "global_fallback_due_to_incomplete_settings"

        try:
            decrypted_password = decrypt_data(
                settings.smtp_password_encrypted
            )
            logger.info(f"Using customer-specific email settings for customer ID: {customer_id}")
            return (
                EmailSettings(
                    smtp_host=settings.smtp_host,
                    smtp_port=settings.smtp_port,
                    smtp_username=settings.smtp_username,
                    smtp_password=decrypted_password,
                    sender_email=settings.sender_email,
                    sender_display_name=settings.sender_display_name,
                ),
                "customer_specific" # Indicate method used
            )
        except Exception as e:
            logger.error(f"Failed to decrypt/load customer-specific email settings for customer ID {customer_id}: {e}. Falling back to global settings.", exc_info=True)
            # Fallback returns tuple as well
            return get_global_email_settings(), "global_fallback_due_to_error"
    else:
        logger.info(f"Customer-specific email settings not found or inactive for customer ID: {customer_id}. Falling back to global settings.")
        # Fallback returns tuple as well
        return get_global_email_settings(), "global"

async def send_email( # Changed to async function
    db: Session, # NEW: Added db session as a parameter for logging purposes
    to_emails: List[str],
    subject_template: str, # Changed to subject_template
    body_template: str,    # Changed to body_template
    template_data: Dict[str, Any], # NEW: Added template_data for subject/body population
    email_settings: EmailSettings,
    cc_emails: Optional[List[str]] = None,
    sender_name: Optional[str] = None, # NEW: Added sender_name
    attachments: Optional[List[EmailAttachment]] = None # NEW: Added attachments parameter
) -> bool:
    """
    Sends an email using the provided SMTP settings.
    Populates subject and body from templates using template_data.

    Args:
        db: SQLAlchemy Session for logging.
        to_emails: A list of recipient email addresses.
        subject_template: The subject line template of the email.
        body_template: The HTML content template of the email body.
        template_data: Dictionary of data to populate placeholders in templates.
        email_settings: An EmailSettings object containing SMTP details.
        cc_emails: An optional list of CC recipient email addresses.
        sender_name: Optional string to use as the display name in the From header.
                     If not provided, email_settings.sender_display_name is used.
        attachments: An optional list of EmailAttachment objects to be attached.

    Returns:
        True if the email was sent successfully, False otherwise.
    """
    if not to_emails:
        logger.warning("No recipient email addresses provided. Email not sent.")
        return False

    subject = subject_template
    body_html = body_template
    
    try:
        final_sender_display_name = sender_name if sender_name else email_settings.sender_display_name

        logger.debug(f"Attempting to send email. Recipients: {to_emails}, CC: {cc_emails}, Subject: {subject}")
        logger.debug(f"Using SMTP Host: {email_settings.smtp_host}, Port: {email_settings.smtp_port}, Username: {email_settings.smtp_username}")
        logger.debug(f"Sender: {final_sender_display_name} <{email_settings.sender_email}>")

        # Use MIMEMultipart('mixed') for attachments
        msg = MIMEMultipart('mixed')

        if final_sender_display_name:
            msg['From'] = f"{final_sender_display_name} <{email_settings.sender_email}>"
        else:
            msg['From'] = email_settings.sender_email

        msg['To'] = ", ".join(to_emails)
        if cc_emails:
            msg['Cc'] = ", ".join(cc_emails)

        msg['Subject'] = subject

        # Create a separate part for the HTML body
        msg.attach(MIMEText(body_html, 'html'))
        
        # Attach files if any
        if attachments:
            for attachment in attachments:
                main_type, sub_type = attachment.mime_type.split('/', 1)
                part = MIMEBase(main_type, sub_type)
                part.set_payload(attachment.content)
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{attachment.filename}"')
                msg.attach(part)


        all_recipients = to_emails[:] # Create a copy to avoid modifying original list
        if cc_emails:
            all_recipients.extend(cc_emails)
        all_recipients = list(set(all_recipients))

        server = None
        if email_settings.smtp_port == 465:
            logger.debug(f"Establishing SMTP_SSL connection to {email_settings.smtp_host}:{email_settings.smtp_port}")
            server = smtplib.SMTP_SSL(email_settings.smtp_host, email_settings.smtp_port)
        
        else:
            logger.debug(f"Establishing SMTP connection to {email_settings.smtp_host}:{email_settings.smtp_port}")
            server = smtplib.SMTP(email_settings.smtp_host, email_settings.smtp_port)
            logger.debug("Starting TLS...")
            server.starttls()
            logger.debug("TLS started.")

        logger.debug("Logging in to SMTP server...")
        server.login(email_settings.smtp_username, email_settings.smtp_password)
        logger.debug("Successfully logged in. Sending message...")
        server.send_message(msg, from_addr=email_settings.sender_email, to_addrs=all_recipients)
        logger.debug("Message sent. Quitting SMTP server.")
        server.quit()

        logger.info(f"Email sent successfully to {to_emails} (CC: {cc_emails}) using settings from {email_settings.sender_email} (Host: {email_settings.smtp_host}).")
        return True
    except smtplib.SMTPConnectError as e:
        logger.error(f"SMTP Connection Error: Could not connect to SMTP host {email_settings.smtp_host}:{email_settings.smtp_port}. Check host/port and firewall. Error: {e}", exc_info=True)
        return False
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP Authentication Error: Failed to log in with username '{email_settings.smtp_username}'. Check username/password. Error: {e}", exc_info=True)
        return False
    except smtplib.SMTPException as e:
        logger.error(f"General SMTP Error: An SMTP protocol error occurred. Error: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during email sending to {to_emails} (CC: {cc_emails}) using settings from {email_settings.sender_email} (Host: {email_settings.smtp_host}). ERROR DETAILS: {e}", exc_info=True)
        return False

# Example usage (for internal testing, won't be called in production FastAPI directly)
if __name__ == "__main__":
    # For local testing, ensure APP_ENCRYPTION_KEY and other EMAIL_* env vars are set
    # e.g., in your shell or by uncommenting load_dotenv() at the top.

    # Mock DB session and models for testing purposes
    class MockCustomerEmailSetting:
        def __init__(self, smtp_host, smtp_port, smtp_username, smtp_password_encrypted, sender_email, sender_display_name, is_active):
            self.smtp_host = smtp_host
            self.smtp_port = smtp_port
            self.smtp_username = smtp_username
            self.smtp_password_encrypted = smtp_password_encrypted
            self.sender_email = sender_email
            self.sender_display_name = sender_display_name
            self.is_active = is_active

    class MockCustomer:
        def __init__(self, id, customer_email_settings=None):
            self.id = id
            self.customer_email_settings = customer_email_settings

    class MockDBSession:
        def query(self, model):
            self.model = model
            return self

        def options(self, *args):
            return self

        def filter(self, *args):
            return self

        def first(self):
            if self.model == Customer:
                # Simulate a customer with custom settings
                if self.customer_id_filter == 1:
                    encrypted_password = "encrypted_customer_pass" # In real app, this comes from DB
                    return MockCustomer(1, MockCustomerEmailSetting("smtp.customer.com", 587, "cust_user", encrypted_password, "cust@domain.com", "Custom Sender", True))
                # Simulate a customer without custom settings
                elif self.customer_id_filter == 2:
                    return MockCustomer(2, None)
            return None # Default for other models

        def get(self, id):
            if self.model == Customer:
                if id == 1:
                    encrypted_password = "encrypted_customer_pass"
                    return MockCustomer(1, MockCustomerEmailSetting("smtp.customer.com", 587, "cust_user", encrypted_password, "cust@domain.com", "Custom Sender", True))
                elif id == 2:
                    return MockCustomer(2, None)
            return None

        def filter_by(self, **kwargs):
            self.customer_id_filter = kwargs.get('id')
            return self

    mock_db = MockDBSession()

    # Mock decrypt_data for testing purposes
    def mock_decrypt_data(encrypted_data):
        if encrypted_data == "encrypted_customer_pass":
            return "decrypted_customer_pass"
        return encrypted_data

    # Temporarily override the actual decrypt_data with the mock
    from app.core import encryption # Corrected import path for encryption
    original_decrypt_data = encryption.decrypt_data
    encryption.decrypt_data = mock_decrypt_data


    # Example global settings (replace with your actual env vars or hardcode for test)
    # IMPORTANT: For testing with Mailtrap or similar, ensure these are correct.
    # For Mailtrap, host is usually smtp.mailtrap.io, port 2525 or 587, username/password from your inbox.
    os.environ["EMAIL_SENDER_EMAIL"] = "global_sender@example.com"
    os.environ["EMAIL_SMTP_HOST"] = "smtp.mailtrap.io" # Example for testing
    os.environ["EMAIL_SMTP_PORT"] = "2525" # Or 587, or 465 if using SSL directly
    os.environ["EMAIL_SMTP_USERNAME"] = "YOUR_MAILTRAP_USERNAME" # <--- REPLACE THIS
    os.environ["EMAIL_SMTP_PASSWORD"] = "YOUR_MAILTRAP_PASSWORD" # <--- REPLACE THIS
    os.environ["GLOBAL_SENDER_DISPLAY_NAME"] = "Global Platform Notifications"

    import asyncio # Import asyncio for running async functions in __main__

    async def test_email_sending_main():
        try:
            # Test global settings
            global_settings_for_test = get_global_email_settings()
            test_to_emails = ["test_recipient@example.com"]
            test_cc_emails = ["test_cc@example.com"]
            test_subject_global = "Test LG Notification (Global)"
            test_body_global = """
            <html><body><p>Global email test.</p></body></html>
            """
            print("\n--- Testing Global Email Settings ---")
            success_global = await send_email(mock_db, test_to_emails, test_subject_global, test_body_global, {}, global_settings_for_test, test_cc_emails)
            print(f"Test email sent with global settings: {success_global}")

            # Test customer-specific settings (customer_id = 1)
            print("\n--- Testing Customer-Specific Email Settings (Customer 1) ---")
            customer_settings_for_test, method_for_log = get_customer_email_settings(mock_db, 1)
            test_subject_customer = "Test LG Notification (Customer 1)"
            test_body_customer = """
            <html><body><p>Customer 1 email test.</p></body></html>
            """
            success_customer = await send_email(mock_db, test_to_emails, test_subject_customer, test_body_customer, {}, customer_settings_for_test, test_cc_emails, sender_name="Customer 1 Dept")
            print(f"Test email sent with customer 1 settings: {success_customer}")
            print(f"Customer 1 settings used: Host={customer_settings_for_test.smtp_host}, Sender={customer_settings_for_test.sender_email}, Method={method_for_log}")

            # Test fallback to global settings (customer_id = 2, no custom settings)
            print("\n--- Testing Fallback to Global Settings (Customer 2) ---\n")
            fallback_settings_for_test, method_for_log = get_customer_email_settings(mock_db, 2)
            test_subject_fallback = "Test LG Notification (Fallback)"
            test_body_fallback = """
            <html><body><p>Fallback email test.</p></body></html>
            """
            success_fallback = await send_email(mock_db, test_to_emails, test_subject_fallback, test_body_fallback, {}, fallback_settings_for_test, test_cc_emails)
            print(f"Test email sent with fallback settings: {success_fallback}")
            print(f"Fallback settings used: Host={fallback_settings_for_test.smtp_host}, Sender={fallback_settings_for_test.sender_email}, Method={method_for_log}")


        except ValueError as e:
            print(f"Configuration error for test: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during test: {e}")
        finally:
            # Restore original decrypt_data
            encryption.decrypt_data = original_decrypt_data

    # Run the async test function
    asyncio.run(test_email_sending_main())