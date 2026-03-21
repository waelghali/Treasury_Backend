# app/core/ai_integration.py
import os
import io
from typing import Dict, Any, Optional, List, Tuple, TYPE_CHECKING
import json
import fitz # PyMuPDF
from dotenv import load_dotenv
import re
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import uuid
from functools import lru_cache
from datetime import datetime, timedelta # Added timedelta for signed URL expiry
import tempfile
from sqlalchemy.orm import Session
if TYPE_CHECKING:
    from app.models import User
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

async def _check_bucket_access(bucket_name: str) -> bool:
    """Verifies if the Service Account has access to the specified bucket."""
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return False
    try:
        def _check():
            client = _get_gcs_client()
            if client:
                client.bucket(bucket_name).reload()
                return True
            return False
        return await asyncio.to_thread(_check)
    except Exception as e:
        logger.warning(f"Access check failed for '{bucket_name}': {e}")
        return False

# --- Configuration for Google Cloud Storage ---
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
if not GCS_BUCKET_NAME:
    logger.warning("Warning: GCS_BUCKET_NAME environment variable not set. GCS integration will be limited.")

# Try to import Google Cloud Vision
try:
    from google.cloud import vision_v1p3beta1 as vision
    from google.cloud import storage
    from google.oauth2 import service_account
    from google.api_core.exceptions import GoogleAPIError # Import GoogleAPIError for specific exception handling
    GOOGLE_CLOUD_LIBRARIES_AVAILABLE = True
except ImportError:
    logger.warning("Warning: google-cloud-vision or google-cloud-storage library not found. AI OCR/GCS functionality will be limited.")
    GOOGLE_CLOUD_LIBRARIES_AVAILABLE = False
    vision = None
    storage = None
    service_account = None
    GoogleAPIError = Exception # Define dummy for type hinting if not available

# Try to import Google Generative AI
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    logger.warning("Warning: google-generativeai library not found. Gemini AI functionality will be limited.")
    GEMINI_AVAILABLE = False
    genai = None

# --- Global Client/Credentials Instantiation ---
_google_credentials = None
_gcs_client = None
_vision_client = None
_gemini_model_global = None

# --- Dynamically set GOOGLE_APPLICATION_CREDENTIALS if JSON is provided ---
# This block replaces the direct os.environ lookup with a temporary file approach.
creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
if creds_json:
    # Use tempfile to create a secure temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_creds_file:
        temp_creds_file.write(creds_json)
        tmp_path = temp_creds_file.name
    # Set the environment variable to point to the temporary file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp_path
    logger.info(f"Google credentials written to temporary file: {tmp_path}")
else:
    logger.warning("GOOGLE_CREDENTIALS_JSON environment variable not found. Attempting to use existing GOOGLE_APPLICATION_CREDENTIALS.")

@lru_cache(maxsize=1)
def _get_google_credentials():
    global _google_credentials
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return None
    if _google_credentials is None and "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        try:
            credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"].strip('\'"')
            if not os.path.exists(credentials_path):
                logger.error(f"Error: Google Cloud credentials file does NOT exist at: {credentials_path}")
                return None
            _google_credentials = service_account.Credentials.from_service_account_file(credentials_path)
            logger.info(f"Google Cloud credentials loaded from {credentials_path}")
        except Exception as e:
            logger.error(f"Error loading Google Cloud credentials: {e}")
            _google_credentials = None
    return _google_credentials

@lru_cache(maxsize=1)
def _get_gcs_client():
    global _gcs_client
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return None
    if _gcs_client is None:
        try:
            credentials = _get_google_credentials()
            if credentials:
                _gcs_client = storage.Client(credentials=credentials)
                logger.info("Google Cloud Storage client initialized.")
            else:
                logger.warning("Cannot initialize GCS client: Credentials not available.")
                _gcs_client = None
        except Exception as e:
            logger.error(f"Error initializing GCS client: {e}")
            _gcs_client = None
    return _gcs_client

@lru_cache(maxsize=1)
def _get_vision_client():
    global _vision_client
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return None
    if _vision_client is None:
        try:
            credentials = _get_google_credentials()
            if credentials:
                _vision_client = vision.ImageAnnotatorClient(credentials=credentials)
                logger.info("Google Vision ImageAnnotatorClient initialized.")
            else:
                logger.warning("Cannot initialize Vision client: Credentials not available.")
                _vision_client = None
        except Exception as e:
            logger.error(f"Error initializing Vision client: {e}")
            _vision_client = None
    return _vision_client

@lru_cache(maxsize=1)
def _get_gemini_model():
    global _gemini_model_global
    if not GEMINI_AVAILABLE:
        logger.warning("Gemini AI is not available.")
        return None
    if _gemini_model_global is None:
        gemini_api_key = os.environ.get('GEMINI_API_KEY')
        if not gemini_api_key:
            logger.error("GEMINI_API_KEY environment variable not set. Gemini features will be disabled.")
            return None
        try:
            genai.configure(api_key=gemini_api_key)
            _gemini_model_global = genai.GenerativeModel('gemini-2.5-flash')
            logger.info("Gemini API configured and model instantiated globally.")
        except Exception as e:
            logger.error(f"Error configuring Gemini API or instantiating model: {e}. Gemini features will be disabled.")
            _gemini_model_global = None
    return _gemini_model_global

# Initial client setup
_get_google_credentials()
_get_gcs_client()
_get_vision_client()
_get_gemini_model()

# --- Local Output Directory Setup ---
BASE_OUTPUT_DIR = os.path.join(tempfile.gettempdir(), "Output")
OCR_INPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "OCR_Input_to_Gemini")
GEMINI_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "Gemini")

# Ensure directories exist
os.makedirs(OCR_INPUT_DIR, exist_ok=True)
os.makedirs(GEMINI_OUTPUT_DIR, exist_ok=True)
logger.info(f"Local AI output directories ensured: {OCR_INPUT_DIR}, {GEMINI_OUTPUT_DIR}")

# --- GCS Operations ---
async def _upload_to_gcs(bucket_name: str, blob_name: str, data: bytes, content_type: str) -> Optional[str]:
    """
    Asynchronously uploads byte data to a specified GCS bucket.
    Returns the gs:// URI of the uploaded object on success.
    """
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE or not GCS_BUCKET_NAME:
        logger.error("GCS libraries not available or bucket name not configured. Cannot upload to GCS.")
        return None

    client = _get_gcs_client()
    if not client:
        logger.error("GCS client not initialized. Cannot upload to GCS.")
        return None

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        await asyncio.to_thread(blob.upload_from_string, data, content_type=content_type)
        logger.info(f"File {blob_name} uploaded to gs://{bucket_name}/{blob_name}.")
        return f"gs://{bucket_name}/{blob_name}"
    except GoogleAPIError as e:
        logger.error(f"Failed to upload {blob_name} to GCS: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during GCS upload for {blob_name}: {e}")
        raise


async def _cleanup_gcs_files(bucket_name: str, prefix: str):
    """
    Asynchronously deletes files from GCS that match a given prefix.
    """
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE or not GCS_BUCKET_NAME:
        logger.warning("GCS libraries not available or bucket name not configured. Skipping GCS cleanup.")
        return

    client = _get_gcs_client()
    if not client:
        logger.error("GCS client not initialized. Cannot perform cleanup.")
        return

    try:
        bucket = client.bucket(bucket_name)
        blobs = await asyncio.to_thread(bucket.list_blobs, prefix=prefix)
        for blob in blobs:
            await asyncio.to_thread(blob.delete)
            logger.info(f"Deleted GCS object: {blob.name}")
    except GoogleAPIError as e:
        logger.error(f"Failed to cleanup GCS files with prefix {prefix}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during GCS cleanup for prefix {prefix}: {e}")

def delete_file_from_gcs(gcs_uri: str) -> bool:
    """Deletes a file directly from GCS using its gs:// URI."""
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        return False
    
    try:
        # uri format: gs://bucket_name/path/to/blob
        parts = gcs_uri[len("gs://"):].split('/', 1)
        bucket_name = parts[0]
        blob_name = parts[1]

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.delete()
        return True
    except Exception as e:
        # Just log it, don't crash the app
        print(f"Error deleting file {gcs_uri}: {e}")
        return False

# --- UPDATED: Generate Signed URL ---
async def generate_signed_gcs_url(gcs_uri: str, expiration: int = 3600) -> Optional[str]:
    """
    Generates a temporary signed URL for viewing a file using the full GCS URI.
    Args:
        gcs_uri: The full GCS URI (e.g., gs://bucket_name/path/to/blob).
        expiration: Time in seconds until the link expires.
    """
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return None

    # --- CRITICAL FIX: Robust GCS URI Parsing ---
    # We must extract bucket_name and blob_name from the full URI
    if not gcs_uri.startswith("gs://"):
        logger.error(f"Invalid GCS URI format: {gcs_uri}. Must start with 'gs://'.")
        return None
        
    path_parts = gcs_uri[5:].split('/', 1)
    if len(path_parts) != 2:
        logger.error(f"Failed to parse bucket and blob name from URI: {gcs_uri}")
        return None
        
    bucket_name, blob_name = path_parts
    # ---------------------------------------------

    try:
        # We run this in a thread because GCS client operations are synchronous
        def _generate():
            client = _get_gcs_client()
            if not client:
                return None
            
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Generate the signed URL
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=expiration),
                method="GET"
            )
            return url

        return await asyncio.to_thread(_generate)
        
    except Exception as e:
        logger.error(f"Error generating signed URL for {gcs_uri}: {e}")
        return None

# --- Google Vision OCR Function ---
async def perform_ocr_with_google_vision(file_uri: str, unique_file_id: str) -> Optional[str]:
    vision_client = _get_vision_client()
    if not vision_client:
        logger.error("Google Vision ImageAnnotatorClient is not available.")
        return None

    text_content = ""
    try:
        image_source = vision.ImageSource(image_uri=file_uri)
        image = vision.Image(source=image_source)
        response = await asyncio.to_thread(vision_client.document_text_detection, image=image)
        full_text_annotation = response.full_text_annotation

        if full_text_annotation:
            text_content = full_text_annotation.text
            logger.info(f"OCR extracted {len(text_content)} characters from {file_uri}.")
            logger.debug(text_content[:500] + "..." if len(text_content) > 500 else text_content)
            
            sanitized_file_id = re.sub(r'[\\/:*?"<>|]', '_', unique_file_id)
            ocr_output_filename = os.path.join(OCR_INPUT_DIR, f"ocr_input_to_gemini_{sanitized_file_id}.txt")
            
            with open(ocr_output_filename, "w", encoding="utf-8") as f:
                f.write(text_content)
            logger.info(f"OCR input for Gemini saved to {ocr_output_filename}")
            logger.info(f"OCR Cost Metric: {len(text_content)} characters processed by Google Vision.")
            return text_content
        else:
            logger.warning(f"No text detected by OCR from {file_uri}.")
            return None
    except GoogleAPIError as e:
        logger.error(f"Google Vision API error during OCR for {file_uri}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during Google Vision OCR for {file_uri}: {e}", exc_info=True)
        return None

# --- PyMuPDF PDF to Image Conversion and GCS Upload ---
async def _convert_pdf_to_images_and_upload_to_gcs(pdf_bytes: bytes, bucket_name: str, lg_number: str = "unknown_lg") -> List[str]:
    gcs_client = _get_gcs_client()
    if not gcs_client or not bucket_name:
        logger.error("GCS client not initialized or bucket name missing. PDF conversion/upload skipped.")
        return []
    if not fitz:
        logger.error("PyMuPDF (fitz) not available. Cannot convert PDF.")
        return []

    image_uris = []
    pdf_document = None
    try:
        pdf_stream = io.BytesIO(pdf_bytes)
        pdf_document = fitz.open(stream=pdf_stream, filetype="pdf")
        
        max_pages_to_process = 5
        num_pages = min(len(pdf_document), max_pages_to_process)
        
        for page_num in range(num_pages):
            page = pdf_document.load_page(page_num)
            pix = page.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
            img_bytes = pix.pil_tobytes(format="PNG")
            
            blob_name = f"lg_scans_temp/{lg_number}/page_{page_num + 1}_{uuid.uuid4().hex}.png"
            gcs_uri = await _upload_to_gcs(bucket_name, blob_name, img_bytes, "image/png")
            if gcs_uri:
                image_uris.append(gcs_uri)
            else:
                logger.warning(f"Failed to upload image for page {page_num + 1} of PDF.")
        logger.info(f"Successfully converted PDF to {len(image_uris)} images and uploaded to GCS.")
    except Exception as e:
        logger.error(f"Error converting PDF to images or uploading to GCS: {e}", exc_info=True)
    finally:
        if pdf_document:
            pdf_document.close()
    return image_uris

# --- Text Sanitization Utility ---
def _sanitize_text_for_json(text: str) -> str:
    text = text.encode('unicode_escape').decode('utf-8')
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
    text = text.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\t', '\\t')
    return text

async def _log_ai_usage_to_db(db: Session, current_user: "User", file_name: str, metadata: dict):
    """Logs token and OCR usage to the database."""
    try:
        from app.models import AIUsageLog
        
        # Get model name dynamically from your existing global model object
        model_instance = _get_gemini_model()
        model_name = model_instance.model_name if model_instance else "gemini-unknown"

        usage_log = AIUsageLog(
            customer_id=current_user.customer_id, # Driven from user data
            user_id=getattr(current_user, 'user_id', None) or getattr(current_user, 'id', None),
            doc_name=file_name,
            model_name=model_name, # Not hardcoded
            prompt_tokens=metadata.get("gemini_prompt_tokens", 0),
            completion_tokens=metadata.get("gemini_completion_tokens", 0),
            total_tokens=metadata.get("gemini_prompt_tokens", 0) + metadata.get("gemini_completion_tokens", 0),
            ocr_characters=metadata.get("ocr_characters", 0),
            total_pages=metadata.get("total_pages_processed", 0)
        )
        db.add(usage_log)
        await db.commit()
        logger.info(f"AI Usage Log saved for customer {current_user.customer_id}")
    except Exception as log_err:
        logger.error(f"Failed to save AI usage log: {log_err}")


def log_ai_usage_sync(
    db: Session,
    customer_id: int,
    user_id: int,
    doc_name: str,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    total_pages: int = 0,
    call_type: str = "unknown",
):
    """Sync-compatible AI usage logger for non-async callers (reconciliation, etc)."""
    try:
        from app.models import AIUsageLog
        model_instance = _get_gemini_model()
        model_name = model_instance.model_name if model_instance else "gemini-unknown"

        usage_log = AIUsageLog(
            customer_id=customer_id,
            user_id=user_id,
            doc_name=f"[{call_type}] {doc_name}",
            model_name=model_name,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
            ocr_characters=0,
            total_pages=total_pages,
        )
        db.add(usage_log)
        db.commit()
        logger.info(f"AI Usage Log (sync) saved: {call_type} for customer {customer_id}")
    except Exception as e:
        logger.error(f"Failed to save sync AI usage log: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
async def extract_structured_data_with_gemini(text_content: str, unique_file_id: str, context: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, int]]]:
    model = _get_gemini_model()
    if not model:
        logger.error("Gemini AI model is not available or not configured.")
        return None, None

    if not text_content:
        logger.warning("No text content provided for Gemini extraction.")
        return None, None

    MAX_TEXT_LENGTH = 100000 
    if len(text_content) > MAX_TEXT_LENGTH:
        text_content = text_content[:MAX_TEXT_LENGTH]
        logger.warning(f"Truncated text to {MAX_TEXT_LENGTH} characters for Gemini.")

    sanitized_text_content = _sanitize_text_for_json(text_content)

    is_amendment = context and "lg_record_details" in context
    
    if is_amendment:
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "is_relevant_amendment": { "type": "BOOLEAN", "description": "Does this document refer to the provided LG number?"},
                "lgNumber": { "type": "STRING", "description": "The LG number as it appears in the amendment document."},
                "amendedFields": {
                    "type": "OBJECT",
                    "description": "A JSON object containing only the fields that are being amended, with their new values.",
                    "properties": {
                        "lgAmount": { "type": "NUMBER", "description": "New LG amount."},
                        "currency": { "type": "STRING", "description": "New currency."},
                        "expiryDate": { "type": "STRING", "format": "date-time", "description": "New expiry date in YYYY-MM-DD format."},
                        "otherConditions": { "type": "ARRAY", "items": { "type": "STRING" }, "description": "New or amended conditions."}
                    },
                }
            },
            "required": ["is_relevant_amendment", "amendedFields"]
        }
        
        lg_number = context["lg_record_details"].get("lgNumber", "")
        prompt = f"""
You are a financial document analyst. You have received a document which may be a bank amendment letter for an existing Letter of Guarantee (LG).

Your task is to:
1.  Verify if this document is a relevant amendment for LG number: "{lg_number}".
2.  If it is, extract *only* the fields that are being amended and their new values. Do not include any fields that are not mentioned as changed.

Return your output as a JSON object with the following fields:

1.  **is_relevant_amendment**: A boolean value (true/false). Set this to `true` if the document explicitly refers to the LG number: "{lg_number}". Otherwise, set to `false`.
2.  **lgNumber**: The LG number as it appears in the amendment document. This should match the provided number for a relevant amendment.
3.  **amendedFields**: A JSON object containing only the fields that have been changed. The keys should be the field names (e.g., `lgAmount`, `expiryDate`) and the values should be the new values from the document. The date format for `expiryDate` must be YYYY-MM-DD. Omit this object if no amendments are found or if the document is not relevant.

---
**Important Notes:**
-   Return **only** the final JSON object—no extra explanations.
-   If the document is not a relevant amendment, the `is_relevant_amendment` field should be `false`, and `amendedFields` should be an empty object or omitted.
-   Dates must be in format: `YYYY-MM-DD`
-   Numbers must be plain (no commas or symbols)

---
Document Text:
{sanitized_text_content}
"""

    else: # Original prompt for new LG documents
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "issuerName": { "type": "STRING" },
                "beneficiaryName": { "type": "STRING" },
                "issuingBankName": { "type": "STRING" },
                "lgNumber": { "type": "STRING" },
                "lgAmount": { "type": "NUMBER" },
                "currency": { "type": "STRING" },
                "lgType": { "type": "STRING" },
                "purpose": { "type": "STRING" },
                "issuanceDate": { "type": "STRING", "format": "date-time" }, 
                "expiryDate": { "type": "STRING", "format": "date-time" },   
                "otherConditions": { "type": "ARRAY", "items": { "type": "STRING" } },
                "issuingBankCountry": { "type": "STRING", "description": "The country of the issuing bank. Use 'Egypt' if from a local Egyptian bank."},
                "advisingBankName": { "type": "STRING", "description": "The name of the advising or confirming bank, if present."},
                "applicableRule": { "type": "STRING", "description": "The name of the applicable rule, e.g., 'URDG 758' or 'ISP98'."},
                "beneficiaryAddress": { "type": "STRING", "description": "The full address of the beneficiary as stated on the LG document."},
                "operationalStatus": { "type": "STRING", "description": "The operational status of the LG — 'Operative' or 'Non-Operative'. Primarily applicable to Advance Payment LGs. Look for terms like 'operative', 'non-operative', 'conditional', 'unconditional', 'مشروط', 'غير مشروط'."},
                # NEW FIELDS FOR FOREIGN BANKS
                "foreign_bank_name": {"type": "STRING", "description": "Manually entered bank name for foreign banks. Prioritize this if the issuing bank is explicitly a foreign bank and not found in the list."},
                "foreign_bank_country": {"type": "STRING", "description": "Manually entered country for foreign banks."},
                "foreign_bank_address": {"type": "STRING", "description": "Manually entered address for foreign banks."},
                "foreign_bank_swift_code": {"type": "STRING", "description": "Manually entered SWIFT code for foreign banks."},
            },
            "required": ["issuerName", "beneficiaryName", "issuingBankName", "lgNumber", "lgAmount", "currency", "lgType", "purpose", "issuanceDate", "expiryDate"]
        }
        prompt = f"""
You are a financial document analyst. Extract structured data from the following Letter of Guarantee (LG) document. The document may contain both English and Arabic (reply in the same language as you find in the document). Use only the provided text—do not rely on outside knowledge or assumptions.

Return your output as a JSON object with the following fields, based strictly on the content of the document.

---

**Fields to Extract:**

1.  **issuerName**:  
    The applicant—the entity on whose behalf the guarantee is issued.  
    Look for phrases like: "طالب الضمان", "on behalf of", "issued at the request of", or similar.

2.  **beneficiaryName**:  
    The entity receiving the guarantee—often mentioned after “To:”, or in the introduction. Extract the full name as it appears and in the same language as it appears.

3.  **issuingBankName**:  
    The bank issuing the LG—usually found in the letterhead or signature block. Extract the full name as it appears and return the bank formal name in English for this field specifically.

4.  **lgNumber**:  
    The reference number of the LG.  
    Look for phrases like "LG No.", "خطاب ضمان رقم", or similar.

5.  **lgAmount**:  
    The value of the guarantee. Extract the number as a float (e.g., 1500000.00), without commas or currency symbols.

6.  **currency**:  
    The currency of the LG. Use ISO codes like EGP, USD, EUR. If unclear, return `"Not Found"`.

7.  **lgType**:
    Identify the type of guarantee based on the provided text. Look for keywords such as "Performance", "Bid Bond", "Advance Payment", "Payment Guarantee", "Tender Bond", "Retention Money", or their Arabic equivalents like "ابتدائي", "نهائي", "دفعة مقدمة".
    If you find any of the following terms: Performance, Final, خطاب ضمان الأداء, ضمان الوفاء, نهائي then return "Performance Guarantee", If you find any of the following terms: "bid bond", "tender bond" or "ابتدائي" then return "Bid Bond LG". Prioritize guarantee titles or explicit phrases when available. If multiple types appear, choose the most specific or clearly stated one.

8.  **purpose**:  
    A short phrase or sentence describing the purpose of the LG.  
    Look near terms like "concerning", "in respect of", "regarding", "بخصوص", "بشأن", etc.

9.  **issuanceDate**:  
    The date the LG was issued. Extract in `YYYY-MM-DD` format.

10. **expiryDate**:  
    The date the LG expires. Extract in `YYYY-MM-DD` format.

11. **issuingBankCountry**:
    The country of the issuing bank. Use 'Egypt' if from a local Egyptian bank, otherwise, specify the country name.

12. **advisingBankName**:
    The name of the advising or confirming bank, if mentioned.

13. **applicableRule**:
    The name of the applicable rule, e.g., 'URDG 758' or 'ISP98'.

14. **otherConditions** (optional):  
    Any additional clauses—e.g., claim period, return conditions, governing law, amendment rules.  
    If present, return as a list of strings. Omit if not found.

15. **beneficiaryAddress**:
    The full address of the beneficiary as stated on the LG. Look near the beneficiary name section, under "Address" or "عنوان المستفيد". Extract as-is.

16. **operationalStatus**:
    Applicable primarily to Advance Payment LGs. Look for terms like "Operative", "Non-Operative", "Conditional", "Unconditional", "مشروط", "غير مشروط". Return either "Operative" or "Non-Operative". If not stated or not applicable, return empty.
    
17. **foreign_bank_name**:
    The name of the foreign bank if the issuing bank is a foreign bank.
    
18. **foreign_bank_country**:
    The country of the foreign bank.

19. **foreign_bank_address**:
    The address of the foreign bank.

20. **foreign_bank_swift_code**:
    The SWIFT code of the foreign bank.


---

**Important Notes:**

-   ⚠️ `issuerName` is the applicant—the party whose obligations are being guaranteed.  
-   ⚠️ `beneficiaryName` is the party receiving the guarantee.  
    Do **not** confuse them.

-   Dates must be in format: `YYYY-MM-DD`  
-   Numbers must be plain (no commas or symbols)  
-   Omit any field not found, unless otherwise specified  
-   Return blank reply for fields not found  
-   Return **only** the final JSON object—no extra explanations

---

Now analyze the following text and extract the required information:

Document Text:

---
{sanitized_text_content}
---
"""

    try:
        generation_config = genai.types.GenerationConfig(
            response_mime_type="application/json",
            response_schema=response_schema
        )

        response = await model.generate_content_async(prompt, generation_config=generation_config)
        extracted_data_str = response.text
        logger.info(f"--- FULL GEMINI JSON RESPONSE START ---")
        logger.info(extracted_data_str)
        logger.info(f"--- FULL GEMINI JSON RESPONSE END ---")
        
        usage_metadata = None
        if response.usage_metadata:
            usage_metadata = {
                "prompt_tokens": response.usage_metadata.prompt_token_count,
                "completion_tokens": response.usage_metadata.candidates_token_count
            }
            logger.info(f"Gemini Token Usage: Prompt Tokens = {usage_metadata['prompt_tokens']}, Completion Tokens = {usage_metadata['completion_tokens']}")
        else:
            logger.warning("Gemini usage_metadata not available in response.")

        sanitized_file_id = re.sub(r'[\\/:*?"<>|]', '_', unique_file_id)
        gemini_output_filename = os.path.join(GEMINI_OUTPUT_DIR, f"gemini_output_{sanitized_file_id}.json")
        
        with open(gemini_output_filename, "w", encoding="utf-8") as f:
            f.write(extracted_data_str)
        logger.info(f"Gemini output saved to {gemini_output_filename}")

        extracted_data = json.loads(extracted_data_str)
        logger.info("Gemini AI extraction successful.")
        return extracted_data, usage_metadata
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error in Gemini response: {e}. Raw response: {extracted_data_str}", exc_info=True)
        return None, None
    except Exception as e:
        logger.error(f"Error during Gemini AI extraction: {e}. Raw response: {extracted_data_str if 'extracted_data_str' in locals() else 'N/A'}", exc_info=True)
        raise

async def process_lg_document_with_ai(
    file_bytes: bytes, 
    mime_type: str, 
    lg_number_hint: str = "unknown_lg", 
    customer_bucket_name: Optional[str] = None,
    # --- ADDED AS OPTIONAL AT THE END ---
    db: Optional[Session] = None, 
    current_user: Optional["User"] = None,
    file_name: str = "unknown_file"
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, int]]]:

    
    logger.info(f"Starting AI processing for MIME type: {mime_type}, LG Hint: {lg_number_hint}")
    target_bucket_name = customer_bucket_name if customer_bucket_name else GCS_BUCKET_NAME
    
    if not target_bucket_name:
        logger.error("No target bucket configured.")
        return None, None
    
    structured_data = None
    total_usage_metadata = {
        "ocr_characters": 0,
        "gemini_prompt_tokens": 0,
        "gemini_completion_tokens": 0,
        "total_pages_processed": 0
    }
    raw_text = ""
    unique_file_id = ""
    temp_files = [] 

    try:
        if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE or not target_bucket_name:
            logger.error("GCS_BUCKET_NAME is not set or Google Cloud libraries not available.")
            return structured_data, total_usage_metadata

        session_id = uuid.uuid4().hex
        unique_file_id = f"{lg_number_hint}_{session_id}"

        # --- OCR Logic ---
        if mime_type.startswith("image/"):
            blob_name = f"lg_scans_temp/{unique_file_id}/image_{uuid.uuid4().hex}.{mime_type.split('/')[-1]}"
            gcs_uri_for_ocr = await _upload_to_gcs(target_bucket_name, blob_name, file_bytes, mime_type)
            if gcs_uri_for_ocr:
                temp_files.append(blob_name)
                raw_text = await perform_ocr_with_google_vision(gcs_uri_for_ocr, unique_file_id)
                total_usage_metadata["ocr_characters"] = len(raw_text) if raw_text else 0
                total_usage_metadata["total_pages_processed"] = 1
        
        elif mime_type == "application/pdf":
            image_uris = await _convert_pdf_to_images_and_upload_to_gcs(file_bytes, target_bucket_name, unique_file_id)
            if not image_uris:
                logger.error("Failed to convert PDF to images.")
                return structured_data, total_usage_metadata
            
            temp_files.extend([uri.replace(f"gs://{target_bucket_name}/", "") for uri in image_uris])
            all_page_texts = []
            for uri in image_uris:
                page_text = await perform_ocr_with_google_vision(uri, unique_file_id)
                if page_text:
                    all_page_texts.append(page_text)
            
            raw_text = "\n".join(all_page_texts)
            total_usage_metadata["ocr_characters"] = len(raw_text) if raw_text else 0
            total_usage_metadata["total_pages_processed"] = len(image_uris)
        else:
            logger.error(f"Unsupported MIME type: {mime_type}")
            return structured_data, total_usage_metadata

        if not raw_text:
            logger.error("OCR failed or no text extracted.")
            return structured_data, total_usage_metadata

        # --- Gemini Processing ---
        structured_data, gemini_usage_metadata = await extract_structured_data_with_gemini(raw_text, unique_file_id)
        
        if gemini_usage_metadata:
            total_usage_metadata["gemini_prompt_tokens"] = gemini_usage_metadata.get("prompt_tokens", 0)
            total_usage_metadata["gemini_completion_tokens"] = gemini_usage_metadata.get("completion_tokens", 0)

        if db and current_user:
            await _log_ai_usage_to_db(db, current_user, file_name, total_usage_metadata)
        else:
            # Use info or debug instead of warning if this is a common/expected state
            logger.info("Usage not logged: DB session or user missing.")

        # 2. Validate Results (Core logic)
        if not structured_data:
            logger.error("Gemini AI structured data extraction failed.")
            # Return early or raise an exception here if the rest of the app expects data
            return None, total_usage_metadata 

        logger.info("AI processing completed successfully.")
        return structured_data, total_usage_metadata

    except Exception as e:
        logger.critical(f"Critical error during AI processing: {e}", exc_info=True)
        return None, total_usage_metadata
    finally:
        if temp_files and unique_file_id:
            await _cleanup_gcs_files(target_bucket_name, f"lg_scans_temp/{unique_file_id}/")
            logger.info(f"Cleaned up temporary GCS files: {unique_file_id}")
# NEW FUNCTION: For amendment-specific AI processing

async def process_amendment_with_ai(
    file_bytes: bytes, 
    mime_type: str, 
    lg_record_details: Dict[str, Any], 
    file_name: str = "amendment_file", 
    db: Optional[Session] = None, 
    current_user: Optional["User"] = None,
    customer_bucket_name: Optional[str] = None
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, int]]]:
    
    lg_number_hint = lg_record_details.get("lgNumber", "unknown_amendment")
    logger.info(f"Starting AI processing for LG amendment. LG Hint: {lg_number_hint}")
    target_bucket_name = customer_bucket_name if customer_bucket_name else GCS_BUCKET_NAME
    if not target_bucket_name:
        logger.error("No target bucket configured.")
        return None, None
        
    structured_data = None
    total_usage_metadata = {
        "ocr_characters": 0,
        "gemini_prompt_tokens": 0,
        "gemini_completion_tokens": 0,
        "total_pages_processed": 0
    }
    raw_text = ""
    unique_file_id = f"{lg_number_hint}_{uuid.uuid4().hex}"
    temp_files = []
    
    try:
        if mime_type.startswith("image/"):
            blob_name = f"lg_amendment_scans_temp/{unique_file_id}/image_{uuid.uuid4().hex}.{mime_type.split('/')[-1]}"
            gcs_uri_for_ocr = await _upload_to_gcs(target_bucket_name, blob_name, file_bytes, mime_type)
            if gcs_uri_for_ocr:
                temp_files.append(blob_name)
                raw_text = await perform_ocr_with_google_vision(gcs_uri_for_ocr, unique_file_id)
                total_usage_metadata["total_pages_processed"] = 1
        elif mime_type == "application/pdf":
            image_uris = await _convert_pdf_to_images_and_upload_to_gcs(file_bytes, target_bucket_name, unique_file_id)
            if not image_uris:
                raise Exception("Failed to convert PDF to images for OCR.")
            temp_files.extend([uri.replace(f"gs://{target_bucket_name}/", "") for uri in image_uris])
            all_page_texts = [await perform_ocr_with_google_vision(uri, unique_file_id) for uri in image_uris]
            raw_text = "\\n".join(filter(None, all_page_texts))
            total_usage_metadata["total_pages_processed"] = len(image_uris)
        else:
            raise ValueError(f"Unsupported MIME type: {mime_type}")

        if not raw_text:
            raise Exception("OCR failed or no text extracted.")
            
        total_usage_metadata["ocr_characters"] = len(raw_text)

        context = {"lg_record_details": lg_record_details}
        structured_data, gemini_usage_metadata = await extract_structured_data_with_gemini(raw_text, unique_file_id, context=context)
        
        if gemini_usage_metadata:
            total_usage_metadata["gemini_prompt_tokens"] = gemini_usage_metadata.get("prompt_tokens", 0)
            total_usage_metadata["gemini_completion_tokens"] = gemini_usage_metadata.get("completion_tokens", 0)
        # 1. Safe DB Logging
        if db and current_user:
            await _log_ai_usage_to_db(db, current_user, file_name, total_usage_metadata)
        else:
            logger.warning("Skipping DB usage log: Missing database session or user.")

        # 2. Strict Validation
        if not structured_data:
            # Raising an exception stops execution; only do this if the caller
            # is prepared to catch it (e.g., in a try/except block or FastAPI handler)
            logger.error("Gemini AI structured data extraction failed.")
            raise Exception("Gemini AI structured data extraction failed.")

        # 3. Success Path
        logger.info("AI amendment processing completed successfully.")
        return structured_data, total_usage_metadata

    except Exception as e:
        logger.critical(f"Critical error during AI amendment processing: {e}", exc_info=True)
        return None, total_usage_metadata
    finally:
        if temp_files:
            await _cleanup_gcs_files(target_bucket_name, f"lg_amendment_scans_temp/{unique_file_id}/")
            logger.info(f"Cleaned up temporary GCS amendment files for session: {unique_file_id}")

# ==============================================================================
# Supporting Document AI Analysis (for Issuance Request Verification)
# ==============================================================================

AI_DOC_MAX_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB

async def analyze_supporting_document(
    pdf_bytes: bytes,
    doc_type: str,
    file_name: str,
    db: Optional[Session] = None,
    customer_id: Optional[int] = None,
    user_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Analyze a reference document (Contract, PO, etc.) by extracting structured fields
    that can be cross-referenced against an issuance request.
    
    Returns:
        {
            "status": "OK" | "ERROR" | "NO_TEXT",
            "message": str | None,
            "extracted_fields": { ... }  # Only present when status == "OK"
        }
    
    Fields extracted (only if found in the document — missing fields = auto-pass):
        - contract_value / po_value / requested_amount (depending on doc_type)
        - currency_code
        - beneficiary_name / parties_involved / vendor_name
        - beneficiary_address
        - lg_type_hint
        - maturity_date or duration
        - purpose
        - special_conditions
        - po_number / reference_number
        - payable_currency
        - summary
    """
    model = _get_gemini_model()
    if not model:
        return {"status": "ERROR", "message": "AI model not available. Submission will proceed without verification."}

    target_bucket_name = GCS_BUCKET_NAME
    if not target_bucket_name or not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return {"status": "ERROR", "message": "Cloud storage not configured. Submission will proceed without verification."}

    unique_file_id = f"doc_analysis_{uuid.uuid4().hex}"
    raw_text = ""
    temp_files = []

    try:
        # --- OCR: Extract text from PDF ---
        mime_type = "application/pdf" if file_name.lower().endswith(".pdf") else "image/png"
        
        if mime_type == "application/pdf":
            image_uris = await _convert_pdf_to_images_and_upload_to_gcs(pdf_bytes, target_bucket_name, unique_file_id)
            if not image_uris:
                return {"status": "ERROR", "message": "Failed to process document for AI analysis."}
            temp_files.extend([uri.replace(f"gs://{target_bucket_name}/", "") for uri in image_uris])
            all_page_texts = []
            for uri in image_uris:
                page_text = await perform_ocr_with_google_vision(uri, unique_file_id)
                if page_text:
                    all_page_texts.append(page_text)
            raw_text = "\n".join(all_page_texts)
        elif mime_type.startswith("image/"):
            blob_name = f"lg_scans_temp/{unique_file_id}/image_{uuid.uuid4().hex}.png"
            gcs_uri = await _upload_to_gcs(target_bucket_name, blob_name, pdf_bytes, mime_type)
            if gcs_uri:
                temp_files.append(blob_name)
                raw_text = await perform_ocr_with_google_vision(gcs_uri, unique_file_id)

        if not raw_text or len(raw_text.strip()) < 20:
            return {"status": "NO_TEXT", "message": "Could not extract text from document. Submission will proceed without verification."}

        # --- Gemini: Extract structured fields from reference document ---
        sanitized_text = _sanitize_text_for_json(raw_text[:80000])  # cap text length

        prompt = f"""You are a financial document analyst. Analyze this {doc_type.replace('_', ' ').title()} document and extract any fields relevant to a Letter of Guarantee (LG) issuance request.

**IMPORTANT:** Only extract fields that are EXPLICITLY mentioned in the document. If a field is not found, DO NOT include it in your response — omit it entirely. Do NOT guess or infer values.

**Fields to look for:**

1. **contract_value**: The monetary value/amount mentioned (as a number, no commas)
2. **currency_code**: Currency as ISO code (e.g., SAR, USD, EGP, EUR)
3. **payable_currency**: If a different payment currency is mentioned
4. **beneficiary_name**: The name of the project owner, employer, or entity requesting the guarantee
5. **beneficiary_address**: Address of the beneficiary if mentioned
6. **lg_type_hint**: Any mention of guarantee type (Performance, Advance Payment, Bid Bond, Financial, Retention)
7. **maturity_date**: LG expiry/maturity DATE in **YYYY-MM-DD format** (DATE ONLY — never include time/hours). If the date is written in words (e.g. "the tenth of September of the year two thousand and thirty"), convert it to YYYY-MM-DD. If the date is numeric, note that in Egypt the common format is DD/MM/YYYY (day first). If only a duration is mentioned (e.g. "valid for 6 months"), return the duration text instead.
8. **duration_text**: Duration mentioned (e.g., "12 months from contract date", "valid for 6 months")
9. **purpose**: The purpose or description of the required guarantee
10. **special_conditions**: Any special terms, conditions, or requirements for the LG (as a list of strings)
11. **reference_number**: Contract number, PO number, or tender reference
12. **lg_percentage**: If the LG is expressed as a percentage of contract value
13. **summary**: A brief 1-2 sentence summary of the document

Return ONLY a JSON object with the fields listed above that you found. Omit any field not found in the document.

---
Document Text:

{sanitized_text}
"""

        response_schema = {
            "type": "OBJECT",
            "properties": {
                "contract_value": {"type": "NUMBER"},
                "currency_code": {"type": "STRING"},
                "payable_currency": {"type": "STRING"},
                "beneficiary_name": {"type": "STRING"},
                "beneficiary_address": {"type": "STRING"},
                "lg_type_hint": {"type": "STRING"},
                "maturity_date": {"type": "STRING"},
                "duration_text": {"type": "STRING"},
                "purpose": {"type": "STRING"},
                "special_conditions": {"type": "ARRAY", "items": {"type": "STRING"}},
                "reference_number": {"type": "STRING"},
                "lg_percentage": {"type": "NUMBER"},
                "summary": {"type": "STRING"},
            }
        }

        generation_config = genai.types.GenerationConfig(
            response_mime_type="application/json",
            response_schema=response_schema
        )

        response = await model.generate_content_async(prompt, generation_config=generation_config)
        extracted_str = response.text
        logger.info(f"Supporting document AI analysis result: {extracted_str[:500]}")

        extracted = json.loads(extracted_str)

        # Map doc_type-specific field names for the comparison layer
        if doc_type == "PURCHASE_ORDER":
            if "contract_value" in extracted and "po_value" not in extracted:
                extracted["po_value"] = extracted["contract_value"]
            if "beneficiary_name" in extracted and "vendor_name" not in extracted:
                extracted["vendor_name"] = extracted["beneficiary_name"]
            if "reference_number" in extracted and "po_number" not in extracted:
                extracted["po_number"] = extracted["reference_number"]
        elif doc_type == "FORMAL_REQUEST":
            if "contract_value" in extracted and "requested_amount" not in extracted:
                extracted["requested_amount"] = extracted["contract_value"]
            if "beneficiary_name" in extracted and "requested_beneficiary" not in extracted:
                extracted["requested_beneficiary"] = extracted["beneficiary_name"]

        # Also keep generic aliases for the enhanced comparison
        if "beneficiary_name" not in extracted and "vendor_name" in extracted:
            extracted["beneficiary_name"] = extracted["vendor_name"]
        if "contract_value" not in extracted and "po_value" in extracted:
            extracted["contract_value"] = extracted["po_value"]
        
        # Keep parties_involved as alias for beneficiary_name (for backward compat)
        if "beneficiary_name" in extracted:
            extracted["parties_involved"] = extracted["beneficiary_name"]

        # Log usage
        if response.usage_metadata and db and customer_id:
            log_ai_usage_sync(
                db, customer_id, user_id or 0,
                f"[doc_verify] {file_name}",
                prompt_tokens=response.usage_metadata.prompt_token_count,
                completion_tokens=response.usage_metadata.candidates_token_count,
                call_type="document_verification"
            )

        return {
            "status": "OK",
            "message": None,
            "extracted_fields": extracted,
        }

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in supporting doc analysis: {e}")
        return {"status": "ERROR", "message": "AI returned invalid response. Submission will proceed."}
    except Exception as e:
        logger.error(f"Supporting document AI analysis failed: {e}", exc_info=True)
        return {"status": "ERROR", "message": f"AI analysis failed: {str(e)[:100]}. Submission will proceed."}
    finally:
        if temp_files:
            await _cleanup_gcs_files(target_bucket_name, f"lg_scans_temp/{unique_file_id}/")

# ==============================================================================
# Bank Form PDF Analysis (for Issuance Module)
# ==============================================================================

async def analyze_bank_form_pdf(
    pdf_bytes: bytes,
    filename: str = "bank_form.pdf",
    detected_fields: list = None,
    form_type: str = "FILLABLE_PDF",
    db: Session = None,
    customer_id: int = None,
    user_id: int = None,
) -> Dict[str, Any]:
    """
    Analyzes a bank's PDF form using Gemini AI to auto-map form fields
    to IssuanceRequest data fields.
    
    Called ONCE when a form is uploaded. The result is cached in BankFormTemplate.field_mapping
    and reused for every subsequent fill operation (no AI calls on fill).
    
    Args:
        pdf_bytes: The bank PDF form as bytes
        filename: Original filename for logging
        detected_fields: Optional list of detected PDF form fields (from get_pdf_form_fields)
    
    Returns:
        {
            "field_mapping": [...],        # The mapping to cache
            "form_title": "...",           # Detected form title
            "unmapped_fields": [...],      # Fields AI couldn't map
            "total_fields": N,
            "mapped_fields": N,
        }
    """
    model = _get_gemini_model()
    if not model:
        raise Exception("Gemini AI model is not available. Cannot analyze bank form.")

    # Step 1: Build fields_info based on form type
    fields_info = ""
    is_overlay = form_type in ("PHYSICAL_OVERLAY", "SCANNED_FILL")
    
    if detected_fields and not is_overlay:
        fields_info = f"""
The PDF contains the following interactive form fields (extracted programmatically):
{json.dumps(detected_fields, indent=2)}
"""
    elif is_overlay:
        fields_info = """This is a PHYSICAL OVERLAY form (non-fillable PDF). There are NO interactive form fields.
You must analyze the visual layout to identify every fillable area."""
    else:
        fields_info = "Note: No interactive form fields were detected programmatically. Analyze the visual layout of the form to identify input fields."

    # Step 2: Build the prompt with comprehensive banking context
    # List ALL issuance request fields that the AI can map to
    available_request_fields = {
        # Beneficiary info
        "beneficiary_name": "Name of the LG beneficiary (the party the guarantee is 'In Favor Of')",
        "beneficiary_address": "Address of the LG beneficiary",
        "beneficiary_contact_person": "Beneficiary contact person",
        "beneficiary_phone": "Beneficiary phone number",
        "beneficiary_email": "Beneficiary email address",
        "beneficiary_country": "Beneficiary country",
        "beneficiary_id_number": "Beneficiary ID/registration number",
        # Amount & Currency
        "amount": "LG amount in numbers/figures",
        "amount_in_words": "LG amount written in words (e.g., 'One Hundred Ten Egyptian Pounds Only')",
        "currency_code": "Currency ISO code (EGP, USD, EUR, etc.)",
        "currency_name": "Full currency name (e.g., 'Egyptian Pounds')",
        "amount_with_currency": "Amount prefixed with currency code (e.g., 'EGP 110.00')",
        # LG Type (for text fields) & checkbox matching
        "lg_type": "Type of guarantee as text (Performance Guarantee, Bid Bond, Advance Payment, Payment Guarantee, etc.)",
        "lg_type_is_bid_bond": "Boolean: true if LG type is Bid Bond / Tender Bond / \u0627\u0628\u062a\u062f\u0627\u0626\u064a",
        "lg_type_is_performance": "Boolean: true if LG type is Performance Guarantee / Final / \u0646\u0647\u0627\u0626\u064a",
        "lg_type_is_advance_payment": "Boolean: true if LG type is any Advance Payment variant",
        "lg_type_is_advance_conditioned": "Boolean: true if LG type is Conditioned Advance Payment (= advance payment + conditional/non-operative). On forms labeled '\u062f\u0641\u0639\u0629 \u0645\u0642\u062f\u0645\u0629 \u0645\u0634\u0631\u0648\u0637\u0629'",
        "lg_type_is_advance_unconditioned": "Boolean: true if LG type is Unconditioned Advance Payment (= advance payment + unconditional/operative). On forms labeled '\u062f\u0641\u0639\u0629 \u0645\u0642\u062f\u0645\u0629 \u063a\u064a\u0631 \u0645\u0634\u0631\u0648\u0637\u0629'",
        "lg_type_is_payment_guarantee": "Boolean: true if LG type is Payment Guarantee / \u0636\u0645\u0627\u0646 \u062f\u0641\u0639",
        # Purpose & Wording
        "lg_purpose": "Purpose/description/subject of the guarantee",
        "purpose": "Alias for lg_purpose",
        # Dates
        "current_date": "Today's date (form submission date)",
        "requested_issue_date": "Requested issue date of the LG",
        "requested_expiry_date": "Requested expiry/validity date of the LG",
        "expiry_date": "Alias for requested_expiry_date",
        # Reference
        "reference_type": "Reference type (Contract, PO, Tender)",
        "reference_number": "Reference/contract number",
        "reference_amount": "Reference/contract total amount",
        # Customer/Applicant (from the bank's perspective, this is 'Customer Name')
        "entity_name": "The company/entity name AS KNOWN TO THE BANK (this is the 'Customer Name' on bank forms)",
        "entity_address": "Entity/company registered address",
        "customer_name": "Company name (same as entity_name in most cases)",
        "company_name": "Company name alias",
        "customer_address": "Company registered address",
        "customer_phone": "Company phone number",
        "customer_email": "Company email",
        # Bank Account details
        "bank_branch": "Bank branch name where the customer account is held",
        "bank_account_number": "Customer's bank account number at this bank",
        "customer_cif_number": "Customer CIF / customer identification number at the bank",
        "iban": "Customer's IBAN at this bank",
        "account_name": "Name on the customer's bank account",
        # Cross-border indicators
        "is_local_lg": "Boolean: true if this is a local/domestic LG",
        "is_cross_border": "Boolean: true if this is a cross-border/foreign LG",
        # LG Format
        "lg_format_is_bank_standard": "Boolean: true if using bank's standard format",
        "lg_format_is_special": "Boolean: true if using special/attached format",
        # Third-party issuance
        "is_third_party": "Boolean: true if LG is issued on behalf of a third party",
        "is_in_own_name": "Boolean: true if LG is issued in the customer's own name (NOT third party)",
        "third_party_name": "Name of the third party (if is_third_party is true)",
        "third_party_address": "Address of the third party",
        # Additional conditions (auto-composed)
        "additional_conditions": "Auto-composed: combines special wording note + cross-border note if applicable",
        # Facility at bank
        "has_facility_at_bank": "Boolean: true if customer has an active credit facility at this bank. Use for facility/credit line checkboxes.",
        "facility_reference": "The bank's facility/credit line reference number (if facility exists)",
        # Requestor
        "requestor_name": "Name of person making the request",
        "requestor_email": "Email of requester",
        "department": "Department making the request",
        "project_name": "Project name",
        "operational_status": "Operative/Non-operative status",
        "serial_number": "Internal request serial number",
        # Custom
        "custom_field_1_value": "Custom field 1 value",
        "custom_field_2_value": "Custom field 2 value",
        # Language selection (for forms with language choice checkboxes)
        "lg_language_is_arabic": "Boolean: true if the LG should be issued in Arabic",
        "lg_language_is_english": "Boolean: true if the LG should be issued in English",
    }

    # --- SHARED CONTEXT (both modes) ---
    shared_context = f"""You are a banking documents expert specializing in Letter of Guarantee (LG) request forms.

## CRITICAL CONTEXT \u2014 Information Direction
This is a **BANK FORM** that a **CUSTOMER (applicant company)** fills out and **SUBMITS TO THE BANK** to request a new Letter of Guarantee. The information flows FROM the customer TO the bank.

This means:
- **"Customer Name"** on the form = the applicant company's name (our field: `entity_name`)
- **"Branch"** = the bank branch where the customer holds their account (our field: `bank_branch`)  
- **"Account No."** = the customer's account number at this bank (our field: `bank_account_number`)
- **"In Favor Of"** or **"\u0644\u0635\u0627\u0644\u062d"** = the beneficiary (our field: `beneficiary_name`)
- **"Date"** at the top = today's date / submission date (our field: `current_date`)
- **"Address"** near "Customer Name" = the customer entity's address (our field: `entity_address`)
- **"Address"** near "In Favor Of" or "Beneficiary" = the beneficiary's address (our field: `beneficiary_address`)

## Form Fields Detected
{fields_info}

## Available Data Fields
Map form fields to these system data fields:
{json.dumps(available_request_fields, indent=2)}
"""

    # --- MODE-SPECIFIC TASK + RESPONSE ---
    if is_overlay:
        task_and_response = """
## Your Task \u2014 PHYSICAL OVERLAY MODE

This is a non-fillable PDF (scanned or flat). You must analyze the visual layout and provide POSITION COORDINATES for each field.

For EACH fillable area you identify on the form (dotted lines, boxes, underscored blanks, text areas):
1. Identify what the field expects based on its visual label, position, and banking context
2. Map it to the most appropriate system field from the available data fields
3. Determine the **field_type**: "text", "checkbox", or "date"
4. **Estimate the x,y position** in PDF points (origin = bottom-left of page):
   - `x`: horizontal distance in points from the LEFT edge
   - `y`: vertical distance in points from the BOTTOM edge  
   - Standard A4 is 595 x 842 points. Letter is 612 x 792 points.
   - Position the text where it would naturally be written/typed on the form
5. Set `page` (0-indexed page number) and `font_size` (usually 8-12 for bank forms)
6. For **date fields**: include `date_format` (e.g., `DD/MM/YYYY`)
7. For **amount in words** fields: map to `amount_in_words`
8. Give each field a descriptive `pdf_field_name` (since there are no real PDF field names)
9. For **strikethrough patterns** (e.g., "IN OUR NAME / IN THE NAME OF ____" where one option must be crossed out):
   - Set `fill_strategy` to `"strikethrough"`
   - Map to the boolean that triggers the strikethrough (e.g., `is_third_party` to strike "IN OUR NAME")
   - Set `x`, `y` to the START of the text to be struck and `width` to the text length in points
   - The system will draw a line OVER the text when the boolean is true
10. For **additional conditions / other conditions** fields: map to `additional_conditions`

## Response Format
Return a JSON object:
```json
{{
  "form_title": "Application for Issuing Letter of Guarantee",
  "bank_name_detected": "Bank Name",
  "field_mapping": [
    {{
      "pdf_field_name": "branch_field",
      "label": "Branch",
      "mapped_to": "bank_branch",
      "field_type": "text",
      "source": "request_data",
      "confidence": 0.85,
      "x": 120,
      "y": 780,
      "page": 0,
      "font_size": 10,
      "notes": "Top-left area, after printed label 'Branch:'"
    }},
    {{
      "pdf_field_name": "date_field",
      "label": "Date",
      "mapped_to": "current_date",
      "field_type": "date",
      "date_format": "DD/MM/YYYY",
      "source": "request_data",
      "confidence": 0.9,
      "x": 120,
      "y": 750,
      "page": 0,
      "font_size": 10,
      "notes": "Below branch, date format area"
    }},
    {{
      "pdf_field_name": "strike_in_our_name",
      "label": "Strike: IN OUR NAME",
      "mapped_to": "is_third_party",
      "field_type": "text",
      "fill_strategy": "strikethrough",
      "source": "request_data",
      "confidence": 0.9,
      "x": 150,
      "y": 500,
      "width": 80,
      "page": 0,
      "font_size": 10,
      "notes": "Strike 'IN OUR NAME' when issuing for third party"
    }}
  ],
  "unmapped_fields": [],
  "form_notes": "Physical overlay form. Coordinates estimated from visual layout."
}}
```
"""
    else:
        task_and_response = """
## Your Task \u2014 FILLABLE PDF MODE

For EACH interactive form field in the PDF:
1. Identify what the field expects based on its label, position, context, and the banking domain
2. Map it to the most appropriate system field
3. Determine the **field_type**: "text", "checkbox", "radio", or "date"
4. For **date fields**: detect the expected date format by examining the field layout carefully:
   - **Count the character boxes/spaces** \u2014 this determines the total character length
   - 8 character boxes with slashes: `DD/MM/YY` (e.g., "08/03/26")
   - 8 character boxes without separators: `DDMMYYYY` (e.g., "08032026")
   - 10 character boxes with slashes: `DD/MM/YYYY` (e.g., "08/03/2026")
   - 6 character boxes without separators: `DDMMYY` (e.g., "080326")
   - Look at pre-printed separators (/, -, .) BETWEEN boxes \u2014 these are NOT counted as fill chars
   - If separators are PRE-PRINTED on the form (visible between boxes), use format WITHOUT separators for the fill value (e.g., `DDMMYYYY`)
   - If the field is ONE continuous text box, use format WITH separators (e.g., `DD/MM/YYYY`)
   - Common formats: `DD/MM/YY`, `DD/MM/YYYY`, `DDMMYYYY`, `DDMMYY`, `YYYYMMDD`, `YYYY-MM-DD`
5. For **checkbox/radio fields** (like LG type selection with Bid Bond \u2610, Performance \u2610):
   - Set `field_type` to "checkbox"
   - Set `fill_strategy` to "boolean_match" \u2014 the system will check this box only if the mapped boolean field is true
   - Map each individual checkbox to the corresponding boolean field (e.g., Bid Bond checkbox \u2192 `lg_type_is_bid_bond`)
6. For **amount in words/letters** fields: map to `amount_in_words`

## Response Format
Return a JSON object:
```json
{{
  "form_title": "Application for Issuing Letter of Guarantee",
  "bank_name_detected": "Emirates NBD",
  "field_mapping": [
    {{
      "pdf_field_name": "exact_pdf_field_name",
      "label": "Customer Name",
      "mapped_to": "entity_name",
      "field_type": "text",
      "language": "en",
      "source": "request_data",
      "confidence": 0.95,
      "notes": "Customer on the form = our entity/company name"
    }},
    {{
      "pdf_field_name": "date_field",
      "label": "Date",
      "mapped_to": "current_date",
      "field_type": "date",
      "date_format": "DD/MM/YYYY",
      "language": "shared",
      "source": "request_data",
      "confidence": 0.9,
      "notes": "Date boxes show DD/MM/YYYY format"
    }},
    {{
      "pdf_field_name": "bid_bond_checkbox",
      "label": "Bid Bond",
      "mapped_to": "lg_type_is_bid_bond",
      "field_type": "checkbox",
      "fill_strategy": "boolean_match",
      "language": "shared",
      "source": "request_data",
      "confidence": 0.95,
      "notes": "Check this box if LG type matches Bid Bond"
    }}
  ],
  "unmapped_fields": ["field_name_1"],
  "form_notes": "Bilingual form (English/Arabic). LG type selection via checkboxes."
}}
```
"""

    # --- SHARED RULES (both modes) ---
    shared_rules = """
## BILINGUAL FORMS (Arabic / English)

Many bank forms in this region are **bilingual** — English on one side and Arabic on the other side. Both sides contain the **SAME** information fields, just in different languages.

**CRITICAL: You MUST tag every field with a `"language"` property:**

- `"en"`: This field is on the **English side** (label in English, positioned on the English column/section)
- `"ar"`: This field is on the **Arabic side** (label in Arabic, positioned on the Arabic column/section)
- `"shared"`: This field is **language-neutral** — it applies regardless of the LG language

**Rules for tagging:**

1. **Two-sided layout** (English left / Arabic right, or split top/bottom): the English-side field gets `"en"`, the Arabic-side mirror gets `"ar"`. They map to the SAME data key.
2. **Middle-field layout** (single input box in the center, with labels on both sides): tag as `"shared"` — the same input box serves both languages.
3. **Checkboxes** (LG type, cross-border, local/foreign, etc.): **always `"shared"`** — a tick is a tick regardless of language.
4. **Date fields**: **always `"shared"`** — dates are numeric and language-neutral.
5. **Account numbers, CIF, IBAN**: **always `"shared"`** — these are numeric identifiers.
6. **Language selection fields** (e.g., "Arabic ☐  English ☐" on the form): map the Arabic checkbox to `lg_language_is_arabic` and the English checkbox to `lg_language_is_english`, tag both as `"shared"`.
7. **Single-language forms**: If the entire form is in just one language, tag ALL fields as `"shared"`.

**Common Arabic-English field pairs** (map BOTH to the same key, but tag differently):
   - "Branch" → `bank_branch` (tag `"en"`) / "فرع" → `bank_branch` (tag `"ar"`)
   - "Customer Name" → `entity_name` (tag `"en"`) / "اسم العميل" → `entity_name` (tag `"ar"`)
   - "Amount" → `amount` (tag `"en"`) / "بمبلغ" → `amount` (tag `"ar"`)
   - "Amount in Words" → `amount_in_words` (tag `"en"`) / "المبلغ بالحروف" → `amount_in_words` (tag `"ar"`)
   - "In Favor Of" → `beneficiary_name` (tag `"en"`) / "لصالح" → `beneficiary_name` (tag `"ar"`)
   - "Account No." → `bank_account_number` (tag `"shared"` — numeric)
   - "Date" / "التاريخ" → `current_date` (tag `"shared"` — date)
   - "Expiry Date" / "تاريخ نهاية الصلاحية" → `requested_expiry_date` (tag `"shared"` — date)

**PDF fields with generic names** like `fill_2_2`, `fill_4`, `Date_2`, `Address_3` are often the Arabic-side equivalents. Determine their language tag from their VISUAL POSITION on the form.

**Fields named "undefined"**: Place in `unmapped_fields` — do NOT map them.

**Do NOT map to `custom_field_1_value` or `custom_field_2_value`** unless the form genuinely shows a custom/free-text entry field.

**"مع" / "بعملية" / "with" / "in connection with"** — these form fields refer to the contract/project details. Map to `lg_purpose` or `reference_number` based on context.

## IMPORTANT RULES:
- **ALWAYS include `"language"` tag** on every field: `"en"`, `"ar"`, or `"shared"`
- Map EVERY field you can identify — text fields, checkboxes, radio buttons, date fields  
- For each **date** field, ALWAYS include `date_format` with the detected expected format
- For **"Amount in Letters"**, **"المبلغ بالحروف"**, or **"Amount & CCY (in letters)"** → map to `amount_in_words`
- For checkbox groups (LG type, Local/Cross Border, LG Format): map EACH checkbox individually  
- "Customer Name" / "اسم العميل" = `entity_name` (NOT `customer_name`)
- "Branch" / "فرع" = `bank_branch` (NOT `entity_name`)  
- "Account No." = `bank_account_number`
- Fields near "In Favor Of" or "لصالح" relate to the BENEFICIARY
- Return ONLY the JSON object, no extra text
"""

    prompt = shared_context + task_and_response + shared_rules

    try:
        # Upload PDF to Gemini for analysis (it can read PDFs directly)
        import google.generativeai as genai_module
        
        # Use Gemini's ability to analyze PDF content
        response = await model.generate_content_async(
            [
                prompt,
                {"mime_type": "application/pdf", "data": pdf_bytes}
            ],
            generation_config=genai_module.types.GenerationConfig(
                response_mime_type="application/json",
            )
        )
        
        result_str = response.text
        logger.info(f"AI Bank Form Analysis complete for '{filename}'")
        logger.debug(f"AI Response: {result_str[:500]}...")

        # H3: Log AI usage to ai_usage_logs
        try:
            usage_meta = getattr(response, 'usage_metadata', None)
            if usage_meta and db is not None and customer_id is not None and user_id is not None:
                log_ai_usage_sync(
                    db, customer_id, user_id, filename,
                    prompt_tokens=getattr(usage_meta, 'prompt_token_count', 0),
                    completion_tokens=getattr(usage_meta, 'candidates_token_count', 0),
                    call_type="bank_form_analysis",
                )
        except Exception as log_err:
            logger.warning(f"Failed to log bank form AI usage: {log_err}")

        result = json.loads(result_str)
        
        # Summarize
        field_mapping = result.get("field_mapping", [])
        unmapped = result.get("unmapped_fields", [])
        
        logger.info(
            f"Bank form analysis: {len(field_mapping)} fields mapped, "
            f"{len(unmapped)} unmapped, "
            f"form title: '{result.get('form_title', 'Unknown')}'"
        )
        
        return {
            "field_mapping": field_mapping,
            "form_title": result.get("form_title", ""),
            "bank_name_detected": result.get("bank_name_detected", ""),
            "unmapped_fields": unmapped,
            "form_notes": result.get("form_notes", ""),
            "total_fields": len(field_mapping) + len(unmapped),
            "mapped_fields": len(field_mapping),
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse AI response for bank form analysis: {e}")
        raise Exception(f"AI returned invalid JSON: {e}")
    except Exception as e:
        logger.error(f"Bank form AI analysis failed for '{filename}': {e}", exc_info=True)
        raise


# ==============================================================================
# H1: Facility Agreement AI Verification
# ==============================================================================

# Shared constant: Maximum file size for AI document processing (5 MB)
AI_DOC_MAX_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB


async def analyze_facility_agreement(
    pdf_bytes: bytes,
    filename: str,
    db: Session = None,
    customer_id: int = None,
    user_id: int = None,
) -> Dict[str, Any]:
    """
    H1: Analyzes a facility agreement PDF to extract key terms.
    Returns advisory comparison data — never blocks.
    """
    # File size guard
    if len(pdf_bytes) > AI_DOC_MAX_SIZE_BYTES:
        return {
            "status": "TOO_LARGE",
            "message": f"Document is too large for AI analysis ({len(pdf_bytes) / (1024*1024):.1f} MB). Maximum is 5 MB.",
            "extracted_terms": None,
        }

    model = _get_gemini_model()
    if not model:
        return {"status": "AI_UNAVAILABLE", "message": "AI model is not available.", "extracted_terms": None}

    import google.generativeai as genai_module

    prompt = """You are a banking document analyst. This PDF is a **bank facility agreement** 
for Letters of Guarantee (LG). Extract the following key terms as a JSON object:

{
  "total_limit": <number or null>,
  "currency_code": "<3-letter ISO code or null>",
  "commission_rate_pct": <number or null>,
  "margin_requirement_pct": <number or null>,
  "sla_days": <integer or null>,
  "start_date": "<YYYY-MM-DD or null>",
  "expiry_date": "<YYYY-MM-DD or null>",
  "review_date": "<YYYY-MM-DD or null>",
  "tenor_months": <integer or null>,
  "special_terms": ["<list of notable clauses or restrictions>"],
  "sub_limits": [{"lg_type": "<type>", "limit_amount": <number>}]
}

Rules:
- Return ONLY valid JSON. No explanations.
- If a field cannot be determined, use null.
- Amounts must be plain numbers (no commas, no currency symbols).
- Dates must be YYYY-MM-DD format.
- For special_terms, include any notable restrictions, covenants, or unusual clauses.
"""

    try:
        response = await model.generate_content_async(
            [prompt, {"mime_type": "application/pdf", "data": pdf_bytes}],
            generation_config=genai_module.types.GenerationConfig(
                response_mime_type="application/json",
            )
        )

        result_str = response.text
        logger.info(f"Facility agreement analysis complete for '{filename}'")

        # Log AI usage
        try:
            usage_meta = getattr(response, 'usage_metadata', None)
            if usage_meta and db is not None and customer_id is not None and user_id is not None:
                log_ai_usage_sync(
                    db, customer_id, user_id, filename,
                    prompt_tokens=getattr(usage_meta, 'prompt_token_count', 0),
                    completion_tokens=getattr(usage_meta, 'candidates_token_count', 0),
                    call_type="facility_agreement_analysis",
                )
        except Exception as log_err:
            logger.warning(f"Failed to log facility agreement AI usage: {log_err}")

        extracted = json.loads(result_str)
        return {
            "status": "OK",
            "message": None,
            "extracted_terms": extracted,
        }

    except json.JSONDecodeError as e:
        logger.error(f"AI returned invalid JSON for facility agreement: {e}")
        return {"status": "AI_ERROR", "message": f"AI returned invalid response: {e}", "extracted_terms": None}
    except Exception as e:
        logger.error(f"Facility agreement AI analysis failed: {e}", exc_info=True)
        return {"status": "AI_ERROR", "message": str(e), "extracted_terms": None}


# ==============================================================================
# H2: Supporting Document Analysis During Request
# ==============================================================================

SUPPORTED_DOC_TYPES = {
    "CONTRACT": {
        "description": "contract or agreement",
        "fields": "contract_value, parties_involved, contract_dates, project_description",
    },
    "PURCHASE_ORDER": {
        "description": "purchase order or procurement document",
        "fields": "po_number, po_value, vendor_name, delivery_date, items_description",
    },
    "FORMAL_REQUEST": {
        "description": "formal request or internal memo requesting an LG",
        "fields": "requested_amount, requested_beneficiary, requested_lg_type, requested_validity_period, purpose",
    },
}


async def analyze_supporting_document(
    pdf_bytes: bytes,
    doc_type: str,
    filename: str,
    db: Session = None,
    customer_id: int = None,
    user_id: int = None,
) -> Dict[str, Any]:
    """
    H2: Analyzes a supporting document and extracts key fields for cross-reference.
    Returns advisory data (highlights, not blocks).
    """
    # File size guard
    if len(pdf_bytes) > AI_DOC_MAX_SIZE_BYTES:
        return {
            "status": "TOO_LARGE",
            "message": f"Document is too large for AI analysis ({len(pdf_bytes) / (1024*1024):.1f} MB). Maximum is 5 MB.",
            "extracted_fields": None,
        }

    doc_config = SUPPORTED_DOC_TYPES.get(doc_type)
    if not doc_config:
        return {"status": "UNSUPPORTED_TYPE", "message": f"Document type '{doc_type}' is not supported.", "extracted_fields": None}

    model = _get_gemini_model()
    if not model:
        return {"status": "AI_UNAVAILABLE", "message": "AI model is not available.", "extracted_fields": None}

    import google.generativeai as genai_module

    prompt = f"""You are a banking document analyst. This PDF is a **{doc_config['description']}** 
related to a Letter of Guarantee (LG) issuance request.

Extract the following fields as a flat JSON object:
{doc_config['fields']}

Additional rules:
- Return ONLY valid JSON. No explanations.
- If a field cannot be determined, use null.
- Amounts must be plain numbers (no commas, no currency symbols).
- ALL dates must be DATE ONLY in YYYY-MM-DD format (never include time/hours). If dates are written in words (e.g. "the tenth of September two thousand thirty"), convert to YYYY-MM-DD. If dates are numeric, note the Egyptian convention is DD/MM/YYYY (day first).
- Include a "currency_code" field if any currency is mentioned (3-letter ISO code).
- Include a "summary" field with a 1-2 sentence summary of the document's purpose.
"""

    try:
        response = await model.generate_content_async(
            [prompt, {"mime_type": "application/pdf", "data": pdf_bytes}],
            generation_config=genai_module.types.GenerationConfig(
                response_mime_type="application/json",
            )
        )

        result_str = response.text
        logger.info(f"Supporting document analysis ({doc_type}) complete for '{filename}'")

        # Log AI usage
        try:
            usage_meta = getattr(response, 'usage_metadata', None)
            if usage_meta and db is not None and customer_id is not None and user_id is not None:
                log_ai_usage_sync(
                    db, customer_id, user_id, filename,
                    prompt_tokens=getattr(usage_meta, 'prompt_token_count', 0),
                    completion_tokens=getattr(usage_meta, 'candidates_token_count', 0),
                    call_type=f"supporting_doc_{doc_type.lower()}",
                )
        except Exception as log_err:
            logger.warning(f"Failed to log supporting doc AI usage: {log_err}")

        extracted = json.loads(result_str)
        return {
            "status": "OK",
            "message": None,
            "extracted_fields": extracted,
        }

    except json.JSONDecodeError as e:
        logger.error(f"AI returned invalid JSON for supporting doc ({doc_type}): {e}")
        return {"status": "AI_ERROR", "message": f"AI returned invalid response: {e}", "extracted_fields": None}
    except Exception as e:
        logger.error(f"Supporting doc AI analysis failed ({doc_type}): {e}", exc_info=True)
        return {"status": "AI_ERROR", "message": str(e), "extracted_fields": None}