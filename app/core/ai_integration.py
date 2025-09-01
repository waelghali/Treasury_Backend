# app/core/ai_integration.py
import os
import io
from typing import Dict, Any, Optional, List, Tuple
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

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
# These calls ensure clients are attempted to be set up on module import
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
        # Use asyncio.to_thread to run blocking I/O in a separate thread
        await asyncio.to_thread(blob.upload_from_string, data, content_type=content_type)
        logger.info(f"File {blob_name} uploaded to gs://{bucket_name}/{blob_name}.")
        return f"gs://{bucket_name}/{blob_name}"
    except GoogleAPIError as e:
        logger.error(f"Failed to upload {blob_name} to GCS: {e}")
        # Only re-raise if it's a critical, unrecoverable error for the caller.
        # For document upload within a larger process, consider handling more gracefully.
        raise # Re-raise if the caller expects to catch specific GCS errors
    except Exception as e:
        logger.error(f"An unexpected error occurred during GCS upload for {blob_name}: {e}")
        # Consider converting to HTTPException or a custom exception if appropriate for API context
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


async def generate_signed_gcs_url(gs_uri: str, valid_for_seconds: int = 900) -> Optional[str]:
    """
    Generates a time-limited signed URL for a private GCS object.
    
    Args:
        gs_uri: The Google Cloud Storage URI of the object (e.g., 'gs://your-bucket/path/to/object.pdf').
        valid_for_seconds: The duration (in seconds) for which the signed URL will be valid.
                           Defaults to 15 minutes (900 seconds).

    Returns:
        A string containing the signed URL, or None if an error occurs.
    """
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        logger.error("Google Cloud client libraries are not available. Cannot generate signed URL.")
        return None

    client = _get_gcs_client()
    if not client:
        logger.error("GCS client not initialized. Cannot generate signed URL.")
        return None

    try:
        # Parse the gs:// URI to get bucket name and blob name
        if not gs_uri.startswith("gs://"):
            logger.error(f"Invalid GCS URI format: {gs_uri}. Must start with 'gs://'.")
            return None
        
        path_parts = gs_uri[len("gs://"):].split('/', 1)
        if len(path_parts) < 2:
            logger.error(f"Invalid GCS URI format: {gs_uri}. Must contain bucket and blob path.")
            return None
            
        bucket_name = path_parts[0]
        blob_name = path_parts[1]

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Generate the signed URL
        signed_url = await asyncio.to_thread(
            blob.generate_signed_url,
            version="v4",
            expiration=timedelta(seconds=valid_for_seconds),
            method="GET"
        )
        logger.info(f"Generated signed URL for gs://{bucket_name}/{blob_name}, valid for {valid_for_seconds} seconds.")
        return signed_url

    except GoogleAPIError as e:
        logger.error(f"Failed to generate signed URL for {gs_uri} due to GCS API error: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while generating signed URL for {gs_uri}: {e}", exc_info=True)
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
        response = await asyncio.to_thread(vision_client.document_text_detection, image=image) # Make blocking call asynchronous
        full_text_annotation = response.full_text_annotation

        if full_text_annotation:
            text_content = full_text_annotation.text
            logger.info(f"OCR extracted {len(text_content)} characters from {file_uri}.")
            logger.info(f"--- FULL OCR TEXT START ({file_uri}) ---")
            # Log snippet, not full text, unless debugging is very specific
            logger.debug(text_content[:500] + "..." if len(text_content) > 500 else text_content)
            logger.info(f"--- FULL OCR TEXT END ({file_uri}) ---")
            
            # CRITICAL FIX: Sanitize the filename to remove invalid path characters
            sanitized_file_id = re.sub(r'[\\/:*?"<>|]', '_', unique_file_id)
            ocr_output_filename = os.path.join(OCR_INPUT_DIR, f"ocr_input_to_gemini_{sanitized_file_id}.txt")
            
            # Save OCR text to local file
            with open(ocr_output_filename, "w", encoding="utf-8") as f:
                f.write(text_content)
            logger.info(f"OCR input for Gemini saved to {ocr_output_filename}")
            # Log OCR cost metric (character count)
            logger.info(f"OCR Cost Metric: {len(text_content)} characters processed by Google Vision.")
            return text_content
        else:
            logger.warning(f"No text detected by OCR from {file_uri}.")
            return None # Ensure None is returned if no text is found.
    except GoogleAPIError as e:
        logger.error(f"Google Vision API error during OCR for {file_uri}: {e}", exc_info=True)
        # CRITICAL FIX: Reraise the exception or explicitly return None on failure
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during Google Vision OCR for {file_uri}: {e}", exc_info=True)
        # CRITICAL FIX: Reraise the exception or explicitly return None on failure
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
    # Remove control characters and ensure proper JSON escaping
    text = text.encode('unicode_escape').decode('utf-8')
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
    text = text.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\t', '\\t')
    return text



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
                "otherConditions": { "type": "ARRAY", "items": { "type": "STRING" } }
            },
            "required": ["issuerName", "beneficiaryName", "issuingBankName", "lgNumber", "lgAmount", "currency", "lgType", "purpose", "issuanceDate", "expiryDate"]
        }
        prompt = f"""
You are a financial document analyst. Extract structured data from the following Letter of Guarantee (LG) document. The document may contain both English and Arabic. Use only the provided text—do not rely on outside knowledge or assumptions.

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

11. **otherConditions** (optional):  
    Any additional clauses—e.g., claim period, return conditions, governing law, amendment rules.  
    If present, return as a list of strings. Omit if not found.

---

**Important Notes:**

-   ⚠️ `issuerName` is the applicant—the party whose obligations are being guaranteed.  
-   ⚠️ `beneficiaryName` is the party receiving the guarantee.  
    Do **not** confuse them.

-   Dates must be in format: `YYYY-MM-DD`  
-   Numbers must be plain (no commas or symbols)  
-   Omit any field not found, unless otherwise specified (e.g., `"Not Found"` for currency if unclear)  
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

        # CRITICAL FIX: Sanitize the filename to remove invalid path characters
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


# --- Main AI Processing Function ---
async def process_lg_document_with_ai(file_bytes: bytes, mime_type: str, lg_number_hint: str = "unknown_lg") -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, int]]]:
    logger.info(f"Starting AI processing for MIME type: {mime_type}, LG Hint: {lg_number_hint}")
    
    # Initialize all return values to None or default empty structures
    structured_data = None
    total_usage_metadata = {
        "ocr_characters": 0,
        "gemini_prompt_tokens": 0,
        "gemini_completion_tokens": 0,
        "total_pages_processed": 0
    }
    raw_text = ""
    gcs_uri_for_ocr = None
    temp_files = [] 
    ocr_cost_metric = 0 
    image_uris = [] # Initialize for PDF processing case

    try:
        if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE or not GCS_BUCKET_NAME:
            logger.error("GCS_BUCKET_NAME is not set or Google Cloud libraries not available. Cannot proceed with file uploads.")
            return structured_data, total_usage_metadata

        # Generate a unique session ID for this scan to avoid file conflicts
        session_id = uuid.uuid4().hex
        unique_file_id = f"{lg_number_hint}_{session_id}"

        if mime_type.startswith("image/"):
            blob_name = f"lg_scans_temp/{unique_file_id}/image_{uuid.uuid4().hex}.{mime_type.split('/')[-1]}"
            gcs_uri_for_ocr = await _upload_to_gcs(GCS_BUCKET_NAME, blob_name, file_bytes, mime_type)
            if gcs_uri_for_ocr:
                temp_files.append(blob_name)
                raw_text = await perform_ocr_with_google_vision(gcs_uri_for_ocr, unique_file_id)
                ocr_cost_metric = len(raw_text) if raw_text else 0
                total_usage_metadata["total_pages_processed"] = 1
        elif mime_type == "application/pdf":
            image_uris = await _convert_pdf_to_images_and_upload_to_gcs(file_bytes, GCS_BUCKET_NAME, unique_file_id)
            if not image_uris:
                logger.error("Failed to convert PDF to images or upload to GCS for OCR.")
                return structured_data, total_usage_metadata # Return early with default usage
            temp_files.extend([uri.replace(f"gs://{GCS_BUCKET_NAME}/", "") for uri in image_uris])
            all_page_texts = []
            for uri in image_uris:
                page_text = await perform_ocr_with_google_vision(uri, unique_file_id)
                if page_text:
                    all_page_texts.append(page_text)
            raw_text = "\\n".join(all_page_texts)
            ocr_cost_metric = len(raw_text) if raw_text else 0
            total_usage_metadata["total_pages_processed"] = len(image_uris)
        else:
            logger.error(f"Unsupported MIME type for OCR: {mime_type}. Expected image/* or application/pdf.")
            return structured_data, total_usage_metadata # Return early with default usage

        if not raw_text:
            logger.error("OCR failed or no text extracted.")
            return structured_data, total_usage_metadata # Return early with default usage

        structured_data, gemini_usage_metadata = await extract_structured_data_with_gemini(raw_text, unique_file_id)
        
        # Update gemini usage in total_usage_metadata
        if gemini_usage_metadata:
            total_usage_metadata["gemini_prompt_tokens"] = gemini_usage_metadata.get("prompt_tokens", 0)
            total_usage_metadata["gemini_completion_tokens"] = gemini_usage_metadata.get("completion_tokens", 0)

        if not structured_data:
            logger.error("Gemini AI structured data extraction failed.")
            return structured_data, total_usage_metadata # Return early with updated gemini usage

        logger.info("AI processing completed successfully.")
        return structured_data, total_usage_metadata

    except Exception as e:
        logger.critical(f"Critical error during AI processing: {e}", exc_info=True)
        # Ensure total_usage_metadata is returned even on critical failure
        return None, total_usage_metadata # Return None for data, but return available usage info
    finally:
        # Clean up temporary GCS files
        if temp_files:
            await _cleanup_gcs_files(GCS_BUCKET_NAME, f"lg_scans_temp/{unique_file_id}/")
            logger.info(f"Cleaned up temporary GCS files for session: {unique_file_id}")

# NEW FUNCTION: For amendment-specific AI processing
async def process_amendment_with_ai(file_bytes: bytes, mime_type: str, lg_record_details: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, int]]]:
    """
    Dedicated AI function to process a bank amendment letter, confirming its relevance
    and extracting only the amended fields.
    """
    lg_number_hint = lg_record_details.get("lgNumber", "unknown_amendment")
    logger.info(f"Starting AI processing for LG amendment. LG Hint: {lg_number_hint}")
    
    # Initialize return values
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
        # Step 1: Upload file and perform OCR (using existing helpers)
        if mime_type.startswith("image/"):
            blob_name = f"lg_amendment_scans_temp/{unique_file_id}/image_{uuid.uuid4().hex}.{mime_type.split('/')[-1]}"
            gcs_uri_for_ocr = await _upload_to_gcs(GCS_BUCKET_NAME, blob_name, file_bytes, mime_type)
            if gcs_uri_for_ocr:
                temp_files.append(blob_name)
                raw_text = await perform_ocr_with_google_vision(gcs_uri_for_ocr, unique_file_id)
                total_usage_metadata["total_pages_processed"] = 1
        elif mime_type == "application/pdf":
            image_uris = await _convert_pdf_to_images_and_upload_to_gcs(file_bytes, GCS_BUCKET_NAME, unique_file_id)
            if not image_uris:
                raise Exception("Failed to convert PDF to images for OCR.")
            temp_files.extend([uri.replace(f"gs://{GCS_BUCKET_NAME}/", "") for uri in image_uris])
            all_page_texts = [await perform_ocr_with_google_vision(uri, unique_file_id) for uri in image_uris]
            raw_text = "\\n".join(filter(None, all_page_texts))
            total_usage_metadata["total_pages_processed"] = len(image_uris)
        else:
            raise ValueError(f"Unsupported MIME type: {mime_type}")

        if not raw_text:
            raise Exception("OCR failed or no text extracted.")
            
        total_usage_metadata["ocr_characters"] = len(raw_text)

        # Step 2: Extract structured data using Gemini with a contextual prompt
        context = {"lg_record_details": lg_record_details}
        structured_data, gemini_usage_metadata = await extract_structured_data_with_gemini(raw_text, unique_file_id, context=context)
        
        if gemini_usage_metadata:
            total_usage_metadata["gemini_prompt_tokens"] = gemini_usage_metadata.get("prompt_tokens", 0)
            total_usage_metadata["gemini_completion_tokens"] = gemini_usage_metadata.get("completion_tokens", 0)

        if not structured_data:
            raise Exception("Gemini AI structured data extraction failed.")

        logger.info("AI amendment processing completed successfully.")
        return structured_data, total_usage_metadata

    except Exception as e:
        logger.critical(f"Critical error during AI amendment processing: {e}", exc_info=True)
        return None, total_usage_metadata
    finally:
        # Step 3: Clean up temporary GCS files
        if temp_files:
            await _cleanup_gcs_files(GCS_BUCKET_NAME, f"lg_amendment_scans_temp/{unique_file_id}/")
            logger.info(f"Cleaned up temporary GCS amendment files for session: {unique_file_id}")

# Example usage (for isolated testing)
if __name__ == "__main__":
    async def test_ai_integration_local():
        dummy_pdf_path = "135.PDF" # Ensure this PDF exists for testing

        print("\n--- Starting Isolated AI Integration Tests ---")

        if os.path.exists(dummy_pdf_path):
            print(f"\n--- Testing with PDF: {dummy_pdf_path} ---")
            try:
                with open(dummy_pdf_path, "rb") as f:
                    pdf_bytes = f.read()
                for i in range(2):
                    print(f"\n--- PDF Test Run {i+1} ---")
                    # Using a unique LG hint for each run to ensure distinct temporary files and logs
                    extracted, usage = await process_lg_document_with_ai(pdf_bytes, "application/pdf", f"pdf_run_{i+1}_{uuid.uuid4().hex}")
                    if extracted:
                        print(f"Extracted data from PDF (Run {i+1}): {json.dumps(extracted, indent=2)}")
                        print(f"Usage data (Run {i+1}): {json.dumps(usage, indent=2)}")
                    else:
                        print(f"Failed to extract data from PDF (Run {i+1}). Check logs above.")
            except Exception as e:
                print(f"Error during PDF test: {e}")
        else:
            print(f"Dummy PDF not found at {dummy_pdf_path}. Skipping PDF test.")

        print("\n--- AI Integration Tests Finished ---")

    asyncio.run(test_ai_integration_local())