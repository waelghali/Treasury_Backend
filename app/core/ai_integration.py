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

# Try to import Google GenAI SDK (unified Vertex AI client)
try:
    from google import genai
    from google.genai import types as genai_types
    GEMINI_AVAILABLE = True
except ImportError:
    logger.warning("google-genai library not found. Gemini AI functionality will be limited.")
    GEMINI_AVAILABLE = False
    genai = None
    genai_types = None

# Model name constant — single source of truth
GEMINI_MODEL_NAME = 'gemini-2.5-flash'

# Try to import Google Document AI
try:
    from google.cloud import documentai_v1 as documentai
    DOCUMENT_AI_AVAILABLE = True
except ImportError:
    logger.warning("Warning: google-cloud-documentai library not found. Document AI form parsing will be disabled.")
    DOCUMENT_AI_AVAILABLE = False
    documentai = None

# --- Global Client/Credentials Instantiation ---
_google_credentials = None
_gcs_client = None
_vision_client = None
_genai_client_global = None
_docai_client = None

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
    logger.info("Google credentials loaded from GOOGLE_CREDENTIALS_JSON env var")
else:
    logger.warning("GOOGLE_CREDENTIALS_JSON not found. Using GOOGLE_APPLICATION_CREDENTIALS.")

@lru_cache(maxsize=1)
def _get_google_credentials():
    global _google_credentials
    if not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return None
    if _google_credentials is None and "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        try:
            credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"].strip('\'"')
            if not os.path.exists(credentials_path):
                logger.error("Credentials file not found at GOOGLE_APPLICATION_CREDENTIALS path")
                return None
            _google_credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            logger.info("Google Cloud credentials loaded successfully")
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
                logger.info("Google Cloud Storage client initialized with explicit credentials.")
            else:
                # Fallback to Application Default Credentials (ADC)
                # This works with gcloud CLI auth, GOOGLE_APPLICATION_CREDENTIALS env var, etc.
                _gcs_client = storage.Client()
                logger.info("Google Cloud Storage client initialized with Application Default Credentials.")
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

# Vertex AI project & location (reuses DOCUMENT_AI_PROJECT_ID as fallback)
VERTEX_AI_PROJECT_ID = os.environ.get('GCP_PROJECT_ID') or os.environ.get('DOCUMENT_AI_PROJECT_ID', '')
VERTEX_AI_LOCATION = os.environ.get('VERTEX_AI_LOCATION', 'us-central1')

@lru_cache(maxsize=1)
def _get_genai_client():
    """Initialize the unified google-genai client for Vertex AI."""
    global _genai_client_global
    if not GEMINI_AVAILABLE:
        logger.warning("google-genai SDK not available. Gemini features disabled.")
        return None
    if _genai_client_global is None:
        if not VERTEX_AI_PROJECT_ID:
            logger.error("GCP_PROJECT_ID not set. Gemini features will be disabled.")
            return None
        try:
            credentials = _get_google_credentials()
            _genai_client_global = genai.Client(
                vertexai=True,
                project=VERTEX_AI_PROJECT_ID,
                location=VERTEX_AI_LOCATION,
                credentials=credentials,
            )
            logger.info(f"Google GenAI client initialized (model={GEMINI_MODEL_NAME})")
        except Exception as e:
            logger.error(f"Error initializing GenAI client: {e}. Gemini features will be disabled.")
            _genai_client_global = None
    return _genai_client_global

# Initial client setup
_get_google_credentials()
_get_gcs_client()
_get_vision_client()
_get_genai_client()

# --- Document AI Configuration ---
DOCUMENT_AI_PROJECT_ID = os.environ.get('DOCUMENT_AI_PROJECT_ID', '')
DOCUMENT_AI_PROCESSOR_ID = os.environ.get('DOCUMENT_AI_PROCESSOR_ID', '')
DOCUMENT_AI_LOCATION = os.environ.get('DOCUMENT_AI_LOCATION', 'us')  # 'us' or 'eu' or full region

logger.info(f"Document AI configured: project={'set' if DOCUMENT_AI_PROJECT_ID else 'not set'}, "
            f"processor={'set' if DOCUMENT_AI_PROCESSOR_ID else 'not set'}, "
            f"location={DOCUMENT_AI_LOCATION}")

@lru_cache(maxsize=1)
def _get_docai_client():
    """Initialize Document AI client using same credentials as other GCP services."""
    global _docai_client
    if not DOCUMENT_AI_AVAILABLE:
        logger.warning("Document AI library not installed.")
        return None
    if _docai_client is None:
        try:
            from google.api_core.client_options import ClientOptions
            credentials = _get_google_credentials()
            opts = ClientOptions(
                api_endpoint=f"{DOCUMENT_AI_LOCATION}-documentai.googleapis.com"
            ) if DOCUMENT_AI_LOCATION else None
            if credentials:
                _docai_client = documentai.DocumentProcessorServiceClient(
                    credentials=credentials, client_options=opts
                )
            else:
                _docai_client = documentai.DocumentProcessorServiceClient(
                    client_options=opts
                )
            logger.info(f"Document AI client initialized (location={DOCUMENT_AI_LOCATION}).")
        except Exception as e:
            logger.error(f"Error initializing Document AI client: {e}")
            _docai_client = None
    return _docai_client


async def _detect_fields_with_document_ai(pdf_bytes: bytes) -> list:
    """
    Use Google Document AI Form Parser to detect form fields with precise
    bounding box coordinates. Returns a list of detected field entries:
    [
        {
            "label": "Date",
            "value_text": "...",
            "x_pct": 45.2,       # horizontal position as % of page width
            "y_pct": 12.8,       # vertical position as % of page height (from top)
            "width_pct": 20.5,   # width as % of page width
            "height_pct": 3.1,   # height as % of page height
            "page": 0,
            "confidence": 0.95,
            "field_type": "text",
        },
        ...
    ]
    """
    client = _get_docai_client()
    if not client or not DOCUMENT_AI_PROJECT_ID or not DOCUMENT_AI_PROCESSOR_ID:
        logger.warning("Document AI not configured. Skipping precise field detection.")
        return []

    try:
        processor_name = client.processor_path(
            DOCUMENT_AI_PROJECT_ID, DOCUMENT_AI_LOCATION, DOCUMENT_AI_PROCESSOR_ID
        )

        raw_document = documentai.RawDocument(
            content=pdf_bytes,
            mime_type="application/pdf",
        )

        request = documentai.ProcessRequest(
            name=processor_name,
            raw_document=raw_document,
        )

        # Run synchronously in a thread to not block the event loop
        import asyncio
        result = await asyncio.to_thread(client.process_document, request=request)
        document = result.document

        detected_fields = []

        for page_idx, page in enumerate(document.pages):
            page_width = page.dimension.width
            page_height = page.dimension.height

            if page_width == 0 or page_height == 0:
                continue

            for form_field in page.form_fields:
                # Extract field label (the printed text)
                label_text = ""
                if form_field.field_name and form_field.field_name.text_anchor:
                    label_segments = form_field.field_name.text_anchor.text_segments
                    if label_segments:
                        for seg in label_segments:
                            start = int(seg.start_index) if seg.start_index else 0
                            end = int(seg.end_index) if seg.end_index else 0
                            label_text += document.text[start:end]
                label_text = label_text.strip()

                # Extract field value (what's currently written, if anything)
                value_text = ""
                if form_field.field_value and form_field.field_value.text_anchor:
                    val_segments = form_field.field_value.text_anchor.text_segments
                    if val_segments:
                        for seg in val_segments:
                            start = int(seg.start_index) if seg.start_index else 0
                            end = int(seg.end_index) if seg.end_index else 0
                            value_text += document.text[start:end]
                value_text = value_text.strip()

                # Get bounding box of the VALUE area (where we need to write)
                value_bbox = form_field.field_value.bounding_poly if form_field.field_value else None
                if not value_bbox or not value_bbox.normalized_vertices:
                    # Fallback: use the label's bounding box
                    value_bbox = form_field.field_name.bounding_poly if form_field.field_name else None

                if not value_bbox or not value_bbox.normalized_vertices:
                    continue

                verts = value_bbox.normalized_vertices
                if len(verts) < 4:
                    continue

                # normalized_vertices are 0-1 range, convert to 0-100 percentages
                x_min = min(v.x for v in verts) * 100
                x_max = max(v.x for v in verts) * 100
                y_min = min(v.y for v in verts) * 100
                y_max = max(v.y for v in verts) * 100

                box_width = x_max - x_min
                box_height = y_max - y_min

                # GENERIC POSITIONING: use the BOTTOM of the value bounding box.
                # On ANY form, the bottom edge of the value area IS the baseline/underline
                # where text naturally sits. This is box-height-independent.
                field_x_pct = round(x_min + 1.0, 1)  # 1% left padding to clear box edge
                field_y_pct = round(y_max - 0.3, 1)   # bottom of box, tiny offset above the line

                # Auto-compute font size from box height (in page points)
                # Typical page height ~842pt (A4), so box_height% * 842/100 ≈ pts
                estimated_font_pts = max(6, min(14, round(box_height * 8.42 * 0.65)))

                # Determine field type from value type
                field_type = "text"
                value_type = getattr(form_field, 'value_type', '')
                if value_type and 'checkbox' in value_type.lower():
                    field_type = "checkbox"

                confidence = form_field.field_name.confidence if form_field.field_name else 0.0

                detected_fields.append({
                    "label": label_text or f"field_{page_idx}_{len(detected_fields)}",
                    "value_text": value_text,
                    "x_pct": field_x_pct,
                    "y_pct": field_y_pct,
                    "width_pct": round(box_width, 1),
                    "height_pct": round(box_height, 1),
                    "page": page_idx,
                    "confidence": round(confidence, 3),
                    "field_type": field_type,
                    "font_size": estimated_font_pts,
                })

        logger.info(f"Document AI detected {len(detected_fields)} form fields.")
        return detected_fields

    except Exception as e:
        logger.error(f"Document AI form detection failed: {e}", exc_info=True)
        return []

# --- Local Output Directory Setup ---
BASE_OUTPUT_DIR = os.path.join(tempfile.gettempdir(), "Output")
OCR_INPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "OCR_Input_to_Gemini")
GEMINI_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "Gemini")

# Ensure directories exist
os.makedirs(OCR_INPUT_DIR, exist_ok=True)
os.makedirs(GEMINI_OUTPUT_DIR, exist_ok=True)
logger.info("Local AI output directories ensured (OCR_INPUT_DIR, GEMINI_OUTPUT_DIR)")

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
        logger.info(f"File uploaded to GCS: {blob_name}")
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

        client = _get_gcs_client()
        if not client:
            logger.error("GCS client not initialized. Cannot delete file.")
            return False
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.delete()
        return True
    except Exception as e:
        logger.error(f"Error deleting file {gcs_uri}: {e}")
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
            logger.info(f"OCR extracted {len(text_content)} characters from GCS source.")
            logger.debug(text_content[:500] + "..." if len(text_content) > 500 else text_content)
            
            sanitized_file_id = re.sub(r'[\\/:*?"<>|]', '_', unique_file_id)
            ocr_output_filename = os.path.join(OCR_INPUT_DIR, f"ocr_input_to_gemini_{sanitized_file_id}.txt")
            
            with open(ocr_output_filename, "w", encoding="utf-8") as f:
                f.write(text_content)
            logger.info(f"OCR input saved (file_id={sanitized_file_id})")
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
        
        # Model name from constant
        model_name = GEMINI_MODEL_NAME

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
        model_name = GEMINI_MODEL_NAME

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
    client = _get_genai_client()
    if not client:
        logger.error("GenAI client is not available or not configured.")
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
        alt_lg_number = context["lg_record_details"].get("alternativeLgNumber", "")
        lg_number_clause = f'LG number: "{lg_number}"'
        if alt_lg_number:
            lg_number_clause += f' (alternative reference: "{alt_lg_number}")'
        prompt = f"""
You are a financial document analyst. You have received a document which may be a bank amendment letter for an existing Letter of Guarantee (LG).

Your task is to:
1.  Verify if this document is a relevant amendment for {lg_number_clause}.
2.  If it is, extract *only* the fields that are being amended and their new values. Do not include any fields that are not mentioned as changed.

Return your output as a JSON object with the following fields:

1.  **is_relevant_amendment**: A boolean value (true/false). Set this to `true` if the document explicitly refers to {lg_number_clause}. If either number matches, set to `true`.
2.  **lgNumber**: The LG number as it appears in the amendment document.
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
        config = genai_types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=response_schema
        )

        response = await client.aio.models.generate_content(
            model=GEMINI_MODEL_NAME, contents=prompt, config=config
        )
        extracted_data_str = response.text
        logger.debug(f"--- FULL GEMINI JSON RESPONSE START ---")
        logger.debug(extracted_data_str)
        logger.debug(f"--- FULL GEMINI JSON RESPONSE END ---")
        
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
        logger.info(f"Gemini output saved (file_id={sanitized_file_id})")

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

AI_DOC_MAX_SIZE_BYTES = 2 * 1024 * 1024  # 2 MB — configurable here

async def analyze_supporting_document(
    pdf_bytes: bytes,
    doc_type: str,
    file_name: str,
    request_data: Optional[Dict[str, Any]] = None,
    db: Optional[Session] = None,
    customer_id: Optional[int] = None,
    user_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    AI-driven document verification: sends BOTH the document text AND the
    user-entered issuance request data to Gemini and asks it to compare them.

    Args:
        pdf_bytes: The document file bytes
        doc_type: One of CONTRACT, PURCHASE_ORDER, FORMAL_REQUEST
        file_name: Original filename for logging
        request_data: Dict with the 9 fields to verify against:
            - contract_value, currency, beneficiary_name, beneficiary_address,
              lg_type, lg_value, lg_currency, lg_expiry_date, lg_purpose
        db, customer_id, user_id: For usage logging

    Returns:
        {
            "status": "OK" | "ERROR" | "NO_TEXT",
            "comparison": [...],       # Per-field verdicts
            "mismatches": int,
            "total_fields_compared": int,
            "summary": str | None,
        }
    """
    client = _get_genai_client()
    if not client:
        return {"status": "ERROR", "message": "AI model not available. Submission will proceed without verification."}

    target_bucket_name = GCS_BUCKET_NAME
    if not target_bucket_name or not GOOGLE_CLOUD_LIBRARIES_AVAILABLE:
        return {"status": "ERROR", "message": "Cloud storage not configured. Submission will proceed without verification."}

    if not request_data:
        request_data = {}

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

        # --- Build the AI-driven comparison prompt ---
        sanitized_text = _sanitize_text_for_json(raw_text[:80000])

        # Format request data for the prompt
        request_fields_text = json.dumps({
            "contract_value": request_data.get("contract_value"),
            "currency": request_data.get("currency"),
            "beneficiary_name": request_data.get("beneficiary_name"),
            "beneficiary_address": request_data.get("beneficiary_address"),
            "lg_type": request_data.get("lg_type"),
            "lg_value": request_data.get("lg_value"),
            "lg_currency": request_data.get("lg_currency"),
            "lg_expiry_date": request_data.get("lg_expiry_date"),
            "lg_purpose": request_data.get("lg_purpose"),
        }, indent=2, ensure_ascii=False)

        prompt = f"""You are a financial document verification specialist. You are given:
1. A {doc_type.replace('_', ' ').title()} document (OCR-extracted text)
2. Data entered by a user for a Letter of Guarantee (LG) issuance request

Your task: Compare each user-entered field against the document. For each field, return one of three verdicts:
- **MATCH**: The document clearly confirms this value (exact or semantically equivalent)
- **MISMATCH**: The document contains a DIFFERENT value for this field
- **COULD_NOT_VALIDATE**: The field is not mentioned in the document, or the document is too ambiguous to verify

**CRITICAL RULES:**
- `contract_value` is the CONTRACT/PO total value. The `lg_value` is the LG amount, which is often a PERCENTAGE of the contract value (e.g., 5-20%). These are DIFFERENT fields. Do NOT flag a mismatch just because lg_value != contract_value.
- For `beneficiary_name`: Accept bilingual matches (Arabic ↔ English), abbreviations, and minor variations as MATCH.
- For `beneficiary_address`: A partial address match (e.g., city matches) counts as MATCH.
- For `currency` and `lg_currency`: Match ISO codes (EGP, USD, EUR, SAR, etc.)
- For `lg_expiry_date`: If the document mentions a duration (e.g., "12 months"), calculate approximate date from today and compare. Within 30 days = MATCH.
- For `lg_type`: Semantic match (e.g., "Performance Bond" = "Performance Guarantee" = "نهائي" = MATCH)
- For `lg_purpose`: Semantic match. If the document's project scope aligns with the stated purpose, it's a MATCH.
- If a user field is null/empty, skip it (do NOT include it in results).

**USER-ENTERED REQUEST DATA:**
{request_fields_text}

**DOCUMENT TEXT ({doc_type.replace('_', ' ').title()}):**
---
{sanitized_text}
---

Return a JSON object with exactly these fields:
- "comparison": array of objects, each with:
  - "field": the field name (e.g., "contract_value")
  - "label": human-readable label (e.g., "Contract Value")
  - "request_value": what the user entered (as string)
  - "document_value": what you found in the document (as string), or null if not found
  - "verdict": one of "MATCH", "MISMATCH", "COULD_NOT_VALIDATE"
  - "note": brief explanation (e.g., "Contract mentions EGP 3,000,000 which matches", or "Beneficiary not found in document")
- "summary": 1-2 sentence summary of the document
"""

        config = genai_types.GenerateContentConfig(
            response_mime_type="application/json",
        )

        response = await client.aio.models.generate_content(
            model=GEMINI_MODEL_NAME, contents=prompt, config=config
        )
        result_str = response.text
        logger.info(f"AI document verification complete ({len(result_str)} chars)")

        result = json.loads(result_str)

        comparison = result.get("comparison", [])

        # Map verdict to match boolean and severity for frontend compatibility
        for item in comparison:
            verdict = item.get("verdict", "COULD_NOT_VALIDATE")
            if verdict == "MATCH":
                item["match"] = True
                item["severity"] = "info"
            elif verdict == "MISMATCH":
                item["match"] = False
                item["severity"] = "warning"
            else:  # COULD_NOT_VALIDATE
                item["match"] = True  # Not a failure — just couldn't check
                item["severity"] = "suggestion"

        mismatches = len([c for c in comparison if c.get("verdict") == "MISMATCH"])

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
            "doc_type": doc_type.upper(),
            "summary": result.get("summary"),
            "comparison": comparison,
            "mismatches": mismatches,
            "total_fields_compared": len(comparison),
        }

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in AI document verification: {e}")
        return {"status": "ERROR", "message": "AI returned invalid response. Submission will proceed."}
    except Exception as e:
        logger.error(f"AI document verification failed: {e}", exc_info=True)
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
    client = _get_genai_client()
    if not client:
        raise Exception("GenAI client is not available. Cannot analyze bank form.")

    # Step 1: Build fields_info based on form type
    fields_info = ""
    is_overlay = form_type in ("PHYSICAL_OVERLAY", "SCANNED_FILL")
    
    # Extract page dimensions from PDF for better coordinate accuracy
    page_dimensions_info = ""
    try:
        from pypdf import PdfReader
        from io import BytesIO
        reader = PdfReader(BytesIO(pdf_bytes))
        if reader.pages:
            first_page = reader.pages[0]
            mb = first_page.mediabox
            pw, ph = float(mb.width), float(mb.height)
            page_dimensions_info = f"""\n## PDF Page Dimensions (IMPORTANT for positioning)
This PDF page is {pw:.0f} x {ph:.0f} points ({pw/72:.1f} x {ph/72:.1f} inches).
The page has {len(reader.pages)} page(s).
"""
    except Exception as e:
        logger.warning(f"Could not extract page dimensions: {e}")
    
    if detected_fields and not is_overlay:
        fields_info = f"""
The PDF contains the following interactive form fields (extracted programmatically):
{json.dumps(detected_fields, indent=2)}
"""
    elif is_overlay:
        # For overlay/scanned forms, try Document AI first for precise coordinates
        docai_fields = []
        try:
            docai_fields = await _detect_fields_with_document_ai(pdf_bytes)
        except Exception as e:
            logger.warning(f"Document AI detection failed, falling back to AI estimation: {e}")

        if docai_fields:
            # Build Document AI reference data for Gemini calibration
            docai_summary = json.dumps([
                {
                    "label": df.get("label", ""),
                    "x_pct": df["x_pct"],
                    "y_pct": df["y_pct"],
                    "width_pct": df["width_pct"],
                    "height_pct": df.get("height_pct", 2),
                    "page": df.get("page", 0),
                    "field_type": df.get("field_type", "text"),
                    "font_size": df.get("font_size", 10),
                }
                for df in docai_fields
            ], indent=2)
            
            # Also get PDFMiner structural blueprint for precise label positions
            pdfminer_data = _extract_pdf_structure(pdf_bytes)
            pdfminer_json = json.dumps(pdfminer_data, indent=2, ensure_ascii=False)
            
            fields_info = f"""This is a PHYSICAL OVERLAY form (non-fillable PDF). There are NO interactive form fields.
You must analyze the visual layout to identify every fillable area.

## REFERENCE DATA — Precise Field Positions (from Document AI)
The following {len(docai_fields)} fields were detected by Document AI with precise bounding box coordinates.
Use these as **calibration anchor points** when estimating positions for other fields.

**Instructions:**
1. For fields that MATCH Document AI entries below, USE their exact coordinates (x_pct, y_pct, width_pct, font_size)
2. For ADDITIONAL fields you find visually that are NOT in this list, ESTIMATE coordinates using these known positions as reference anchors
3. You MUST still visually scan the ENTIRE form — Document AI may have missed fields

{docai_summary}

## PDFMiner STRUCTURAL BLUEPRINT — Exact positions of EVERY printed label
This data shows every printed text element's position (100% accurate from the PDF's own data).
Use it to calculate fill positions: if a label ends at x_end_pct=28%, the fill area starts at ~29%.
{pdfminer_json}

{page_dimensions_info}"""
            logger.info(f"Document AI detected {len(docai_fields)} fields — included as calibration data for Gemini.")
        else:
            fields_info = f"""This is a PHYSICAL OVERLAY form (non-fillable PDF). There are NO interactive form fields.
You must analyze the visual layout to identify every fillable area.
{page_dimensions_info}"""
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
- **"In Favor Of"** or **"لصالح"** = the beneficiary (our field: `beneficiary_name`)
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
## Your Task — PHYSICAL OVERLAY MODE

This is a non-fillable PDF (scanned or flat). You must analyze the visual layout and provide POSITION COORDINATES for each field.

### STEP 1: Analyze the form's visual structure
Before placing any fields, mentally divide the page into 10 equal horizontal rows (each row = 10% of page height):
- Row 0-10%: Very top (logo area, bank name, form title)
- Row 10-20%: Header area (date, branch, ref number fields)
- Row 20-30%: Customer/applicant info section
- Row 30-50%: LG details (type checkboxes, amounts, in favor of)
- Row 50-70%: Guarantee details (purpose, conditions, reference)
- Row 70-85%: Additional info, terms, signatures
- Row 85-100%: Footer, final signatures

Identify which visual row each blank/fillable area falls in. This determines `y_pct`.

### STEP 2: For EACH fillable area you identify:
1. Identify what the field expects based on its visual label and banking context
2. Map it to the most appropriate system field from the available data fields
3. Determine the **field_type**: "text", "checkbox", or "date"
4. **Estimate the position as PERCENTAGES of page width and height:**
   - `x_pct`: horizontal position (0 = left edge, 100 = right edge)
   - `y_pct`: vertical position (0 = TOP edge, 100 = BOTTOM edge)
   - Also provide `width_pct` (width of the fillable area as % of page width)

### STEP 3: Positioning Rules (CRITICAL — read carefully)

**WHERE TO PLACE the value:**
- Find the BLANK AREA (dotted line, empty box, underlined space, or gap between labels)
- Place `x_pct` at the START of where text would be WRITTEN, NOT on the printed label
- For "Date: ________" → `x_pct` should be AFTER "Date:", on the blank line

**BILINGUAL FORM LAYOUT (Arabic + English on same page) — VERY IMPORTANT:**
Many bank forms are bilingual with a SPLIT LAYOUT on each row:
```
[English Label: ___EN fill area___|___AR fill area___  :Arabic Label]
LEFT side (0-50%)                                     RIGHT side (50-100%)
```

Rules for bilingual forms:
- The page is divided roughly in HALF: English content on the LEFT, Arabic on the RIGHT
- For each field row, there is an English label on the LEFT and an Arabic label on the RIGHT
- **CRITICAL**: When English and Arabic labels are on the SAME ROW (same y position), there is typically ONE shared fill area between them.
  - Create ONE field entry with `language: "shared"` tag
  - Position `x_pct` at the start of the fill area (right after the English label ends)
  - Set `width_pct` to cover the gap between labels (from after English label to before Arabic label)
  - Do NOT create two separate `_en` and `_ar` fields for the same row
- **EXCEPTION**: If the form has SEPARATE SECTIONS for each language (e.g., English block on top, Arabic block below, or different pages), then create separate `_en` and `_ar` fields with their own coordinates.
- For checkboxes: only one entry per checkbox, with `language: "shared"`

If the form is SINGLE LANGUAGE (English or Arabic only), ignore the split layout and use the full page width.

**VERTICAL positioning (y_pct):**
- Count the visual line number from the top of the page
- A form with ~30 visible lines: each line ≈ 3.3% height
- The FIRST fillable line (usually Date/Branch) is typically at y_pct 10-15
- Each subsequent line adds approximately 2.5-4% depending on spacing
- Checkbox lines: position at the CENTER of the checkbox square

**HORIZONTAL positioning (x_pct):**
- For bilingual forms: use the split layout rules above
- Left-aligned English fields: x_pct ≈ 15-25 (after the English label)
- Right-aligned Arabic fields: x_pct ≈ 55-75 (near the Arabic label)
- For single-language forms: x_pct wherever the fill gap starts
- For checkboxes: position at the CENTER of the checkbox square

5. Set `page` (0-indexed page number) and `font_size` (usually 8-12 for bank forms)
6. For **date fields**: include `date_format` (e.g., `DD/MM/YYYY`)
7. For **amount in words** fields: map to `amount_in_words`
8. Give each field a descriptive `pdf_field_name`
9. For **strikethrough patterns**: set `fill_strategy` to `"strikethrough"`, with `x_pct`, `y_pct`, `width_pct`
10. For **additional conditions**: map to `additional_conditions`

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
      "x_pct": 20,
      "y_pct": 8,
      "width_pct": 25,
      "page": 0,
      "font_size": 10,
      "notes": "Top-left area, after printed label 'Branch:' — blank line starts at ~20% from left"
    }},
    {{
      "pdf_field_name": "date_field",
      "label": "Date",
      "mapped_to": "current_date",
      "field_type": "date",
      "date_format": "DD/MM/YYYY",
      "source": "request_data",
      "confidence": 0.9,
      "x_pct": 20,
      "y_pct": 11,
      "width_pct": 15,
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
      "x_pct": 25,
      "y_pct": 40,
      "width_pct": 15,
      "page": 0,
      "font_size": 10,
      "notes": "Strike 'IN OUR NAME' when issuing for third party"
    }}
  ],
  "unmapped_fields": [],
  "form_notes": "Physical overlay form. Coordinates as percentages of page dimensions."
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

**Common Arabic-English field pairs** (for bilingual same-row forms, map to ONE entry with `"shared"` tag):
   - "Branch" / "فرع" → `bank_branch` (tag `"shared"`)
   - "Customer Name" / "اسم العميل" → `entity_name` (tag `"shared"`)
   - "Amount" / "بمبلغ" → `amount` (tag `"shared"`)
   - "Amount in Words" / "المبلغ بالحروف" → `amount_in_words` (tag `"shared"`)
   - "In Favor Of" / "لصالح" → `beneficiary_name` (tag `"shared"`)
   - "Account No." → `bank_account_number` (tag `"shared"`)
   - "Date" / "التاريخ" → `current_date` (tag `"shared"`)
   - "Expiry Date" / "تاريخ نهاية الصلاحية" → `requested_expiry_date` (tag `"shared"`)

**PDF fields with generic names** like `fill_2_2`, `fill_4`, `Date_2`, `Address_3` are often the Arabic-side equivalents. Determine their language tag from their VISUAL POSITION on the form.

**Fields named "undefined"**: Place in `unmapped_fields` — do NOT map them.

**Do NOT map to `custom_field_1_value` or `custom_field_2_value`** unless the form genuinely shows a custom/free-text entry field.

**"مع" / "بعملية" / "with" / "in connection with"** — these form fields refer to the contract/project details. Map to `lg_purpose` or `reference_number` based on context.

## IMPORTANT RULES:
- **ALWAYS include `"language"` tag** on every field: `"en"`, `"ar"`, or `"shared"`. Use `"shared"` for bilingual same-row fields.
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
        # Use Gemini's ability to analyze PDF content
        pdf_part = genai_types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")
        response = await client.aio.models.generate_content(
            model=GEMINI_MODEL_NAME,
            contents=[
                prompt,
                pdf_part,
            ],
            config=genai_types.GenerateContentConfig(
                temperature=0.0,
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
        
        # --- OPTIONAL: Refine Gemini coordinates with Document AI precision ---
        # Gemini does all detection/mapping. If Document AI found matching fields,
        # swap in their precise coordinates. Otherwise, keep Gemini's estimates.
        field_mapping = result.get("field_mapping", [])
        
        if is_overlay and docai_fields:
            import re
            
            def _normalize_label(text):
                """Normalize a label for matching: lowercase, strip punctuation/spaces."""
                text = (text or "").strip().lower()
                # Remove common suffixes/prefixes/punctuation
                text = re.sub(r'[:\-–—/\\.,;!?()（）\[\]{}\'\"]+', ' ', text)
                # Remove Arabic diacritics (tashkeel)
                text = re.sub(r'[\u0610-\u061A\u064B-\u065F\u0670]', '', text)
                # Collapse whitespace
                text = re.sub(r'\s+', ' ', text).strip()
                return text
            
            # Build lookup: normalized_label → Document AI field data
            docai_lookup = {}
            docai_entries = []  # Keep original + normalized for multi-strategy matching
            for df in docai_fields:
                raw_label = (df.get("label", "") or "").strip()
                norm = _normalize_label(raw_label)
                if norm:
                    docai_lookup[norm] = df
                    docai_entries.append((norm, raw_label, df))
            
            refined_count = 0
            unmatched_labels = []
            
            for field in field_mapping:
                raw_field_label = (field.get("label", "") or "").strip()
                field_label = _normalize_label(raw_field_label)
                if not field_label:
                    continue
                
                # Strategy 1: Exact normalized match
                matched = docai_lookup.get(field_label)
                
                # Strategy 2: Contains / substring match
                if not matched:
                    for dk, _, dv in docai_entries:
                        if dk and (dk in field_label or field_label in dk):
                            matched = dv
                            break
                
                # Strategy 3: Word-overlap scoring (pick best match)
                if not matched:
                    field_words = set(field_label.split())
                    if len(field_words) >= 1:
                        best_score = 0
                        best_match = None
                        for dk, _, dv in docai_entries:
                            dk_words = set(dk.split())
                            overlap = len(field_words & dk_words)
                            # Require at least 1 overlapping word and >40% overlap
                            min_len = min(len(field_words), len(dk_words))
                            if overlap > 0 and min_len > 0 and (overlap / min_len) > 0.4:
                                if overlap > best_score:
                                    best_score = overlap
                                    best_match = dv
                        matched = best_match
                
                if matched:
                    # Refine with Document AI's precise coordinates
                    field["x_pct"] = matched["x_pct"]
                    field["y_pct"] = matched["y_pct"]
                    field["width_pct"] = matched["width_pct"]
                    field["height_pct"] = matched.get("height_pct", 3)
                    field["font_size"] = matched.get("font_size", 10)
                    field["page"] = matched.get("page", field.get("page", 0))
                    refined_count += 1
                else:
                    unmatched_labels.append(raw_field_label)
            
            logger.info(f"Document AI coordinate refinement: {refined_count}/{len(field_mapping)} fields refined with precise coordinates.")
            if unmatched_labels:
                logger.info(f"Unmatched fields (kept Gemini estimates): {unmatched_labels[:10]}")
        
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
# PDFMiner Structural Blueprint Extraction
# ==============================================================================

def _extract_pdf_structure(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Uses PDFMiner to extract exact coordinates of every text element in the PDF.
    Returns a list of text elements per page with percentage-based coordinates.
    This provides ground-truth label positions for precise fill placement.
    """
    import io
    from pdfminer.high_level import extract_pages
    from pdfminer.layout import LTTextBox, LTTextLine, LTChar, LTAnno, LAParams
    
    pages_data = []
    
    try:
        laparams = LAParams(
            line_margin=0.3,
            word_margin=0.15,
            char_margin=2.0,
            boxes_flow=0.5,
            detect_vertical=True,
        )
        
        pdf_stream = io.BytesIO(pdf_bytes)
        
        for page_idx, page_layout in enumerate(extract_pages(pdf_stream, laparams=laparams)):
            if page_idx >= 2:  # Max 2 pages
                break
                
            page_width = page_layout.width
            page_height = page_layout.height
            
            elements = []
            
            for element in page_layout:
                if isinstance(element, (LTTextBox, LTTextLine)):
                    text = element.get_text().strip()
                    if not text or len(text) < 1:
                        continue
                    
                    # PDFMiner uses bottom-left origin, so y0 is bottom
                    x0, y0_bottom, x1, y1_top = element.bbox
                    
                    # Convert to top-left origin percentages (matching our coordinate system)
                    x_pct_start = round((x0 / page_width) * 100, 1)
                    x_pct_end = round((x1 / page_width) * 100, 1)
                    # Flip Y: PDFMiner y0=bottom, we want y_pct 0=top
                    y_pct_top = round(((page_height - y1_top) / page_height) * 100, 1)
                    y_pct_bottom = round(((page_height - y0_bottom) / page_height) * 100, 1)
                    
                    # Get dominant font size from characters
                    font_sizes = []
                    if isinstance(element, LTTextBox):
                        for line in element:
                            if isinstance(line, LTTextLine):
                                for char in line:
                                    if isinstance(char, LTChar):
                                        font_sizes.append(round(char.size, 1))
                    elif isinstance(element, LTTextLine):
                        for char in element:
                            if isinstance(char, LTChar):
                                font_sizes.append(round(char.size, 1))
                    
                    avg_font_size = round(sum(font_sizes) / len(font_sizes), 1) if font_sizes else 10.0
                    
                    elements.append({
                        "text": text[:100],  # Truncate very long text
                        "x_start_pct": x_pct_start,
                        "x_end_pct": x_pct_end,
                        "y_top_pct": y_pct_top,
                        "y_bottom_pct": y_pct_bottom,
                        "font_size": avg_font_size,
                    })
            
            # Sort by y position (top to bottom), then x (left to right)
            elements.sort(key=lambda e: (e["y_top_pct"], e["x_start_pct"]))
            
            pages_data.append({
                "page": page_idx,
                "width_pts": round(page_width, 1),
                "height_pts": round(page_height, 1),
                "elements_count": len(elements),
                "elements": elements,
            })
        
        total = sum(p["elements_count"] for p in pages_data)
        logger.info(f"PDFMiner: extracted {total} text elements from {len(pages_data)} pages")
        
    except Exception as e:
        logger.warning(f"PDFMiner extraction failed: {e}. Continuing without structural data.")
        pages_data = [{"page": 0, "elements": [], "note": f"Extraction failed: {str(e)}"}]
    
    return pages_data


# ==============================================================================
# ENHANCE: Visual Feedback Loop for Coordinate Correction
# ==============================================================================


async def enhance_bank_form_mapping(
    template_pdf_bytes: bytes,
    filled_pdf_bytes: bytes,
    current_mapping: List[Dict[str, Any]],
    form_type: str = "SCANNED_FILL",
    filename: str = "form.pdf",
) -> List[Dict[str, Any]]:
    """
    Visual feedback loop: sends the original form + filled preview to Gemini,
    along with the current mapping AND PDFMiner structural blueprint.
    Gemini uses exact label positions to calculate precise fill coordinates.
    
    Returns: corrected field_mapping list (only positions are updated, 
             semantic mappings are preserved).
    """
    client = _get_genai_client()
    if not client:
        raise Exception("GenAI client not available for enhancement.")
    
    import fitz  # PyMuPDF for rendering PDF to image
    
    # ── Step 1: Extract structural blueprint via PDFMiner ──
    structural_data = _extract_pdf_structure(template_pdf_bytes)
    
    # ── Step 2: Render original + filled as images ──
    original_doc = fitz.open(stream=template_pdf_bytes, filetype="pdf")
    filled_doc = fitz.open(stream=filled_pdf_bytes, filetype="pdf")
    
    image_parts = []
    page_dimensions = []
    num_pages = min(original_doc.page_count, 2)
    
    for pg_idx in range(num_pages):
        page = original_doc[pg_idx]
        page_dimensions.append({
            "page": pg_idx,
            "width_pts": round(page.rect.width, 1),
            "height_pts": round(page.rect.height, 1),
        })
        
        orig_pix = page.get_pixmap(dpi=150)
        orig_bytes = orig_pix.tobytes("png")
        image_parts.append(genai_types.Part.from_bytes(data=orig_bytes, mime_type="image/png"))
        
        if pg_idx < filled_doc.page_count:
            fill_pix = filled_doc[pg_idx].get_pixmap(dpi=150)
            fill_bytes = fill_pix.tobytes("png")
            image_parts.append(genai_types.Part.from_bytes(data=fill_bytes, mime_type="image/png"))
    
    original_doc.close()
    filled_doc.close()
    
    # ── Step 3: Build mapping summary ──
    mapping_summary = json.dumps([
        {
            "pdf_field_name": f.get("pdf_field_name", ""),
            "label": f.get("label", ""),
            "mapped_to": f.get("mapped_to", ""),
            "field_type": f.get("field_type", "text"),
            "x_pct": f.get("x_pct"),
            "y_pct": f.get("y_pct"),
            "width_pct": f.get("width_pct"),
            "page": f.get("page", 0),
            "font_size": f.get("font_size", 10),
            "language": f.get("language", ""),
        }
        for f in current_mapping
    ], indent=2)
    
    structural_json = json.dumps(structural_data, indent=2, ensure_ascii=False)
    page_dims_str = json.dumps(page_dimensions, indent=2)
    
    # ── Step 4: Build enhanced prompt with structural data ──
    prompt = f"""You are a precision form-filling coordinate corrector.

## Your Input
1. ORIGINAL blank bank form image(s)
2. FILLED preview image(s) — showing where our system placed text
3. Current field mapping with coordinates (JSON)
4. **PDFMiner STRUCTURAL BLUEPRINT** — EXACT coordinates of every printed text element in the PDF (labels, headers, boxes). These are 100% accurate — the PDF's own coordinate data.

## Page Dimensions
{page_dims_str}

## PDFMiner Structural Blueprint (GROUND TRUTH — exact label positions)
This data shows the precise position of every printed label in the form. Use it to calculate where fill-areas should be:
- If a label like "Branch:" ends at x_pct=28%, the fill text should START at x_pct≈29%
- If a label row is at y_pct=15%, the fill text baseline should be at y_pct≈15% (same line)
- For fields BELOW a label, add ~3-4% to the label's y_pct
```json
{structural_json}
```

## Current Mapping (what we need to fix)
```json
{mapping_summary}
```

## Your Job
For EACH field in the current mapping:
1. Find the corresponding label in the structural blueprint
2. Calculate the correct fill position based on the label's EXACT coordinates
3. If the current coordinates are wrong, provide corrected values

## CRITICAL Rules
- x_pct: 0=left edge, 100=right edge. START of where text should be written.
- y_pct: 0=top edge, 100=bottom edge. BASELINE of text.
- width_pct: fill area width as % of page width.
- TRUST the structural blueprint coordinates over visual estimation.
- For BILINGUAL forms (Arabic + English on same line):
  - English fill: starts right after the English label ends (left half)
  - Arabic fill: starts right after the Arabic label ends (right half)
- Only correct fields that are MISPLACED. Keep correct ones unchanged.

## Response Format
Return JSON with ONLY fields that need corrections:
{{
  "corrections": [
    {{
      "pdf_field_name": "field_name",
      "x_pct": 25.0,
      "y_pct": 35.2,
      "width_pct": 20.0,
      "font_size": 9,
      "reason": "Label 'Branch:' ends at x=28%, placed fill at x=29%"
    }}
  ],
  "summary": "Brief description of fixes"
}}

If ALL fields are correctly placed: {{"corrections": [], "summary": "All fields correctly positioned"}}
"""
    
    # Build content: images first, then prompt
    content_parts = []
    for i in range(num_pages):
        content_parts.append(f"--- PAGE {i} ORIGINAL ---")
        content_parts.append(image_parts[i * 2])  # original
        content_parts.append(f"--- PAGE {i} FILLED PREVIEW ---")
        if i * 2 + 1 < len(image_parts):
            content_parts.append(image_parts[i * 2 + 1])  # filled
    content_parts.append(prompt)
    
    logger.info(f"Enhance: sending {num_pages} page pairs to Gemini for visual correction...")
    
    response = client.models.generate_content(
        model=GEMINI_MODEL_NAME,
        contents=content_parts,
        config=genai_types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=16384,
            response_mime_type="application/json",
        ),
    )
    
    response_text = response.text.strip()
    
    try:
        result = json.loads(response_text)
    except json.JSONDecodeError:
        # Fallback: strip markdown fences
        clean_text = response_text
        if '```json' in clean_text:
            clean_text = clean_text.split('```json', 1)[1]
            if '```' in clean_text:
                clean_text = clean_text.split('```', 1)[0]
        clean_text = clean_text.strip()
        try:
            result = json.loads(clean_text)
        except json.JSONDecodeError as e:
            logger.warning(f"Enhance: JSON parse failed: {e}. Response: {response_text[:500]}")
            return current_mapping
    corrections = result.get("corrections", [])
    summary = result.get("summary", "")
    
    if not corrections:
        logger.info(f"Enhance: No corrections needed — {summary}")
        return current_mapping
    
    # Apply corrections to the mapping
    corrections_by_name = {c["pdf_field_name"]: c for c in corrections}
    corrected_count = 0
    
    enhanced_mapping = []
    for field in current_mapping:
        field_copy = dict(field)
        fname = field_copy.get("pdf_field_name", "")
        if fname in corrections_by_name:
            corr = corrections_by_name[fname]
            if "x_pct" in corr:
                field_copy["x_pct"] = corr["x_pct"]
            if "y_pct" in corr:
                field_copy["y_pct"] = corr["y_pct"]
            if "width_pct" in corr:
                field_copy["width_pct"] = corr["width_pct"]
            if "font_size" in corr:
                field_copy["font_size"] = corr["font_size"]
            corrected_count += 1
            logger.debug(f"Enhance: corrected '{fname}' — {corr.get('reason', '')}")
        enhanced_mapping.append(field_copy)
    
    logger.info(f"Enhance complete: {corrected_count}/{len(current_mapping)} fields corrected. {summary}")
    return enhanced_mapping




# Shared constant: Maximum file size for facility agreement AI processing (5 MB)
FACILITY_DOC_MAX_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB


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
    if len(pdf_bytes) > FACILITY_DOC_MAX_SIZE_BYTES:
        return {
            "status": "TOO_LARGE",
            "message": f"Document is too large for AI analysis ({len(pdf_bytes) / (1024*1024):.1f} MB). Maximum is 5 MB.",
            "extracted_terms": None,
        }

    client = _get_genai_client()
    if not client:
        return {"status": "AI_UNAVAILABLE", "message": "AI model is not available.", "extracted_terms": None}

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
        response = await client.aio.models.generate_content(
            model=GEMINI_MODEL_NAME,
            contents=[prompt, genai_types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")],
            config=genai_types.GenerateContentConfig(
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

