# app/core/file_security.py
"""
Secure File Upload & Magic-Byte Validator
Provides defense against malicious file uploads, extension spoofing, and path traversal.
"""
import os
import re
from typing import Tuple, Optional

# Magic byte signatures for authorized financial & document formats
MAGIC_SIGNATURES = {
    "pdf": [b"%PDF-"],
    "png": [b"\x89PNG\r\n\x1a\n"],
    "jpg": [b"\xff\xd8\xff"],
    "jpeg": [b"\xff\xd8\xff"],
    "xlsx": [b"PK\x03\x04", b"PK\x05\x06"],
    "xls": [b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"],
}

# Maximum allowed upload size default: 25 MB
DEFAULT_MAX_SIZE_BYTES = 25 * 1024 * 1024


def sanitize_filename(filename: str) -> str:
    """
    Strips path traversal sequences (../, ..\\) and dangerous characters.
    """
    if not filename:
        return "unnamed_document"
    
    # Strip path separators
    clean_name = os.path.basename(filename)
    # Remove control characters and path traversal patterns
    clean_name = re.sub(r"[^\w\s\-\.\(\)\[\]]", "_", clean_name)
    # Prevent hidden files
    clean_name = clean_name.lstrip(".")
    return clean_name or "document"


def validate_file_content(content: bytes, allowed_extensions: Optional[list] = None) -> Tuple[bool, str]:
    """
    Validates file byte buffer against magic byte signatures.
    
    Returns:
        (is_valid: bool, error_or_detected_type: str)
    """
    if not content or len(content) == 0:
        return False, "File is empty"

    if len(content) > DEFAULT_MAX_SIZE_BYTES:
        return False, f"File size ({len(content) / (1024*1024):.1f}MB) exceeds maximum limit (25MB)"

    detected_ext = None
    for ext, signatures in MAGIC_SIGNATURES.items():
        for sig in signatures:
            if content.startswith(sig):
                detected_ext = ext
                break
        if detected_ext:
            break

    if not detected_ext:
        # If no strict magic byte match, but it's plain text / CSV
        if content[:1024].isascii():
            detected_ext = "txt"
        else:
            return False, "Unsupported or unrecognized file format"

    if allowed_extensions:
        allowed_normalized = [e.lower().lstrip(".") for e in allowed_extensions]
        # Treat jpg/jpeg interchangeably
        if detected_ext in ("jpg", "jpeg") and ("jpg" in allowed_normalized or "jpeg" in allowed_normalized):
            return True, detected_ext

        if detected_ext not in allowed_normalized:
            return False, f"File type '.{detected_ext}' is not permitted (allowed: {', '.join(allowed_normalized)})"

    return True, detected_ext
