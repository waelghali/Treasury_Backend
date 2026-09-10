# core/document_generator.py
import asyncio
import io
import os
import gc
import ctypes
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def _render_pdf_worker(html_content: str) -> Optional[bytes]:
    """
    Renders HTML to PDF bytes using WeasyPrint and immediately frees native C-heap memory.
    """
    try:
        from weasyprint import HTML
        html = HTML(string=html_content)
        pdf_bytes = html.write_pdf()
        del html
        return pdf_bytes
    finally:
        gc.collect()
        try:
            # On Linux (Docker/Kubernetes containers), force glibc to release free arenas back to the OS
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass

async def generate_pdf_from_html(html_content: str, filename_hint: str = "document") -> Optional[bytes]:
    """
    Generates a PDF from HTML content using WeasyPrint in a separate thread with memory cleanup.
    Returns the PDF as bytes.
    """
    logger.debug(f"generate_pdf_from_html: Attempting to generate PDF for '{filename_hint}' (length: {len(html_content)} chars).")

    try:
        pdf_bytes = await asyncio.to_thread(_render_pdf_worker, html_content)
        if pdf_bytes:
            logger.info(f"generate_pdf_from_html: Successfully generated PDF bytes for '{filename_hint}' (size: {len(pdf_bytes)} bytes).")
            return pdf_bytes
        return None
    except Exception as e:
        logger.error(f"generate_pdf_from_html: Error generating PDF from HTML for '{filename_hint}': {e}", exc_info=True)
        return None
    finally:
        gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass

