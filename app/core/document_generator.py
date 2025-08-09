# core/document_generator.py
import io
import os
from typing import Optional
import logging

# Import WeasyPrint
from weasyprint import HTML, CSS

logger = logging.getLogger(__name__)

async def generate_pdf_from_html(html_content: str, filename_hint: str = "document") -> Optional[bytes]:
    """
    Generates a PDF from HTML content using WeasyPrint.
    Returns the PDF as bytes.
    """
    logger.debug(f"generate_pdf_from_html: Attempting to generate PDF for '{filename_hint}'.")
    logger.debug(f"generate_pdf_from_html: HTML content length: {len(html_content)} characters.")
    # You might want to log a snippet of html_content, but be careful with very large content
    # logger.debug(f"generate_pdf_from_html: HTML content snippet: {html_content[:500]}...")

    try:
        # Create an HTML object from the string content
        html = HTML(string=html_content)
        
        # You can optionally add CSS here if your templates use external CSS or need specific print styles
        # For example:
        # css = CSS(string='@page { size: A4; margin: 1cm; } body { font-family: sans-serif; }')
        # pdf_bytes = html.write_pdf(stylesheets=[css])

        pdf_bytes = html.write_pdf()

        logger.info(f"generate_pdf_from_html: Successfully generated PDF bytes for '{filename_hint}' (size: {len(pdf_bytes)} bytes).")
        return pdf_bytes
    except Exception as e:
        logger.error(f"generate_pdf_from_html: Error generating PDF from HTML for '{filename_hint}': {e}", exc_info=True)
        return None
