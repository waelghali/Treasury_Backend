# core/document_generator.py
import asyncio
import io
import os
import gc
import ctypes
from typing import Optional, List
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

def _apply_template_date_formatting(html_content: str) -> str:
    """
    Honors data-date-format="DD-MMM-YYYY" attributes inside HTML templates by converting
    any ISO date ('YYYY-MM-DD') inside those elements to 'DD-MMM-YYYY' (e.g., '03-Nov-2026')
    prior to WeasyPrint static PDF rendering.
    """
    if not html_content or 'data-date-format="DD-MMM-YYYY"' not in html_content:
        return html_content

    def _iso_to_dd_mmm_yyyy(match: re.Match) -> str:
        try:
            return datetime.strptime(match.group(0), "%Y-%m-%d").strftime("%d-%b-%Y")
        except ValueError:
            return match.group(0)

    def _format_span(span_match: re.Match) -> str:
        return re.sub(r"\b\d{4}-\d{2}-\d{2}\b", _iso_to_dd_mmm_yyyy, span_match.group(0))

    return re.sub(
        r'<span[^>]*data-date-format="DD-MMM-YYYY"[^>]*>.*?</span>',
        _format_span,
        html_content,
        flags=re.DOTALL,
    )

def _render_pdf_worker(html_content: str) -> Optional[bytes]:
    """
    Renders HTML to PDF bytes using WeasyPrint and immediately frees native C-heap memory.
    """
    try:
        from weasyprint import HTML
        html = HTML(string=_apply_template_date_formatting(html_content))
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

def _render_and_merge_pdfs_worker(html_segments: List[str]) -> Optional[bytes]:
    """
    Renders each HTML segment as an independent PDF (so each letter has its own @page headers,
    footers, and independent Page 1 of 1 numbering) and merges the resulting PDFs into a single PDF stream.
    """
    try:
        from weasyprint import HTML
        from pypdf import PdfReader, PdfWriter

        writer = PdfWriter()
        for segment in html_segments:
            if not segment or not segment.strip():
                continue
            html_obj = HTML(string=_apply_template_date_formatting(segment))
            seg_bytes = html_obj.write_pdf()
            del html_obj
            if seg_bytes:
                reader = PdfReader(io.BytesIO(seg_bytes))
                for page in reader.pages:
                    writer.add_page(page)

        if len(writer.pages) == 0:
            return None

        out_stream = io.BytesIO()
        writer.write(out_stream)
        return out_stream.getvalue()
    finally:
        gc.collect()
        try:
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

async def generate_merged_pdf_from_html_list(html_segments: List[str], filename_hint: str = "consolidated_documents") -> Optional[bytes]:
    """
    Renders a list of independent HTML documents into individual PDFs and merges them into one PDF,
    preserving isolated @page counters and headers/footers per letter.
    """
    logger.debug(f"generate_merged_pdf_from_html_list: Rendering and merging {len(html_segments)} letters for '{filename_hint}'.")
    try:
        pdf_bytes = await asyncio.to_thread(_render_and_merge_pdfs_worker, html_segments)
        if pdf_bytes:
            logger.info(f"generate_merged_pdf_from_html_list: Successfully generated merged PDF for '{filename_hint}' ({len(html_segments)} letters, size: {len(pdf_bytes)} bytes).")
            return pdf_bytes
        return None
    except Exception as e:
        logger.error(f"generate_merged_pdf_from_html_list: Error generating merged PDF for '{filename_hint}': {e}", exc_info=True)
        return None
    finally:
        gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass
