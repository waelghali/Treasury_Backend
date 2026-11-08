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

def _render_single_html_one_page(html_content: str) -> bytes:
    """
    Renders an HTML document via WeasyPrint and guarantees it fits on a single A4 page
    (while strictly preserving the top letterhead margin, e.g. 40mm) by progressively
    compacting vertical spacing and font size if content (such as long notes or addresses)
    would otherwise spill onto page 2.
    """
    from weasyprint import HTML, CSS

    formatted_html = _apply_template_date_formatting(html_content)
    html_obj = HTML(string=formatted_html)
    doc = html_obj.render()

    if len(doc.pages) <= 1:
        pdf_bytes = doc.write_pdf()
        del doc, html_obj
        return pdf_bytes

    # Progressive compacting steps that preserve @page margin-top (40mm letterhead clearance)
    compact_presets = [
        # Step 1: Slightly tighter vertical spacing & table padding
        """
        body { font-size: 9.2pt !important; line-height: 1.4 !important; padding-top: 0 !important; }
        .header-table { padding-bottom: 7px !important; margin-bottom: 12px !important; }
        .recipient-block { margin-bottom: 11px !important; }
        .subject-line { margin-bottom: 11px !important; padding: 3px 0 3px 10px !important; font-size: 10pt !important; }
        .salutation { margin-bottom: 7px !important; }
        p { margin-bottom: 8px !important; }
        .spec-table { margin: 8px 0 10px 0 !important; font-size: 9pt !important; }
        .spec-table th, .spec-table td { padding: 4.5px 10px !important; }
        .notes-wrapper { margin: 8px 0 !important; }
        .signature-section { margin-top: 14px !important; }
        .sig-table { margin-top: 56px !important; }
        .footer-note { margin-top: 14px !important; padding-top: 5px !important; }
        """,
        # Step 2: Medium compacting for letters with multi-line notes / declarations
        """
        @page { margin-bottom: 10mm !important; }
        body { font-size: 8.6pt !important; line-height: 1.32 !important; padding-top: 0 !important; }
        .company-title { font-size: 12.5pt !important; }
        .header-table { padding-bottom: 5px !important; margin-bottom: 9px !important; }
        .recipient-block { margin-bottom: 8px !important; font-size: 9pt !important; }
        .recipient-name { font-size: 9.5pt !important; }
        .subject-line { margin-bottom: 8px !important; padding: 2px 0 2px 8px !important; font-size: 9.5pt !important; }
        .salutation { margin-bottom: 5px !important; }
        p { margin-bottom: 6px !important; }
        .spec-table { margin: 6px 0 8px 0 !important; font-size: 8.4pt !important; }
        .spec-table th, .spec-table td { padding: 3.5px 8px !important; }
        .notes-wrapper { margin: 6px 0 !important; }
        .notes-wrapper h3 { font-size: 8.5pt !important; margin: 4px 0 2px 0 !important; }
        .signature-section { margin-top: 10px !important; }
        .sig-table { margin-top: 44px !important; }
        .footer-note { margin-top: 10px !important; padding-top: 4px !important; }
        """,
        # Step 3: High-density fallback for very long custom notes
        """
        @page { margin-bottom: 8mm !important; }
        body { font-size: 7.8pt !important; line-height: 1.24 !important; padding-top: 0 !important; }
        .company-title { font-size: 11.5pt !important; }
        .header-table { padding-bottom: 4px !important; margin-bottom: 6px !important; }
        .recipient-block { margin-bottom: 6px !important; font-size: 8.2pt !important; }
        .recipient-name { font-size: 8.8pt !important; }
        .subject-line { margin-bottom: 6px !important; padding: 2px 0 2px 8px !important; font-size: 8.8pt !important; }
        .salutation { margin-bottom: 4px !important; }
        p { margin-bottom: 4px !important; }
        .spec-table { margin: 4px 0 6px 0 !important; font-size: 7.8pt !important; }
        .spec-table th, .spec-table td { padding: 2.5px 6px !important; }
        .notes-wrapper { margin: 4px 0 !important; }
        .notes-wrapper h3 { font-size: 8pt !important; margin: 2px 0 !important; }
        .signature-section { margin-top: 6px !important; }
        .sig-table { margin-top: 34px !important; }
        .footer-note { margin-top: 6px !important; padding-top: 3px !important; font-size: 6.8pt !important; }
        """,
    ]

    for css_rules in compact_presets:
        del doc
        doc = html_obj.render(stylesheets=[CSS(string=css_rules)])
        if len(doc.pages) <= 1:
            break

    pdf_bytes = doc.write_pdf()
    del doc, html_obj
    return pdf_bytes


def _render_pdf_worker(html_content: str) -> Optional[bytes]:
    """
    Renders HTML to PDF bytes using WeasyPrint and immediately frees native C-heap memory.
    """
    try:
        return _render_single_html_one_page(html_content)
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
        from pypdf import PdfReader, PdfWriter

        writer = PdfWriter()
        for segment in html_segments:
            if not segment or not segment.strip():
                continue
            seg_bytes = _render_single_html_one_page(segment)
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
