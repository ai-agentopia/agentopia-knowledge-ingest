"""PDF normalizer using pdfplumber for text extraction.

pdfplumber is preferred over PyPDF2 for better text layout handling,
especially for multi-column PDFs and tables.
"""

import io
import logging
import re

from normalizer.base import NormalizedDocument

logger = logging.getLogger(__name__)


def parse_pdf(raw_bytes: bytes) -> NormalizedDocument:
    """Extract text and structure hints from PDF bytes.

    Uses pdfplumber for page-by-page extraction. Preserves paragraph
    structure by joining page text with double newlines.

    Raises:
        ImportError: If pdfplumber is not installed.
        Exception: If the PDF is corrupted, encrypted, or unreadable.
    """
    import pdfplumber

    pages_text: list[str] = []
    heading_candidates: list[str] = []
    has_tables = False

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            # Extract tables first to detect structure
            tables = page.extract_tables()
            if tables:
                has_tables = True

            text = page.extract_text(x_tolerance=3, y_tolerance=3)
            if not text:
                continue

            pages_text.append(text)

            # Heuristic heading detection: short lines (< 80 chars) that are
            # not mid-sentence (no trailing comma/semicolon), no numbers at start
            for line in text.split("\n"):
                line = line.strip()
                if (
                    5 < len(line) < 80
                    and not line.endswith((",", ";", ":", "—", "-"))
                    and not re.match(r"^\d+\s", line)
                    and line[0].isupper()
                ):
                    heading_candidates.append(line)

    if not pages_text:
        raise ValueError("PDF contains no extractable text (may be scanned or image-only)")

    full_text = "\n\n".join(pages_text)

    # Deduplicate heading candidates while preserving order
    seen: set[str] = set()
    headings: list[str] = []
    for h in heading_candidates:
        if h not in seen:
            seen.add(h)
            headings.append(h)

    code_block_count = full_text.count("```")
    words = full_text.split()
    return NormalizedDocument(
        text=full_text,
        headings=headings[:50],  # cap at 50 to avoid noise from junk PDFs
        structure_hints={
            "has_tables": has_tables,
            "has_code_blocks": code_block_count > 0,
            "heading_count": len(headings),
            "page_count": len(pages_text),
            "estimated_tokens": len(words),
        },
    )
