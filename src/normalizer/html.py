"""HTML normalizer using BeautifulSoup4."""

import logging
import re

from normalizer.base import NormalizedDocument

logger = logging.getLogger(__name__)

_HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}


def parse_html(raw_bytes: bytes) -> NormalizedDocument:
    """Extract text and heading structure from HTML bytes.

    Removes script, style, nav, header, footer elements before extraction.
    Preserves paragraph boundaries by inserting newlines at block elements.

    Raises:
        ImportError: If beautifulsoup4 is not installed.
    """
    from bs4 import BeautifulSoup, NavigableString, Tag

    # Prefer html.parser (built-in); lxml is faster if installed
    try:
        soup = BeautifulSoup(raw_bytes, "lxml")
    except Exception:
        soup = BeautifulSoup(raw_bytes, "html.parser")

    # Remove noise elements
    for tag in soup(["script", "style", "nav", "header", "footer", "noscript", "iframe"]):
        tag.decompose()

    headings: list[str] = []
    paragraphs: list[str] = []

    # Walk the document in order, capturing heading text and block text
    _BLOCK_TAGS = {"p", "div", "section", "article", "li", "td", "th", "pre", "blockquote"}

    def _collect(element) -> None:
        if isinstance(element, NavigableString):
            return
        if isinstance(element, Tag):
            tag_name = element.name.lower() if element.name else ""
            if tag_name in _HEADING_TAGS:
                text = element.get_text(" ", strip=True)
                if text:
                    headings.append(text)
                    paragraphs.append(text)
            elif tag_name in _BLOCK_TAGS:
                text = element.get_text(" ", strip=True)
                if text:
                    paragraphs.append(text)
            else:
                for child in element.children:
                    _collect(child)

    body = soup.body or soup
    for child in body.children:
        _collect(child)

    full_text = "\n\n".join(p for p in paragraphs if p.strip())
    if not full_text.strip():
        # Fallback: extract all text if structured walk found nothing
        full_text = soup.get_text(separator="\n", strip=True)

    if not full_text.strip():
        raise ValueError("HTML contains no extractable text")

    has_code_blocks = bool(soup.find(["code", "pre"]))
    words = full_text.split()
    return NormalizedDocument(
        text=full_text,
        headings=headings,
        structure_hints={
            "has_tables": bool(soup.find("table")),
            "has_code_blocks": has_code_blocks,
            "heading_count": len(headings),
            "estimated_tokens": len(words),
        },
    )
