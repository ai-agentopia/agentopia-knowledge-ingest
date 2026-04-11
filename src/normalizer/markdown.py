"""Markdown normalizer.

No external library required — markdown structure is parsed directly
from the raw text using heading and code-fence detection.
"""

import logging
import re

from normalizer.base import NormalizedDocument

logger = logging.getLogger(__name__)

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)
_CODE_FENCE_RE = re.compile(r"^```", re.MULTILINE)
_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def parse_markdown(raw_bytes: bytes) -> NormalizedDocument:
    """Extract text and structure from Markdown bytes.

    Strips YAML frontmatter before processing.
    Headings are extracted in document order.
    Code blocks are detected and counted.
    """
    text = raw_bytes.decode("utf-8", errors="replace")

    # Strip YAML frontmatter
    fm_match = _FRONTMATTER_RE.match(text)
    if fm_match:
        text = text[fm_match.end():]

    # Extract headings in order
    headings = [m.group(2).strip() for m in _HEADING_RE.finditer(text)]

    # Count code fences (each ``` pair = one block, so count/2)
    code_fence_count = len(_CODE_FENCE_RE.findall(text))
    has_code_blocks = code_fence_count > 0

    # Remove code fence delimiters for clean text (keep code content)
    clean_text = text.strip()

    if not clean_text:
        raise ValueError("Markdown file contains no extractable text")

    has_tables = bool(re.search(r"^\|.+\|", clean_text, re.MULTILINE))
    words = clean_text.split()

    return NormalizedDocument(
        text=clean_text,
        headings=headings,
        structure_hints={
            "has_tables": has_tables,
            "has_code_blocks": has_code_blocks,
            "heading_count": len(headings),
            "estimated_tokens": len(words),
            "has_numbered_lists": bool(re.search(r"^\d+\.\s", clean_text, re.MULTILINE)),
        },
    )
