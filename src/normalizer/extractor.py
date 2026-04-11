"""Heuristic metadata extractor.

Reads normalized.json artifacts and produces extracted.json.
No LLM calls. Uses regex and structural heuristics only.

Partial extraction: if a field cannot be detected, it is set to its
empty default. The pipeline always proceeds to indexing — partial
metadata is acceptable, missing metadata is not a failure.
"""

import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)
_DATE_RE = re.compile(
    r"\b(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{2}\.\d{2}\.\d{4})\b"
)


@dataclass
class HierarchyNode:
    level: int
    heading: str
    children: list["HierarchyNode"] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "level": self.level,
            "heading": self.heading,
            "children": [c.to_dict() for c in self.children],
        }


@dataclass
class ExtractedMetadata:
    """Output of the Extractor stage. Written to extracted.json."""

    title: str = ""
    author: str = ""
    date: str = ""
    language: str = "en"
    hierarchy: list[HierarchyNode] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    extraction_method: str = "heuristic"
    extractor_version: str = "1.0.0"
    partial: bool = False
    """True if one or more fields could not be confidently extracted."""

    def to_json_dict(self, document_id: str, version: int) -> dict:
        return {
            "document_id": document_id,
            "version": version,
            "title": self.title,
            "author": self.author,
            "date": self.date,
            "language": self.language,
            "hierarchy": [n.to_dict() for n in self.hierarchy],
            "tags": self.tags,
            "extraction_method": self.extraction_method,
            "extractor_version": self.extractor_version,
            "partial": self.partial,
        }

    def section_path_from_hierarchy(self) -> list[str]:
        """Return a flat list of top-level section headings for the Qdrant payload."""
        return [n.heading for n in self.hierarchy]


def extract(normalized_json: dict) -> ExtractedMetadata:
    """Extract structured metadata from a normalized.json dict.

    Args:
        normalized_json: Parsed contents of normalized.json artifact.

    Returns:
        ExtractedMetadata with populated fields. partial=True if any
        field could not be detected.
    """
    text: str = normalized_json.get("text", "")
    headings: list[str] = normalized_json.get("headings", [])
    fmt: str = normalized_json.get("format", "text")

    missing: list[str] = []

    # ── Title ────────────────────────────────────────────────────────────────
    title = _extract_title(text, headings, fmt)
    if not title:
        missing.append("title")

    # ── Author ───────────────────────────────────────────────────────────────
    author = _extract_author(text, fmt)
    if not author:
        missing.append("author")

    # ── Date ─────────────────────────────────────────────────────────────────
    date = _extract_date(text, fmt)
    if not date:
        missing.append("date")

    # ── Section hierarchy ─────────────────────────────────────────────────────
    hierarchy = _build_hierarchy(text)

    # ── Tags from headings ────────────────────────────────────────────────────
    tags = _extract_tags(headings)

    partial = len(missing) > 0
    if partial:
        logger.info("extractor: partial extraction — missing fields: %s", missing)

    return ExtractedMetadata(
        title=title,
        author=author,
        date=date,
        language="en",
        hierarchy=hierarchy,
        tags=tags,
        partial=partial,
    )


def extract_from_bytes(extracted_bytes: bytes) -> "ExtractedMetadata":
    """Parse an existing extracted.json bytes back into ExtractedMetadata."""
    d = json.loads(extracted_bytes)
    nodes = [
        _dict_to_node(n) for n in d.get("hierarchy", [])
    ]
    return ExtractedMetadata(
        title=d.get("title", ""),
        author=d.get("author", ""),
        date=d.get("date", ""),
        language=d.get("language", "en"),
        hierarchy=nodes,
        tags=d.get("tags", []),
        extraction_method=d.get("extraction_method", "heuristic"),
        partial=d.get("partial", False),
    )


# ── Internal helpers ──────────────────────────────────────────────────────────


def _extract_title(text: str, headings: list[str], fmt: str) -> str:
    """Extract document title.

    Priority:
    1. YAML frontmatter `title:` field (markdown)
    2. First H1 heading in the text
    3. First heading from the headings list
    4. First non-empty line if it's short enough to be a title
    """
    # 1. Frontmatter
    fm = _parse_frontmatter(text)
    if fm.get("title"):
        return str(fm["title"]).strip()

    # 2. First H1 in text
    for m in _HEADING_RE.finditer(text):
        if m.group(1) == "#":
            return m.group(2).strip()

    # 3. First heading from normalizer output
    if headings:
        return headings[0].strip()

    # 4. First short line
    for line in text.split("\n"):
        line = line.strip()
        if 5 < len(line) < 100 and not line.endswith((".", ",", ";")):
            return line

    return ""


def _extract_author(text: str, fmt: str) -> str:
    """Extract author name.

    Sources (in priority order):
    1. YAML frontmatter `author:` or `authors:` field
    2. "By <Name>" or "Author: <Name>" pattern near document start
    """
    fm = _parse_frontmatter(text)
    if fm.get("author"):
        return str(fm["author"]).strip()
    if fm.get("authors"):
        authors = fm["authors"]
        if isinstance(authors, list) and authors:
            return str(authors[0]).strip()

    # Scan first 500 chars for by-line
    snippet = text[:500]
    for pattern in [
        r"[Bb]y[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)",
        r"[Aa]uthor[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)",
        r"[Ww]ritten by[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)",
    ]:
        m = re.search(pattern, snippet)
        if m:
            return m.group(1).strip()

    return ""


def _extract_date(text: str, fmt: str) -> str:
    """Extract document date.

    Sources:
    1. YAML frontmatter `date:` field
    2. First ISO date (YYYY-MM-DD) found in the first 1000 chars
    """
    fm = _parse_frontmatter(text)
    if fm.get("date"):
        raw = str(fm["date"]).strip()
        # Normalize to YYYY-MM-DD if possible
        m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
        if m:
            return m.group(1)
        return raw

    snippet = text[:1000]
    m = _DATE_RE.search(snippet)
    if m:
        raw = m.group(1)
        # Normalize DD/MM/YYYY or DD.MM.YYYY to YYYY-MM-DD
        if "/" in raw or "." in raw:
            sep = "/" if "/" in raw else "."
            parts = raw.split(sep)
            if len(parts) == 3:
                # Assume DD/MM/YYYY for European format
                try:
                    return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                except Exception:
                    pass
        return raw

    return ""


def _build_hierarchy(text: str) -> list[HierarchyNode]:
    """Build a nested section hierarchy from heading markers in text.

    Processes # through ###### markers in document order.
    Returns a flat list of root nodes (level 1 sections), each with
    nested children.
    """
    matches = list(_HEADING_RE.finditer(text))
    if not matches:
        return []

    roots: list[HierarchyNode] = []
    stack: list[HierarchyNode] = []

    for m in matches:
        level = len(m.group(1))
        heading = m.group(2).strip()
        node = HierarchyNode(level=level, heading=heading)

        # Pop stack until we find a parent with lower level
        while stack and stack[-1].level >= level:
            stack.pop()

        if stack:
            stack[-1].children.append(node)
        else:
            roots.append(node)

        stack.append(node)

    return roots


def _extract_tags(headings: list[str]) -> list[str]:
    """Extract tags from the heading list.

    Returns lowercase single-word headings that look like topic labels.
    Capped at 10 tags.
    """
    tags: list[str] = []
    for h in headings:
        words = h.lower().split()
        if len(words) <= 3:
            tag = "-".join(re.sub(r"[^a-z0-9-]", "", w) for w in words)
            if tag and tag not in tags:
                tags.append(tag)
        if len(tags) >= 10:
            break
    return tags


def _parse_frontmatter(text: str) -> dict:
    """Parse YAML frontmatter from markdown text. Returns {} if none found."""
    m = _FRONTMATTER_RE.match(text)
    if not m:
        return {}
    try:
        import yaml
        return yaml.safe_load(m.group(1)) or {}
    except Exception:
        # yaml not installed or parse error — return empty
        return {}


def _dict_to_node(d: dict) -> HierarchyNode:
    node = HierarchyNode(
        level=d.get("level", 1),
        heading=d.get("heading", ""),
    )
    node.children = [_dict_to_node(c) for c in d.get("children", [])]
    return node
