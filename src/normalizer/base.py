"""Base types and retry-wrapped dispatch for document normalization."""

import functools
import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = {"pdf", "docx", "html", "htm", "md", "markdown", "txt", "text"}


@dataclass
class NormalizedDocument:
    """Output of the Normalizer stage.

    Written to normalized.json in S3/local storage.
    """

    text: str
    """Full extracted plain text. Preserves paragraph breaks with newlines."""

    headings: list[str] = field(default_factory=list)
    """Detected headings in document order."""

    structure_hints: dict = field(default_factory=dict)
    """Flags and counts for downstream extraction and chunking decisions."""

    normalizer_version: str = "1.0.0"

    def to_json_dict(self, document_id: str, version: int, format: str) -> dict:
        """Return a JSON-serializable dict matching the normalized.json schema."""
        return {
            "document_id": document_id,
            "version": version,
            "format": format,
            "text": self.text,
            "headings": self.headings,
            "structure_hints": self.structure_hints,
            "normalizer_version": self.normalizer_version,
        }


class NormalizationError(Exception):
    """Raised when a document cannot be parsed."""


def _retry(max_attempts: int = 3, backoff_base: float = 1.0):
    """Decorator: retry on any exception with exponential backoff.

    Raises the last exception if all attempts fail.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        sleep_secs = backoff_base * (2 ** (attempt - 1))
                        logger.warning(
                            "normalizer: attempt %d/%d failed (%s): %s — retrying in %.1fs",
                            attempt, max_attempts, fn.__name__, exc, sleep_secs,
                        )
                        time.sleep(sleep_secs)
                    else:
                        logger.error(
                            "normalizer: attempt %d/%d failed (%s): %s — exhausted",
                            attempt, max_attempts, fn.__name__, exc,
                        )
            raise NormalizationError(f"Normalization failed after {max_attempts} attempts") from last_exc
        return wrapper
    return decorator


@_retry(max_attempts=3, backoff_base=1.0)
def normalize(raw_bytes: bytes, format: str) -> NormalizedDocument:
    """Parse raw bytes into a NormalizedDocument.

    Dispatches to format-specific parsers. Retries up to 3 times with
    exponential backoff (1s, 2s) before raising NormalizationError.

    Args:
        raw_bytes: Raw file bytes from S3/upload.
        format: Lowercase format string: pdf | docx | html | htm | md | markdown | txt | text

    Returns:
        NormalizedDocument with extracted text and structure hints.

    Raises:
        NormalizationError: If parsing fails after all retries.
        ValueError: If the format is not supported.
    """
    fmt = format.lower().strip()

    if fmt == "pdf":
        from normalizer.pdf import parse_pdf
        return parse_pdf(raw_bytes)
    elif fmt == "docx":
        from normalizer.docx import parse_docx
        return parse_docx(raw_bytes)
    elif fmt in ("html", "htm"):
        from normalizer.html import parse_html
        return parse_html(raw_bytes)
    elif fmt in ("md", "markdown"):
        from normalizer.markdown import parse_markdown
        return parse_markdown(raw_bytes)
    elif fmt in ("txt", "text"):
        text = raw_bytes.decode("utf-8", errors="replace").strip()
        return NormalizedDocument(
            text=text,
            headings=[],
            structure_hints={"has_tables": False, "has_code_blocks": False, "heading_count": 0,
                             "estimated_tokens": len(text.split())},
        )
    else:
        raise ValueError(f"Unsupported format: '{fmt}'. Supported: {sorted(SUPPORTED_FORMATS)}")


def detect_format(filename: str) -> str:
    """Infer format string from filename extension."""
    import pathlib
    ext = pathlib.Path(filename).suffix.lower().lstrip(".")
    ext_map = {
        "pdf": "pdf",
        "docx": "docx",
        "doc": "docx",
        "html": "html",
        "htm": "html",
        "md": "markdown",
        "markdown": "markdown",
        "txt": "txt",
        "text": "txt",
    }
    return ext_map.get(ext, "txt")
