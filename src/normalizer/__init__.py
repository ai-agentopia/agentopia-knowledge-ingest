"""Document normalizer package.

Each module implements a format-specific parser that converts raw bytes
into a NormalizedDocument — plain text plus structural metadata.
"""

from normalizer.base import NormalizedDocument, normalize, SUPPORTED_FORMATS

__all__ = ["NormalizedDocument", "normalize", "SUPPORTED_FORMATS"]
