"""Domain exception types for content extraction workflows."""

from __future__ import annotations


class ContentExtractionError(Exception):
    """Base error for content extraction failures."""


class UnsupportedContentTypeError(ContentExtractionError):
    """Raised when no extractor supports the requested content type."""


class ContentReadError(ContentExtractionError):
    """Raised when source content cannot be read."""


class ContentParseError(ContentExtractionError):
    """Raised when source content cannot be parsed."""


class ContentTooLargeError(ContentExtractionError):
    """Raised when content exceeds supported processing limits."""


class EncryptedContentError(ContentExtractionError):
    """Raised when content is encrypted and cannot be processed."""
