from __future__ import annotations

import domain.content_extraction as contracts
from domain.content_extraction.exceptions import (
    ContentExtractionError,
    ContentParseError,
    ContentReadError,
    ContentTooLargeError,
    EncryptedContentError,
    UnsupportedContentTypeError,
)


def test_every_specific_exception_inherits_from_content_extraction_error() -> None:
    exception_types = (
        UnsupportedContentTypeError,
        ContentReadError,
        ContentParseError,
        ContentTooLargeError,
        EncryptedContentError,
    )

    for exc_type in exception_types:
        assert issubclass(exc_type, ContentExtractionError)


def test_exception_messages_are_preserved() -> None:
    message = "extraction failed"

    exc = ContentParseError(message)

    assert str(exc) == message


def test_package_exports_expose_all_public_contracts() -> None:
    expected_exports = {
        "ContentExtractionError",
        "ContentParseError",
        "ContentReadError",
        "ContentTooLargeError",
        "ContentExtractor",
        "EncryptedContentError",
        "ExtractedContent",
        "ExtractedSection",
        "ExtractedTable",
        "ExtractionWarning",
        "UnsupportedContentTypeError",
    }

    assert set(contracts.__all__) == expected_exports
    for name in expected_exports:
        assert hasattr(contracts, name)
