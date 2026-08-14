"""Domain exceptions for content chunking."""


class ChunkingError(Exception):
    """Base error for content chunking failures."""


class InvalidChunkingConfigurationError(ChunkingError, ValueError):
    """Raised when chunking configuration violates its invariants."""