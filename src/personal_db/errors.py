"""Custom exceptions for the database project."""


class DatabaseError(Exception):
    """Base exception for database-related failures."""


class KeyNotFoundError(DatabaseError):
    """Raised when a requested key does not exist."""


class LogFormatError(DatabaseError):
    """Raised when the append-only log contains invalid data."""


class PageFormatError(DatabaseError):
    """Raised when a page file contains invalid data."""


class PageOverflowError(DatabaseError):
    """Raised when a record cannot fit inside a fixed-size page."""
