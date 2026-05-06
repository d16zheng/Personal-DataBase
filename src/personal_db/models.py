"""Shared data models for the database project."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


@dataclass
class Record:
    """A simple value wrapper with metadata that future stages can reuse."""

    key: str
    value: str
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def update(self, value: str) -> None:
        self.value = value
        self.updated_at = utc_now()

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Record":
        return cls(
            key=payload["key"],
            value=payload["value"],
            created_at=datetime.fromisoformat(payload["created_at"]),
            updated_at=datetime.fromisoformat(payload["updated_at"]),
        )


class ClothingType(str, Enum):
    """Supported clothing categories for the first personal use case."""

    SWEATER = "sweater"
    SHIRT = "shirt"
    PANTS = "pants"
    SHORTS = "shorts"
    JACKET = "jacket"


class MeasurementUnit(str, Enum):
    """Units supported for clothing measurements."""

    INCHES = "in"
    CENTIMETERS = "cm"


class JacketSize(str, Enum):
    """Supported jacket sizes."""

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class BookStatus(str, Enum):
    """Lifecycle states for books."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass
class ClothingItem:
    """Typed personal record for a clothing item."""

    clothing_type: ClothingType
    brand: str | None = None
    tags: list[str] = field(default_factory=list)
    measurement_unit: MeasurementUnit | None = None
    jacket_size: JacketSize | None = None
    chest: float | None = None
    shoulder: float | None = None
    body_length: float | None = None
    hem: float | None = None
    waist: float | None = None
    length: float | None = None
    front_rise: float | None = None
    id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass
class Book:
    """Typed personal record for a book."""

    title: str
    status: BookStatus
    author: str | None = None
    tags: list[str] = field(default_factory=list)
    date_started: date | None = None
    id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
