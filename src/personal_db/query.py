"""Typed query objects for the tiny query layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from .models import BookStatus, ClothingType


class RecordKind(str, Enum):
    """Record categories supported by the tiny query layer."""

    CLOTHING_ITEM = "clothing_item"
    BOOK = "book"


@dataclass(frozen=True)
class ClothingQuery:
    """Filters for clothing-item queries."""

    clothing_type: ClothingType | None = None
    brand_prefix: str | None = None
    tag: str | None = None
    updated_after: datetime | None = None


@dataclass(frozen=True)
class BookQuery:
    """Filters for book queries."""

    status: BookStatus | None = None
    title_prefix: str | None = None
    author_prefix: str | None = None
    tag: str | None = None
    updated_after: datetime | None = None


@dataclass(frozen=True)
class RecordSummary:
    """Small cross-entity result for tag and timestamp queries."""

    record_kind: RecordKind
    record_id: str
    label: str
    updated_at: datetime
    tags: list[str] = field(default_factory=list)
