"""Shared data models for the database project."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class Record:
    """A simple value wrapper with metadata that future stages can reuse."""

    key: str
    value: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def update(self, value: str) -> None:
        self.value = value
        self.updated_at = datetime.now(timezone.utc)

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
