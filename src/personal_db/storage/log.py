"""Append-only log support for durable writes and startup recovery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from ..errors import LogFormatError


@dataclass
class LogEntry:
    """A single operation stored in the append-only log."""

    operation: str
    key: str
    value: str | None
    timestamp: datetime

    def to_json(self) -> str:
        payload = {
            "operation": self.operation,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
        }
        return json.dumps(payload, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw_line: str) -> "LogEntry":
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise LogFormatError("Could not decode log entry.") from exc

        if payload.get("operation") not in {"put", "delete"}:
            raise LogFormatError("Log entry contains an unsupported operation.")
        if not isinstance(payload.get("key"), str):
            raise LogFormatError("Log entry is missing a valid key.")

        value = payload.get("value")
        if value is not None and not isinstance(value, str):
            raise LogFormatError("Log entry value must be a string or null.")

        timestamp_raw = payload.get("timestamp")
        if not isinstance(timestamp_raw, str):
            raise LogFormatError("Log entry is missing a valid timestamp.")

        try:
            timestamp = datetime.fromisoformat(timestamp_raw)
        except ValueError as exc:
            raise LogFormatError("Log entry timestamp is invalid.") from exc

        return cls(
            operation=payload["operation"],
            key=payload["key"],
            value=value,
            timestamp=timestamp,
        )


class AppendOnlyLog:
    """Persists mutations by appending each operation to a log file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def append_put(self, key: str, value: str) -> None:
        self._append(
            LogEntry(
                operation="put",
                key=key,
                value=value,
                timestamp=datetime.now(timezone.utc),
            )
        )

    def append_delete(self, key: str) -> None:
        self._append(
            LogEntry(
                operation="delete",
                key=key,
                value=None,
                timestamp=datetime.now(timezone.utc),
            )
        )

    def replay(self) -> Iterator[LogEntry]:
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    yield LogEntry.from_json(line)
                except LogFormatError as exc:
                    raise LogFormatError(
                        f"Invalid log entry on line {line_number}."
                    ) from exc

    def clear(self) -> None:
        self.path.write_text("", encoding="utf-8")

    def _append(self, entry: LogEntry) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(entry.to_json())
            handle.write("\n")
