"""Logical database interface built on top of a storage backend."""

from __future__ import annotations

from pathlib import Path

from .models import Record
from .storage import LogStructuredStorage


class InMemoryKeyValueStore:
    """Database API layer that delegates persistence to the storage package."""

    def __init__(
        self,
        log_path: str | Path | None = None,
        page_path: str | Path | None = None,
        page_size: int = 4096,
    ) -> None:
        self._storage = LogStructuredStorage(
            log_path=log_path,
            page_path=page_path,
            page_size=page_size,
        )

    def put(self, key: str, value: str) -> Record:
        return self._storage.put(key, value)

    def get(self, key: str) -> Record:
        return self._storage.get(key)

    def delete(self, key: str) -> Record:
        return self._storage.delete(key)

    def list_records(self) -> list[Record]:
        return self._storage.list_records()

    def size(self) -> int:
        return self._storage.size()
