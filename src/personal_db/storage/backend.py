"""Storage backends for the key-value database."""

from __future__ import annotations

from pathlib import Path

from ..errors import KeyNotFoundError
from ..models import Record
from .log import AppendOnlyLog
from .page import PageFile


class LogStructuredStorage:
    """In-memory records backed by pages plus an optional recovery log."""

    def __init__(
        self,
        log_path: str | Path | None = None,
        page_path: str | Path | None = None,
        page_size: int = 4096,
    ) -> None:
        self._records: dict[str, Record] = {}
        self._log = AppendOnlyLog(Path(log_path)) if log_path is not None else None
        self._pages = (
            PageFile(path=Path(page_path), page_size=page_size)
            if page_path is not None
            else None
        )

        if self._pages is not None:
            self._load_pages()
        if self._log is not None:
            replayed_entries = self._replay_log()
            if replayed_entries and self._pages is not None:
                self._persist_pages()
                self._log.clear()

    def put(self, key: str, value: str) -> Record:
        if self._log is not None:
            self._log.append_put(key, value)
        record = self._apply_put(key, value)
        self._persist_pages()
        return record

    def get(self, key: str) -> Record:
        try:
            return self._records[key]
        except KeyError as exc:
            raise KeyNotFoundError(f"Key {key!r} does not exist.") from exc

    def delete(self, key: str) -> Record:
        try:
            deleted = self._records.pop(key)
        except KeyError as exc:
            raise KeyNotFoundError(f"Key {key!r} does not exist.") from exc
        if self._log is not None:
            self._log.append_delete(key)
        self._persist_pages()
        return deleted

    def list_records(self) -> list[Record]:
        return sorted(self._records.values(), key=lambda record: record.key)

    def size(self) -> int:
        return len(self._records)

    def _load_pages(self) -> None:
        assert self._pages is not None
        for record in self._pages.read_records():
            self._records[record.key] = record

    def _replay_log(self) -> bool:
        assert self._log is not None
        replayed_entries = False
        for entry in self._log.replay():
            replayed_entries = True
            if entry.operation == "put":
                assert entry.value is not None
                self._apply_put(entry.key, entry.value)
            else:
                self._records.pop(entry.key, None)
        return replayed_entries

    def _persist_pages(self) -> None:
        if self._pages is None:
            return
        self._pages.write_records(self.list_records())
        if self._log is not None:
            self._log.clear()

    def _apply_put(self, key: str, value: str) -> Record:
        if key in self._records:
            self._records[key].update(value)
        else:
            self._records[key] = Record(key=key, value=value)
        return self._records[key]
