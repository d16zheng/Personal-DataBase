"""Fixed-size page primitives for the storage engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ..errors import PageFormatError, PageOverflowError
from ..models import Record

PAGE_HEADER_SIZE = 4
DEFAULT_PAGE_SIZE = 4096


@dataclass
class Page:
    """A fixed-size page with a tiny header and variable payload."""

    page_id: int
    payload: bytes

    def to_bytes(self, page_size: int) -> bytes:
        max_payload_size = page_size - PAGE_HEADER_SIZE
        if len(self.payload) > max_payload_size:
            raise PageOverflowError(
                f"Page {self.page_id} payload exceeds {max_payload_size} bytes."
            )

        header = len(self.payload).to_bytes(PAGE_HEADER_SIZE, byteorder="little")
        padding = b"\x00" * (max_payload_size - len(self.payload))
        return header + self.payload + padding

    @classmethod
    def from_bytes(cls, page_id: int, raw_page: bytes) -> "Page":
        if len(raw_page) < PAGE_HEADER_SIZE:
            raise PageFormatError("Page is too small to contain a valid header.")

        payload_length = int.from_bytes(
            raw_page[:PAGE_HEADER_SIZE],
            byteorder="little",
        )
        max_payload_size = len(raw_page) - PAGE_HEADER_SIZE
        if payload_length > max_payload_size:
            raise PageFormatError("Page header declares more bytes than the page holds.")

        payload = raw_page[PAGE_HEADER_SIZE : PAGE_HEADER_SIZE + payload_length]
        return cls(page_id=page_id, payload=payload)


class PageFile:
    """Stores records as a sequence of fixed-size pages on disk."""

    def __init__(self, path: Path, page_size: int = DEFAULT_PAGE_SIZE) -> None:
        self.path = path
        self.page_size = page_size
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def write_records(self, records: list[Record]) -> None:
        pages = self._pack_records(records)
        with self.path.open("wb") as handle:
            for page in pages:
                handle.write(page.to_bytes(self.page_size))

    def read_records(self) -> list[Record]:
        records: list[Record] = []
        with self.path.open("rb") as handle:
            page_id = 0
            while True:
                raw_page = handle.read(self.page_size)
                if not raw_page:
                    break
                if len(raw_page) != self.page_size:
                    raise PageFormatError("Encountered a partial page at the end of file.")

                page = Page.from_bytes(page_id=page_id, raw_page=raw_page)
                records.extend(self._decode_page(page))
                page_id += 1

        return records

    def _pack_records(self, records: list[Record]) -> list[Page]:
        max_payload_size = self.page_size - PAGE_HEADER_SIZE
        if not records:
            return []

        pages: list[Page] = []
        current_lines: list[bytes] = []
        current_size = 0

        for record in sorted(records, key=lambda item: item.key):
            line = json.dumps(record.to_dict(), separators=(",", ":")).encode("utf-8")
            line += b"\n"

            if len(line) > max_payload_size:
                raise PageOverflowError(
                    f"Record {record.key!r} is too large for a single page."
                )

            if current_lines and current_size + len(line) > max_payload_size:
                payload = b"".join(current_lines)
                pages.append(Page(page_id=len(pages), payload=payload))
                current_lines = []
                current_size = 0

            current_lines.append(line)
            current_size += len(line)

        if current_lines:
            payload = b"".join(current_lines)
            pages.append(Page(page_id=len(pages), payload=payload))

        return pages

    def _decode_page(self, page: Page) -> list[Record]:
        if not page.payload:
            return []

        try:
            raw_lines = page.payload.decode("utf-8").splitlines()
        except UnicodeDecodeError as exc:
            raise PageFormatError("Page payload is not valid UTF-8.") from exc

        records: list[Record] = []
        for line in raw_lines:
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PageFormatError("Page payload contains invalid JSON.") from exc

            records.append(Record.from_dict(payload))

        return records
