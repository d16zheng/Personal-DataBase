"""Logical database interface built on top of a storage backend."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from .models import (
    Book,
    BookStatus,
    ClothingItem,
    ClothingType,
    JacketSize,
    MeasurementUnit,
    Record,
)
from .query import BookQuery, ClothingQuery, RecordSummary
from .storage import LogStructuredStorage, SQLitePersonalStorage

_UNSET = object()


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


class PersonalDatabase:
    """SQLite-backed personal database for typed records."""

    def __init__(self, sqlite_path: str | Path | None = None) -> None:
        path = Path(sqlite_path) if sqlite_path is not None else Path("data/personal.db.sqlite")
        self._storage = SQLitePersonalStorage(path)

    def close(self) -> None:
        self._storage.close()

    @contextmanager
    def transaction(self) -> Iterator["PersonalDatabase"]:
        with self._storage.transaction():
            yield self

    def checkpoint(self) -> None:
        self._storage.checkpoint()

    def schema_version(self) -> int:
        return self._storage.schema_version()

    def add_clothing_item(
        self,
        *,
        clothing_type: ClothingType,
        brand: str | None = None,
        tags: list[str] | None = None,
        measurement_unit: MeasurementUnit | None = None,
        jacket_size: JacketSize | None = None,
        chest: float | None = None,
        shoulder: float | None = None,
        body_length: float | None = None,
        hem: float | None = None,
        waist: float | None = None,
        length: float | None = None,
        front_rise: float | None = None,
    ) -> ClothingItem:
        return self._storage.add_clothing_item(
            ClothingItem(
                clothing_type=clothing_type,
                brand=brand,
                tags=list(tags or []),
                measurement_unit=measurement_unit,
                jacket_size=jacket_size,
                chest=chest,
                shoulder=shoulder,
                body_length=body_length,
                hem=hem,
                waist=waist,
                length=length,
                front_rise=front_rise,
            )
        )

    def get_clothing_item(self, item_id: str) -> ClothingItem:
        return self._storage.get_clothing_item(item_id)

    def update_clothing_item(
        self,
        item_id: str,
        *,
        clothing_type: ClothingType | object = _UNSET,
        brand: str | None | object = _UNSET,
        tags: list[str] | object = _UNSET,
        measurement_unit: MeasurementUnit | None | object = _UNSET,
        jacket_size: JacketSize | None | object = _UNSET,
        chest: float | None | object = _UNSET,
        shoulder: float | None | object = _UNSET,
        body_length: float | None | object = _UNSET,
        hem: float | None | object = _UNSET,
        waist: float | None | object = _UNSET,
        length: float | None | object = _UNSET,
        front_rise: float | None | object = _UNSET,
    ) -> ClothingItem:
        updates = {
            "clothing_type": clothing_type,
            "brand": brand,
            "tags": tags,
            "measurement_unit": measurement_unit,
            "jacket_size": jacket_size,
            "chest": chest,
            "shoulder": shoulder,
            "body_length": body_length,
            "hem": hem,
            "waist": waist,
            "length": length,
            "front_rise": front_rise,
        }
        return self._storage.update_clothing_item(
            item_id,
            **{key: value for key, value in updates.items() if value is not _UNSET},
        )

    def delete_clothing_item(self, item_id: str) -> ClothingItem:
        return self._storage.delete_clothing_item(item_id)

    def list_clothing_items(
        self,
        clothing_type: ClothingType | None = None,
    ) -> list[ClothingItem]:
        return self._storage.list_clothing_items(clothing_type=clothing_type)

    def query_clothing(self, query: ClothingQuery) -> list[ClothingItem]:
        return self._storage.query_clothing(query)

    def add_book(
        self,
        *,
        title: str,
        status: BookStatus,
        author: str | None = None,
        tags: list[str] | None = None,
        date_started: date | None = None,
    ) -> Book:
        return self._storage.add_book(
            Book(
                title=title,
                status=status,
                author=author,
                tags=list(tags or []),
                date_started=date_started,
            )
        )

    def get_book(self, book_id: str) -> Book:
        return self._storage.get_book(book_id)

    def update_book(
        self,
        book_id: str,
        *,
        title: str | object = _UNSET,
        status: BookStatus | object = _UNSET,
        author: str | None | object = _UNSET,
        tags: list[str] | object = _UNSET,
        date_started: date | None | object = _UNSET,
    ) -> Book:
        updates = {
            "title": title,
            "status": status,
            "author": author,
            "tags": tags,
            "date_started": date_started,
        }
        return self._storage.update_book(
            book_id,
            **{key: value for key, value in updates.items() if value is not _UNSET},
        )

    def delete_book(self, book_id: str) -> Book:
        return self._storage.delete_book(book_id)

    def list_books(self, status: BookStatus | None = None) -> list[Book]:
        return self._storage.list_books(status=status)

    def query_books(self, query: BookQuery) -> list[Book]:
        return self._storage.query_books(query)

    def list_records_by_tag(self, tag: str) -> list[RecordSummary]:
        return self._storage.list_records_by_tag(tag)

    def list_records_updated_after(
        self,
        updated_after: datetime,
    ) -> list[RecordSummary]:
        return self._storage.list_records_updated_after(updated_after)
