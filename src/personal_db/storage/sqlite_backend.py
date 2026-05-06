"""SQLite-backed typed storage for personal records."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from ..errors import RecordNotFoundError, RecordValidationError
from ..models import (
    Book,
    BookStatus,
    ClothingItem,
    ClothingType,
    JacketSize,
    MeasurementUnit,
    utc_now,
)
from ..query import BookQuery, ClothingQuery, RecordKind, RecordSummary
from .migrations import CURRENT_SCHEMA_VERSION, apply_migrations

_UNSET = object()


class SQLitePersonalStorage:
    """Stores typed personal records in a local SQLite database."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self._path, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._transaction_depth = 0
        self._savepoint_counter = 0
        self._configure_connection()
        self._schema_version = apply_migrations(self._connection)

    def close(self) -> None:
        if self._transaction_depth > 0:
            self._connection.execute("ROLLBACK")
            self._transaction_depth = 0
        self._connection.close()

    def schema_version(self) -> int:
        """Return the current SQLite schema version."""

        return self._schema_version

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Wrap one or more writes in an atomic transaction."""

        savepoint_name: str | None = None
        started_outer_transaction = self._transaction_depth == 0

        if started_outer_transaction:
            self._connection.execute("BEGIN IMMEDIATE")
        else:
            self._savepoint_counter += 1
            savepoint_name = f"sp_{self._savepoint_counter}"
            self._connection.execute(f"SAVEPOINT {savepoint_name}")

        self._transaction_depth += 1
        try:
            yield
        except Exception:
            self._transaction_depth -= 1
            if started_outer_transaction:
                self._connection.execute("ROLLBACK")
            else:
                assert savepoint_name is not None
                self._connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                self._connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            raise
        else:
            self._transaction_depth -= 1
            if started_outer_transaction:
                self._connection.execute("COMMIT")
            else:
                assert savepoint_name is not None
                self._connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")

    def checkpoint(self) -> None:
        """Merge committed WAL contents back into the main database file."""

        self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def add_clothing_item(self, item: ClothingItem) -> ClothingItem:
        item = self._prepared_clothing_item(item)
        try:
            with self.transaction():
                self._insert_or_replace_clothing_item(item, update_existing=False)
        except sqlite3.IntegrityError as exc:
            raise RecordValidationError(str(exc)) from exc
        return item

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
        existing = self.get_clothing_item(item_id)
        updated = ClothingItem(
            id=existing.id,
            clothing_type=existing.clothing_type
            if clothing_type is _UNSET
            else clothing_type,
            brand=existing.brand if brand is _UNSET else brand,
            tags=list(existing.tags) if tags is _UNSET else list(tags),
            measurement_unit=existing.measurement_unit
            if measurement_unit is _UNSET
            else measurement_unit,
            jacket_size=existing.jacket_size
            if jacket_size is _UNSET
            else jacket_size,
            chest=existing.chest if chest is _UNSET else chest,
            shoulder=existing.shoulder if shoulder is _UNSET else shoulder,
            body_length=existing.body_length if body_length is _UNSET else body_length,
            hem=existing.hem if hem is _UNSET else hem,
            waist=existing.waist if waist is _UNSET else waist,
            length=existing.length if length is _UNSET else length,
            front_rise=existing.front_rise if front_rise is _UNSET else front_rise,
            created_at=existing.created_at,
            updated_at=utc_now(),
        )
        updated = self._prepared_clothing_item(updated)

        try:
            with self.transaction():
                self._insert_or_replace_clothing_item(updated, update_existing=True)
        except sqlite3.IntegrityError as exc:
            raise RecordValidationError(str(exc)) from exc
        return updated

    def delete_clothing_item(self, item_id: str) -> ClothingItem:
        deleted = self.get_clothing_item(item_id)
        with self.transaction():
            cursor = self._connection.execute(
                "DELETE FROM clothing_items WHERE id = ?",
                (item_id,),
            )
            if cursor.rowcount == 0:
                raise RecordNotFoundError(f"Clothing item {item_id!r} does not exist.")
        return deleted

    def get_clothing_item(self, item_id: str) -> ClothingItem:
        row = self._connection.execute(
            "SELECT * FROM clothing_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        if row is None:
            raise RecordNotFoundError(f"Clothing item {item_id!r} does not exist.")
        return self._row_to_clothing_item(row)

    def list_clothing_items(
        self,
        clothing_type: ClothingType | None = None,
    ) -> list[ClothingItem]:
        return self.query_clothing(ClothingQuery(clothing_type=clothing_type))

    def query_clothing(self, query: ClothingQuery) -> list[ClothingItem]:
        sql = [
            "SELECT DISTINCT clothing_items.*",
            "FROM clothing_items",
        ]
        params: list[object] = []
        conditions: list[str] = []

        if query.tag is not None:
            sql.append(
                "JOIN clothing_item_tags ON clothing_item_tags.record_id = clothing_items.id"
            )
            conditions.append("clothing_item_tags.tag = ?")
            params.append(self._normalize_tag(query.tag))

        if query.clothing_type is not None:
            conditions.append("clothing_items.clothing_type = ?")
            params.append(query.clothing_type.value)

        if query.brand_prefix is not None:
            conditions.append("LOWER(COALESCE(clothing_items.brand, '')) LIKE ?")
            params.append(f"{query.brand_prefix.strip().lower()}%")

        if query.updated_after is not None:
            conditions.append("clothing_items.updated_at > ?")
            params.append(query.updated_after.isoformat())

        if conditions:
            sql.append("WHERE " + " AND ".join(conditions))

        sql.append("ORDER BY clothing_items.created_at, clothing_items.id")
        rows = self._connection.execute("\n".join(sql), params).fetchall()
        return [self._row_to_clothing_item(row) for row in rows]

    def add_book(self, book: Book) -> Book:
        book = self._prepared_book(book)
        try:
            with self.transaction():
                self._insert_or_replace_book(book, update_existing=False)
        except sqlite3.IntegrityError as exc:
            raise RecordValidationError(str(exc)) from exc
        return book

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
        existing = self.get_book(book_id)
        updated = Book(
            id=existing.id,
            title=existing.title if title is _UNSET else title,
            status=existing.status if status is _UNSET else status,
            author=existing.author if author is _UNSET else author,
            tags=list(existing.tags) if tags is _UNSET else list(tags),
            date_started=existing.date_started
            if date_started is _UNSET
            else date_started,
            created_at=existing.created_at,
            updated_at=utc_now(),
        )
        updated = self._prepared_book(updated)

        try:
            with self.transaction():
                self._insert_or_replace_book(updated, update_existing=True)
        except sqlite3.IntegrityError as exc:
            raise RecordValidationError(str(exc)) from exc
        return updated

    def delete_book(self, book_id: str) -> Book:
        deleted = self.get_book(book_id)
        with self.transaction():
            cursor = self._connection.execute(
                "DELETE FROM books WHERE id = ?",
                (book_id,),
            )
            if cursor.rowcount == 0:
                raise RecordNotFoundError(f"Book {book_id!r} does not exist.")
        return deleted

    def get_book(self, book_id: str) -> Book:
        row = self._connection.execute(
            "SELECT * FROM books WHERE id = ?",
            (book_id,),
        ).fetchone()
        if row is None:
            raise RecordNotFoundError(f"Book {book_id!r} does not exist.")
        return self._row_to_book(row)

    def list_books(self, status: BookStatus | None = None) -> list[Book]:
        return self.query_books(BookQuery(status=status))

    def query_books(self, query: BookQuery) -> list[Book]:
        sql = [
            "SELECT DISTINCT books.*",
            "FROM books",
        ]
        params: list[object] = []
        conditions: list[str] = []

        if query.tag is not None:
            sql.append("JOIN book_tags ON book_tags.record_id = books.id")
            conditions.append("book_tags.tag = ?")
            params.append(self._normalize_tag(query.tag))

        if query.status is not None:
            conditions.append("books.status = ?")
            params.append(query.status.value)

        if query.title_prefix is not None:
            conditions.append("LOWER(books.title) LIKE ?")
            params.append(f"{query.title_prefix.strip().lower()}%")

        if query.author_prefix is not None:
            conditions.append("LOWER(COALESCE(books.author, '')) LIKE ?")
            params.append(f"{query.author_prefix.strip().lower()}%")

        if query.updated_after is not None:
            conditions.append("books.updated_at > ?")
            params.append(query.updated_after.isoformat())

        if conditions:
            sql.append("WHERE " + " AND ".join(conditions))

        sql.append("ORDER BY books.created_at, books.id")
        rows = self._connection.execute("\n".join(sql), params).fetchall()
        return [self._row_to_book(row) for row in rows]

    def list_records_by_tag(self, tag: str) -> list[RecordSummary]:
        normalized_tag = self._normalize_tag(tag)
        clothing = [
            self._clothing_to_summary(item)
            for item in self.query_clothing(ClothingQuery(tag=normalized_tag))
        ]
        books = [
            self._book_to_summary(book)
            for book in self.query_books(BookQuery(tag=normalized_tag))
        ]
        return sorted(
            clothing + books,
            key=lambda item: (item.updated_at, item.record_kind.value, item.record_id),
        )

    def list_records_updated_after(self, updated_after: datetime) -> list[RecordSummary]:
        clothing = [
            self._clothing_to_summary(item)
            for item in self.query_clothing(ClothingQuery(updated_after=updated_after))
        ]
        books = [
            self._book_to_summary(book)
            for book in self.query_books(BookQuery(updated_after=updated_after))
        ]
        return sorted(
            clothing + books,
            key=lambda item: (item.updated_at, item.record_kind.value, item.record_id),
        )

    def _configure_connection(self) -> None:
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA foreign_keys = ON")

    def _insert_or_replace_clothing_item(
        self,
        item: ClothingItem,
        *,
        update_existing: bool,
    ) -> None:
        statement = """
            INSERT INTO clothing_items (
                id,
                clothing_type,
                brand,
                measurement_unit,
                jacket_size,
                chest,
                shoulder,
                body_length,
                hem,
                waist,
                length,
                front_rise,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        if update_existing:
            statement = """
                UPDATE clothing_items
                SET clothing_type = ?,
                    brand = ?,
                    measurement_unit = ?,
                    jacket_size = ?,
                    chest = ?,
                    shoulder = ?,
                    body_length = ?,
                    hem = ?,
                    waist = ?,
                    length = ?,
                    front_rise = ?,
                    created_at = ?,
                    updated_at = ?
                WHERE id = ?
            """
            cursor = self._connection.execute(
                statement,
                (
                    item.clothing_type.value,
                    item.brand,
                    item.measurement_unit.value
                    if item.measurement_unit is not None
                    else None,
                    item.jacket_size.value if item.jacket_size is not None else None,
                    item.chest,
                    item.shoulder,
                    item.body_length,
                    item.hem,
                    item.waist,
                    item.length,
                    item.front_rise,
                    item.created_at.isoformat(),
                    item.updated_at.isoformat(),
                    item.id,
                ),
            )
            if cursor.rowcount == 0:
                raise RecordNotFoundError(f"Clothing item {item.id!r} does not exist.")
        else:
            self._connection.execute(
                statement,
                (
                    item.id,
                    item.clothing_type.value,
                    item.brand,
                    item.measurement_unit.value
                    if item.measurement_unit is not None
                    else None,
                    item.jacket_size.value if item.jacket_size is not None else None,
                    item.chest,
                    item.shoulder,
                    item.body_length,
                    item.hem,
                    item.waist,
                    item.length,
                    item.front_rise,
                    item.created_at.isoformat(),
                    item.updated_at.isoformat(),
                ),
            )
        self._replace_tags("clothing_item_tags", item.id, item.tags)

    def _insert_or_replace_book(self, book: Book, *, update_existing: bool) -> None:
        statement = """
            INSERT INTO books (
                id,
                title,
                author,
                status,
                date_started,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        if update_existing:
            statement = """
                UPDATE books
                SET title = ?,
                    author = ?,
                    status = ?,
                    date_started = ?,
                    created_at = ?,
                    updated_at = ?
                WHERE id = ?
            """
            cursor = self._connection.execute(
                statement,
                (
                    book.title,
                    book.author,
                    book.status.value,
                    book.date_started.isoformat()
                    if book.date_started is not None
                    else None,
                    book.created_at.isoformat(),
                    book.updated_at.isoformat(),
                    book.id,
                ),
            )
            if cursor.rowcount == 0:
                raise RecordNotFoundError(f"Book {book.id!r} does not exist.")
        else:
            self._connection.execute(
                statement,
                (
                    book.id,
                    book.title,
                    book.author,
                    book.status.value,
                    book.date_started.isoformat()
                    if book.date_started is not None
                    else None,
                    book.created_at.isoformat(),
                    book.updated_at.isoformat(),
                ),
            )
        self._replace_tags("book_tags", book.id, book.tags)

    def _replace_tags(self, table_name: str, record_id: str, tags: list[str]) -> None:
        self._connection.execute(
            f"DELETE FROM {table_name} WHERE record_id = ?",
            (record_id,),
        )
        for tag in tags:
            self._connection.execute(
                f"INSERT INTO {table_name} (record_id, tag) VALUES (?, ?)",
                (record_id, tag),
            )

    def _prepared_clothing_item(self, item: ClothingItem) -> ClothingItem:
        item.tags = self._normalize_tags(item.tags)
        self._validate_clothing_item(item)
        return item

    def _prepared_book(self, book: Book) -> Book:
        book.tags = self._normalize_tags(book.tags)
        self._validate_book(book)
        return book

    def _normalize_tags(self, tags: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for tag in tags:
            normalized_tag = self._normalize_tag(tag)
            if normalized_tag not in seen:
                normalized.append(normalized_tag)
                seen.add(normalized_tag)
        return normalized

    def _normalize_tag(self, tag: str) -> str:
        normalized = tag.strip().lower()
        if not normalized:
            raise RecordValidationError("Tags must not be empty.")
        return normalized

    def _get_tags(self, table_name: str, record_id: str) -> list[str]:
        rows = self._connection.execute(
            f"""
            SELECT tag FROM {table_name}
            WHERE record_id = ?
            ORDER BY tag
            """,
            (record_id,),
        ).fetchall()
        return [row["tag"] for row in rows]

    def _validate_clothing_item(self, item: ClothingItem) -> None:
        required_by_type = {
            ClothingType.SWEATER: {"chest", "shoulder", "body_length", "hem"},
            ClothingType.SHIRT: {"chest", "shoulder", "body_length", "hem", "waist"},
            ClothingType.PANTS: {"waist", "length", "front_rise"},
            ClothingType.SHORTS: {"waist", "length"},
            ClothingType.JACKET: {"jacket_size"},
        }
        allowed_by_type = {
            ClothingType.SWEATER: {
                "measurement_unit",
                "brand",
                "chest",
                "shoulder",
                "body_length",
                "hem",
            },
            ClothingType.SHIRT: {
                "measurement_unit",
                "brand",
                "chest",
                "shoulder",
                "body_length",
                "hem",
                "waist",
            },
            ClothingType.PANTS: {
                "measurement_unit",
                "brand",
                "waist",
                "length",
                "front_rise",
            },
            ClothingType.SHORTS: {"measurement_unit", "brand", "waist", "length"},
            ClothingType.JACKET: {"brand", "jacket_size"},
        }

        values = {
            "measurement_unit": item.measurement_unit,
            "jacket_size": item.jacket_size,
            "chest": item.chest,
            "shoulder": item.shoulder,
            "body_length": item.body_length,
            "hem": item.hem,
            "waist": item.waist,
            "length": item.length,
            "front_rise": item.front_rise,
        }

        missing = [
            name
            for name in sorted(required_by_type[item.clothing_type])
            if values[name] is None
        ]
        if missing:
            raise RecordValidationError(
                f"{item.clothing_type.value} is missing required fields: "
                f"{', '.join(missing)}."
            )

        disallowed = [
            name
            for name, value in values.items()
            if value is not None and name not in allowed_by_type[item.clothing_type]
        ]
        if disallowed:
            raise RecordValidationError(
                f"{item.clothing_type.value} does not allow fields: "
                f"{', '.join(sorted(disallowed))}."
            )

    def _validate_book(self, book: Book) -> None:
        if not book.title.strip():
            raise RecordValidationError("Book title must not be empty.")
        if book.status is BookStatus.NOT_STARTED and book.date_started is not None:
            raise RecordValidationError(
                "date_started must be empty when status is not_started."
            )
        if book.status is not BookStatus.NOT_STARTED and book.date_started is None:
            raise RecordValidationError(
                "date_started is required when a book is in progress or completed."
            )

    def _row_to_clothing_item(self, row: sqlite3.Row) -> ClothingItem:
        return ClothingItem(
            id=row["id"],
            clothing_type=ClothingType(row["clothing_type"]),
            brand=row["brand"],
            tags=self._get_tags("clothing_item_tags", row["id"]),
            measurement_unit=MeasurementUnit(row["measurement_unit"])
            if row["measurement_unit"] is not None
            else None,
            jacket_size=JacketSize(row["jacket_size"])
            if row["jacket_size"] is not None
            else None,
            chest=row["chest"],
            shoulder=row["shoulder"],
            body_length=row["body_length"],
            hem=row["hem"],
            waist=row["waist"],
            length=row["length"],
            front_rise=row["front_rise"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _row_to_book(self, row: sqlite3.Row) -> Book:
        return Book(
            id=row["id"],
            title=row["title"],
            author=row["author"],
            tags=self._get_tags("book_tags", row["id"]),
            status=BookStatus(row["status"]),
            date_started=date.fromisoformat(row["date_started"])
            if row["date_started"] is not None
            else None,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _clothing_to_summary(self, item: ClothingItem) -> RecordSummary:
        label = item.brand if item.brand is not None else item.clothing_type.value
        return RecordSummary(
            record_kind=RecordKind.CLOTHING_ITEM,
            record_id=item.id,
            label=label,
            updated_at=item.updated_at,
            tags=item.tags,
        )

    def _book_to_summary(self, book: Book) -> RecordSummary:
        return RecordSummary(
            record_kind=RecordKind.BOOK,
            record_id=book.id,
            label=book.title,
            updated_at=book.updated_at,
            tags=book.tags,
        )


__all__ = ["CURRENT_SCHEMA_VERSION", "SQLitePersonalStorage"]
