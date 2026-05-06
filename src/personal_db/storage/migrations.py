"""Versioned SQLite schema migrations for typed personal records."""

from __future__ import annotations

import sqlite3
from typing import Callable

from ..errors import MigrationError

CURRENT_SCHEMA_VERSION = 2


def apply_migrations(connection: sqlite3.Connection) -> int:
    """Bring the SQLite database up to the current schema version."""

    current_version = _current_schema_version(connection)
    if current_version > CURRENT_SCHEMA_VERSION:
        raise MigrationError(
            f"Database schema version {current_version} is newer than supported "
            f"version {CURRENT_SCHEMA_VERSION}."
        )

    if current_version == CURRENT_SCHEMA_VERSION:
        _set_user_version(connection, current_version)
        return current_version

    for version in range(current_version + 1, CURRENT_SCHEMA_VERSION + 1):
        MIGRATIONS[version](connection)

    return CURRENT_SCHEMA_VERSION


def _current_schema_version(connection: sqlite3.Connection) -> int:
    stored_version = connection.execute("PRAGMA user_version").fetchone()[0]
    if stored_version:
        return int(stored_version)

    tables = _table_names(connection)
    has_base_tables = {"books", "clothing_items"}.issubset(tables)
    has_tag_tables = {"book_tags", "clothing_item_tags"}.issubset(tables)

    if not has_base_tables and not has_tag_tables:
        return 0
    if has_tag_tables and not has_base_tables:
        raise MigrationError("Found tag tables without the base entity tables.")
    if "books" in tables or "clothing_items" in tables:
        if not has_base_tables:
            raise MigrationError("Found only part of the base typed-record schema.")
        if has_tag_tables:
            return 2
        return 1

    return 0


def _table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    return {row[0] for row in rows}


def _set_user_version(connection: sqlite3.Connection, version: int) -> None:
    connection.execute(f"PRAGMA user_version = {version}")


def _migration_1_create_typed_entities(connection: sqlite3.Connection) -> None:
    _run_migration_script(
        connection,
        f"""
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS clothing_items (
            id TEXT PRIMARY KEY,
            clothing_type TEXT NOT NULL CHECK (
                clothing_type IN ('sweater', 'shirt', 'pants', 'shorts', 'jacket')
            ),
            brand TEXT,
            measurement_unit TEXT CHECK (
                measurement_unit IN ('in', 'cm')
            ),
            jacket_size TEXT CHECK (
                jacket_size IN ('small', 'medium', 'large')
            ),
            chest REAL CHECK (chest IS NULL OR chest > 0),
            shoulder REAL CHECK (shoulder IS NULL OR shoulder > 0),
            body_length REAL CHECK (body_length IS NULL OR body_length > 0),
            hem REAL CHECK (hem IS NULL OR hem > 0),
            waist REAL CHECK (waist IS NULL OR waist > 0),
            length REAL CHECK (length IS NULL OR length > 0),
            front_rise REAL CHECK (front_rise IS NULL OR front_rise > 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK (
                clothing_type != 'sweater' OR (
                    measurement_unit IS NOT NULL
                    AND jacket_size IS NULL
                    AND chest IS NOT NULL
                    AND shoulder IS NOT NULL
                    AND body_length IS NOT NULL
                    AND hem IS NOT NULL
                    AND waist IS NULL
                    AND length IS NULL
                    AND front_rise IS NULL
                )
            ),
            CHECK (
                clothing_type != 'shirt' OR (
                    measurement_unit IS NOT NULL
                    AND jacket_size IS NULL
                    AND chest IS NOT NULL
                    AND shoulder IS NOT NULL
                    AND body_length IS NOT NULL
                    AND hem IS NOT NULL
                    AND waist IS NOT NULL
                    AND length IS NULL
                    AND front_rise IS NULL
                )
            ),
            CHECK (
                clothing_type != 'pants' OR (
                    measurement_unit IS NOT NULL
                    AND jacket_size IS NULL
                    AND chest IS NULL
                    AND shoulder IS NULL
                    AND body_length IS NULL
                    AND hem IS NULL
                    AND waist IS NOT NULL
                    AND length IS NOT NULL
                    AND front_rise IS NOT NULL
                )
            ),
            CHECK (
                clothing_type != 'shorts' OR (
                    measurement_unit IS NOT NULL
                    AND jacket_size IS NULL
                    AND chest IS NULL
                    AND shoulder IS NULL
                    AND body_length IS NULL
                    AND hem IS NULL
                    AND waist IS NOT NULL
                    AND length IS NOT NULL
                    AND front_rise IS NULL
                )
            ),
            CHECK (
                clothing_type != 'jacket' OR (
                    measurement_unit IS NULL
                    AND jacket_size IS NOT NULL
                    AND chest IS NULL
                    AND shoulder IS NULL
                    AND body_length IS NULL
                    AND hem IS NULL
                    AND waist IS NULL
                    AND length IS NULL
                    AND front_rise IS NULL
                )
            )
        );

        CREATE INDEX IF NOT EXISTS idx_clothing_items_type
        ON clothing_items (clothing_type);

        CREATE TABLE IF NOT EXISTS books (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT,
            status TEXT NOT NULL CHECK (
                status IN ('not_started', 'in_progress', 'completed')
            ),
            date_started TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK (status != 'not_started' OR date_started IS NULL),
            CHECK (status = 'not_started' OR date_started IS NOT NULL)
        );

        CREATE INDEX IF NOT EXISTS idx_books_status
        ON books (status);
        PRAGMA user_version = 1;
        COMMIT;
        """,
    )


def _migration_2_add_tags_and_query_indexes(connection: sqlite3.Connection) -> None:
    _run_migration_script(
        connection,
        f"""
        BEGIN IMMEDIATE;
        CREATE INDEX IF NOT EXISTS idx_clothing_items_brand
        ON clothing_items (brand);

        CREATE INDEX IF NOT EXISTS idx_clothing_items_updated_at
        ON clothing_items (updated_at);

        CREATE TABLE IF NOT EXISTS clothing_item_tags (
            record_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            PRIMARY KEY (record_id, tag),
            FOREIGN KEY (record_id) REFERENCES clothing_items(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_clothing_item_tags_tag
        ON clothing_item_tags (tag);

        CREATE INDEX IF NOT EXISTS idx_books_title
        ON books (title);

        CREATE INDEX IF NOT EXISTS idx_books_author
        ON books (author);

        CREATE INDEX IF NOT EXISTS idx_books_updated_at
        ON books (updated_at);

        CREATE TABLE IF NOT EXISTS book_tags (
            record_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            PRIMARY KEY (record_id, tag),
            FOREIGN KEY (record_id) REFERENCES books(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_book_tags_tag
        ON book_tags (tag);
        PRAGMA user_version = 2;
        COMMIT;
        """,
    )


def _run_migration_script(connection: sqlite3.Connection, script: str) -> None:
    try:
        connection.executescript(script)
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migration_1_create_typed_entities,
    2: _migration_2_add_tags_and_query_indexes,
}
