from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sqlite3

from src.personal_db.errors import (
    KeyNotFoundError,
    RecordNotFoundError,
    RecordValidationError,
)
from src.personal_db.index import BTreeIndex
from src.personal_db.models import (
    Book,
    BookStatus,
    ClothingItem,
    ClothingType,
    JacketSize,
    MeasurementUnit,
)
from src.personal_db.query import BookQuery, ClothingQuery, RecordKind
from src.personal_db.storage import (
    CURRENT_SCHEMA_VERSION,
    DEFAULT_PAGE_SIZE,
    LogStructuredStorage,
    PageFile,
)
from src.personal_db.store import InMemoryKeyValueStore, PersonalDatabase


def test_put_and_get_round_trip() -> None:
    store = InMemoryKeyValueStore()

    store.put("favorite_book", "The Dispossessed")

    record = store.get("favorite_book")
    assert record.value == "The Dispossessed"


def test_put_updates_existing_key() -> None:
    store = InMemoryKeyValueStore()

    store.put("current_focus", "databases")
    updated = store.put("current_focus", "storage engines")

    assert updated.value == "storage engines"
    assert store.size() == 1


def test_delete_removes_key() -> None:
    store = InMemoryKeyValueStore()
    store.put("today", "build stage one")

    deleted = store.delete("today")

    assert deleted.key == "today"
    assert store.size() == 0


def test_missing_key_raises_custom_error() -> None:
    store = InMemoryKeyValueStore()

    try:
        store.get("missing")
    except KeyNotFoundError:
        pass
    else:
        raise AssertionError("Expected KeyNotFoundError for missing key.")


def test_list_records_returns_sorted_keys() -> None:
    store = InMemoryKeyValueStore()
    store.put("zeta", "last")
    store.put("alpha", "first")

    keys = [record.key for record in store.list_records()]

    assert keys == ["alpha", "zeta"]


def test_store_reloads_records_from_log(tmp_path: Path) -> None:
    log_path = tmp_path / "personal.db.log"
    page_path = tmp_path / "personal.db.pages"

    first_store = InMemoryKeyValueStore(log_path=log_path, page_path=page_path)
    first_store.put("favorite_movie", "Arrival")
    first_store.put("current_focus", "append-only logging")

    second_store = InMemoryKeyValueStore(log_path=log_path, page_path=page_path)

    assert second_store.get("favorite_movie").value == "Arrival"
    assert second_store.get("current_focus").value == "append-only logging"


def test_store_replays_deletions_from_log(tmp_path: Path) -> None:
    log_path = tmp_path / "personal.db.log"
    page_path = tmp_path / "personal.db.pages"

    first_store = InMemoryKeyValueStore(log_path=log_path, page_path=page_path)
    first_store.put("temporary_key", "to be removed")
    first_store.delete("temporary_key")

    second_store = InMemoryKeyValueStore(log_path=log_path, page_path=page_path)

    try:
        second_store.get("temporary_key")
    except KeyNotFoundError:
        pass
    else:
        raise AssertionError("Expected deleted key to stay deleted after replay.")


def test_storage_backend_reloads_records_from_log(tmp_path: Path) -> None:
    log_path = tmp_path / "backend.db.log"
    page_path = tmp_path / "backend.db.pages"

    first_storage = LogStructuredStorage(log_path=log_path, page_path=page_path)
    first_storage.put("favorite_album", "Helplessness Blues")

    second_storage = LogStructuredStorage(log_path=log_path, page_path=page_path)

    assert second_storage.get("favorite_album").value == "Helplessness Blues"


def test_page_file_uses_fixed_size_pages(tmp_path: Path) -> None:
    page_path = tmp_path / "fixed.pages"
    page_file = PageFile(page_path, page_size=256)
    store = InMemoryKeyValueStore(page_path=page_path, page_size=256)

    store.put("alpha", "first")
    store.put("beta", "second")

    records = page_file.read_records()

    assert [record.key for record in records] == ["alpha", "beta"]
    assert page_path.stat().st_size % 256 == 0


def test_storage_backend_can_load_from_pages_without_log(tmp_path: Path) -> None:
    page_path = tmp_path / "pages-only.db.pages"

    first_storage = LogStructuredStorage(page_path=page_path, page_size=512)
    first_storage.put("favorite_game", "Disco Elysium")

    second_storage = LogStructuredStorage(page_path=page_path, page_size=512)

    assert second_storage.get("favorite_game").value == "Disco Elysium"


def test_log_is_cleared_after_page_checkpoint(tmp_path: Path) -> None:
    log_path = tmp_path / "checkpoint.db.log"
    page_path = tmp_path / "checkpoint.db.pages"

    store = InMemoryKeyValueStore(log_path=log_path, page_path=page_path)
    store.put("status", "checkpointed")

    assert log_path.read_text(encoding="utf-8") == ""
    assert page_path.stat().st_size == DEFAULT_PAGE_SIZE


def test_btree_index_returns_records_in_sorted_order() -> None:
    index = BTreeIndex(minimum_degree=2)

    for key in ["delta", "alpha", "charlie", "bravo", "echo"]:
        store = InMemoryKeyValueStore()
        record = store.put(key, f"value-{key}")
        index.put(record)

    keys = [record.key for record in index.list_records()]

    assert keys == ["alpha", "bravo", "charlie", "delta", "echo"]


def test_btree_index_handles_node_splits_and_lookup() -> None:
    index = BTreeIndex(minimum_degree=2)

    for number in range(10):
        key = f"key-{number:02d}"
        store = InMemoryKeyValueStore()
        record = store.put(key, f"value-{number}")
        index.put(record)

    assert index.get("key-07") is not None
    assert index.get("key-07").value == "value-7"
    assert index.size() == 10


def test_storage_backend_uses_btree_for_sorted_persistence(tmp_path: Path) -> None:
    page_path = tmp_path / "indexed.pages"
    storage = LogStructuredStorage(page_path=page_path, page_size=256)

    storage.put("zeta", "last")
    storage.put("alpha", "first")
    storage.put("middle", "third")

    reloaded = LogStructuredStorage(page_path=page_path, page_size=256)
    keys = [record.key for record in reloaded.list_records()]

    assert keys == ["alpha", "middle", "zeta"]


def test_personal_database_persists_typed_clothing_records(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "personal.sqlite"

    first_database = PersonalDatabase(sqlite_path=sqlite_path)
    sweater = first_database.add_clothing_item(
        clothing_type=ClothingType.SWEATER,
        brand="Dehen 1920",
        tags=["winter", "wool"],
        measurement_unit=MeasurementUnit.INCHES,
        chest=22.5,
        shoulder=18.0,
        body_length=26.0,
        hem=18.5,
    )
    first_database.close()

    second_database = PersonalDatabase(sqlite_path=sqlite_path)
    loaded = second_database.get_clothing_item(sweater.id)

    assert loaded.id == sweater.id
    assert loaded.clothing_type is ClothingType.SWEATER
    assert loaded.brand == "Dehen 1920"
    assert loaded.tags == ["winter", "wool"]
    assert loaded.measurement_unit is MeasurementUnit.INCHES
    assert loaded.created_at == sweater.created_at
    assert loaded.updated_at == sweater.updated_at
    second_database.close()


def test_personal_database_supports_jackets_and_book_queries(tmp_path: Path) -> None:
    database = PersonalDatabase(sqlite_path=tmp_path / "personal.sqlite")

    database.add_clothing_item(
        clothing_type=ClothingType.JACKET,
        brand="Barbour",
        tags=["outerwear"],
        jacket_size=JacketSize.MEDIUM,
    )
    database.add_book(
        title="A Wizard of Earthsea",
        author="Ursula K. Le Guin",
        status=BookStatus.IN_PROGRESS,
        tags=["fantasy", "owned"],
        date_started=date(2026, 5, 1),
    )
    database.add_book(
        title="The Left Hand of Darkness",
        author="Ursula K. Le Guin",
        status=BookStatus.NOT_STARTED,
    )

    jackets = database.list_clothing_items(clothing_type=ClothingType.JACKET)
    in_progress_books = database.list_books(status=BookStatus.IN_PROGRESS)

    assert len(jackets) == 1
    assert jackets[0].jacket_size is JacketSize.MEDIUM
    assert len(in_progress_books) == 1
    assert in_progress_books[0].title == "A Wizard of Earthsea"
    assert in_progress_books[0].tags == ["fantasy", "owned"]
    assert in_progress_books[0].date_started == date(2026, 5, 1)
    database.close()


def test_personal_database_rejects_invalid_typed_records(tmp_path: Path) -> None:
    database = PersonalDatabase(sqlite_path=tmp_path / "personal.sqlite")

    try:
        database.add_clothing_item(
            clothing_type=ClothingType.PANTS,
            measurement_unit=MeasurementUnit.INCHES,
            waist=32.0,
            length=30.0,
        )
    except RecordValidationError:
        pass
    else:
        raise AssertionError("Expected pants without a front rise to be rejected.")

    try:
        database.add_book(
            title="",
            status=BookStatus.COMPLETED,
            date_started=date(2026, 4, 10),
        )
    except RecordValidationError:
        pass
    else:
        raise AssertionError("Expected blank book titles to be rejected.")

    try:
        database.get_book("missing")
    except RecordNotFoundError:
        pass
    else:
        raise AssertionError("Expected missing typed records to raise RecordNotFoundError.")

    database.close()


def test_personal_database_updates_typed_records_and_refreshes_timestamps(
    tmp_path: Path,
) -> None:
    sqlite_path = tmp_path / "personal.sqlite"
    database = PersonalDatabase(sqlite_path=sqlite_path)

    book = database.add_book(
        title="Tehanu",
        author="Ursula K. Le Guin",
        status=BookStatus.IN_PROGRESS,
        tags=["owned"],
        date_started=date(2026, 4, 1),
    )
    sweater = database.add_clothing_item(
        clothing_type=ClothingType.SWEATER,
        brand="Dehen 1920",
        tags=["winter"],
        measurement_unit=MeasurementUnit.INCHES,
        chest=22.5,
        shoulder=18.0,
        body_length=26.0,
        hem=18.5,
    )

    updated_book = database.update_book(
        book.id,
        status=BookStatus.COMPLETED,
        tags=["owned", "favorite"],
    )
    updated_sweater = database.update_clothing_item(
        sweater.id,
        brand="Inverallan",
        tags=["winter", "wool"],
        chest=23.0,
    )
    database.close()

    reloaded = PersonalDatabase(sqlite_path=sqlite_path)
    persisted_book = reloaded.get_book(book.id)
    persisted_sweater = reloaded.get_clothing_item(sweater.id)

    assert updated_book.updated_at > book.updated_at
    assert persisted_book.status is BookStatus.COMPLETED
    assert persisted_book.tags == ["favorite", "owned"]
    assert persisted_book.created_at == book.created_at
    assert persisted_book.updated_at == updated_book.updated_at

    assert updated_sweater.updated_at > sweater.updated_at
    assert persisted_sweater.brand == "Inverallan"
    assert persisted_sweater.tags == ["winter", "wool"]
    assert persisted_sweater.chest == 23.0
    assert persisted_sweater.created_at == sweater.created_at
    assert persisted_sweater.updated_at == updated_sweater.updated_at
    reloaded.close()


def test_personal_database_deletes_typed_records(tmp_path: Path) -> None:
    database = PersonalDatabase(sqlite_path=tmp_path / "personal.sqlite")

    book = database.add_book(
        title="The Left Hand of Darkness",
        author="Ursula K. Le Guin",
        status=BookStatus.NOT_STARTED,
        tags=["owned"],
    )
    jacket = database.add_clothing_item(
        clothing_type=ClothingType.JACKET,
        brand="Barbour",
        tags=["outerwear"],
        jacket_size=JacketSize.MEDIUM,
    )

    deleted_book = database.delete_book(book.id)
    deleted_jacket = database.delete_clothing_item(jacket.id)

    assert deleted_book.id == book.id
    assert deleted_jacket.id == jacket.id
    assert database.list_records_by_tag("owned") == []
    assert database.list_records_by_tag("outerwear") == []

    try:
        database.get_book(book.id)
    except RecordNotFoundError:
        pass
    else:
        raise AssertionError("Expected deleted book lookup to fail.")

    try:
        database.get_clothing_item(jacket.id)
    except RecordNotFoundError:
        pass
    else:
        raise AssertionError("Expected deleted clothing lookup to fail.")

    database.close()


def test_tiny_query_layer_supports_prefix_tag_and_type_filters(tmp_path: Path) -> None:
    database = PersonalDatabase(sqlite_path=tmp_path / "personal.sqlite")

    database.add_clothing_item(
        clothing_type=ClothingType.SWEATER,
        brand="Dehen 1920",
        tags=["wool", "winter"],
        measurement_unit=MeasurementUnit.INCHES,
        chest=22.5,
        shoulder=18.0,
        body_length=26.0,
        hem=18.5,
    )
    database.add_clothing_item(
        clothing_type=ClothingType.SHIRT,
        brand="Drake's",
        tags=["cotton"],
        measurement_unit=MeasurementUnit.CENTIMETERS,
        chest=56.0,
        shoulder=46.0,
        body_length=74.0,
        hem=53.0,
        waist=54.0,
    )
    database.add_book(
        title="Dune",
        author="Frank Herbert",
        status=BookStatus.IN_PROGRESS,
        tags=["owned", "science-fiction"],
        date_started=date(2026, 4, 2),
    )
    database.add_book(
        title="Dubliners",
        author="James Joyce",
        status=BookStatus.NOT_STARTED,
        tags=["owned", "classic"],
    )

    wool_clothing = database.query_clothing(
        ClothingQuery(clothing_type=ClothingType.SWEATER, tag="wool")
    )
    books_by_prefix = database.query_books(BookQuery(title_prefix="Du"))
    tagged_records = database.list_records_by_tag("owned")

    assert len(wool_clothing) == 1
    assert wool_clothing[0].brand == "Dehen 1920"
    assert [book.title for book in books_by_prefix] == ["Dune", "Dubliners"]
    assert [record.record_kind for record in tagged_records] == [
        RecordKind.BOOK,
        RecordKind.BOOK,
    ]
    assert sorted(record.label for record in tagged_records) == ["Dubliners", "Dune"]
    database.close()


def test_tiny_query_layer_lists_records_updated_after_timestamp(tmp_path: Path) -> None:
    database = PersonalDatabase(sqlite_path=tmp_path / "personal.sqlite")
    older_timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer_timestamp = older_timestamp + timedelta(days=2)

    database._storage.add_clothing_item(
        ClothingItem(
            id="cloth-1",
            clothing_type=ClothingType.SWEATER,
            brand="Older Sweater",
            tags=["archive"],
            measurement_unit=MeasurementUnit.INCHES,
            chest=20.0,
            shoulder=17.0,
            body_length=25.0,
            hem=17.0,
            created_at=older_timestamp,
            updated_at=older_timestamp,
        )
    )
    database._storage.add_book(
        Book(
            id="book-1",
            title="Newer Book",
            author="Someone",
            status=BookStatus.IN_PROGRESS,
            tags=["active"],
            date_started=date(2026, 1, 2),
            created_at=newer_timestamp,
            updated_at=newer_timestamp,
        )
    )

    recent_records = database.list_records_updated_after(
        older_timestamp + timedelta(hours=1)
    )

    assert len(recent_records) == 1
    assert recent_records[0].record_kind is RecordKind.BOOK
    assert recent_records[0].label == "Newer Book"
    database.close()


def test_transactions_roll_back_partial_failures(tmp_path: Path) -> None:
    database = PersonalDatabase(sqlite_path=tmp_path / "personal.sqlite")

    try:
        with database.transaction():
            database.add_book(
                title="The Tombs of Atuan",
                author="Ursula K. Le Guin",
                status=BookStatus.IN_PROGRESS,
                tags=["owned"],
                date_started=date(2026, 5, 2),
            )
            database.add_clothing_item(
                clothing_type=ClothingType.PANTS,
                measurement_unit=MeasurementUnit.INCHES,
                waist=31.0,
                length=30.0,
            )
    except RecordValidationError:
        pass
    else:
        raise AssertionError("Expected invalid transaction contents to trigger rollback.")

    assert database.list_books() == []
    assert database.list_clothing_items() == []
    database.close()


def test_sqlite_migrations_upgrade_legacy_schema_in_place(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "legacy.sqlite"
    connection = sqlite3.connect(sqlite_path)
    connection.executescript(
        """
        PRAGMA user_version = 1;

        CREATE TABLE clothing_items (
            id TEXT PRIMARY KEY,
            clothing_type TEXT NOT NULL,
            brand TEXT,
            measurement_unit TEXT,
            jacket_size TEXT,
            chest REAL,
            shoulder REAL,
            body_length REAL,
            hem REAL,
            waist REAL,
            length REAL,
            front_rise REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE books (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT,
            status TEXT NOT NULL,
            date_started TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    connection.execute(
        """
        INSERT INTO books (
            id,
            title,
            author,
            status,
            date_started,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-book",
            "The Tombs of Atuan",
            "Ursula K. Le Guin",
            "in_progress",
            "2026-04-10",
            "2026-04-10T00:00:00+00:00",
            "2026-04-10T00:00:00+00:00",
        ),
    )
    connection.commit()
    connection.close()

    database = PersonalDatabase(sqlite_path=sqlite_path)
    migrated_book = database.get_book("legacy-book")

    assert database.schema_version() == CURRENT_SCHEMA_VERSION
    assert migrated_book.title == "The Tombs of Atuan"
    assert migrated_book.tags == []

    updated_book = database.update_book("legacy-book", tags=["owned", "fantasy"])
    assert sorted(updated_book.tags) == ["fantasy", "owned"]
    database.close()

    reopened = sqlite3.connect(sqlite_path)
    user_version = reopened.execute("PRAGMA user_version").fetchone()[0]
    tag_tables = {
        row[0]
        for row in reopened.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name IN ('book_tags', 'clothing_item_tags')
            """
        ).fetchall()
    }
    reopened.close()

    assert user_version == CURRENT_SCHEMA_VERSION
    assert tag_tables == {"book_tags", "clothing_item_tags"}


def test_recovery_reopens_only_committed_sqlite_state(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "personal.sqlite"
    first_database = PersonalDatabase(sqlite_path=sqlite_path)

    first_database.add_book(
        title="Tehanu",
        author="Ursula K. Le Guin",
        status=BookStatus.COMPLETED,
        tags=["owned", "fantasy"],
        date_started=date(2026, 4, 1),
    )
    try:
        with first_database.transaction():
            first_database.add_book(
                title="The Farthest Shore",
                author="Ursula K. Le Guin",
                status=BookStatus.IN_PROGRESS,
                tags=["owned"],
                date_started=date(2026, 5, 3),
            )
            first_database.add_book(
                title="",
                status=BookStatus.COMPLETED,
                date_started=date(2026, 5, 4),
            )
    except RecordValidationError:
        pass
    else:
        raise AssertionError("Expected failing transaction to roll back before restart.")

    first_database.checkpoint()
    first_database.close()

    second_database = PersonalDatabase(sqlite_path=sqlite_path)
    books = second_database.list_books()

    assert [book.title for book in books] == ["Tehanu"]
    assert books[0].tags == ["fantasy", "owned"]
    second_database.close()
