from pathlib import Path

from src.personal_db.errors import KeyNotFoundError
from src.personal_db.storage import DEFAULT_PAGE_SIZE, LogStructuredStorage, PageFile
from src.personal_db.store import InMemoryKeyValueStore


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
