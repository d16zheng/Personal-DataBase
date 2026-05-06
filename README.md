# Personal Database Project

A small Python database project with two active tracks:

- a practical `SQLite`-backed personal database for typed records
- a lower-level custom storage-engine track built around a key-value store,
  append-only log, fixed-size pages, and an in-memory B-tree

The SQLite path is the main usable system today. The custom engine remains in
the repository as a foundation for learning and future experimentation.

## Current Capabilities

### Typed personal records

The `PersonalDatabase` API currently supports:

- clothing items with type-specific validation for sweaters, shirts, pants,
  shorts, and jackets
- books with title, author, reading status, and optional `date_started`
- create, read, update, and delete operations
- tags on books and clothing items
- typed queries by status, type, prefix, tag, and updated timestamp
- cross-record summaries by tag and updated timestamp

### Durability and recovery

The SQLite-backed layer currently includes:

- explicit transactions through `PersonalDatabase.transaction()`
- nested savepoints for composable writes
- WAL mode with `synchronous=FULL`
- explicit `checkpoint()` support
- versioned schema migrations via `PRAGMA user_version`

### Custom storage-engine track

The repository also contains a separate key-value engine that currently
supports:

- `put`, `get`, `delete`, `list`, and `size`
- append-only log replay on startup
- fixed-size page persistence
- an in-memory B-tree primary index for ordered scans and lookups

## Repository Layout

- `src/personal_db/store.py`: public API for both database tracks
- `src/personal_db/storage/sqlite_backend.py`: SQLite-backed typed records
- `src/personal_db/storage/migrations.py`: schema versioning and upgrades
- `src/personal_db/query.py`: typed query objects
- `src/personal_db/models.py`: shared record models
- `src/personal_db/storage/`: log, page, and storage-engine primitives
- `src/personal_db/index/`: B-tree index
- `src/personal_db/cli.py`: tiny CLI for the key-value engine
- `tests/`: persistence, query, transaction, and migration tests

## Running

Run the key-value CLI:

```bash
python3 -m src.personal_db.cli
```

Run the test suite:

```bash
python3 -m pytest
```

## Notes

- commit code, tests, and sample data only
- do not commit real personal records or secrets
- treat local database files under `data/` as private

## Next Areas

- expand typed personal record coverage
- add richer querying and parsing
- continue evolving the custom engine toward more realistic storage internals
