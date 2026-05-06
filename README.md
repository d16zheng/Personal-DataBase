# Personal Database Project

This repository starts with an in-memory key-value store and is intended to grow
into a personal database engine that can eventually support notes, tasks, media,
finance, learning, and health records.

Because this repository should be safe to keep public:

- commit code, tests, and sample data only
- never commit real personal records
- never commit secrets or local environment files
- treat `data/`, `backups/`, and exported database files as private

## Stage 1

The current milestone is an in-memory key-value store with:

- `put`
- `get`
- `delete`
- `list`

## Stage 2

The store now also supports an append-only log:

- every `put` is appended to a local log file
- every `delete` is appended to the same log file
- startup recovery replays the log to rebuild in-memory state
- the storage layer lives under `src/personal_db/storage/` so the API and
  persistence concerns stay separated

By default the CLI writes to `data/personal.db.log`, which is ignored by git so
real local data stays out of the public repository.

## Stage 3

The storage engine now checkpoints records into fixed-size pages:

- the main data file is a sequence of fixed-size pages
- each page holds a compact set of serialized records
- the append-only log acts like a short recovery journal rather than the only
  source of truth
- recovery loads page data first, then replays any uncheckpointed log entries

## Stage 4

The store now uses a B-tree primary index for keys:

- keyed lookups use a B-tree instead of an unordered record map
- ordered scans come directly from the tree traversal
- the index is rebuilt from page storage on startup
- this creates a clean stepping stone toward page-backed B-tree nodes later

## Stage 5

The project now also includes typed personal records backed by SQLite:

- `clothing_items` stores sweaters, shirts, pants, shorts, and jackets with
  type-specific constraints
- `books` stores titles, authors, reading status, and `date_started`
- all typed records include `id`, `created_at`, and `updated_at`
- SQLite now acts as the durable store for real personal entities while the
  earlier stages remain in the repository as foundational engine exercises

This keeps the lower-level storage experiments intact while creating a more
practical path for actual personal database use cases.

## Stage 6

The SQLite layer now has a tiny typed query API plus explicit transaction and
recovery rules:

- `ClothingQuery` supports filtering by clothing type, brand prefix, tag, and
  updated-after timestamp
- `BookQuery` supports filtering by status, title prefix, author prefix, tag,
  and updated-after timestamp
- records can be tagged and queried by tag across both books and clothing
- `PersonalDatabase.transaction()` groups multiple writes into one atomic unit
- failed transactions roll back all nested writes instead of leaving partial
  state behind
- SQLite runs in WAL mode with `synchronous=FULL`, so committed changes recover
  on restart while uncommitted changes are discarded
- `checkpoint()` merges committed WAL contents back into the main database file

## Stage 7

Typed records now support full lifecycle operations plus versioned schema
migrations:

- `update_book`, `update_clothing_item`, `delete_book`, and
  `delete_clothing_item` are available through `PersonalDatabase`
- updates preserve `created_at`, refresh `updated_at`, and still run through the
  same validation rules as inserts
- deletes remove the primary row and any related tags in the same transaction
- SQLite startup now runs versioned migrations using `PRAGMA user_version`
- older typed-record files can be upgraded in place instead of rebuilt
- migration and record lifecycle behavior are covered by persistence tests

## Running

```bash
python3 -m src.personal_db.cli
```

## Testing

```bash
python3 -m pytest
```

## Roadmap

1. In-memory key-value store
2. Append-only log for persistence
3. Page-based storage
4. B-tree index
5. Typed SQLite records for personal data
6. Tiny typed query layer
7. Typed record updates, deletes, and migrations
8. Query parser
9. Richer transaction scheduling and recovery tooling
