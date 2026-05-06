# Stage 7 Architecture

## Goal

Build the smallest useful database core, then start layering real personal
records, queries, lifecycle operations, and durable write rules on top of it.
The repository now contains both the low-level storage experiments and a typed
SQLite-backed path for practical data.

## Why This Structure

- `store.py` is the public database API layer
- `index/btree.py` provides the primary key index for lookups and ordered scans
- `models.py` defines both low-level records and typed personal entities
- `query.py` defines the tiny typed query layer for prefix, tag, and timestamp filters
- `errors.py` gives the project domain-specific failure handling
- `storage/backend.py` owns record state, replay, and durable writes
- `storage/log.py` adds durable mutation logging and startup recovery
- `storage/migrations.py` owns versioned SQLite schema upgrades
- `storage/page.py` introduces fixed-size page files for primary storage
- `storage/sqlite_backend.py` persists typed clothing and book records in SQLite, including updates, deletes, and atomic transaction behavior
- `cli.py` provides a way to exercise the API manually
- `tests/` locks in the behavior before persistence is added

## Future Growth

This layout is meant to grow without a big rewrite:

- persistence begins in `storage/` with an append-only log
- `store.py` can keep a stable interface while storage backends evolve
- page storage creates the boundary needed for future free lists, buffer caches,
  and B-tree nodes
- the current B-tree is rebuilt from persisted records, which keeps indexing and
  storage concerns separate before moving to page-backed index nodes
- SQLite gives the project a practical home for typed entities before a future
  custom engine takes over more of that responsibility
- WAL mode plus explicit transaction scopes gives the project a concrete
  recovery story while the custom engine is still evolving
- versioned migrations let the SQLite file evolve without forcing rebuilds when
  new tables, indexes, or constraints are added
- indexing can be added in `index/` later
- parsing and query planning can be added in `query/` later
- domain tables can evolve on top of this core interface

## Transaction And Recovery Rules

- single-record writes are atomic because each insert runs in its own SQLite transaction
- multi-step writes can be wrapped in `PersonalDatabase.transaction()` so they commit or roll back together
- nested writes use savepoints, which keeps inner helpers composable without breaking outer atomicity
- write failures leave the database in its last committed state
- SQLite WAL recovery restores committed changes after restart and drops uncommitted work
- `checkpoint()` can be used to fold committed WAL pages back into the main database file

## Migration Rules

- the SQLite schema version is tracked with `PRAGMA user_version`
- startup upgrades run in order until the database reaches the current schema version
- each migration runs inside a transaction so partially applied schema changes roll back cleanly
- older recognized schemas are upgraded in place, including legacy typed-record databases that predate schema version stamping
- unsupported or inconsistent partial schemas raise an explicit migration error instead of guessing
