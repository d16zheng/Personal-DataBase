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

This stage teaches the logical API of a database before persistence, indexing,
or query planning are introduced.

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
5. Query parser
6. Transactions and recovery
