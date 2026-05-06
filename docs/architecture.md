# Stage 3 Architecture

## Goal

Build the smallest useful database core: an in-memory key-value store with a
stable interface that later stages can preserve while the storage engine grows
more sophisticated.

## Why This Structure

- `store.py` is the public database API layer
- `models.py` defines reusable record metadata
- `errors.py` gives the project domain-specific failure handling
- `storage/backend.py` owns record state, replay, and durable writes
- `storage/log.py` adds durable mutation logging and startup recovery
- `storage/page.py` introduces fixed-size page files for primary storage
- `cli.py` provides a way to exercise the API manually
- `tests/` locks in the behavior before persistence is added

## Future Growth

This layout is meant to grow without a big rewrite:

- persistence begins in `storage/` with an append-only log
- `store.py` can keep a stable interface while storage backends evolve
- page storage creates the boundary needed for future free lists, buffer caches,
  and B-tree nodes
- indexing can be added in `index/` later
- parsing and query planning can be added in `query/` later
- domain tables can evolve on top of this core interface
