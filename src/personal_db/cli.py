"""Tiny command-line interface for interacting with the in-memory store."""

from __future__ import annotations

from pathlib import Path

from .errors import KeyNotFoundError
from .store import InMemoryKeyValueStore


HELP_TEXT = """Commands:
  put <key> <value>
  get <key>
  delete <key>
  list
  size
  help
  exit
"""


def main() -> None:
    store = InMemoryKeyValueStore(
        log_path=Path("data/personal.db.log"),
        page_path=Path("data/personal.db.pages"),
    )
    print("Personal DB: in-memory key-value store with paged storage")
    print(HELP_TEXT)

    while True:
        raw = input("db> ").strip()
        if not raw:
            continue

        parts = raw.split(maxsplit=2)
        command = parts[0].lower()

        if command == "exit":
            print("Bye.")
            return

        if command == "help":
            print(HELP_TEXT)
            continue

        try:
            if command == "put" and len(parts) == 3:
                record = store.put(parts[1], parts[2])
                print(f"stored {record.key}={record.value}")
            elif command == "get" and len(parts) == 2:
                record = store.get(parts[1])
                print(record.value)
            elif command == "delete" and len(parts) == 2:
                record = store.delete(parts[1])
                print(f"deleted {record.key}")
            elif command == "list" and len(parts) == 1:
                for record in store.list_records():
                    print(f"{record.key}={record.value}")
            elif command == "size" and len(parts) == 1:
                print(store.size())
            else:
                print("Invalid command. Type 'help' for usage.")
        except KeyNotFoundError as exc:
            print(exc)


if __name__ == "__main__":
    main()
