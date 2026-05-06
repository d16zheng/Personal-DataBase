"""B-tree primary index for key-based lookups and ordered scans."""

from __future__ import annotations

from dataclasses import dataclass, field

from ..models import Record


@dataclass
class BTreeNode:
    """A single B-tree node."""

    leaf: bool = True
    keys: list[str] = field(default_factory=list)
    values: list[Record] = field(default_factory=list)
    children: list["BTreeNode"] = field(default_factory=list)


class BTreeIndex:
    """A small B-tree keyed by record key."""

    def __init__(self, minimum_degree: int = 2) -> None:
        if minimum_degree < 2:
            raise ValueError("minimum_degree must be at least 2.")

        self.minimum_degree = minimum_degree
        self.root = BTreeNode()
        self._size = 0

    def get(self, key: str) -> Record | None:
        return self._search(self.root, key)

    def put(self, record: Record) -> Record:
        existing = self.get(record.key)
        if existing is not None:
            existing.value = record.value
            existing.created_at = record.created_at
            existing.updated_at = record.updated_at
            return existing

        root = self.root
        if len(root.keys) == self._max_keys:
            new_root = BTreeNode(leaf=False, children=[root])
            self._split_child(new_root, 0)
            self.root = new_root
            self._insert_non_full(new_root, record)
        else:
            self._insert_non_full(root, record)

        self._size += 1
        return record

    def delete(self, key: str) -> Record | None:
        existing = self.get(key)
        if existing is None:
            return None

        remaining_records = [
            record for record in self.list_records() if record.key != key
        ]
        self.root = BTreeNode()
        self._size = 0
        for record in remaining_records:
            self.put(record)
        return existing

    def list_records(self) -> list[Record]:
        records: list[Record] = []
        self._collect_in_order(self.root, records)
        return records

    def size(self) -> int:
        return self._size

    @property
    def _max_keys(self) -> int:
        return (2 * self.minimum_degree) - 1

    def _search(self, node: BTreeNode, key: str) -> Record | None:
        index = 0
        while index < len(node.keys) and key > node.keys[index]:
            index += 1

        if index < len(node.keys) and key == node.keys[index]:
            return node.values[index]

        if node.leaf:
            return None

        return self._search(node.children[index], key)

    def _split_child(self, parent: BTreeNode, child_index: int) -> None:
        degree = self.minimum_degree
        child = parent.children[child_index]
        sibling = BTreeNode(leaf=child.leaf)

        median_key = child.keys[degree - 1]
        median_value = child.values[degree - 1]

        sibling.keys = child.keys[degree:]
        sibling.values = child.values[degree:]
        child.keys = child.keys[: degree - 1]
        child.values = child.values[: degree - 1]

        if not child.leaf:
            sibling.children = child.children[degree:]
            child.children = child.children[:degree]

        parent.keys.insert(child_index, median_key)
        parent.values.insert(child_index, median_value)
        parent.children.insert(child_index + 1, sibling)

    def _insert_non_full(self, node: BTreeNode, record: Record) -> None:
        index = len(node.keys) - 1

        if node.leaf:
            node.keys.append("")
            node.values.append(record)

            while index >= 0 and record.key < node.keys[index]:
                node.keys[index + 1] = node.keys[index]
                node.values[index + 1] = node.values[index]
                index -= 1

            node.keys[index + 1] = record.key
            node.values[index + 1] = record
            return

        while index >= 0 and record.key < node.keys[index]:
            index -= 1
        index += 1

        if len(node.children[index].keys) == self._max_keys:
            self._split_child(node, index)
            if record.key > node.keys[index]:
                index += 1

        self._insert_non_full(node.children[index], record)

    def _collect_in_order(self, node: BTreeNode, records: list[Record]) -> None:
        for index, record in enumerate(node.values):
            if not node.leaf:
                self._collect_in_order(node.children[index], records)
            records.append(record)

        if not node.leaf:
            self._collect_in_order(node.children[len(node.values)], records)
