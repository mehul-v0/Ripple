"""Merkle anti-entropy over the manifest set.

Gossip is best-effort, so a dropped message can leave a node missing a manifest
with nothing to notice. Two identical nodes compare one root hash; two that
differ descend only into disagreeing subtrees, so cost tracks the size of the
divergence rather than the dataset. Cassandra and DynamoDB repair replicas the
same way.
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional

EMPTY = "0" * 64


def _h(*parts: str) -> str:
    d = hashlib.sha256()
    for p in parts:
        d.update(p.encode())
        d.update(b"\x1f")
    return d.hexdigest()


class MerkleTree:
    """A fixed-width Merkle tree over key -> value-hash pairs."""

    def __init__(self, leaves: int = 256):
        if leaves & (leaves - 1):
            raise ValueError("leaf count must be a power of two")
        self.n_leaves = leaves
        self.buckets: List[Dict[str, str]] = [dict() for _ in range(leaves)]
        self._levels: Optional[List[List[str]]] = None

    def bucket_of(self, key: str) -> int:
        return int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % self.n_leaves

    def set(self, key: str, value_hash: str) -> None:
        self.buckets[self.bucket_of(key)][key] = value_hash
        self._levels = None

    def remove(self, key: str) -> None:
        self.buckets[self.bucket_of(key)].pop(key, None)
        self._levels = None

    def _leaf_hash(self, i: int) -> str:
        items = self.buckets[i]
        if not items:
            return EMPTY
        # Sorted so the hash is independent of insertion order across nodes.
        return _h(*[x for k in sorted(items) for x in (k, items[k])])

    def levels(self) -> List[List[str]]:
        """levels[0] is the leaf row; the last level holds the root."""
        if self._levels is not None:
            return self._levels
        level = [self._leaf_hash(i) for i in range(self.n_leaves)]
        out = [level]
        while len(level) > 1:
            level = [_h(level[i], level[i + 1]) for i in range(0, len(level), 2)]
            out.append(level)
        self._levels = out
        return out

    def root(self) -> str:
        return self.levels()[-1][0]

    def bucket_keys(self, index: int) -> Dict[str, str]:
        return dict(self.buckets[index])

    def all_keys(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for b in self.buckets:
            out.update(b)
        return out


def differing_buckets(local: MerkleTree, remote_levels: List[List[str]]) -> List[int]:
    """Descend both trees together, returning only the leaves that disagree.

    O(differences * log leaves) rather than O(leaves).
    """
    local_levels = local.levels()
    if not remote_levels or len(local_levels) != len(remote_levels):
        return list(range(local.n_leaves))
    if local_levels[-1][0] == remote_levels[-1][0]:
        return []

    depth = len(local_levels) - 1
    frontier = [0]
    while depth > 0 and frontier:
        child, rchild = local_levels[depth - 1], remote_levels[depth - 1]
        nxt = []
        for idx in frontier:
            for c in (idx * 2, idx * 2 + 1):
                if c < len(child) and child[c] != rchild[c]:
                    nxt.append(c)
        frontier = nxt
        depth -= 1
    return frontier
