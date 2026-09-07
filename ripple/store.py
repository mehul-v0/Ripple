"""Content-addressed chunk store.

Chunks are filed under their own SHA-256, which gives cluster-wide dedup across
files for free and makes integrity intrinsic: a chunk's name is a checksum of
its content, so any node can verify anything it receives.

Writes are atomic (temp, fsync, rename) so a crash cannot leave a torn chunk
that would later be indistinguishable from disk corruption.
"""

from __future__ import annotations

import hashlib
import os
import threading
from typing import Dict, Iterator, List, Optional


def atomic_write(path: str, data: bytes, do_fsync: bool = True) -> None:
    """Write `data` to `path` so readers see all of it or none of it."""
    tmp = path + ".tmp.%d" % os.getpid()
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(tmp, "wb") as fh:
        fh.write(data)
        if do_fsync:
            fh.flush()
            os.fsync(fh.fileno())
    os.replace(tmp, path)  # atomic on POSIX and on Windows (MoveFileEx)


class ChunkStore:
    def __init__(self, root: str, fsync: bool = False):
        self.root = root
        self.objects = os.path.join(root, "objects")
        self.fsync = fsync
        os.makedirs(self.objects, exist_ok=True)
        self._lock = threading.RLock()
        self._index: Dict[str, int] = {}   # chunk hash -> size
        self._logical_bytes = 0            # bytes if we stored every reference
        self._scan()

    def _path(self, h: str) -> str:
        # Two-level fan-out keeps any one directory small enough to stay fast at
        # millions of chunks.
        return os.path.join(self.objects, h[:2], h[2:4], h)

    def _scan(self) -> None:
        for d1 in os.listdir(self.objects):
            p1 = os.path.join(self.objects, d1)
            if not os.path.isdir(p1):
                continue
            for d2 in os.listdir(p1):
                p2 = os.path.join(p1, d2)
                if not os.path.isdir(p2):
                    continue
                for name in os.listdir(p2):
                    if len(name) == 64:
                        self._index[name] = os.path.getsize(os.path.join(p2, name))

    def has(self, h: str) -> bool:
        with self._lock:
            return h in self._index

    def missing(self, hashes) -> List[str]:
        with self._lock:
            seen = set()
            out = []
            for h in hashes:
                if h not in self._index and h not in seen:
                    seen.add(h)
                    out.append(h)
            return out

    def put(self, data: bytes, expected: Optional[str] = None) -> str:
        """Store a chunk, verifying it against its claimed hash.

        Verification is not optional: peers are untrusted, and storing mislabelled data
        would poison every node that later replicates from us.
        """
        h = hashlib.sha256(data).hexdigest()
        if expected is not None and expected != h:
            raise ValueError("chunk hash mismatch: claimed %s, computed %s" % (expected, h))
        with self._lock:
            self._logical_bytes += len(data)
            if h in self._index:
                return h  # dedup hit: the bytes are already here
        path = self._path(h)
        if not os.path.exists(path):
            atomic_write(path, data, self.fsync)
        with self._lock:
            self._index[h] = len(data)
        return h

    def get(self, h: str) -> Optional[bytes]:
        if not self.has(h):
            return None
        try:
            with open(self._path(h), "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            with self._lock:
                self._index.pop(h, None)
            return None

    def verify(self, h: str) -> bool:
        """Re-read a chunk and confirm it still hashes to its own name."""
        data = self.get(h)
        return data is not None and hashlib.sha256(data).hexdigest() == h

    def drop(self, h: str) -> None:
        with self._lock:
            self._index.pop(h, None)
        try:
            os.remove(self._path(h))
        except OSError:
            pass

    def damage(self, h: str) -> bool:
        """Flip a byte on disk without updating the index. Chaos testing only.

        Simulates silent corruption: right size, still present, nothing notices until
        something verifies it.
        """
        if not self.has(h):
            return False
        path = self._path(h)
        with open(path, "r+b") as fh:
            data = bytearray(fh.read())
            if not data:
                return False
            data[len(data) // 2] ^= 0xFF
            fh.seek(0)
            fh.write(bytes(data))
        return True

    def hashes(self) -> List[str]:
        with self._lock:
            return list(self._index)

    def __len__(self) -> int:
        with self._lock:
            return len(self._index)

    def physical_bytes(self) -> int:
        with self._lock:
            return sum(self._index.values())

    def logical_bytes(self) -> int:
        """Bytes we would have stored with no deduplication."""
        with self._lock:
            return self._logical_bytes

    def dedup_ratio(self) -> float:
        phys = self.physical_bytes()
        return (self._logical_bytes / phys) if phys else 1.0

    def iter_hashes(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._index))
