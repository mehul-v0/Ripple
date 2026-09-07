from __future__ import annotations

import hashlib
import os
import threading
import time
from typing import Dict, Iterator, List, Optional


def atomic_write(path: str, data: bytes, do_fsync: bool = True) -> None:
    tmp = path + ".tmp.%d" % os.getpid()
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(tmp, "wb") as fh:
        fh.write(data)
        if do_fsync:
            fh.flush()
            os.fsync(fh.fileno())
    os.replace(tmp, path)


class ChunkStore:
    def __init__(self, root: str, fsync: bool = False):
        self.root = root
        self.objects = os.path.join(root, "objects")
        self.fsync = fsync
        os.makedirs(self.objects, exist_ok=True)
        self._lock = threading.RLock()
        self._index: Dict[str, int] = {}
        self._logical_bytes = 0
        self._atime: Dict[str, float] = {}
        self._scan()

    def _path(self, h: str) -> str:
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
                        full = os.path.join(p2, name)
                        self._index[name] = os.path.getsize(full)
                        self._atime[name] = os.path.getmtime(full)

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
        h = hashlib.sha256(data).hexdigest()
        if expected is not None and expected != h:
            raise ValueError("chunk hash mismatch: claimed %s, computed %s" % (expected, h))
        with self._lock:
            self._logical_bytes += len(data)
            if h in self._index:
                self._atime[h] = time.time()
                return h
        path = self._path(h)
        if not os.path.exists(path):
            atomic_write(path, data, self.fsync)
        with self._lock:
            self._index[h] = len(data)
            self._atime[h] = time.time()
        return h

    def get(self, h: str) -> Optional[bytes]:
        if not self.has(h):
            return None
        try:
            with open(self._path(h), "rb") as fh:
                data = fh.read()
        except FileNotFoundError:
            with self._lock:
                self._index.pop(h, None)
                self._atime.pop(h, None)
            return None
        with self._lock:
            self._atime[h] = time.time()
        return data

    def coldest(self, candidates) -> List[str]:
        with self._lock:
            return sorted(candidates, key=lambda h: self._atime.get(h, 0.0))

    def verify(self, h: str) -> bool:
        data = self.get(h)
        return data is not None and hashlib.sha256(data).hexdigest() == h

    def drop(self, h: str) -> None:
        with self._lock:
            self._index.pop(h, None)
            self._atime.pop(h, None)
        try:
            os.remove(self._path(h))
        except OSError:
            pass

    def damage(self, h: str) -> bool:
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

    def size_of(self, h: str) -> int:
        with self._lock:
            return self._index.get(h, 0)

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
        with self._lock:
            return self._logical_bytes

    def dedup_ratio(self) -> float:
        phys = self.physical_bytes()
        return (self._logical_bytes / phys) if phys else 1.0

    def iter_hashes(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._index))
