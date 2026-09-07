from __future__ import annotations

import base64
import hashlib
import math
from typing import Iterable, Optional


_HASH_CACHE: dict = {}
_HASH_CACHE_MAX = 200_000


def _halves(item: str):
    hv = _HASH_CACHE.get(item)
    if hv is None:
        d = hashlib.sha256(item.encode()).digest()
        hv = (int.from_bytes(d[:8], "big"), int.from_bytes(d[8:16], "big") | 1)
        if len(_HASH_CACHE) < _HASH_CACHE_MAX:
            _HASH_CACHE[item] = hv
    return hv


class BloomFilter:
    __slots__ = ("m", "k", "bits", "count")

    def __init__(self, m: int, k: int, bits: Optional[bytearray] = None, count: int = 0):
        self.m = max(8, m)
        self.k = max(1, k)
        self.bits = bits if bits is not None else bytearray((self.m + 7) // 8)
        self.count = count

    @classmethod
    def for_items(cls, n: int, fp_rate: float = 0.01) -> "BloomFilter":
        n = max(1, n)
        m = int(math.ceil(-n * math.log(fp_rate) / (math.log(2) ** 2)))
        k = max(1, int(round(m / n * math.log(2))))
        return cls(m, k)

    def _indices(self, item: str) -> Iterable[int]:
        h1, h2 = _halves(item)
        m = self.m
        for i in range(self.k):
            yield (h1 + i * h2) % m

    def add(self, item: str) -> None:
        h1, h2 = _halves(item)
        m, bits = self.m, self.bits
        for i in range(self.k):
            idx = (h1 + i * h2) % m
            bits[idx >> 3] |= 1 << (idx & 7)
        self.count += 1

    def __contains__(self, item: str) -> bool:
        h1, h2 = _halves(item)
        m, bits = self.m, self.bits
        for i in range(self.k):
            idx = (h1 + i * h2) % m
            if not bits[idx >> 3] & (1 << (idx & 7)):
                return False
        return True

    def fill_ratio(self) -> float:
        return sum(bin(b).count("1") for b in self.bits) / self.m

    def estimated_fp_rate(self) -> float:
        return self.fill_ratio() ** self.k

    def nbytes(self) -> int:
        return len(self.bits)

    def to_dict(self) -> dict:
        return {"m": self.m, "k": self.k, "count": self.count,
                "bits": base64.b64encode(bytes(self.bits)).decode()}

    @classmethod
    def from_dict(cls, d: dict) -> "BloomFilter":
        return cls(d["m"], d["k"], bytearray(base64.b64decode(d["bits"])), d.get("count", 0))
