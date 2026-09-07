from __future__ import annotations

import hashlib
import random
from typing import Iterator, List, NamedTuple

MASK64 = (1 << 64) - 1

_rng = random.Random(0x5253594E43)
GEAR: List[int] = [_rng.getrandbits(64) for _ in range(256)]

DEFAULT_MIN = 2 * 1024
DEFAULT_AVG = 8 * 1024
DEFAULT_MAX = 64 * 1024

_MASK_SHIFT = 12


def _mask(bits: int) -> int:
    return ((1 << bits) - 1) << _MASK_SHIFT


class Chunk(NamedTuple):
    hash: str
    offset: int
    size: int
    data: bytes


PROFILES = [
    (8 << 20,    2 << 10,   8 << 10,   64 << 10),
    (256 << 20,  24 << 10,  64 << 10,  512 << 10),
    (4 << 30,    192 << 10, 512 << 10, 4 << 20),
    (1 << 62,    384 << 10, 1 << 20,   8 << 20),
]


class Chunker:
    def __init__(self, min_size: int = DEFAULT_MIN, avg_size: int = DEFAULT_AVG,
                 max_size: int = DEFAULT_MAX):
        if not (0 < min_size <= avg_size <= max_size):
            raise ValueError("require 0 < min <= avg <= max")
        self.min_size = min_size
        self.avg_size = avg_size
        self.max_size = max_size
        avg_bits = max(1, (avg_size - 1).bit_length())
        self.mask_strict = _mask(min(avg_bits + 2, 64 - _MASK_SHIFT))
        self.mask_lenient = _mask(max(avg_bits - 2, 1))

    @classmethod
    def for_file_size(cls, size: int) -> "Chunker":
        for bound, mn, avg, mx in PROFILES:
            if size <= bound:
                return cls(mn, avg, mx)
        bound, mn, avg, mx = PROFILES[-1]
        return cls(mn, avg, mx)

    def describe(self) -> str:
        return "min=%dK avg=%dK max=%dK" % (self.min_size >> 10,
                                            self.avg_size >> 10, self.max_size >> 10)

    def expected_chunks(self, size: int) -> int:
        return max(1, size // self.avg_size)

    def cut_points(self, data: bytes) -> List[int]:
        n = len(data)
        cuts: List[int] = []
        start = 0
        while start < n:
            cuts.append(start + self._next_cut(data, start, n))
            start = cuts[-1]
        return cuts

    def _next_cut(self, data: bytes, start: int, n: int) -> int:
        remaining = n - start
        if remaining <= self.min_size:
            return remaining
        limit = min(self.max_size, remaining)
        normal = min(self.avg_size, limit)

        gear = GEAR
        mask64 = MASK64
        h = 0
        base = start + self.min_size

        strict = self.mask_strict
        for i, b in enumerate(data[base:start + normal]):
            h = ((h << 1) + gear[b]) & mask64
            if not h & strict:
                return self.min_size + i + 1

        lenient = self.mask_lenient
        for i, b in enumerate(data[start + normal:start + limit]):
            h = ((h << 1) + gear[b]) & mask64
            if not h & lenient:
                return normal + i + 1
        return limit

    def chunk_bytes(self, data: bytes) -> Iterator[Chunk]:
        start = 0
        n = len(data)
        while start < n:
            size = self._next_cut(data, start, n)
            block = data[start:start + size]
            yield Chunk(hashlib.sha256(block).hexdigest(), start, size, block)
            start += size

    def chunk_file(self, path: str, buf_size: int = 8 << 20) -> Iterator[Chunk]:
        offset = 0
        carry = b""
        with open(path, "rb") as fh:
            while True:
                block = fh.read(buf_size)
                if not block:
                    break
                buf = carry + block
                pos = 0
                while len(buf) - pos > self.max_size:
                    size = self._next_cut(buf, pos, len(buf))
                    piece = buf[pos:pos + size]
                    yield Chunk(hashlib.sha256(piece).hexdigest(), offset, size, piece)
                    offset += size
                    pos += size
                carry = buf[pos:]
        pos = 0
        while pos < len(carry):
            size = self._next_cut(carry, pos, len(carry))
            piece = carry[pos:pos + size]
            yield Chunk(hashlib.sha256(piece).hexdigest(), offset, size, piece)
            offset += size
            pos += size


def chunk_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
