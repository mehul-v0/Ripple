from __future__ import annotations

import os
from typing import Dict, List, Tuple

_POLY = 0x11D

EXP: List[int] = [0] * 255
LOG: List[int] = [0] * 256


def _build_tables() -> None:
    x = 1
    for i in range(255):
        EXP[i] = x
        LOG[x] = i
        x <<= 1
        if x & 0x100:
            x ^= _POLY


_build_tables()

_SCALAR_TABLE_CACHE: Dict[int, bytes] = {}


def gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return EXP[(LOG[a] + LOG[b]) % 255]


def gf_div(a: int, b: int) -> int:
    if a == 0:
        return 0
    if b == 0:
        raise ZeroDivisionError("division by zero in GF(256)")
    return EXP[(LOG[a] - LOG[b]) % 255]


def gf_inv(a: int) -> int:
    if a == 0:
        raise ZeroDivisionError("zero has no inverse in GF(256)")
    return EXP[(255 - LOG[a]) % 255]


def _scalar_table(coeff: int) -> bytes:
    t = _SCALAR_TABLE_CACHE.get(coeff)
    if t is None:
        t = bytes(gf_mul(coeff, v) for v in range(256))
        _SCALAR_TABLE_CACHE[coeff] = t
    return t


def _scalar_mul_bytes(data: bytes, coeff: int) -> bytes:
    if coeff == 0:
        return bytes(len(data))
    if coeff == 1:
        return data
    return data.translate(_scalar_table(coeff))


def _xor_into(acc: bytes, term: bytes) -> bytes:
    n = len(acc)
    return (int.from_bytes(acc, "big") ^ int.from_bytes(term, "big")).to_bytes(n, "big")


Matrix = List[List[int]]


def _mat_mul(a: Matrix, b: Matrix) -> Matrix:
    p, q, r = len(a), len(a[0]), len(b[0])
    out = [[0] * r for _ in range(p)]
    for i in range(p):
        ai = a[i]
        for t in range(q):
            v = ai[t]
            if v == 0:
                continue
            bt, ot = b[t], out[i]
            for j in range(r):
                ot[j] ^= gf_mul(v, bt[j])
    return out


def _mat_inv(m: Matrix) -> Matrix:
    n = len(m)
    aug = [row[:] + [1 if i == j else 0 for j in range(n)] for i, row in enumerate(m)]
    for col in range(n):
        piv = next((r for r in range(col, n) if aug[r][col] != 0), None)
        if piv is None:
            raise ValueError("matrix is singular over GF(256)")
        aug[col], aug[piv] = aug[piv], aug[col]
        inv = gf_inv(aug[col][col])
        aug[col] = [gf_mul(inv, v) for v in aug[col]]
        for r in range(n):
            if r != col and aug[r][col] != 0:
                factor = aug[r][col]
                pivot_row = aug[col]
                aug[r] = [aug[r][j] ^ gf_mul(factor, pivot_row[j]) for j in range(2 * n)]
    return [row[n:] for row in aug]


def _cauchy(xs: List[int], ys: List[int]) -> Matrix:
    return [[gf_inv(x ^ y) for y in ys] for x in xs]


_GEN_CACHE: Dict[Tuple[int, int], Matrix] = {}


def generator_matrix(k: int, m: int) -> Matrix:
    key = (k, m)
    cached = _GEN_CACHE.get(key)
    if cached is not None:
        return cached
    if k < 1 or m < 0:
        raise ValueError("require k >= 1 and m >= 0")
    n = k + m
    if n + k > 256:
        raise ValueError("k+m too large for a GF(256) construction (k+m+k must be <= 256)")
    ys = list(range(k))
    xs = list(range(k, k + n))
    f = _cauchy(xs, ys)
    f_top_inv = _mat_inv(f[:k])
    b = _mat_mul(f, f_top_inv)
    _GEN_CACHE[key] = b
    return b


def encode(data: bytes, k: int, m: int) -> Tuple[List[bytes], int]:
    if k < 1:
        raise ValueError("k must be >= 1")
    if m < 0:
        raise ValueError("m must be >= 0")
    shard_len = max(1, -(-len(data) // k))
    padded = data + b"\x00" * (shard_len * k - len(data))
    shards = [padded[i * shard_len:(i + 1) * shard_len] for i in range(k)]
    if m == 0:
        return list(shards), shard_len

    gen = generator_matrix(k, m)
    parity: List[bytes] = []
    for row in gen[k:]:
        acc = bytes(shard_len)
        for j, coeff in enumerate(row):
            if coeff == 0:
                continue
            acc = _xor_into(acc, _scalar_mul_bytes(shards[j], coeff))
        parity.append(acc)
    return list(shards) + parity, shard_len


def decode(k: int, m: int, shards: Dict[int, bytes], shard_len: int) -> bytes:
    have = sorted(i for i in shards if 0 <= i < k + m)
    if len(have) < k:
        raise ValueError("need at least %d fragments, have %d" % (k, len(have)))
    have = have[:k]
    gen = generator_matrix(k, m)
    sub = [gen[i] for i in have]
    inv = _mat_inv(sub)
    known = [shards[i] for i in have]

    out: List[bytes] = []
    for row in inv:
        acc = bytes(shard_len)
        for j, coeff in enumerate(row):
            if coeff == 0:
                continue
            acc = _xor_into(acc, _scalar_mul_bytes(known[j], coeff))
        out.append(acc)
    return b"".join(out)


def reconstruct(k: int, m: int, shard_len: int, orig_len: int,
                shards: Dict[int, bytes]) -> bytes:
    return decode(k, m, shards, shard_len)[:orig_len]


def self_test() -> None:
    import itertools

    for k, m, size in ((4, 2, 997), (3, 3, 4096), (1, 3, 17), (5, 1, 0), (2, 0, 33)):
        data = os.urandom(size)
        frags, shard_len = encode(data, k, m)
        assert len(frags) == k + m
        assert all(len(f) == shard_len for f in frags)
        assert frags[:k] == encode(data, k, m)[0][:k]
        n = k + m
        for combo in itertools.combinations(range(n), k):
            got = reconstruct(k, m, shard_len, size, {i: frags[i] for i in combo})
            assert got == data, "mismatch for k=%d m=%d combo=%s" % (k, m, combo)
        if m > 0:
            try:
                decode(k, m, {i: frags[i] for i in range(k - 1)}, shard_len)
            except ValueError:
                pass
            else:
                raise AssertionError("decode() accepted fewer than k fragments")


if __name__ == "__main__":
    self_test()
    print("erasure.self_test() passed")
