from __future__ import annotations

from typing import Dict

VV = Dict[str, int]

EQUAL = "equal"
BEFORE = "before"
AFTER = "after"
CONCURRENT = "concurrent"


def bump(vv: VV, node_id: str) -> VV:
    out = dict(vv)
    out[node_id] = out.get(node_id, 0) + 1
    return out


def merge(a: VV, b: VV) -> VV:
    out = dict(a)
    for k, v in b.items():
        if v > out.get(k, 0):
            out[k] = v
    return out


def compare(a: VV, b: VV) -> str:
    a_gt = b_gt = False
    for key in set(a) | set(b):
        av, bv = a.get(key, 0), b.get(key, 0)
        if av > bv:
            a_gt = True
        elif bv > av:
            b_gt = True
    if a_gt and b_gt:
        return CONCURRENT
    if a_gt:
        return AFTER
    if b_gt:
        return BEFORE
    return EQUAL


def descends(a: VV, b: VV) -> bool:
    return compare(a, b) in (EQUAL, AFTER)
