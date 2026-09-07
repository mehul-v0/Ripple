from __future__ import annotations

import os
import re
from typing import List, Optional, Tuple

YAML_EXT = {".yaml", ".yml"}
JSON_EXT = {".json"}
INI_EXT = {".ini", ".cfg", ".conf", ".toml", ".properties"}

MAX_STRUCTURED = 8 << 20

Piece = Tuple[str, int, int]

_YAML_TOP = re.compile(rb"^([A-Za-z_][\w.\-]*|\"[^\"]+\"|'[^']+')\s*:")
_INI_SECTION = re.compile(rb"^\[([^\]]+)\]\s*$")
_INI_KEY = re.compile(rb"^([A-Za-z_][\w.\-]*)\s*[=:]")
_INI_SECTION_ANY = re.compile(_INI_SECTION.pattern, re.MULTILINE)
_INI_KEY_ANY = re.compile(_INI_KEY.pattern, re.MULTILINE)
_YAML_TOP_ANY = re.compile(_YAML_TOP.pattern, re.MULTILINE)


def detect(path: str, data: bytes) -> Optional[str]:
    if len(data) > MAX_STRUCTURED or not data:
        return None
    ext = os.path.splitext(path)[1].lower()
    if ext in JSON_EXT:
        return "json"
    if ext in YAML_EXT:
        return "yaml"
    if ext in INI_EXT:
        head = data[:4096]
        if _INI_SECTION_ANY.search(head) or _INI_KEY_ANY.search(head):
            return "ini"
        return "yaml" if _YAML_TOP_ANY.search(head) else None
    return None


def _line_starts(data: bytes) -> List[int]:
    out, i = [0], data.find(b"\n")
    while i != -1:
        out.append(i + 1)
        i = data.find(b"\n", i + 1)
    return out


def _split_line_oriented(data: bytes, is_top) -> Optional[List[Piece]]:
    starts = _line_starts(data)
    cuts: List[Tuple[int, str]] = []
    for s in starts:
        e = data.find(b"\n", s)
        line = data[s:e if e != -1 else len(data)]
        if not line.strip() or line.lstrip().startswith((b"#", b";")):
            continue
        if line[:1].isspace():
            continue
        label = is_top(line)
        if label is not None:
            cuts.append((s, label))
    if len(cuts) < 2:
        return None

    pieces: List[Piece] = []
    if cuts[0][0] > 0:
        pieces.append(("<preamble>", 0, cuts[0][0]))
    for i, (start, label) in enumerate(cuts):
        end = cuts[i + 1][0] if i + 1 < len(cuts) else len(data)
        pieces.append((label, start, end))
    return pieces


def _yaml_top(line: bytes) -> Optional[str]:
    m = _YAML_TOP.match(line)
    if not m:
        return None
    return m.group(1).strip(b"\"'").decode("utf-8", "replace")


def _ini_top(line: bytes) -> Optional[str]:
    m = _INI_SECTION.match(line)
    if m:
        return m.group(1).decode("utf-8", "replace")
    m = _INI_KEY.match(line)
    if m:
        return m.group(1).decode("utf-8", "replace")
    return None


def _split_json(data: bytes) -> Optional[List[Piece]]:
    i, n = 0, len(data)
    while i < n and data[i:i + 1].isspace():
        i += 1
    if data[i:i + 1] != b"{":
        return None

    depth, in_str, esc = 0, False, False
    key: Optional[str] = None
    member_start = None
    pieces: List[Piece] = []
    pending_key = None
    j = i
    while j < n:
        ch = data[j:j + 1]
        if in_str:
            if esc:
                esc = False
            elif ch == b"\\":
                esc = True
            elif ch == b'"':
                in_str = False
                if depth == 1 and pending_key is None:
                    pending_key = data[key_start:j].decode("utf-8", "replace")
        elif ch == b'"':
            in_str = True
            key_start = j + 1
        elif ch in b"{[":
            depth += 1
            if depth == 1:
                member_start = j + 1
        elif ch in b"}]":
            depth -= 1
            if depth == 0:
                if pending_key is not None and member_start is not None:
                    pieces.append((pending_key, member_start, j))
                pieces.append(("<close>", j, n))
                break
        elif ch == b"," and depth == 1:
            if pending_key is not None and member_start is not None:
                pieces.append((pending_key, member_start, j + 1))
            member_start = j + 1
            pending_key = None
        j += 1

    if len(pieces) < 3:
        return None
    if i + 1 > 0:
        pieces.insert(0, ("<open>", 0, pieces[0][1]))
    return pieces


def split(path: str, data: bytes) -> Optional[List[Tuple[str, bytes]]]:
    fmt = detect(path, data)
    if fmt is None:
        return None
    if fmt == "json":
        pieces = _split_json(data)
    elif fmt == "yaml":
        pieces = _split_line_oriented(data, _yaml_top)
    else:
        pieces = _split_line_oriented(data, _ini_top)
    if not pieces:
        return None

    out, pos = [], 0
    for label, start, end in pieces:
        if start != pos or end < start:
            return None
        out.append((label, data[start:end]))
        pos = end
    if pos != len(data):
        return None
    if b"".join(b for _, b in out) != data:
        return None
    return out


def drift(labelled: dict) -> List[dict]:
    all_labels = set()
    for m in labelled.values():
        all_labels.update(m)
    report = []
    for label in sorted(all_labels):
        by_hash: dict = {}
        for node, m in labelled.items():
            by_hash.setdefault(m.get(label), []).append(node)
        if len(by_hash) > 1:
            groups = sorted(by_hash.items(), key=lambda kv: -len(kv[1]))
            majority = groups[0]
            report.append({
                "key": label,
                "majority_hash": majority[0],
                "majority_count": len(majority[1]),
                "outliers": [{"hash": h, "nodes": sorted(ns)}
                             for h, ns in groups[1:]],
            })
    return report
