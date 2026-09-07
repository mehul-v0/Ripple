"""Manifests: the unit Ripple synchronises.

Everything about a file except its bytes -- name, size, mode, mtime, version
vector, and the ordered chunk hashes. About 70 bytes per chunk, so manifest size
tracks chunk count and chunk size scales with file size (chunker.PROFILES).

Manifests are immutable and content-addressed by their own digest, so editing a
file produces a new one and the old stays valid at negligible cost. That is what
makes rollback a pointer swap.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Dict, List, Optional

from .versions import VV, bump

# File materialisation states, in order.
PHANTOM = "PHANTOM"    # manifest known, zero chunks local: the file "exists"
PARTIAL = "PARTIAL"    # some chunks local, usually mid-read or mid-transfer
RESIDENT = "RESIDENT"  # every chunk local, readable with no network at all


class Manifest:
    __slots__ = ("path", "size", "mode", "mtime", "chunks", "sizes", "vv",
                 "origin", "created", "deleted", "note", "labels", "_digest")

    def __init__(self, path: str, size: int, chunks: List[str], sizes: List[int],
                 vv: VV, origin: str, mode: int = 0o644,
                 mtime: Optional[float] = None, created: Optional[float] = None,
                 deleted: bool = False, note: str = "",
                 labels: Optional[List[str]] = None):
        self.path = path
        self.size = size
        self.chunks = chunks
        self.sizes = sizes
        self.vv = dict(vv)
        self.origin = origin
        self.mode = mode
        self.mtime = mtime if mtime is not None else time.time()
        self.created = created if created is not None else time.time()
        self.deleted = deleted
        self.note = note
        # One label per chunk when the file was split on its grammar (structured.py);
        # None for ordinary content-defined chunks.
        self.labels = labels
        self._digest: Optional[str] = None

    def to_dict(self) -> dict:
        return {"path": self.path, "size": self.size, "mode": self.mode,
                "mtime": self.mtime, "chunks": self.chunks, "sizes": self.sizes,
                "vv": self.vv, "origin": self.origin, "created": self.created,
                "deleted": self.deleted, "note": self.note, "labels": self.labels}

    @classmethod
    def from_dict(cls, d: dict) -> "Manifest":
        return cls(d["path"], d["size"], d["chunks"], d["sizes"], d["vv"],
                   d["origin"], d.get("mode", 0o644), d.get("mtime"),
                   d.get("created"), d.get("deleted", False), d.get("note", ""),
                   d.get("labels"))

    def digest(self) -> str:
        """Stable content address for this version.

        sort_keys matters: two nodes must derive the same digest for the same manifest
        or Merkle comparison would report divergence that is not there.
        """
        if self._digest is None:
            blob = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
            self._digest = hashlib.sha256(blob.encode()).hexdigest()
        return self._digest

    def nbytes(self) -> int:
        return len(json.dumps(self.to_dict(), separators=(",", ":")).encode())

    def unique_chunks(self) -> List[str]:
        seen, out = set(), []
        for h in self.chunks:
            if h not in seen:
                seen.add(h)
                out.append(h)
        return out

    def chunk_offsets(self) -> List[int]:
        offs, acc = [], 0
        for s in self.sizes:
            offs.append(acc)
            acc += s
        return offs

    def chunks_for_range(self, offset: int, length: int) -> List[int]:
        """Indices of the chunks a read of [offset, offset+length) touches.

        Reading 4 KB of a 10 GB file fetches one chunk.
        """
        if length <= 0:
            return []
        end = offset + length
        out, acc = [], 0
        for i, s in enumerate(self.sizes):
            if acc >= end:
                break
            if acc + s > offset:
                out.append(i)
            acc += s
        return out

    def label_map(self) -> Dict[str, str]:
        """label -> chunk hash for structured files; empty for opaque ones."""
        if not self.labels:
            return {}
        return {lab: h for lab, h in zip(self.labels, self.chunks)
                if not lab.startswith("<")}

    def bumped(self, node_id: str) -> VV:
        return bump(self.vv, node_id)

    def __repr__(self) -> str:
        return "<Manifest %s %dB %dc v%s>" % (self.path, self.size,
                                              len(self.chunks), self.vv)


class ManifestStore:
    """Every version of every manifest, plus a pointer to the current one.

    History is kept in full; a manifest is a few KB, so rollback is a pointer swap
    rather than a restore.
    """

    def __init__(self, root: str):
        self.dir = os.path.join(root, "manifests")
        os.makedirs(os.path.join(self.dir, "versions"), exist_ok=True)
        self.refs: Dict[str, str] = {}             # path -> current digest
        self.versions: Dict[str, Manifest] = {}    # digest -> manifest
        self.history: Dict[str, List[str]] = {}    # path -> [digest, ...]
        self.conflicts: Dict[str, List[str]] = {}  # path -> [losing digest, ...]
        self._load()

    def _load(self) -> None:
        vdir = os.path.join(self.dir, "versions")
        for name in os.listdir(vdir):
            if not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(vdir, name)) as fh:
                    m = Manifest.from_dict(json.load(fh))
            except (OSError, ValueError, KeyError):
                continue
            self.versions[m.digest()] = m
            self.history.setdefault(m.path, []).append(m.digest())
        refs = os.path.join(self.dir, "refs.json")
        if os.path.exists(refs):
            try:
                with open(refs) as fh:
                    self.refs = json.load(fh)
            except (OSError, ValueError):
                self.refs = {}
        for digests in self.history.values():
            digests.sort(key=lambda d: self.versions[d].created)

    def _persist_refs(self) -> None:
        from .store import atomic_write
        atomic_write(os.path.join(self.dir, "refs.json"),
                     json.dumps(self.refs).encode(), do_fsync=False)

    def add_version(self, m: Manifest) -> str:
        from .store import atomic_write
        dg = m.digest()
        if dg not in self.versions:
            self.versions[dg] = m
            self.history.setdefault(m.path, []).append(dg)
            atomic_write(os.path.join(self.dir, "versions", dg + ".json"),
                         json.dumps(m.to_dict()).encode(), do_fsync=False)
        return dg

    def set_current(self, m: Manifest) -> str:
        dg = self.add_version(m)
        self.refs[m.path] = dg
        self._persist_refs()
        return dg

    def current(self, path: str) -> Optional[Manifest]:
        dg = self.refs.get(path)
        return self.versions.get(dg) if dg else None

    def get(self, digest: str) -> Optional[Manifest]:
        return self.versions.get(digest)

    def paths(self) -> List[str]:
        return list(self.refs)

    def all_current(self) -> List[Manifest]:
        return [self.versions[d] for d in self.refs.values() if d in self.versions]

    def record_conflict(self, path: str, digest: str) -> None:
        losers = self.conflicts.setdefault(path, [])
        if digest not in losers:
            losers.append(digest)

    def version_list(self, path: str) -> List[Manifest]:
        return [self.versions[d] for d in self.history.get(path, []) if d in self.versions]
