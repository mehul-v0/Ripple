from __future__ import annotations

import random
import threading
import time
import zlib
from typing import Dict, Iterable, List, Optional, Tuple

from .bloom import BloomFilter
from .peers import Peer

DENY_SECONDS = 20.0


class ChunkAvailability:

    def __init__(self, deny_seconds: float = DENY_SECONDS):
        self._lock = threading.RLock()
        self.filters: Dict[str, BloomFilter] = {}
        self.exact: Dict[str, set] = {}
        self.denied: Dict[str, Dict[str, float]] = {}
        self.deny_seconds = deny_seconds

    def update(self, node_id: str, bf: BloomFilter) -> None:
        with self._lock:
            self.filters[node_id] = bf

    def confirm(self, node_id: str, chunk: str, present: bool) -> None:
        with self._lock:
            s = self.exact.setdefault(node_id, set())
            d = self.denied.setdefault(node_id, {})
            if present:
                s.add(chunk)
                d.pop(chunk, None)
            else:
                s.discard(chunk)
                d[chunk] = time.time() + self.deny_seconds

    def forget(self, node_id: str) -> None:
        with self._lock:
            self.filters.pop(node_id, None)
            self.exact.pop(node_id, None)
            self.denied.pop(node_id, None)

    def holders(self, chunk: str, candidates: Iterable[Peer]) -> List[Peer]:
        now = time.time()
        with self._lock:
            out = []
            for p in candidates:
                expiry = self.denied.get(p.node_id, {}).get(chunk)
                if expiry is not None:
                    if expiry > now:
                        continue
                    self.denied[p.node_id].pop(chunk, None)
                if chunk in self.exact.get(p.node_id, ()):
                    out.append(p)
                    continue
                bf = self.filters.get(p.node_id)
                if bf is not None and chunk in bf:
                    out.append(p)
            return out

    def replica_count(self, chunk: str) -> int:
        with self._lock:
            n = 0
            for nid, bf in self.filters.items():
                if chunk in self.exact.get(nid, ()) or chunk in bf:
                    n += 1
            return n


class TransferPlan:
    __slots__ = ("chunk", "peer", "rarity")

    def __init__(self, chunk: str, peer: Peer, rarity: int):
        self.chunk, self.peer, self.rarity = chunk, peer, rarity

    def __repr__(self) -> str:
        return "<fetch %s from %s (replicas=%d)>" % (self.chunk[:8], self.peer.node_id, self.rarity)


class Scheduler:

    def __init__(self, availability: ChunkAvailability, max_per_peer: int = 4,
                 window: int = 128, seed: int = 0, super_seed: bool = False,
                 salt: bool = True):
        self.avail = availability
        self.max_per_peer = max_per_peer
        self.window = window
        self.super_seed = super_seed
        self.salt_enabled = salt
        self._rng = random.Random(seed or None)
        self._salt = self._rng.getrandbits(32) if salt else 0
        self._lock = threading.RLock()
        self.inflight: Dict[str, str] = {}
        self.peer_load: Dict[str, int] = {}

    def plan(self, wanted: List[str], peers: List[Peer], sorter, limit: int = 16,
             origins: frozenset = frozenset()) -> List[TransferPlan]:
        with self._lock:
            todo = [c for c in wanted if c not in self.inflight]
        if not todo:
            return []

        if len(todo) > self.window:
            todo = self._rng.sample(todo, self.window)

        holders_of = {c: self.avail.holders(c, peers) for c in todo}
        rarity = {c: len(holders_of[c]) for c in todo}

        from_swarm, from_origin = [], []
        for c in todo:
            hs = holders_of[c]
            if not hs:
                continue
            if self.super_seed and not any(p.node_id not in origins for p in hs):
                from_origin.append(c)
            else:
                from_swarm.append(c)

        if self.salt_enabled:
            salt = self._salt
            key = lambda c: (rarity[c], zlib.crc32(c.encode(), salt))
        else:
            key = lambda c: (rarity[c], c)
        from_swarm.sort(key=key)
        from_origin.sort(key=key)

        plans: List[TransferPlan] = []
        with self._lock:
            load = dict(self.peer_load)
            for chunk in from_swarm + from_origin:
                if len(plans) >= limit:
                    break
                for peer in sorter(holders_of[chunk]):
                    if load.get(peer.node_id, 0) < self.max_per_peer:
                        load[peer.node_id] = load.get(peer.node_id, 0) + 1
                        self.inflight[chunk] = peer.node_id
                        self.peer_load[peer.node_id] = load[peer.node_id]
                        plans.append(TransferPlan(chunk, peer, rarity[chunk]))
                        break
        return plans

    def complete(self, chunk: str, node_id: str, ok: bool) -> None:
        with self._lock:
            self.inflight.pop(chunk, None)
            if node_id in self.peer_load:
                self.peer_load[node_id] = max(0, self.peer_load[node_id] - 1)
        if ok:
            self.avail.confirm(node_id, chunk, True)

    def release_peer(self, node_id: str) -> None:
        with self._lock:
            for c, n in list(self.inflight.items()):
                if n == node_id:
                    del self.inflight[c]
            self.peer_load.pop(node_id, None)

    def stats(self) -> dict:
        with self._lock:
            return {"inflight": len(self.inflight),
                    "busy_peers": sum(1 for v in self.peer_load.values() if v)}
