from __future__ import annotations

import random
import threading
import time
from typing import Dict, List, Optional

ALIVE = "alive"
SUSPECT = "suspect"
DEAD = "dead"


class Peer:
    __slots__ = ("node_id", "host", "port", "rack", "heartbeat", "last_seen",
                 "state", "rtt", "have_count", "failures")

    def __init__(self, node_id: str, host: str, port: int, rack: str = "r0"):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.rack = rack
        self.heartbeat = 0
        self.last_seen = time.time()
        self.state = ALIVE
        self.rtt = 0.05
        self.have_count = 0
        self.failures = 0

    @property
    def addr(self):
        return (self.host, self.port)

    def observe_rtt(self, sample: float) -> None:
        self.rtt = 0.75 * self.rtt + 0.25 * sample

    def to_dict(self) -> dict:
        return {"id": self.node_id, "host": self.host, "port": self.port,
                "rack": self.rack, "hb": self.heartbeat, "state": self.state,
                "rtt": round(self.rtt, 4), "have": self.have_count}


class Membership:
    def __init__(self, node_id: str, host: str, port: int, rack: str = "r0",
                 suspect_after: float = 6.0, dead_after: float = 15.0):
        self.node_id = node_id
        self.host, self.port, self.rack = host, port, rack
        self.heartbeat = 0
        self.suspect_after = suspect_after
        self.dead_after = dead_after
        self.peers: Dict[str, Peer] = {}
        self._lock = threading.RLock()
        self._rng = random.Random(hash(node_id) & 0xFFFFFFFF)

    def self_record(self) -> dict:
        return {"id": self.node_id, "host": self.host, "port": self.port,
                "rack": self.rack, "hb": self.heartbeat, "state": ALIVE}

    def tick(self) -> None:
        with self._lock:
            self.heartbeat += 1
            now = time.time()
            for p in self.peers.values():
                age = now - p.last_seen
                if age > self.dead_after:
                    p.state = DEAD
                elif age > self.suspect_after:
                    if p.state == ALIVE:
                        p.state = SUSPECT
                else:
                    p.state = ALIVE

    def add(self, node_id: str, host: str, port: int, rack: str = "r0") -> Optional[Peer]:
        if node_id == self.node_id:
            return None
        with self._lock:
            p = self.peers.get(node_id)
            if p is None:
                p = Peer(node_id, host, port, rack)
                self.peers[node_id] = p
            else:
                p.host, p.port, p.rack = host, port, rack
            return p

    def merge(self, records: List[dict]) -> List[str]:
        learned = []
        with self._lock:
            now = time.time()
            for r in records:
                nid = r.get("id")
                if not nid or nid == self.node_id:
                    continue
                p = self.peers.get(nid)
                if p is None:
                    p = Peer(nid, r["host"], r["port"], r.get("rack", "r0"))
                    self.peers[nid] = p
                    learned.append(nid)
                    p.heartbeat = r.get("hb", 0)
                    p.last_seen = now
                elif r.get("hb", 0) > p.heartbeat:
                    p.heartbeat = r["hb"]
                    p.last_seen = now
                    p.state = ALIVE
                    p.host, p.port = r["host"], r["port"]
        return learned

    def digest(self) -> List[dict]:
        with self._lock:
            return [self.self_record()] + [p.to_dict() for p in self.peers.values()]

    def alive(self) -> List[Peer]:
        with self._lock:
            return [p for p in self.peers.values() if p.state != DEAD]

    def all(self) -> List[Peer]:
        with self._lock:
            return list(self.peers.values())

    def get(self, node_id: str) -> Optional[Peer]:
        with self._lock:
            return self.peers.get(node_id)

    def sample(self, k: int, exclude: Optional[set] = None) -> List[Peer]:
        cand = [p for p in self.alive() if not exclude or p.node_id not in exclude]
        if len(cand) <= k:
            return cand
        return self._rng.sample(cand, k)

    def by_proximity(self, candidates: List[Peer]) -> List[Peer]:
        return sorted(candidates,
                      key=lambda p: (p.failures, 0 if p.rack == self.rack else 1, p.rtt))
