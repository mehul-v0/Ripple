from __future__ import annotations

import random
import threading
import time
from typing import Dict, Optional, Set


class Chaos:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._lock = threading.RLock()
        self.killed = False
        self.partitioned: Set[str] = set()
        self.latency_ms = 0.0
        self.throughput_bps = 0.0
        self.drop_rate = 0.0
        self.byzantine = False
        self._rng = random.Random()

    def snapshot(self) -> dict:
        with self._lock:
            return {"killed": self.killed, "partitioned": sorted(self.partitioned),
                    "latency_ms": self.latency_ms, "throughput_bps": self.throughput_bps,
                    "drop_rate": self.drop_rate, "byzantine": self.byzantine}

    def apply(self, action: str, **kw) -> dict:
        with self._lock:
            if action == "kill":
                self.killed = True
            elif action == "revive":
                self.killed = False
            elif action == "partition":
                self.partitioned.update(kw.get("peers", []))
            elif action == "heal":
                if kw.get("peers"):
                    self.partitioned.difference_update(kw["peers"])
                else:
                    self.partitioned.clear()
            elif action == "latency":
                self.latency_ms = float(kw.get("ms", 0))
            elif action == "throttle":
                self.throughput_bps = float(kw.get("bps", 0))
            elif action == "drop":
                self.drop_rate = float(kw.get("rate", 0))
            elif action == "byzantine":
                self.byzantine = bool(kw.get("on", True))
            elif action == "reset":
                self.killed = False
                self.partitioned.clear()
                self.latency_ms = 0.0
                self.throughput_bps = 0.0
                self.drop_rate = 0.0
                self.byzantine = False
            else:
                return {"ok": False, "reason": "unknown action %s" % action}
        return {"ok": True, "state": self.snapshot()}

    def blocks(self, peer_id: Optional[str]) -> bool:
        with self._lock:
            if self.killed:
                return True
            return bool(peer_id and peer_id in self.partitioned)

    def maybe_drop(self) -> bool:
        with self._lock:
            return self.drop_rate > 0 and self._rng.random() < self.drop_rate

    def delay(self, nbytes: int = 0) -> None:
        with self._lock:
            lat = self.latency_ms / 1000.0
            bps = self.throughput_bps
        wait = lat
        if bps > 0 and nbytes:
            wait += nbytes / bps
        if wait > 0:
            time.sleep(wait)


class LatencyMatrix:

    def __init__(self, intra_ms: float = 0.5, inter_ms: float = 12.0, jitter: float = 0.3):
        self.intra_ms = intra_ms
        self.inter_ms = inter_ms
        self.jitter = jitter
        self._rng = random.Random(1234)

    def delay_for(self, rack_a: str, rack_b: str) -> float:
        base = self.intra_ms if rack_a == rack_b else self.inter_ms
        return max(0.0, base * (1 + self._rng.uniform(-self.jitter, self.jitter))) / 1000.0
