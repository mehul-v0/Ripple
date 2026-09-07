"""Fault injection, exposed as a control surface rather than a fixture.

The hooks live on the send and receive paths in node.py, so injected faults look
to the rest of the system exactly like real ones. The dashboard drives them, so
a demo can hand over the controls.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Dict, Optional, Set


class Chaos:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._lock = threading.RLock()
        self.killed = False                  # node behaves as if powered off
        self.partitioned: Set[str] = set()   # peers unreachable in both directions
        self.latency_ms = 0.0                # extra delay on every message
        self.throughput_bps = 0.0            # 0 = unthrottled
        self.drop_rate = 0.0                 # fraction of messages silently lost
        self._rng = random.Random()

    def snapshot(self) -> dict:
        with self._lock:
            return {"killed": self.killed, "partitioned": sorted(self.partitioned),
                    "latency_ms": self.latency_ms, "throughput_bps": self.throughput_bps,
                    "drop_rate": self.drop_rate}

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
            elif action == "reset":
                self.killed = False
                self.partitioned.clear()
                self.latency_ms = 0.0
                self.throughput_bps = 0.0
                self.drop_rate = 0.0
            else:
                return {"ok": False, "reason": "unknown action %s" % action}
        return {"ok": True, "state": self.snapshot()}

    # --- hooks called from the network paths -------------------------------

    def blocks(self, peer_id: Optional[str]) -> bool:
        with self._lock:
            if self.killed:
                return True
            return bool(peer_id and peer_id in self.partitioned)

    def maybe_drop(self) -> bool:
        with self._lock:
            return self.drop_rate > 0 and self._rng.random() < self.drop_rate

    def delay(self, nbytes: int = 0) -> None:
        """Sleep for the injected latency plus the implied serialisation time."""
        with self._lock:
            lat = self.latency_ms / 1000.0
            bps = self.throughput_bps
        wait = lat
        if bps > 0 and nbytes:
            wait += nbytes / bps
        if wait > 0:
            time.sleep(wait)


class LatencyMatrix:
    """Synthetic rack topology for single-host demos.

    Fifty containers on one laptop all see sub-millisecond RTT, which makes
    latency-aware peer selection untestable. This models the topology we cannot
    physically build.
    """

    def __init__(self, intra_ms: float = 0.5, inter_ms: float = 12.0, jitter: float = 0.3):
        self.intra_ms = intra_ms
        self.inter_ms = inter_ms
        self.jitter = jitter
        self._rng = random.Random(1234)

    def delay_for(self, rack_a: str, rack_b: str) -> float:
        base = self.intra_ms if rack_a == rack_b else self.inter_ms
        return max(0.0, base * (1 + self._rng.uniform(-self.jitter, self.jitter))) / 1000.0
