"""Instrumentation.

Every byte crossing a socket is counted where it crosses. `source_egress` --
bytes sent by the node that published a file -- is counted separately from total
traffic, because that is the quantity the scaling argument rests on.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, Dict, List, Optional


class Metrics:
    def __init__(self, node_id: str, window: int = 400):
        self.node_id = node_id
        self.started = time.time()
        self._lock = threading.Lock()
        self.counters: Dict[str, float] = {}
        self.events: Deque[dict] = deque(maxlen=window)
        self.timers: Dict[str, List[float]] = {}

    def incr(self, name: str, value: float = 1) -> None:
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + value

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self.counters[name] = value

    def get(self, name: str) -> float:
        with self._lock:
            return self.counters.get(name, 0)

    def observe(self, name: str, seconds: float) -> None:
        with self._lock:
            self.timers.setdefault(name, []).append(seconds)

    def event(self, kind: str, text: str, **extra) -> None:
        """Append to the rolling event log the dashboard renders."""
        rec = {"t": time.time(), "kind": kind, "text": text, "node": self.node_id}
        rec.update(extra)
        with self._lock:
            self.events.append(rec)

    def recent(self, since: float = 0.0) -> List[dict]:
        with self._lock:
            return [e for e in self.events if e["t"] > since]

    def snapshot(self) -> dict:
        with self._lock:
            counters = dict(self.counters)
            timers = {k: {"n": len(v), "mean": sum(v) / len(v), "max": max(v)}
                      for k, v in self.timers.items() if v}
        counters["uptime"] = time.time() - self.started
        return {"node": self.node_id, "counters": counters, "timers": timers}


class Stopwatch:
    def __init__(self, metrics: Optional[Metrics] = None, name: str = ""):
        self.metrics, self.name = metrics, name
        self.elapsed = 0.0

    def __enter__(self) -> "Stopwatch":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        self.elapsed = time.perf_counter() - self._t0
        if self.metrics and self.name:
            self.metrics.observe(self.name, self.elapsed)


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return "%.1f %s" % (n, unit) if unit != "B" else "%d B" % n
        n /= 1024
    return "%.1f TB" % n
