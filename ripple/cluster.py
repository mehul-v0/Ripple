"""Spin up a cluster of nodes in one process.

Used by the benchmarks, the demo and the dashboard. Each node is a real
RippleNode with a real listening socket -- threads replace containers, not the
network.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
from typing import Dict, List, Optional

from .node import RippleNode


class LocalCluster:
    def __init__(self, n: int, root: Optional[str] = None, racks: int = 4,
                 fetch_policy: str = "lazy", source_policy: str = "swarm",
                 fanout: int = 3, seeds: int = 1, scrub_period: float = 20.0,
                 super_seed: bool = False, salt: bool = True):
        self.n = n
        self.owns_root = root is None
        self.root = root or tempfile.mkdtemp(prefix="ripple-cluster-")
        self.racks = racks
        self.nodes: List[RippleNode] = []
        self.fetch_policy = fetch_policy
        self.source_policy = source_policy
        self.fanout = fanout
        self.seeds = seeds
        self.scrub_period = scrub_period
        self.super_seed = super_seed
        self.salt = salt

    def start(self, wait: bool = True, timeout: float = 60.0) -> "LocalCluster":
        for i in range(self.n):
            node = RippleNode(
                "n%03d" % i, root=os.path.join(self.root, "n%03d" % i),
                rack="rack%d" % (i % self.racks), fetch_policy=self.fetch_policy,
                source_policy=self.source_policy, fanout=self.fanout,
                scrub_period=self.scrub_period,
                super_seed=self.super_seed, salt=self.salt)
            node.start()
            self.nodes.append(node)

        # Every node contacts a few seeds; gossip discovers the rest. Deliberately not
        # a full mesh at join time.
        seeds = self.nodes[:self.seeds]
        for node in self.nodes[self.seeds:]:
            for s in seeds:
                node.join(s.host, s.port)
        if wait:
            self.await_membership(timeout)
        return self

    def await_membership(self, timeout: float = 60.0, fraction: float = 1.0) -> bool:
        target = int((self.n - 1) * fraction)
        end = time.time() + timeout
        while time.time() < end:
            if all(len(nd.membership.alive()) >= target for nd in self.nodes):
                return True
            time.sleep(0.05)
        return False

    def await_manifest(self, path: str, timeout: float = 60.0) -> float:
        """Seconds until every node knows the file exists."""
        t0 = time.perf_counter()
        end = time.time() + timeout
        while time.time() < end:
            if all(nd.manifests.current(path) for nd in self.nodes):
                return time.perf_counter() - t0
            time.sleep(0.005)
        return float("nan")

    def await_resident(self, path: str, timeout: float = 300.0,
                       nodes: Optional[List[RippleNode]] = None) -> float:
        """Seconds until every node holds every byte."""
        targets = nodes if nodes is not None else self.nodes
        t0 = time.perf_counter()
        end = time.time() + timeout
        while time.time() < end:
            if all(nd.state_of(path) == "RESIDENT" for nd in targets):
                return time.perf_counter() - t0
            time.sleep(0.01)
        return float("nan")

    def materialise_all(self, path: str, skip: int = 0) -> None:
        for nd in self.nodes[skip:]:
            nd.materialise(path, block=False)

    def total(self, counter: str) -> float:
        return sum(nd.metrics.get(counter) for nd in self.nodes)

    def drift(self, path: str) -> List[dict]:
        """Which config keys disagree across the cluster, and where.

        Reads no file bytes: every node already holds every manifest.
        """
        from .structured import drift as _drift
        labelled = {}
        for nd in self.nodes:
            m = nd.manifests.current(path)
            if m is not None and m.labels:
                labelled[nd.node_id] = m.label_map()
        return _drift(labelled)

    def show_drift(self, path: str) -> None:
        """Print a drift report, fetching only the stanzas that differ."""
        report = self.drift(path)
        if not report:
            print("  no drift: every node agrees on %s" % path)
            return
        for item in report:
            majority = item["majority_count"]
            print("  %s: %d node(s) agree" % (item["key"], majority))
            for grp in item["outliers"]:
                # The stanza may live on any node; the outliers are usually PHANTOM here.
                sample = None
                for nd in self.nodes:
                    blob = nd.store.get(grp["hash"])
                    if blob is not None:
                        sample = blob.decode("utf-8", "replace").strip()
                        break
                print("    %-24s differs on %s%s"
                      % (item["key"], ", ".join(grp["nodes"]),
                         (" -> %s" % sample) if sample else ""))

    def stop(self) -> None:
        for nd in self.nodes:
            nd.stop()
        time.sleep(0.15)
        if self.owns_root:
            shutil.rmtree(self.root, ignore_errors=True)

    def __enter__(self) -> "LocalCluster":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
