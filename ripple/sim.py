"""Discrete-event simulator for the Ripple protocol.

A laptop runs about fifty real nodes before thread scheduling, not the protocol,
is what gets measured. This models the rest: gossip with random fanout, manifest
propagation, rarest-piece-first scheduling from each node's partial view,
latency-aware peer selection, concurrency limits and node failure. No I/O, so a
1,000-node run takes seconds.

Belief is snapshot-based -- a node learns what a peer held when they last
gossiped, never what it holds now -- and each node tracks a bounded working set
of peers, because nobody has a global view here or in production.

host_slots and host_throughput model the harness, one machine shared by all N
nodes; zero (the default) is the deployment case. bench/validate.py checks all
of this against the real cluster.
"""

from __future__ import annotations

import heapq
import itertools
import json
import random
from typing import Dict, List, Optional, Set, Tuple


class SimConfig:
    def __init__(self, **kw):
        # Defaults calibrated against the loopback cluster; see bench/validate.py.
        self.gossip_interval = kw.get("gossip_interval", 0.35)
        self.gossip_fanout = kw.get("gossip_fanout", 3)
        self.gossip_cost = kw.get("gossip_cost", 0.004)        # seconds per round
        self.intra_rack_rtt = kw.get("intra_rack_rtt", 0.0004)
        self.inter_rack_rtt = kw.get("inter_rack_rtt", 0.004)
        self.upload_slots = kw.get("upload_slots", 4)
        self.download_slots = kw.get("download_slots", 8)
        self.slot_bandwidth = kw.get("slot_bandwidth", 60e6)   # bytes/s per slot
        self.racks = kw.get("racks", 4)
        self.chunk_size = kw.get("chunk_size", 8192)
        self.manifest_overhead = kw.get("manifest_overhead", 0.0008)
        # Shared-host modelling. A real cluster gives every node its own CPU and NIC,
        # so host_slots = 0 is the deployment case. Our loopback harness runs all N
        # nodes in one process on one machine and saturates at a fixed aggregate
        # throughput, which makes its convergence linear in N for reasons that have
        # nothing to do with the protocol.
        self.host_slots = kw.get("host_slots", 0)            # 0 = unlimited
        self.host_throughput = kw.get("host_throughput", 0)  # bytes/s, aggregate
        self.peer_working_set = kw.get("peer_working_set", 24)
        self.plan_window = kw.get("plan_window", 48)
        self.plan_refresh = kw.get("plan_refresh", 24)   # pumps between re-rankings
        # The real node re-plans on a timer (node.py _fetch_loop) rather than the
        # instant a chunk lands, and starts at most plan_limit fetches per pass.
        self.fetch_tick = kw.get("fetch_tick", 0.05)
        self.stall_after = kw.get("stall_after", 5.0)   # sim-seconds without progress
        self.plan_limit = kw.get("plan_limit", 16)
        self.seed = kw.get("seed", 42)

    def to_dict(self) -> dict:
        return dict(self.__dict__)


class SimNode:
    __slots__ = ("nid", "rack", "have", "missing", "acq", "seq", "knows_manifest",
                 "up_busy", "down_busy", "alive", "done_at", "believed",
                 "served_bytes", "fetched_bytes", "queue", "queue_age", "retry_armed",
                 "salt")

    def __init__(self, nid: int, rack: int):
        self.nid = nid
        self.rack = rack
        self.have: Set[int] = set()
        self.missing: Set[int] = set()
        # acq[piece] = seq when this node obtained it, so any observer can reconstruct
        # what the node held at time T without anyone copying a set.
        self.acq: Dict[int, int] = {}
        self.seq = 0
        self.knows_manifest = False
        self.up_busy = 0
        self.down_busy = 0
        self.alive = True
        self.done_at: Optional[float] = None
        # believed[peer] = (peer_node, seq_at_last_gossip)
        self.believed: Dict[int, Tuple["SimNode", int]] = {}
        self.served_bytes = 0
        self.fetched_bytes = 0
        # Rarity-ordered work queue, refreshed periodically rather than per event.
        self.queue: List[int] = []
        self.queue_age = 0
        self.retry_armed = False
        # Per-node tiebreak salt, mirroring scheduler.py. Without it every node walks
        # an identical piece order and has nothing to trade.
        self.salt = 0

    def gain(self, piece: int) -> None:
        if piece not in self.have:
            self.have.add(piece)
            self.missing.discard(piece)
            self.seq += 1
            self.acq[piece] = self.seq


def _believed_has(entry: Tuple[SimNode, int], piece: int) -> bool:
    """Did `peer` hold `piece` as of the snapshot we took of it?"""
    peer, snap = entry
    a = peer.acq.get(piece)
    return a is not None and a <= snap


class Simulation:
    """One experiment: publish a file on node 0, watch N nodes converge."""

    def __init__(self, n_nodes: int, n_pieces: int, cfg: Optional[SimConfig] = None,
                 source_policy: str = "swarm", kill_source_at: Optional[float] = None,
                 kill_fraction: float = 0.0, kill_at: Optional[float] = None,
                 kill_source_after_coverage: bool = False):
        self.cfg = cfg or SimConfig()
        self.n = n_nodes
        self.n_chunks = n_pieces
        self.source_policy = source_policy
        self.kill_source_at = kill_source_at
        self.kill_fraction = kill_fraction
        self.kill_at = kill_at
        # Kill the publisher once every piece exists somewhere else. Before coverage,
        # some pieces exist in one place and no design recovers them.
        self.kill_source_after_coverage = kill_source_after_coverage
        self.coverage_at: Optional[float] = None
        self._covered: Set[int] = set()
        self.rng = random.Random(self.cfg.seed)
        self.now = 0.0
        self._q: List[Tuple[float, int, str, tuple]] = []
        self._seq = itertools.count()
        self.nodes = [SimNode(i, i % self.cfg.racks) for i in range(n_nodes)]
        for _nd in self.nodes:
            _nd.salt = self.rng.getrandbits(30)
        self._alive: List[int] = list(range(n_nodes))   # cached; O(N) rebuild on death
        self.manifest_known_at: Dict[int, float] = {}
        self.transfers = 0
        self.bytes_moved = 0
        self.gossip_messages = 0
        self._host_busy = 0        # transfers in flight cluster-wide (shared-host mode)
        self._inflight = 0         # transfers in flight, always counted
        self.last_progress = 0.0   # sim-time of the most recent delivery
        self.stalled = False
        self.n_complete = 0        # live nodes that know the file and hold all of it
        self.log: List[dict] = []

    # --- event plumbing -----------------------------------------------------

    def _at(self, t: float, kind: str, args: tuple = ()) -> None:
        heapq.heappush(self._q, (t, next(self._seq), kind, args))

    def rtt(self, a: SimNode, b: SimNode) -> float:
        base = self.cfg.intra_rack_rtt if a.rack == b.rack else self.cfg.inter_rack_rtt
        return base * self.rng.uniform(0.8, 1.4)

    def _pick_alive(self, k: int, exclude: int) -> List[SimNode]:
        """k distinct random live peers.

        Samples a cached id list so a gossip round is O(k) rather than O(N).
        """
        alive = self._alive
        if len(alive) <= 1:
            return []
        out, seen = [], {exclude}
        tries = 0
        while len(out) < k and tries < k * 6:
            tries += 1
            nid = alive[self.rng.randrange(len(alive))]
            if nid in seen:
                continue
            seen.add(nid)
            out.append(self.nodes[nid])
        return out

    # --- protocol behaviour -------------------------------------------------

    def _gossip(self, nid: int) -> None:
        """One gossip round for one node, then reschedule itself."""
        node = self.nodes[nid]
        if not node.alive:
            return
        for t in self._pick_alive(self.cfg.gossip_fanout, nid):
            self.gossip_messages += 1
            self._at(self.now + self.rtt(node, t) + self.cfg.gossip_cost,
                     "gossip_recv", (nid, t.nid))
        self._at(self.now + self.cfg.gossip_interval * self.rng.uniform(0.85, 1.15),
                 "gossip", (nid,))

    def _gossip_recv(self, src: int, dst: int) -> None:
        s, d = self.nodes[src], self.nodes[dst]
        if not d.alive or not s.alive:
            return
        # The availability summary rides along with the gossip: this is the Bloom
        # filter exchange, minus the false positives.
        self._remember(d, s)
        self._remember(s, d)
        if s.knows_manifest and not d.knows_manifest:
            self._learn_manifest(d, self.now + self.cfg.manifest_overhead)

    def _remember(self, node: SimNode, peer: SimNode) -> None:
        b = node.believed
        b[peer.nid] = (peer, peer.seq)
        cap = self.cfg.peer_working_set
        if len(b) > cap:
            # Evict at random rather than by age: the real node's table is
            # refreshed by random gossip partners, so what it can reason about is
            # a random sample of the cluster, not its oldest or nearest peers.
            victims = [k for k in b if k != peer.nid]
            for v in self.rng.sample(victims, len(b) - cap):
                del b[v]

    def _learn_manifest(self, node: SimNode, at: float) -> None:
        if node.knows_manifest:
            return
        node.knows_manifest = True
        self.manifest_known_at[node.nid] = at
        node.missing = set(range(self.n_chunks)) - node.have
        self._at(at, "pump", (node.nid,))

    def _pump(self, nid: int) -> None:
        """Start as many fetches as this node's slots and the swarm allow."""
        node = self.nodes[nid]
        if not node.alive or not node.knows_manifest:
            return
        if not node.missing:
            if node.done_at is None:
                node.done_at = self.now
                self.n_complete += 1
            return
        if node.down_busy >= self.cfg.download_slots:
            return  # nothing to decide until a slot frees

        believed = list(node.believed.values())
        if self.source_policy == "star":
            believed = [e for e in believed if e[0].nid == 0]

        # Rank a bounded window, as scheduler.py does.
        node.queue = [p for p in node.queue if p in node.missing]
        if not node.queue or node.queue_age >= self.cfg.plan_refresh:
            missing = node.missing
            if len(missing) > self.cfg.plan_window:
                cand = self.rng.sample(list(missing), self.cfg.plan_window)
            else:
                cand = list(missing)
            rarity = {c: sum(1 for e in believed if _believed_has(e, c)) for c in cand}
            # Super-seeding, mirroring scheduler.py: take from the swarm what it can
            # supply and ask the publisher only for what nothing else has.
            swarm_has = {c: any(e[0].nid != 0 and _believed_has(e, c) for e in believed)
                         for c in cand} if self.source_policy != "star" else {}
            salt = node.salt
            cand.sort(key=lambda c: (0 if swarm_has.get(c) else 1, rarity[c],
                                     (c * 2654435761 ^ salt) & 0xFFFFFFFF))
            node.queue = cand
            node.queue_age = 0
        node.queue_age += 1

        started: List[int] = []
        host_cap = self.cfg.host_slots
        plan_limit = self.cfg.plan_limit
        for piece in list(node.queue):
            if node.down_busy >= self.cfg.download_slots or len(started) >= plan_limit:
                break
            if host_cap and self._host_busy >= host_cap:
                break   # the machine itself is saturated, not this node
            holders = [e[0] for e in believed
                       if _believed_has(e, piece) and e[0].alive
                       and e[0].up_busy < self.cfg.upload_slots]
            if not holders:
                continue
            # Nearest first: same rack, then load.
            holders.sort(key=lambda h: (1 if (h.nid == 0 and self.source_policy != "star")
                                        else 0,
                                        0 if h.rack == node.rack else 1,
                                        h.up_busy))
            src = holders[0]
            src.up_busy += 1
            node.down_busy += 1
            self._host_busy += 1
            self._inflight += 1
            shared = host_cap and self.cfg.host_throughput
            bw = self.cfg.host_throughput / host_cap if shared else self.cfg.slot_bandwidth
            dur = 2 * self.rtt(node, src) + self.cfg.chunk_size / bw
            self._at(self.now + dur, "chunk_done", (src.nid, node.nid, piece))
            started.append(piece)
        for piece in started:
            node.queue.remove(piece)

        # If nothing could be started -- every believed holder is saturated, or
        # we simply do not know anyone who has these pieces yet -- the node must
        # wake itself up and look again. Without this it waits for an event that
        # is never coming: real nodes re-plan on a 50 ms timer, and the model has
        # to do the same or it reports stalls the real system does not have.
        if not started and node.missing and not node.retry_armed:
            node.retry_armed = True
            self._at(self.now + 0.1, "retry", (nid,))

    def _next_tick(self) -> float:
        """When the fetch loop next wakes.

        Aligning to a tick grid reproduces the real behaviour: a chunk landing just
        before a tick is replaced almost immediately, one landing just after waits.
        """
        t = self.cfg.fetch_tick
        if t <= 0:
            return self.now
        return (int(self.now / t) + 1) * t

    def _retry(self, nid: int) -> None:
        self.nodes[nid].retry_armed = False
        self._pump(nid)

    def _chunk_done(self, src_id: int, dst_id: int, piece: int) -> None:
        src, dst = self.nodes[src_id], self.nodes[dst_id]
        src.up_busy = max(0, src.up_busy - 1)
        dst.down_busy = max(0, dst.down_busy - 1)
        self._host_busy = max(0, self._host_busy - 1)
        self._inflight = max(0, self._inflight - 1)
        if not dst.alive:
            return
        if not src.alive:
            # Sender died in flight: nothing is delivered and the receiver re-plans the
            # piece against someone else.
            self._at(self.now + 0.01, "pump", (dst_id,))
            return
        dst.gain(piece)
        if dst.nid != 0 and piece not in self._covered:
            self._covered.add(piece)
            if len(self._covered) >= self.n_chunks and self.coverage_at is None:
                self.coverage_at = self.now
                if self.kill_source_after_coverage:
                    self._kill(0)
        src.served_bytes += self.cfg.chunk_size
        dst.fetched_bytes += self.cfg.chunk_size
        self.transfers += 1
        self.bytes_moved += self.cfg.chunk_size
        self.last_progress = self.now
        if not dst.missing and dst.done_at is None:
            dst.done_at = self.now
            self.n_complete += 1
        self._at(self._next_tick(), "pump", (dst_id,))

    def _kill(self, nid: int) -> None:
        node = self.nodes[nid]
        if not node.alive:
            return
        node.alive = False
        if node.done_at is not None:
            self.n_complete -= 1
        self._alive = [i for i in self._alive if i != nid]
        self.log.append({"t": round(self.now, 4), "event": "kill", "node": nid})

    def _kill_many(self, fraction: float) -> None:
        victims = self.rng.sample([i for i in self._alive if i != 0],
                                  int(fraction * (self.n - 1)))
        for v in victims:
            self._kill(v)

    # --- driver -------------------------------------------------------------

    def run(self, horizon: float = 3600.0) -> dict:
        src = self.nodes[0]
        for p in range(self.n_chunks):
            src.gain(p)
        src.knows_manifest = True
        src.done_at = 0.0
        self.n_complete = 1   # the publisher counts as complete from t=0
        self.manifest_known_at[0] = 0.0
        for node in self.nodes:
            node.believed[0] = (src, src.seq)
            self._at(self.rng.uniform(0, self.cfg.gossip_interval), "gossip", (node.nid,))
        if self.kill_source_at is not None:
            self._at(self.kill_source_at, "kill", (0,))
        if self.kill_fraction and self.kill_at is not None:
            self._at(self.kill_at, "kill_many", (self.kill_fraction,))

        handlers = {"gossip": self._gossip, "gossip_recv": self._gossip_recv,
                    "pump": self._pump, "chunk_done": self._chunk_done,
                    "kill": self._kill, "kill_many": self._kill_many,
                    "retry": self._retry}
        check = 0
        while self._q:
            t, _, kind, args = heapq.heappop(self._q)
            if t > horizon:
                break
            self.now = t
            handlers[kind](*args)
            # Completion is checked periodically; the test is O(N).
            check += 1
            if check % 256 == 0:
                if self._all_done():
                    break
                # Stop when no progress is possible rather than grinding to the horizon.
                if (self._inflight == 0
                        and self.now - self.last_progress > self.cfg.stall_after):
                    self.stalled = True
                    break
        return self.report()

    def _all_done(self) -> bool:
        """Every live node knows the file and holds all of it.

        Checking only `missing` was wrong: a node that has not heard the manifest has an
        empty missing-set for the trivial reason that it does not know what it lacks.
        """
        # O(1) via a running count of completed live nodes.
        if self.n_complete < len(self._alive):
            return False
        for n in self.nodes:
            if n.alive and (not n.knows_manifest or n.missing):
                return False
        return True

    def report(self) -> dict:
        alive = [n for n in self.nodes if n.alive]
        done = [n.done_at for n in alive if n.done_at is not None]
        mt = list(self.manifest_known_at.values())
        file_bytes = self.n_chunks * self.cfg.chunk_size
        return {
            "nodes": self.n,
            "pieces": self.n_chunks,
            "file_bytes": int(file_bytes),
            "policy": self.source_policy,
            "alive": len(alive),
            "converged": len(done) == len(alive),
            "stalled": self.stalled,
            "coverage_s": round(self.coverage_at, 4) if self.coverage_at else None,
            "convergence_s": round(max(done), 4) if done else None,
            "manifest_all_known_s": round(max(mt), 4) if mt else None,
            "source_egress": int(self.nodes[0].served_bytes),
            "source_egress_ratio": round(self.nodes[0].served_bytes / max(1, file_bytes), 2),
            "total_bytes_moved": int(self.bytes_moved),
            "gossip_messages": self.gossip_messages,
            "transfers": self.transfers,
        }


def sweep(node_counts, file_bytes: int, cfg: Optional[SimConfig] = None,
          policy: str = "swarm", pieces: int = 512, **sim_kw) -> List[dict]:
    """Run the same experiment at several cluster sizes.

    `pieces` is modelling resolution, not the real chunk size: event count is
    nodes x pieces, so a 64 MB file at its true 8 KB granularity would be 8 million
    transfer events for a result that barely moves.
    """
    base = (cfg or SimConfig()).to_dict()
    out = []
    for n in node_counts:
        c = SimConfig(**base)
        n_pieces = max(1, min(pieces, max(1, file_bytes // 4096)))
        c.chunk_size = file_bytes / n_pieces
        out.append(Simulation(n, n_pieces, c, source_policy=policy, **sim_kw).run())
    return out


def main() -> int:
    import argparse
    import time
    ap = argparse.ArgumentParser(description="Ripple protocol simulator")
    ap.add_argument("--nodes", default="10,50,100,300,1000")
    ap.add_argument("--size", type=int, default=64 << 20, help="file size in bytes")
    ap.add_argument("--policy", default="swarm", choices=["swarm", "star"])
    ap.add_argument("--pieces", type=int, default=512, help="modelling resolution")
    ap.add_argument("--kill-fraction", type=float, default=0.0)
    ap.add_argument("--kill-at", type=float, default=None)
    ap.add_argument("--json", default="")
    a = ap.parse_args()

    counts = [int(x) for x in a.nodes.split(",")]
    kw = {}
    if a.kill_fraction:
        kw = {"kill_fraction": a.kill_fraction, "kill_at": a.kill_at or 0.5}
    t0 = time.perf_counter()
    rows = sweep(counts, a.size, policy=a.policy, pieces=a.pieces, **kw)
    wall = time.perf_counter() - t0

    print("file %.0f MB, policy=%s, %d pieces\n" % (a.size / 1e6, a.policy, a.pieces))
    print("%-7s %-13s %-14s %-14s %-9s %s"
          % ("nodes", "converge(s)", "manifest(s)", "src egress", "x file", "converged"))
    for r in rows:
        print("%-7d %-13s %-14s %-14s %-9s %s"
              % (r["nodes"], r["convergence_s"], r["manifest_all_known_s"],
                 "%.1f MB" % (r["source_egress"] / 1e6), r["source_egress_ratio"],
                 r["converged"]))
    print("\n(%d simulated runs in %.1fs of wall clock)" % (len(rows), wall))
    if a.json:
        with open(a.json, "w") as fh:
            json.dump(rows, fh, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
