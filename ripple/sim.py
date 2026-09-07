from __future__ import annotations

import heapq
import itertools
import json
import random
from typing import Dict, List, Optional, Set, Tuple


class SimConfig:
    def __init__(self, **kw):
        self.gossip_interval = kw.get("gossip_interval", 0.35)
        self.gossip_fanout = kw.get("gossip_fanout", 3)
        self.gossip_cost = kw.get("gossip_cost", 0.004)
        self.intra_rack_rtt = kw.get("intra_rack_rtt", 0.0004)
        self.inter_rack_rtt = kw.get("inter_rack_rtt", 0.004)
        self.upload_slots = kw.get("upload_slots", 4)
        self.download_slots = kw.get("download_slots", 8)
        self.slot_bandwidth = kw.get("slot_bandwidth", 60e6)
        self.racks = kw.get("racks", 4)
        self.chunk_size = kw.get("chunk_size", 8192)
        self.manifest_overhead = kw.get("manifest_overhead", 0.0008)
        self.host_slots = kw.get("host_slots", 0)
        self.host_throughput = kw.get("host_throughput", 0)
        self.peer_working_set = kw.get("peer_working_set", 24)
        self.plan_window = kw.get("plan_window", 48)
        self.plan_refresh = kw.get("plan_refresh", 24)
        self.fetch_tick = kw.get("fetch_tick", 0.05)
        self.stall_after = kw.get("stall_after", 5.0)
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
        self.acq: Dict[int, int] = {}
        self.seq = 0
        self.knows_manifest = False
        self.up_busy = 0
        self.down_busy = 0
        self.alive = True
        self.done_at: Optional[float] = None
        self.believed: Dict[int, Tuple["SimNode", int]] = {}
        self.served_bytes = 0
        self.fetched_bytes = 0
        self.queue: List[int] = []
        self.queue_age = 0
        self.retry_armed = False
        self.salt = 0

    def gain(self, piece: int) -> None:
        if piece not in self.have:
            self.have.add(piece)
            self.missing.discard(piece)
            self.seq += 1
            self.acq[piece] = self.seq


def _believed_has(entry: Tuple[SimNode, int], piece: int) -> bool:
    peer, snap = entry
    a = peer.acq.get(piece)
    return a is not None and a <= snap


class Simulation:

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
        self._alive: List[int] = list(range(n_nodes))
        self.manifest_known_at: Dict[int, float] = {}
        self.transfers = 0
        self.bytes_moved = 0
        self.gossip_messages = 0
        self._host_busy = 0
        self._inflight = 0
        self.last_progress = 0.0
        self.stalled = False
        self.n_complete = 0
        self.log: List[dict] = []

    def _at(self, t: float, kind: str, args: tuple = ()) -> None:
        heapq.heappush(self._q, (t, next(self._seq), kind, args))

    def rtt(self, a: SimNode, b: SimNode) -> float:
        base = self.cfg.intra_rack_rtt if a.rack == b.rack else self.cfg.inter_rack_rtt
        return base * self.rng.uniform(0.8, 1.4)

    def _pick_alive(self, k: int, exclude: int) -> List[SimNode]:
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

    def _gossip(self, nid: int) -> None:
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
        self._remember(d, s)
        self._remember(s, d)
        if s.knows_manifest and not d.knows_manifest:
            self._learn_manifest(d, self.now + self.cfg.manifest_overhead)

    def _remember(self, node: SimNode, peer: SimNode) -> None:
        b = node.believed
        b[peer.nid] = (peer, peer.seq)
        cap = self.cfg.peer_working_set
        if len(b) > cap:
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
        node = self.nodes[nid]
        if not node.alive or not node.knows_manifest:
            return
        if not node.missing:
            if node.done_at is None:
                node.done_at = self.now
                self.n_complete += 1
            return
        if node.down_busy >= self.cfg.download_slots:
            return

        believed = list(node.believed.values())
        if self.source_policy == "star":
            believed = [e for e in believed if e[0].nid == 0]

        node.queue = [p for p in node.queue if p in node.missing]
        if not node.queue or node.queue_age >= self.cfg.plan_refresh:
            missing = node.missing
            if len(missing) > self.cfg.plan_window:
                cand = self.rng.sample(list(missing), self.cfg.plan_window)
            else:
                cand = list(missing)
            rarity = {c: sum(1 for e in believed if _believed_has(e, c)) for c in cand}
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
                break
            holders = [e[0] for e in believed
                       if _believed_has(e, piece) and e[0].alive
                       and e[0].up_busy < self.cfg.upload_slots]
            if not holders:
                continue
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

        if not started and node.missing and not node.retry_armed:
            node.retry_armed = True
            self._at(self.now + 0.1, "retry", (nid,))

    def _next_tick(self) -> float:
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

    def run(self, horizon: float = 3600.0) -> dict:
        src = self.nodes[0]
        for p in range(self.n_chunks):
            src.gain(p)
        src.knows_manifest = True
        src.done_at = 0.0
        self.n_complete = 1
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
            check += 1
            if check % 256 == 0:
                if self._all_done():
                    break
                if (self._inflight == 0
                        and self.now - self.last_progress > self.cfg.stall_after):
                    self.stalled = True
                    break
        return self.report()

    def _all_done(self) -> bool:
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
