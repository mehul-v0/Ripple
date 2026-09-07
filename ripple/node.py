"""The node: gossip, lazy reads, anti-entropy, scrub.

Reading order: publish() takes a file in, _gossip_round() spreads its manifest,
read() serves a range on a node holding no bytes, _fetch_loop() pulls chunks in
the background, _antientropy() repairs what gossip missed, _scrub() catches disks
that lie.

fetch_policy and source_policy configure a node to behave like the naive designs
(materialise everything; fetch only from the publisher) so the benchmark can
compare against them using this same code.
"""

from __future__ import annotations

import json
import os
import socket
import socketserver
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set, Tuple

from . import protocol as P
from .bloom import BloomFilter
from .chaos import Chaos
from .chunker import Chunker
from .manifest import PARTIAL, PHANTOM, RESIDENT, Manifest, ManifestStore
from .merkle import MerkleTree, differing_buckets
from .metrics import Metrics
from .peers import Membership, Peer
from .scheduler import ChunkAvailability, Scheduler
from .store import ChunkStore, atomic_write
from . import structured
from .versions import AFTER, BEFORE, CONCURRENT, EQUAL, bump, compare, merge

GOSSIP_INTERVAL = 0.35
GOSSIP_FANOUT = 3
PREFETCH_AHEAD = 8
SCRUB_INTERVAL = 5.0
# Every chunk is verified at least once per this many seconds. Short here so a
# demo shows self-healing; production would set it to hours.
SCRUB_PERIOD = 20.0
ANTIENTROPY_INTERVAL = 4.0
# How many chunk reads a node serves at once. A real node is limited by its
# NIC; on loopback nothing is, so without a cap the publisher can serve the
# whole cluster and the benchmark measures a network we do not have.
UPLOAD_SLOTS = 4


class RippleNode:
    def __init__(self, node_id: str, host: str = "127.0.0.1", port: int = 0,
                 root: Optional[str] = None, rack: str = "r0",
                 fetch_policy: str = "lazy", source_policy: str = "swarm",
                 fanout: int = GOSSIP_FANOUT, fsync: bool = False,
                 upload_slots: int = UPLOAD_SLOTS,
                 scrub_period: float = SCRUB_PERIOD,
                 super_seed: bool = False, salt: bool = True):
        self.node_id = node_id
        self.rack = rack
        self.root = root or os.path.join(".ripple", node_id)
        os.makedirs(self.root, exist_ok=True)

        self.store = ChunkStore(self.root, fsync=fsync)
        self.manifests = ManifestStore(self.root)
        self.chunker = Chunker()
        self.metrics = Metrics(node_id)
        self.chaos = Chaos(node_id)
        self.avail = ChunkAvailability()
        self.sched = Scheduler(self.avail, super_seed=super_seed, salt=salt)

        self.fetch_policy = fetch_policy      # lazy | eager
        self.source_policy = source_policy    # swarm | star
        self.fanout = fanout

        self._clients: Dict[str, P.PeerClient] = {}
        self._clients_lock = threading.Lock()
        self._want: Set[str] = set()          # chunk hashes we are trying to get
        self._want_lock = threading.Lock()
        self._materialising: Set[str] = set()  # paths targeted for full residency
        self._stop = threading.Event()
        self._serving = False
        self._threads: List[threading.Thread] = []
        # Two pools: control-plane work (pulling a manifest after an advert) blocks on
        # network I/O and would otherwise occupy every worker and starve the transfers
        # it just scheduled.
        self._pool = ThreadPoolExecutor(max_workers=12,
                                        thread_name_prefix="rf-%s" % node_id)
        self._ctrl = ThreadPoolExecutor(max_workers=4,
                                        thread_name_prefix="rc-%s" % node_id)
        self._merkle_dirty = True
        self._merkle = MerkleTree()
        self._upload = threading.Semaphore(upload_slots)
        self.upload_slots = upload_slots
        self.scrub_period = scrub_period
        self._scrub_cursor = 0

        self._server = _Server((host, port), self)
        self.host, self.port = self._server.server_address[:2]
        self.membership = Membership(node_id, self.host, self.port, rack)

    # ---------------------------------------------------------------- lifecycle

    def start(self) -> "RippleNode":
        self._serving = True
        t = threading.Thread(target=self._server.serve_forever, daemon=True,
                             name="srv-%s" % self.node_id)
        t.start()
        self._threads.append(t)
        for fn, name in ((self._gossip_loop, "gossip"), (self._fetch_loop, "fetch"),
                         (self._maint_loop, "maint")):
            th = threading.Thread(target=fn, daemon=True,
                                  name="%s-%s" % (name, self.node_id))
            th.start()
            self._threads.append(th)
        self.metrics.event("node", "node %s listening on %s:%d" %
                           (self.node_id, self.host, self.port))
        return self

    def stop(self) -> None:
        self._stop.set()
        try:
            # shutdown() blocks until serve_forever() acknowledges, so it must not be
            # called on a node that was constructed but never started.
            if self._serving:
                self._serving = False
                self._server.shutdown()
            self._server.server_close()
        except Exception:
            pass
        self._pool.shutdown(wait=False)
        self._ctrl.shutdown(wait=False)
        with self._clients_lock:
            for c in self._clients.values():
                c.close()
            self._clients.clear()

    def join(self, host: str, port: int) -> bool:
        """Introduce ourselves to a seed and adopt its view of the cluster.

        One reachable seed is enough; gossip does the rest.
        """
        try:
            client = P.PeerClient((host, port))
            head, _, sent, recv = client.request(
                {"type": P.HELLO, **self.membership.self_record()})
            self.metrics.incr("bytes_sent", sent)
            self.metrics.incr("bytes_recv", recv)
            self.membership.add(head["id"], host, port, head.get("rack", "r0"))
            self.membership.merge(head.get("peers", []))
            with self._clients_lock:
                self._clients[head["id"]] = client
            return True
        except Exception as e:
            self.metrics.event("error", "join %s:%d failed: %s" % (host, port, e))
            return False

    # ------------------------------------------------------------------ helpers

    def _client(self, peer: Peer) -> P.PeerClient:
        with self._clients_lock:
            c = self._clients.get(peer.node_id)
            if c is None or c.addr != peer.addr:
                if c is not None:
                    c.close()
                c = P.PeerClient(peer.addr)
                self._clients[peer.node_id] = c
            return c

    def _ask(self, peer: Peer, msg: dict, payload: bytes = b"") -> Optional[Tuple[dict, bytes]]:
        """One request to one peer, with fault injection and accounting applied.

        All outbound messages funnel through here, so the chaos hooks cannot be bypassed
        and the byte counters cannot miss anything.
        """
        if self.chaos.blocks(peer.node_id) or self.chaos.maybe_drop():
            raise ConnectionError("chaos: link to %s is down" % peer.node_id)
        # Stamp every request with our identity. Only hello and gossip used to carry
        # it, so a partitioned node still answered manifest_get/merkle_get from peers
        # it was supposedly cut off from -- anti-entropy tunnelled through partitions.
        msg = dict(msg, **{"from": self.node_id})
        t0 = time.perf_counter()
        try:
            head, body, sent, recv = self._client(peer).request(msg, payload)
        except Exception:
            peer.failures += 1
            self.avail.forget(peer.node_id)
            self.sched.release_peer(peer.node_id)
            with self._clients_lock:
                c = self._clients.pop(peer.node_id, None)
            if c:
                c.close()
            raise
        self.chaos.delay(sent + recv)
        dt = time.perf_counter() - t0
        peer.observe_rtt(dt)
        peer.failures = 0
        self.metrics.incr("bytes_sent", sent)
        self.metrics.incr("bytes_recv", recv)
        self.metrics.incr("msgs_sent")
        return head, body

    def have_filter(self) -> BloomFilter:
        hashes = self.store.hashes()
        bf = BloomFilter.for_items(max(64, len(hashes)), 0.02)
        for h in hashes:
            bf.add(h)
        return bf

    def merkle(self) -> MerkleTree:
        """Merkle tree over (path -> current manifest digest)."""
        if self._merkle_dirty:
            t = MerkleTree()
            for path, dg in list(self.manifests.refs.items()):
                t.set(path, dg)
            self._merkle = t
            self._merkle_dirty = False
        return self._merkle

    # ------------------------------------------------------------------ publish

    def publish(self, path: str, data: Optional[bytes] = None,
                src_file: Optional[str] = None, note: str = "") -> Manifest:
        """Chunk a file, store it locally, and announce it to the cluster.

        The chunk-size profile comes from the file's size (chunker.PROFILES).
        """
        t0 = time.perf_counter()
        chunks: List[str] = []
        sizes: List[int] = []
        labels: Optional[List[str]] = None
        total = 0
        if src_file:
            chunker = Chunker.for_file_size(os.path.getsize(src_file))
            for c in chunker.chunk_file(src_file):
                self.store.put(c.data, c.hash)
                chunks.append(c.hash)
                sizes.append(c.size)
                total += c.size
        else:
            data = data or b""
            # Config-shaped files get cut on their own grammar instead, one
            # chunk per top-level key. Costs nothing when it does not apply --
            # split() declines and we fall through to content-defined chunking.
            pieces = structured.split(path, data)
            if pieces:
                labels = []
                for label, blob in pieces:
                    h = self.store.put(blob)
                    chunks.append(h)
                    sizes.append(len(blob))
                    labels.append(label)
                    total += len(blob)
            else:
                chunker = Chunker.for_file_size(len(data))
                for c in chunker.chunk_bytes(data):
                    self.store.put(c.data, c.hash)
                    chunks.append(c.hash)
                    sizes.append(c.size)
                    total += c.size

        prev = self.manifests.current(path)
        vv = bump(prev.vv, self.node_id) if prev else {self.node_id: 1}
        m = Manifest(path, total, chunks, sizes, vv, self.node_id, note=note,
                     labels=labels)
        self.manifests.set_current(m)
        self._merkle_dirty = True

        chunk_time = time.perf_counter() - t0
        self.metrics.observe("publish_seconds", chunk_time)
        self.metrics.incr("files_published")
        self.metrics.event("publish", "published %s (%d chunks, manifest %d B)" %
                           (path, len(chunks), m.nbytes()), path=path)
        # Push immediately rather than waiting for the next gossip tick.
        self._ctrl.submit(self._gossip_round, True)
        return m

    def edit(self, path: str, offset: int, data: bytes) -> Optional[Manifest]:
        """Splice bytes into an existing file and republish.

        Only chunks overlapping the edit change identity, so only those are new to the
        cluster.
        """
        m = self.manifests.current(path)
        if not m:
            return None
        full = bytearray(self.read(path, 0, m.size))
        end = offset + len(data)
        if end > len(full):
            full.extend(b"\0" * (end - len(full)))
        full[offset:end] = data
        before = set(m.chunks)
        new = self.publish(path, bytes(full), note="edit at %d" % offset)
        changed = [c for c in new.unique_chunks() if c not in before]
        moved = sum(new.sizes[new.chunks.index(c)] for c in changed)
        self.metrics.event("edit", "%s: %d/%d chunks changed, %d B new of %d B file"
                           % (path, len(changed), len(new.chunks), moved, new.size),
                           path=path, changed_bytes=moved, file_bytes=new.size)
        self.metrics.gauge("last_edit_new_bytes", moved)
        self.metrics.gauge("last_edit_file_bytes", new.size)
        return new

    def rollback(self, path: str, digest: str) -> Optional[Manifest]:
        """Restore a previous version cluster-wide.

        Costs one manifest write: the old chunks were never deleted, so this is a pointer
        change rather than a data restore, independent of file size.
        """
        old = self.manifests.get(digest)
        if not old or old.path != path:
            return None
        cur = self.manifests.current(path)
        vv = bump(cur.vv if cur else {}, self.node_id)
        restored = Manifest(path, old.size, list(old.chunks), list(old.sizes), vv,
                            self.node_id, note="rollback to %s" % digest[:12])
        self.manifests.set_current(restored)
        self._merkle_dirty = True
        self.metrics.event("rollback", "%s rolled back to %s" % (path, digest[:12]),
                           path=path)
        self._ctrl.submit(self._gossip_round, True)
        return restored

    # --------------------------------------------------------------------- read

    def state_of(self, path: str) -> str:
        m = self.manifests.current(path)
        if not m:
            return "ABSENT"
        uniq = m.unique_chunks()
        if not uniq:
            return RESIDENT
        have = sum(1 for c in uniq if self.store.has(c))
        if have == 0:
            return PHANTOM
        return RESIDENT if have == len(uniq) else PARTIAL

    def read(self, path: str, offset: int = 0, length: Optional[int] = None,
             timeout: float = 30.0) -> bytes:
        """Read a byte range, materialising only what the read touches.

        A PHANTOM file is readable: the manifest says which chunks back the range, we
        fetch those and nothing else.
        """
        m = self.manifests.current(path)
        if not m:
            raise FileNotFoundError(path)
        if length is None:
            length = m.size - offset
        idxs = m.chunks_for_range(offset, length)
        needed = [m.chunks[i] for i in idxs]
        missing = self.store.missing(needed)
        if missing:
            t0 = time.perf_counter()
            self.metrics.incr("reads_faulted")
            self._fetch_now(missing, timeout)
            self.metrics.observe("read_fault_seconds", time.perf_counter() - t0)

        # Reads are overwhelmingly sequential, so pull the next few chunks in the
        # background.
        if idxs:
            ahead = m.chunks[idxs[-1] + 1: idxs[-1] + 1 + PREFETCH_AHEAD]
            if ahead:
                self._add_want(self.store.missing(ahead))
                self.metrics.incr("prefetch_queued", len(ahead))

        offsets = m.chunk_offsets()
        buf = bytearray()
        for i in idxs:
            data = self.store.get(m.chunks[i])
            if data is None:
                raise IOError("chunk %s unavailable for %s" % (m.chunks[i][:8], path))
            buf.extend(data)
        start = offset - (offsets[idxs[0]] if idxs else 0)
        self.metrics.incr("bytes_read", length)
        return bytes(buf[start:start + length])

    def materialise(self, path: str, timeout: float = 60.0, block: bool = True) -> bool:
        """Bring every chunk of a file local."""
        m = self.manifests.current(path)
        if not m:
            return False
        self._materialising.add(path)
        missing = self.store.missing(m.unique_chunks())
        if not missing:
            return True
        self._add_want(missing)
        if not block:
            return False
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.store.missing(m.unique_chunks()):
                self.metrics.event("materialise", "%s fully resident" % path, path=path)
                return True
            time.sleep(0.02)
        return False

    def _add_want(self, chunks) -> None:
        with self._want_lock:
            self._want.update(chunks)

    def _fetch_now(self, chunks: List[str], timeout: float) -> None:
        """Blocking fetch for a read fault, driven by the same scheduler."""
        self._add_want(chunks)
        deadline = time.time() + timeout
        while time.time() < deadline:
            remaining = self.store.missing(chunks)
            if not remaining:
                return
            self._drive_fetches(remaining, blocking=True)
            if self.store.missing(chunks):
                time.sleep(0.01)
        raise TimeoutError("could not materialise %d chunk(s) in time" %
                           len(self.store.missing(chunks)))

    def _origins(self) -> Set[str]:
        return {m.origin for m in self.manifests.all_current()}

    def _candidates(self) -> List[Peer]:
        peers = [p for p in self.membership.alive()
                 if not self.chaos.blocks(p.node_id)]
        if self.source_policy == "star":
            # Baseline mode: only ever pull from the publisher.
            peers = [p for p in peers if p.node_id in self._origins()] or peers
        return peers

    def _peer_sorter(self):
        """Rank candidate sources: proximity, and optionally avoid the publisher.

        Origin avoidance is part of super-seeding and is off by default; see
        docs/ARCHITECTURE.md (L3).
        """
        base = self.membership.by_proximity
        if self.source_policy == "star" or not self.sched.super_seed:
            return base
        origins = self._origins()

        def sorter(holders: List[Peer]) -> List[Peer]:
            # sorted() is stable, so proximity order survives within each group.
            return sorted(base(holders), key=lambda p: 1 if p.node_id in origins else 0)
        return sorter

    def _drive_fetches(self, wanted: List[str], blocking: bool = False) -> int:
        peers = self._candidates()
        if not peers:
            return 0
        origins = frozenset() if self.source_policy == "star" else frozenset(self._origins())
        plans = self.sched.plan(wanted, peers, self._peer_sorter(),
                                limit=8 if blocking else 16, origins=origins)
        if not plans:
            return 0
        futures = [self._pool.submit(self._fetch_one, p) for p in plans]
        if blocking:
            for f in futures:
                try:
                    f.result(timeout=10)
                except Exception:
                    pass
        return len(plans)

    def _fetch_one(self, plan) -> bool:
        ok = False
        try:
            head, body = self._ask(plan.peer, {"type": P.CHUNK_GET, "hash": plan.chunk})
            if head.get("type") == "chunk_data" and body:
                self.store.put(body, plan.chunk)   # verifies the hash
                self.metrics.incr("chunks_fetched")
                self.metrics.incr("bytes_fetched", len(body))
                if plan.peer.rack != self.rack:
                    self.metrics.incr("cross_rack_bytes", len(body))
                ok = True
                with self._want_lock:
                    self._want.discard(plan.chunk)
            elif head.get("type") == "chunk_busy":
                # Has it but is at capacity. Leave availability alone and try someone else.
                self.metrics.incr("chunk_busy")
            else:
                # Bloom false positive, or the peer dropped it. Record the exact answer so we
                # never ask this peer again.
                self.avail.confirm(plan.peer.node_id, plan.chunk, False)
                self.metrics.incr("chunk_misses")
        except ValueError:
            self.metrics.incr("corrupt_received")
            self.metrics.event("error", "peer %s served a chunk failing verification"
                               % plan.peer.node_id)
        except Exception:
            self.metrics.incr("fetch_errors")
        finally:
            self.sched.complete(plan.chunk, plan.peer.node_id, ok)
        return ok

    # ------------------------------------------------------------------- gossip

    def _gossip_loop(self) -> None:
        while not self._stop.wait(GOSSIP_INTERVAL):
            if self.chaos.killed:
                continue
            try:
                self._gossip_round()
            except Exception as e:
                self.metrics.event("error", "gossip round failed: %s" % e)

    def _adverts(self, limit: int = 200) -> List[dict]:
        out = []
        for m in self.manifests.all_current()[:limit]:
            out.append({"path": m.path, "digest": m.digest(), "vv": m.vv,
                        "size": m.size, "n": len(m.chunks)})
        return out

    def _gossip_round(self, urgent: bool = False) -> None:
        """Swap membership and manifest adverts with a few random peers.

        Small random fanout is what gives gossip O(log N) propagation with no
        load-bearing node.
        """
        self.membership.tick()
        targets = self.membership.sample(self.fanout if not urgent else self.fanout + 2)
        if not targets:
            return
        bf = self.have_filter()
        adverts = self._adverts()
        msg = {"type": P.GOSSIP, "self": self.membership.self_record(),
               "members": self.membership.digest(), "adverts": adverts,
               "have": bf.to_dict()}
        self.metrics.incr("gossip_rounds")
        for peer in targets:
            try:
                head, _ = self._ask(peer, msg)
            except Exception:
                continue
            self.metrics.incr("gossip_ok")
            self._absorb(head, peer)

    def _absorb(self, head: dict, peer: Peer) -> None:
        for nid in self.membership.merge(head.get("members", [])):
            self.metrics.event("member", "discovered node %s" % nid)
        if head.get("have"):
            bf = BloomFilter.from_dict(head["have"])
            self.avail.update(peer.node_id, bf)
            peer.have_count = bf.count
        for adv in head.get("adverts", []):
            self._consider_advert(adv, peer)

    def _consider_advert(self, adv: dict, peer: Peer) -> None:
        """Decide whether a peer's version of a file is news to us."""
        path, digest = adv.get("path"), adv.get("digest")
        if not path or not digest:
            return
        local = self.manifests.current(path)
        if local is not None and local.digest() == digest:
            return
        if local is not None:
            rel = compare(local.vv, adv.get("vv", {}))
            if rel in (AFTER, EQUAL):
                return  # ours is at least as new; nothing to pull
        try:
            head, _ = self._ask(peer, {"type": P.MANIFEST_GET, "path": path,
                                       "digest": digest})
        except Exception:
            return
        if head.get("type") != "manifest_data" or not head.get("manifest"):
            return
        self.accept_manifest(Manifest.from_dict(head["manifest"]))

    def accept_manifest(self, remote: Manifest) -> str:
        """Integrate a manifest received from a peer.

        The interesting case is CONCURRENT: two nodes edited the same path without
        seeing each other. File contents cannot be merged safely, so we pick a winner
        deterministically (higher digest, so every node picks the same one without
        coordinating) and keep the loser at a sidecar path.
        """
        path = remote.path
        local = self.manifests.current(path)
        self.manifests.add_version(remote)
        if local is None:
            self.manifests.set_current(remote)
            self._merkle_dirty = True
            self._on_new_manifest(remote)
            return "accepted"

        rel = compare(local.vv, remote.vv)
        if rel == EQUAL and local.digest() == remote.digest():
            return "duplicate"
        if rel == AFTER:
            return "stale"
        if rel == BEFORE:
            self.manifests.set_current(remote)
            self._merkle_dirty = True
            self._on_new_manifest(remote)
            return "accepted"

        winner, loser = ((remote, local) if remote.digest() > local.digest()
                         else (local, remote))
        merged_vv = merge(local.vv, remote.vv)

        # The resolved manifest must be a pure function of its two inputs so every
        # node independently builds a byte-identical one. Letting Manifest default
        # mtime/created to time.time() made each node's resolution unique, so nodes
        # saw each other's resolutions as new conflicts and never converged.
        resolved = Manifest(path, winner.size, list(winner.chunks), list(winner.sizes),
                            merged_vv, winner.origin, mode=winner.mode,
                            mtime=winner.mtime, created=winner.created,
                            note="conflict winner")
        self.manifests.set_current(resolved)
        side = "%s.conflict-%s-%s" % (path, loser.origin, loser.digest()[:8])
        if not self.manifests.current(side):
            self.manifests.set_current(
                Manifest(side, loser.size, list(loser.chunks), list(loser.sizes),
                         dict(loser.vv), loser.origin, mode=loser.mode,
                         mtime=loser.mtime, created=loser.created,
                         note="conflict copy"))
        self.manifests.record_conflict(path, loser.digest())
        self._merkle_dirty = True
        self.metrics.incr("conflicts")
        self.metrics.event("conflict",
                           "%s: concurrent edits, kept both (loser at %s)" % (path, side),
                           path=path)
        self._on_new_manifest(resolved)
        return "conflict"

    def _on_new_manifest(self, m: Manifest) -> None:
        self.metrics.incr("manifests_received")
        self.metrics.event("manifest", "%s now known (%s, %d chunks)" %
                           (m.path, self.state_of(m.path), len(m.chunks)), path=m.path)
        if self.fetch_policy == "eager" or m.path in self._materialising:
            self._add_want(self.store.missing(m.unique_chunks()))

    # ---------------------------------------------------------- background work

    def _fetch_loop(self) -> None:
        while not self._stop.wait(0.05):
            if self.chaos.killed:
                continue
            with self._want_lock:
                wanted = list(self._want)
            if not wanted:
                continue
            still = self.store.missing(wanted)
            if len(still) != len(wanted):
                with self._want_lock:
                    self._want.intersection_update(still)
            if still:
                try:
                    self._drive_fetches(still)
                except Exception:
                    pass

    def _maint_loop(self) -> None:
        last_scrub = last_ae = 0.0
        while not self._stop.wait(0.5):
            if self.chaos.killed:
                continue
            now = time.time()
            if now - last_ae > ANTIENTROPY_INTERVAL:
                last_ae = now
                try:
                    self._antientropy()
                except Exception:
                    pass
            if now - last_scrub > SCRUB_INTERVAL:
                last_scrub = now
                try:
                    self._scrub()
                except Exception:
                    pass

    def _antientropy(self) -> None:
        """Repair whatever gossip missed, at log cost when nothing is wrong."""
        peers = self.membership.sample(1)
        if not peers:
            return
        peer = peers[0]
        local = self.merkle()
        try:
            head, _ = self._ask(peer, {"type": P.MERKLE_GET, "want": "root"})
            if head.get("root") == local.root():
                self.metrics.incr("antientropy_clean")
                return  # identical manifest sets; 32 bytes settled it
            head, _ = self._ask(peer, {"type": P.MERKLE_GET, "want": "levels"})
        except Exception:
            return
        buckets = differing_buckets(local, head.get("levels", []))
        if not buckets:
            return
        self.metrics.incr("antientropy_repairs")
        self.metrics.event("repair", "divergence with %s in %d/%d buckets" %
                           (peer.node_id, len(buckets), local.n_leaves))
        for b in buckets[:16]:
            try:
                head, _ = self._ask(peer, {"type": P.BUCKET_GET, "index": b})
            except Exception:
                return
            for path, digest in head.get("keys", {}).items():
                cur = self.manifests.current(path)
                if cur is None or cur.digest() != digest:
                    self._consider_advert({"path": path, "digest": digest,
                                           "vv": head.get("vvs", {}).get(path, {})}, peer)

    def _scrub(self) -> None:
        """Verify stored chunks against their hashes and re-fetch failures.

        A rotating cursor rather than a random sample, so every chunk is verified within
        scrub_period whatever the store size. Random sampling gives no coverage
        guarantee at all.
        """
        hashes = sorted(self.store.hashes())
        if not hashes:
            return
        # Size the batch so a full sweep completes within scrub_period.
        passes = max(1.0, self.scrub_period / SCRUB_INTERVAL)
        batch = max(16, int(len(hashes) / passes) + 1)
        start = self._scrub_cursor % len(hashes)
        window = hashes[start:start + batch]
        if len(window) < batch:                       # wrap around the end
            window += hashes[:batch - len(window)]
        self._scrub_cursor = (start + batch) % len(hashes)

        self.metrics.incr("chunks_scrubbed", len(window))
        for h in window:
            if self.store.verify(h):
                continue
            self.store.drop(h)
            self._add_want([h])
            self.metrics.incr("corruption_detected")
            self.metrics.event("heal", "chunk %s failed verification, re-fetching" % h[:12])

    # ---------------------------------------------------------------- reporting

    def file_states(self) -> Dict[str, dict]:
        out = {}
        for m in self.manifests.all_current():
            uniq = m.unique_chunks()
            have = sum(1 for c in uniq if self.store.has(c))
            out[m.path] = {"state": self.state_of(m.path), "size": m.size,
                           "chunks": len(uniq), "have": have,
                           "digest": m.digest()[:12], "origin": m.origin,
                           "vv": m.vv, "manifest_bytes": m.nbytes()}
        return out

    def state(self) -> dict:
        snap = self.metrics.snapshot()
        return {
            "node": self.node_id, "host": self.host, "port": self.port,
            "rack": self.rack, "policy": "%s/%s" % (self.fetch_policy, self.source_policy),
            "peers": [p.to_dict() for p in self.membership.all()],
            "alive": len(self.membership.alive()),
            "files": self.file_states(),
            "chunks": len(self.store), "store_bytes": self.store.physical_bytes(),
            "logical_bytes": self.store.logical_bytes(),
            "dedup_ratio": round(self.store.dedup_ratio(), 3),
            "counters": snap["counters"], "timers": snap["timers"],
            "scheduler": self.sched.stats(), "chaos": self.chaos.snapshot(),
            "merkle_root": self.merkle().root()[:16],
            "events": list(self.metrics.recent(0))[-40:],
        }


class _Handler(socketserver.BaseRequestHandler):
    """One long-lived connection; frames handled in arrival order."""

    def handle(self) -> None:
        node: RippleNode = self.server.node
        sock: socket.socket = self.request
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            while not node._stop.is_set():
                try:
                    head, body = P.read_frame(sock)
                except (ConnectionError, OSError, P.ProtocolError):
                    return
                # A killed node must not answer; closing looks like a real crash to the caller.
                if node.chaos.killed:
                    return
                sender = (head.get("from") or head.get("self", {}).get("id")
                          or head.get("id"))
                if sender and node.chaos.blocks(sender):
                    return
                node.chaos.delay(len(body))
                try:
                    reply, payload = self.dispatch(node, head, body)
                except Exception as e:
                    reply, payload = P.error(str(e)), b""
                node.metrics.incr("msgs_recv")
                try:
                    P.send_frame(sock, reply, payload)
                except OSError:
                    return
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def dispatch(self, node: RippleNode, head: dict, body: bytes) -> Tuple[dict, bytes]:
        t = head.get("type")

        if t == P.HELLO:
            node.membership.add(head["id"], head["host"], head["port"], head.get("rack", "r0"))
            return ({"type": "hello_ok", "id": node.node_id, "rack": node.rack,
                     "peers": node.membership.digest()}, b"")

        if t == P.GOSSIP:
            me = head.get("self") or {}
            if me:
                node.membership.add(me["id"], me["host"], me["port"], me.get("rack", "r0"))
            peer = node.membership.get(me.get("id", "")) if me else None
            node.membership.merge(head.get("members", []))
            if peer is not None:
                if head.get("have"):
                    bf = BloomFilter.from_dict(head["have"])
                    node.avail.update(peer.node_id, bf)
                    peer.have_count = bf.count
                for adv in head.get("adverts", []):
                    # Off-thread: pulling a manifest here would block the sender's gossip round
                    # on our network round-trip.
                    node._ctrl.submit(node._consider_advert, adv, peer)
            return ({"type": "gossip_ok", "members": node.membership.digest(),
                     "adverts": node._adverts(), "have": node.have_filter().to_dict()}, b"")

        if t == P.MANIFEST_GET:
            m = (node.manifests.get(head["digest"]) if head.get("digest") else None) \
                or node.manifests.current(head.get("path", ""))
            if m is None:
                return ({"type": "manifest_miss"}, b"")
            return ({"type": "manifest_data", "manifest": m.to_dict()}, b"")

        if t == P.CHUNK_GET:
            # Refuse rather than queue: a busy peer should send the requester to another
            # holder immediately. Blocking would just move the bottleneck into a queue.
            if not node._upload.acquire(blocking=False):
                node.metrics.incr("upload_rejected")
                return ({"type": "chunk_busy", "hash": head["hash"]}, b"")
            try:
                data = node.store.get(head["hash"])
                if data is None:
                    node.metrics.incr("served_misses")
                    return ({"type": "chunk_miss", "hash": head["hash"]}, b"")
                node.metrics.incr("chunks_served")
                node.metrics.incr("bytes_served", len(data))
                return ({"type": "chunk_data", "hash": head["hash"]}, data)
            finally:
                node._upload.release()

        if t == P.CHUNK_PROBE:
            hs = head.get("hashes", [])
            return ({"type": "probe_result",
                     "present": [h for h in hs if node.store.has(h)]}, b"")

        if t == P.HAVE_GET:
            return ({"type": "have_data", "have": node.have_filter().to_dict()}, b"")

        if t == P.PING:
            return ({"type": "pong", "t": head.get("t"), "id": node.node_id}, b"")

        if t == P.MERKLE_GET:
            tree = node.merkle()
            if head.get("want") == "root":
                return ({"type": "merkle_root", "root": tree.root()}, b"")
            return ({"type": "merkle_levels", "levels": tree.levels()}, b"")

        if t == P.BUCKET_GET:
            keys = node.merkle().bucket_keys(int(head["index"]))
            vvs = {}
            for path in keys:
                m = node.manifests.current(path)
                if m:
                    vvs[path] = m.vv
            return ({"type": "bucket_keys", "keys": keys, "vvs": vvs}, b"")

        if t == P.STATE_GET:
            return ({"type": "state_data", "state": node.state()}, b"")

        if t == P.CHAOS:
            return ({"type": "chaos_ok",
                     **node.chaos.apply(head["action"], **head.get("args", {}))}, b"")

        return (P.error("unknown message type %r" % t), b"")


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, addr, node: RippleNode):
        self.node = node
        super().__init__(addr, _Handler)
