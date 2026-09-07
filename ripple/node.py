from __future__ import annotations

import hashlib
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
from . import erasure
from .manifest import PARTIAL, PHANTOM, RESIDENT, Manifest, ManifestStore
from .merkle import MerkleTree, differing_buckets
from .metrics import Metrics, human_bytes
from .peers import Membership, Peer
from .scheduler import ChunkAvailability, Scheduler
from .store import ChunkStore, atomic_write
from . import structured
from .versions import AFTER, BEFORE, CONCURRENT, EQUAL, bump, compare, merge

GOSSIP_INTERVAL = 0.35
GOSSIP_FANOUT = 3
PREFETCH_AHEAD = 8
SCRUB_INTERVAL = 5.0
SCRUB_PERIOD = 20.0
ANTIENTROPY_INTERVAL = 4.0
UPLOAD_SLOTS = 4
EVICT_LOW_WATER = 0.85
EVICT_INTERVAL = 1.0


class RippleNode:
    def __init__(self, node_id: str, host: str = "127.0.0.1", port: int = 0,
                 root: Optional[str] = None, rack: str = "r0",
                 fetch_policy: str = "lazy", source_policy: str = "swarm",
                 fanout: int = GOSSIP_FANOUT, fsync: bool = False,
                 upload_slots: int = UPLOAD_SLOTS,
                 scrub_period: float = SCRUB_PERIOD,
                 super_seed: bool = False, salt: bool = True,
                 capacity_bytes: int = 0):
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

        self.fetch_policy = fetch_policy
        self.source_policy = source_policy
        self.fanout = fanout

        self._clients: Dict[str, P.PeerClient] = {}
        self._clients_lock = threading.Lock()
        self._want: Set[str] = set()
        self._want_lock = threading.Lock()
        self._materialising: Set[str] = set()
        self._stop = threading.Event()
        self._serving = False
        self._threads: List[threading.Thread] = []
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
        self._ec_paths: Set[str] = {m.path for m in self.manifests.all_current() if m.ec}
        self._ec_pending_since: Dict[str, float] = {}
        self._ec_fallback_logged: Set[str] = set()
        self.capacity_bytes = capacity_bytes
        self._publishing: Set[str] = set()
        self._pins: Dict[str, int] = {}
        self._pin_lock = threading.Lock()

        self._server = _Server((host, port), self)
        self.host, self.port = self._server.server_address[:2]
        self.membership = Membership(node_id, self.host, self.port, rack)

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
        if self.chaos.blocks(peer.node_id) or self.chaos.maybe_drop():
            raise ConnectionError("chaos: link to %s is down" % peer.node_id)
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
        if self._merkle_dirty:
            t = MerkleTree()
            for path, dg in list(self.manifests.refs.items()):
                t.set(path, dg)
            self._merkle = t
            self._merkle_dirty = False
        return self._merkle

    def publish(self, path: str, data: Optional[bytes] = None,
                src_file: Optional[str] = None, note: str = "",
                erasure_k: int = 0, erasure_m: int = 0) -> Manifest:
        t0 = time.perf_counter()
        chunks: List[str] = []
        sizes: List[int] = []
        labels: Optional[List[str]] = None
        ec_map: Optional[Dict[str, dict]] = None
        total = 0
        use_ec = erasure_k > 0
        if use_ec and structured.detect(path, data or b"") is not None:
            use_ec = False

        def _store_chunk(chunk_hash: str, chunk_bytes: bytes) -> None:
            if not use_ec:
                self._publishing.add(chunk_hash)
                self.store.put(chunk_bytes, chunk_hash)
                return
            frags, shard_len = erasure.encode(chunk_bytes, erasure_k, erasure_m)
            frag_hashes = []
            for f in frags:
                h = hashlib.sha256(f).hexdigest()
                self._publishing.add(h)
                frag_hashes.append(self.store.put(f, h))
            ec_map[chunk_hash] = {"k": erasure_k, "m": erasure_m,
                                  "frags": frag_hashes, "shard_len": shard_len,
                                  "orig_len": len(chunk_bytes)}

        if use_ec:
            ec_map = {}
        if src_file:
            chunker = Chunker.for_file_size(os.path.getsize(src_file))
            for c in chunker.chunk_file(src_file):
                _store_chunk(c.hash, c.data)
                chunks.append(c.hash)
                sizes.append(c.size)
                total += c.size
        else:
            data = data or b""
            pieces = None if use_ec else structured.split(path, data)
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
                    _store_chunk(c.hash, c.data)
                    chunks.append(c.hash)
                    sizes.append(c.size)
                    total += c.size

        prev = self.manifests.current(path)
        vv = bump(prev.vv, self.node_id) if prev else {self.node_id: 1}
        m = Manifest(path, total, chunks, sizes, vv, self.node_id, note=note,
                     labels=labels, ec=ec_map)
        self.manifests.set_current(m)
        self._publishing.difference_update(chunks)
        for grp in (ec_map or {}).values():
            self._publishing.difference_update(grp["frags"])
        self._merkle_dirty = True
        if ec_map:
            self._ec_paths.add(path)
            self.metrics.incr("ec_files_published")
            self.metrics.event("erasure",
                               "%s: %d chunk(s) erasure-coded k=%d m=%d "
                               "(%.2fx storage, survives losing any %d fragment holders)"
                               % (path, len(ec_map), erasure_k, erasure_m,
                                  (erasure_k + erasure_m) / erasure_k, erasure_m),
                               path=path)

        chunk_time = time.perf_counter() - t0
        self.metrics.observe("publish_seconds", chunk_time)
        self.metrics.incr("files_published")
        self.metrics.event("publish", "published %s (%d chunks, manifest %d B)" %
                           (path, len(chunks), m.nbytes()), path=path)
        self._ctrl.submit(self._gossip_round, True)
        return m

    def edit(self, path: str, offset: int, data: bytes) -> Optional[Manifest]:
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
        old = self.manifests.get(digest)
        if not old or old.path != path:
            return None
        cur = self.manifests.current(path)
        vv = bump(cur.vv if cur else {}, self.node_id)
        restored = Manifest(path, old.size, list(old.chunks), list(old.sizes), vv,
                            self.node_id, note="rollback to %s" % digest[:12],
                            labels=list(old.labels) if old.labels else None,
                            ec=dict(old.ec) if old.ec else None)
        self.manifests.set_current(restored)
        self._merkle_dirty = True
        if restored.ec:
            self._ec_paths.add(path)
        self.metrics.event("rollback", "%s rolled back to %s" % (path, digest[:12]),
                           path=path)
        self._ctrl.submit(self._gossip_round, True)
        return restored

    def namespace_root(self) -> str:
        t = MerkleTree()
        for m in self.manifests.all_current():
            t.set(m.path, m.content_id())
        return t.root()

    def snapshot(self, note: str = "") -> dict:
        return {"root": self.namespace_root(), "at": time.time(), "note": note,
                "entries": {m.path: m.digest() for m in self.manifests.all_current()}}

    def restore(self, snap: dict) -> dict:
        entries = snap.get("entries", {})
        restored, already, unknown = [], [], []
        for path, digest in entries.items():
            cur = self.manifests.current(path)
            if cur is not None and cur.digest() == digest:
                already.append(path)
                continue
            if self.manifests.get(digest) is None:
                unknown.append(path)
                continue
            if self.rollback(path, digest) is not None:
                restored.append(path)
        extra = [p for p in self.manifests.paths() if p not in entries]
        self.metrics.event("rollback",
                           "namespace restore: %d file(s) rolled back, %d already "
                           "correct, %d unknown, %d created since"
                           % (len(restored), len(already), len(unknown), len(extra)))
        return {"restored": restored, "already": already,
                "unknown": unknown, "created_since": extra,
                "root_now": self.namespace_root(), "root_target": snap.get("root")}

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

    def _pin(self, hashes) -> None:
        with self._pin_lock:
            for h in hashes:
                self._pins[h] = self._pins.get(h, 0) + 1

    def _unpin(self, hashes) -> None:
        with self._pin_lock:
            for h in hashes:
                left = self._pins.get(h, 0) - 1
                if left > 0:
                    self._pins[h] = left
                else:
                    self._pins.pop(h, None)

    def read(self, path: str, offset: int = 0, length: Optional[int] = None,
             timeout: float = 30.0) -> bytes:
        m = self.manifests.current(path)
        if not m:
            raise FileNotFoundError(path)
        if length is None:
            length = m.size - offset
        idxs = m.chunks_for_range(offset, length)
        needed = [m.chunks[i] for i in idxs]
        self._pin(needed)
        try:
            return self._read_pinned(m, path, idxs, needed, offset, length, timeout)
        finally:
            self._unpin(needed)

    def _read_pinned(self, m, path, idxs, needed, offset, length, timeout) -> bytes:
        missing = self.store.missing(needed)
        if missing:
            t0 = time.perf_counter()
            self.metrics.incr("reads_faulted")
            self._fetch_now(missing, timeout)
            self.metrics.observe("read_fault_seconds", time.perf_counter() - t0)

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
            peers = [p for p in peers if p.node_id in self._origins()] or peers
        return peers

    def _peer_sorter(self):
        base = self.membership.by_proximity
        if self.source_policy == "star" or not self.sched.super_seed:
            return base
        origins = self._origins()

        def sorter(holders: List[Peer]) -> List[Peer]:
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
                self.store.put(body, plan.chunk)
                self.metrics.incr("chunks_fetched")
                self.metrics.incr("bytes_fetched", len(body))
                if plan.peer.rack != self.rack:
                    self.metrics.incr("cross_rack_bytes", len(body))
                ok = True
                with self._want_lock:
                    self._want.discard(plan.chunk)
            elif head.get("type") == "chunk_busy":
                self.metrics.incr("chunk_busy")
            else:
                self.avail.confirm(plan.peer.node_id, plan.chunk, False)
                self.metrics.incr("chunk_misses")
        except ValueError:
            self.avail.confirm(plan.peer.node_id, plan.chunk, False)
            self.metrics.incr("corrupt_received")
            self.metrics.event("error", "peer %s served bytes that failed verification "
                                        "for %s -- rejected, re-planning elsewhere"
                               % (plan.peer.node_id, plan.chunk[:12]))
        except Exception:
            self.metrics.incr("fetch_errors")
        finally:
            self.sched.complete(plan.chunk, plan.peer.node_id, ok)
        return ok

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
        path, digest = adv.get("path"), adv.get("digest")
        if not path or not digest:
            return
        local = self.manifests.current(path)
        if local is not None and local.digest() == digest:
            return
        if local is not None:
            rel = compare(local.vv, adv.get("vv", {}))
            if rel in (AFTER, EQUAL):
                return
        try:
            head, _ = self._ask(peer, {"type": P.MANIFEST_GET, "path": path,
                                       "digest": digest})
        except Exception:
            return
        if head.get("type") != "manifest_data" or not head.get("manifest"):
            return
        self.accept_manifest(Manifest.from_dict(head["manifest"]))

    def accept_manifest(self, remote: Manifest) -> str:
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
        if m.ec:
            self._ec_paths.add(m.path)
        else:
            self._ec_paths.discard(m.path)
        if self.fetch_policy == "eager" or m.path in self._materialising:
            self._add_want(self.store.missing(m.unique_chunks()))

    def _fetch_loop(self) -> None:
        while not self._stop.wait(0.05):
            if self.chaos.killed:
                continue
            with self._want_lock:
                wanted = list(self._want)
            if wanted:
                still = self.store.missing(wanted)
                if len(still) != len(wanted):
                    with self._want_lock:
                        self._want.intersection_update(still)
                if still:
                    try:
                        self._drive_fetches(still)
                    except Exception:
                        pass
                    if self._ec_paths:
                        self._ec_fallback(still)
            if self._ec_paths:
                try:
                    self._reconstruct_pending()
                except Exception:
                    pass

    def _maint_loop(self) -> None:
        last_scrub = last_ae = last_evict = 0.0
        while not self._stop.wait(0.5):
            if self.chaos.killed:
                continue
            now = time.time()
            if self.capacity_bytes and now - last_evict > EVICT_INTERVAL:
                last_evict = now
                try:
                    self._evict()
                except Exception:
                    pass
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
        peers = self.membership.sample(1)
        if not peers:
            return
        peer = peers[0]
        local = self.merkle()
        try:
            head, _ = self._ask(peer, {"type": P.MERKLE_GET, "want": "root"})
            if head.get("root") == local.root():
                self.metrics.incr("antientropy_clean")
                return
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
        hashes = sorted(self.store.hashes())
        if not hashes:
            return
        passes = max(1.0, self.scrub_period / SCRUB_INTERVAL)
        batch = max(16, int(len(hashes) / passes) + 1)
        start = self._scrub_cursor % len(hashes)
        window = hashes[start:start + batch]
        if len(window) < batch:
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

    def _protected_chunks(self) -> Set[str]:
        protected: Set[str] = set(self._publishing)
        with self._pin_lock:
            protected.update(self._pins)
        for m in self.manifests.all_current():
            if m.origin != self.node_id:
                continue
            protected.update(m.chunks)
            for grp in (m.ec or {}).values():
                protected.update(grp["frags"])
        return protected

    def _evict(self) -> int:
        cap = self.capacity_bytes
        if not cap:
            return 0
        physical = self.store.physical_bytes()
        if physical <= cap:
            return 0
        target = int(cap * EVICT_LOW_WATER)
        protected = self._protected_chunks()
        with self._want_lock:
            wanted = set(self._want)
        candidates = [h for h in self.store.hashes()
                      if h not in protected and h not in wanted]
        freed = n = 0
        for h in self.store.coldest(candidates):
            if physical - freed <= target:
                break
            size = self.store.size_of(h)
            self.store.drop(h)
            freed += size
            n += 1
        if n:
            self.metrics.incr("chunks_evicted", n)
            self.metrics.incr("bytes_evicted", freed)
            self.metrics.event(
                "evict", "over budget (%s > %s): evicted %d cold chunk(s), "
                         "freed %s -- all still readable, re-fetched on demand"
                         % (human_bytes(physical), human_bytes(cap), n,
                            human_bytes(freed)))
        elif physical > cap:
            self.metrics.incr("evict_blocked")
        return n

    def overcommit(self) -> dict:
        current = self.manifests.all_current()
        logical = sum(m.size for m in current)
        physical = self.store.physical_bytes()
        return {"capacity_bytes": self.capacity_bytes,
                "physical_bytes": physical,
                "presented_bytes": logical,
                "files": len(current),
                "ratio": round(logical / physical, 2) if physical else None,
                "over_budget": bool(self.capacity_bytes and physical > self.capacity_bytes)}

    def _ec_group_for(self, chunk_hash: str) -> Optional[dict]:
        for path in list(self._ec_paths):
            m = self.manifests.current(path)
            if m and m.ec and chunk_hash in m.ec:
                return m.ec[chunk_hash]
        return None

    def _ec_fallback(self, missing_chunks: List[str]) -> None:
        now = time.time()
        candidates = self._candidates()
        for h in missing_chunks:
            grp = self._ec_group_for(h)
            if not grp:
                self._ec_pending_since.pop(h, None)
                continue
            first_seen = self._ec_pending_since.setdefault(h, now)
            if now - first_seen < 0.3:
                continue
            if self.avail.holders(h, candidates):
                continue
            self._add_want(grp["frags"])
            if h not in self._ec_fallback_logged:
                self._ec_fallback_logged.add(h)
                self.metrics.incr("ec_fallback_triggered")
                self.metrics.event(
                    "erasure",
                    "no direct holder for %s -- reconstructing from any %d "
                    "of %d fragments" % (h[:12], grp["k"], grp["k"] + grp["m"]))

    def _reconstruct_pending(self) -> None:
        with self._want_lock:
            wanted_now = set(self._want)
        for path in list(self._ec_paths):
            m = self.manifests.current(path)
            if not m or not m.ec:
                continue
            for chunk_hash, grp in m.ec.items():
                if self.store.has(chunk_hash):
                    continue
                if (m.origin == self.node_id and path not in self._materialising
                        and chunk_hash not in wanted_now):
                    continue
                frags = grp["frags"]
                k = grp["k"]
                have = {i: self.store.get(f) for i, f in enumerate(frags)
                        if self.store.has(f)}
                if len(have) < k:
                    continue
                used = sorted(have)[:k]
                try:
                    data = erasure.reconstruct(k, grp["m"], grp["shard_len"],
                                               grp["orig_len"],
                                               {i: have[i] for i in used})
                except Exception:
                    continue
                try:
                    self.store.put(data, chunk_hash)
                except ValueError:
                    self.metrics.incr("ec_reconstruct_failed")
                    for i in list(have)[:k]:
                        self.store.drop(frags[i])
                    continue
                self.metrics.incr("ec_reconstructions")
                self._ec_pending_since.pop(chunk_hash, None)
                self._ec_fallback_logged.discard(chunk_hash)
                with self._want_lock:
                    self._want.difference_update(frags)
                missing_idx = [i for i in range(len(frags)) if i not in used]
                self.metrics.event(
                    "erasure",
                    "reconstructed %s using fragments %s of %d (any %d "
                    "sufficed; %s never needed)"
                    % (chunk_hash[:12], used, len(frags), k,
                       missing_idx or "none missing"),
                    path=path)

    def file_states(self) -> Dict[str, dict]:
        out = {}
        for m in self.manifests.all_current():
            uniq = m.unique_chunks()
            have = sum(1 for c in uniq if self.store.has(c))
            entry = {"state": self.state_of(m.path), "size": m.size,
                     "chunks": len(uniq), "have": have,
                     "digest": m.digest()[:12], "origin": m.origin,
                     "vv": m.vv, "manifest_bytes": m.nbytes(),
                     "labels": bool(m.labels)}
            if m.ec:
                first = next(iter(m.ec.values()))
                frag_have = sum(1 for g in m.ec.values()
                               for f in g["frags"] if self.store.has(f))
                frag_total = sum(len(g["frags"]) for g in m.ec.values())
                entry["ec"] = {"k": first["k"], "m": first["m"],
                              "overhead": round((first["k"] + first["m"]) / first["k"], 2),
                              "tolerates": first["m"], "groups": len(m.ec),
                              "fragments_present": frag_have, "fragments_total": frag_total}
            out[m.path] = entry
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
            "overcommit": self.overcommit(),
            "events": list(self.metrics.recent(0))[-160:],
        }


class _Handler(socketserver.BaseRequestHandler):

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
            if not node._upload.acquire(blocking=False):
                node.metrics.incr("upload_rejected")
                return ({"type": "chunk_busy", "hash": head["hash"]}, b"")
            try:
                data = node.store.get(head["hash"])
                if data is None:
                    node.metrics.incr("served_misses")
                    return ({"type": "chunk_miss", "hash": head["hash"]}, b"")
                if node.chaos.byzantine:
                    node.metrics.incr("byzantine_served")
                    return ({"type": "chunk_data", "hash": head["hash"]},
                            os.urandom(len(data)))
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
