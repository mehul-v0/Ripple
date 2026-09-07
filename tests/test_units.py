"""Unit tests for the pieces that have no network in them.

Fast (under a second) so they can run on every commit, unlike the integration
suite which spins up real clusters.
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ripple.bloom import BloomFilter
from ripple.chunker import Chunker
from ripple.manifest import Manifest, ManifestStore
from ripple.merkle import MerkleTree, differing_buckets
from ripple.scheduler import ChunkAvailability, Scheduler
from ripple.peers import Membership, Peer
from ripple.store import ChunkStore
from ripple import structured
from ripple.versions import AFTER, BEFORE, CONCURRENT, EQUAL, bump, compare, merge


class TestChunker(unittest.TestCase):
    def setUp(self):
        self.c = Chunker()

    def test_chunks_reassemble_exactly(self):
        data = os.urandom(3_000_000)
        self.assertEqual(b"".join(x.data for x in self.c.chunk_bytes(data)), data)

    def test_boundaries_are_deterministic(self):
        data = os.urandom(500_000)
        a = [x.hash for x in self.c.chunk_bytes(data)]
        b = [x.hash for x in Chunker().chunk_bytes(data)]
        self.assertEqual(a, b, "two nodes must chunk identical bytes identically")

    def test_insertion_shifts_only_local_chunks(self):
        """The property fixed-size blocks cannot provide."""
        data = os.urandom(2_000_000)
        before = {x.hash for x in self.c.chunk_bytes(data)}
        after = {x.hash for x in self.c.chunk_bytes(b"PREFIX" + data)}
        shared = len(before & after) / len(before)
        self.assertGreater(shared, 0.95,
                           "only %.1f%% of chunks survived a 6-byte prepend" % (100 * shared))

    def test_size_bounds_respected(self):
        data = os.urandom(2_000_000)
        sizes = [x.size for x in self.c.chunk_bytes(data)]
        self.assertLessEqual(max(sizes), self.c.max_size)
        # every chunk but the last must respect the minimum
        self.assertTrue(all(s >= self.c.min_size for s in sizes[:-1]))

    def test_file_and_memory_chunking_agree(self):
        data = os.urandom(1_500_000)
        tmp = tempfile.mkdtemp()
        try:
            p = os.path.join(tmp, "f.bin")
            with open(p, "wb") as fh:
                fh.write(data)
            a = [(x.hash, x.offset, x.size) for x in self.c.chunk_bytes(data)]
            b = [(x.hash, x.offset, x.size) for x in self.c.chunk_file(p, buf_size=64 * 1024)]
            self.assertEqual(a, b, "streaming and in-memory chunking diverged")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestBloom(unittest.TestCase):
    def test_never_false_negative(self):
        bf = BloomFilter.for_items(2000, 0.01)
        items = [os.urandom(16).hex() for _ in range(2000)]
        for i in items:
            bf.add(i)
        self.assertTrue(all(i in bf for i in items))

    def test_false_positive_rate_near_target(self):
        bf = BloomFilter.for_items(5000, 0.01)
        for _ in range(5000):
            bf.add(os.urandom(16).hex())
        fp = sum(1 for _ in range(5000) if os.urandom(16).hex() in bf) / 5000
        self.assertLess(fp, 0.03, "fp rate %.3f far above the 1%% target" % fp)

    def test_survives_serialisation(self):
        bf = BloomFilter.for_items(500)
        items = [os.urandom(8).hex() for _ in range(500)]
        for i in items:
            bf.add(i)
        rt = BloomFilter.from_dict(bf.to_dict())
        self.assertTrue(all(i in rt for i in items))
        self.assertEqual(rt.count, bf.count)

    def test_smaller_than_raw_ids(self):
        bf = BloomFilter.for_items(10000, 0.02)
        self.assertLess(bf.nbytes(), 10000 * 64 / 10)


class TestVersionVectors(unittest.TestCase):
    def test_ordering(self):
        a = {"n1": 1}
        self.assertEqual(compare(a, a), EQUAL)
        self.assertEqual(compare(bump(a, "n1"), a), AFTER)
        self.assertEqual(compare(a, bump(a, "n1")), BEFORE)

    def test_concurrent_detected(self):
        base = {"n0": 3}
        self.assertEqual(compare(bump(base, "a"), bump(base, "b")), CONCURRENT)

    def test_merge_takes_maximum(self):
        self.assertEqual(merge({"a": 3, "b": 1}, {"a": 1, "b": 7}), {"a": 3, "b": 7})

    def test_merge_dominates_both(self):
        a, b = {"x": 2, "y": 5}, {"y": 1, "z": 9}
        m = merge(a, b)
        self.assertIn(compare(m, a), (AFTER, EQUAL))
        self.assertIn(compare(m, b), (AFTER, EQUAL))


class TestMerkle(unittest.TestCase):
    def test_identical_sets_share_a_root(self):
        a, b = MerkleTree(), MerkleTree()
        for i in range(500):
            a.set("k%d" % i, "v%d" % i)
        for i in reversed(range(500)):     # different insertion order on purpose
            b.set("k%d" % i, "v%d" % i)
        self.assertEqual(a.root(), b.root())
        self.assertEqual(differing_buckets(a, b.levels()), [])

    def test_single_difference_isolated(self):
        a, b = MerkleTree(), MerkleTree()
        for i in range(1000):
            a.set("k%d" % i, "v%d" % i)
            b.set("k%d" % i, "v%d" % i)
        b.set("k500", "CHANGED")
        d = differing_buckets(a, b.levels())
        self.assertEqual(len(d), 1)
        self.assertIn("k500", a.bucket_keys(d[0]))

    def test_missing_key_detected(self):
        a, b = MerkleTree(), MerkleTree()
        for i in range(100):
            a.set("k%d" % i, "v")
            b.set("k%d" % i, "v")
        b.remove("k42")
        self.assertNotEqual(a.root(), b.root())
        self.assertEqual(len(differing_buckets(a, b.levels())), 1)


class TestStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = ChunkStore(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_roundtrip_and_dedup(self):
        data = os.urandom(4096)
        h = self.s.put(data)
        self.assertEqual(self.s.get(h), data)
        phys = self.s.physical_bytes()
        for _ in range(9):
            self.s.put(data)
        self.assertEqual(self.s.physical_bytes(), phys, "duplicate bytes hit the disk")
        self.assertAlmostEqual(self.s.dedup_ratio(), 10.0, places=5)

    def test_rejects_mislabelled_chunk(self):
        with self.assertRaises(ValueError):
            self.s.put(b"actual bytes", expected="0" * 64)

    def test_verify_catches_corruption(self):
        h = self.s.put(os.urandom(2048))
        self.assertTrue(self.s.verify(h))
        self.assertTrue(self.s.damage(h))
        self.assertFalse(self.s.verify(h), "silent corruption went undetected")

    def test_index_survives_reopen(self):
        hs = [self.s.put(os.urandom(1024)) for _ in range(20)]
        reopened = ChunkStore(self.tmp)
        self.assertTrue(all(reopened.has(h) for h in hs))
        self.assertEqual(len(reopened), 20)


class TestManifest(unittest.TestCase):
    def _m(self, sizes):
        return Manifest("/f", sum(sizes), ["h%d" % i for i in range(len(sizes))],
                        list(sizes), {"n0": 1}, "n0")

    def test_range_maps_to_minimal_chunk_set(self):
        m = self._m([100] * 10)
        self.assertEqual(m.chunks_for_range(0, 1), [0])
        self.assertEqual(m.chunks_for_range(150, 100), [1, 2])
        self.assertEqual(m.chunks_for_range(0, 1000), list(range(10)))
        self.assertEqual(m.chunks_for_range(999, 1), [9])
        self.assertEqual(m.chunks_for_range(0, 0), [])

    def test_digest_is_order_independent_but_content_sensitive(self):
        a = self._m([10, 20])
        b = Manifest.from_dict(a.to_dict())
        self.assertEqual(a.digest(), b.digest())
        c = self._m([10, 21])
        self.assertNotEqual(a.digest(), c.digest())

    def test_manifest_is_tiny_relative_to_file(self):
        big = Manifest("/vm.img", 10 << 30, ["a" * 64] * 1310720,
                       [8192] * 1310720, {"n0": 1}, "n0")
        # ~90 bytes per chunk of metadata to describe 10 GB
        self.assertLess(big.nbytes(), big.size / 100)

    def test_history_and_rollback_target(self):
        tmp = tempfile.mkdtemp()
        try:
            ms = ManifestStore(tmp)
            v1 = self._m([10, 10])
            ms.set_current(v1)
            v2 = Manifest("/f", 30, ["x", "y", "z"], [10, 10, 10], {"n0": 2}, "n0")
            ms.set_current(v2)
            self.assertEqual(ms.current("/f").digest(), v2.digest())
            self.assertEqual(len(ms.version_list("/f")), 2)
            # the old version is still fully retrievable: this is what makes
            # rollback a pointer swap
            self.assertIsNotNone(ms.get(v1.digest()))
            reopened = ManifestStore(tmp)
            self.assertEqual(reopened.current("/f").digest(), v2.digest())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestScheduler(unittest.TestCase):
    def _peers(self, n):
        return [Peer("n%d" % i, "127.0.0.1", 7000 + i, "rack%d" % (i % 2)) for i in range(n)]

    def test_prefers_rarest_chunk(self):
        avail = ChunkAvailability()
        peers = self._peers(4)
        common, rare = "c" * 64, "r" * 64
        for p in peers:
            bf = BloomFilter.for_items(8)
            bf.add(common)
            if p.node_id == "n3":
                bf.add(rare)
            avail.update(p.node_id, bf)
        sched = Scheduler(avail)
        plans = sched.plan([common, rare], peers, lambda x: x, limit=1)
        self.assertEqual(plans[0].chunk, rare,
                         "scheduler should chase the scarce chunk first")

    def test_respects_per_peer_concurrency(self):
        avail = ChunkAvailability()
        peers = self._peers(1)
        chunks = ["%064d" % i for i in range(20)]
        bf = BloomFilter.for_items(32)
        for c in chunks:
            bf.add(c)
        avail.update("n0", bf)
        sched = Scheduler(avail, max_per_peer=3)
        plans = sched.plan(chunks, peers, lambda x: x, limit=20)
        self.assertEqual(len(plans), 3, "one peer should not be handed 20 requests")

    def test_dead_peer_releases_its_work(self):
        avail = ChunkAvailability()
        peers = self._peers(1)
        c = "%064d" % 1
        bf = BloomFilter.for_items(4)
        bf.add(c)
        avail.update("n0", bf)
        sched = Scheduler(avail)
        self.assertEqual(len(sched.plan([c], peers, lambda x: x)), 1)
        self.assertEqual(sched.plan([c], peers, lambda x: x), [],
                         "an in-flight chunk should not be scheduled twice")
        sched.release_peer("n0")
        self.assertEqual(len(sched.plan([c], peers, lambda x: x)), 1,
                         "work must be re-plannable after its peer dies")


class TestMembership(unittest.TestCase):
    def test_merge_is_monotonic(self):
        m = Membership("me", "127.0.0.1", 7000)
        m.merge([{"id": "a", "host": "h", "port": 1, "hb": 5}])
        m.merge([{"id": "a", "host": "h", "port": 1, "hb": 2}])   # stale replay
        self.assertEqual(m.get("a").heartbeat, 5, "a stale gossip must not win")

    def test_ignores_self(self):
        m = Membership("me", "127.0.0.1", 7000)
        m.merge([{"id": "me", "host": "x", "port": 9, "hb": 99}])
        self.assertEqual(m.all(), [])

    def test_proximity_prefers_same_rack_then_rtt(self):
        m = Membership("me", "127.0.0.1", 7000, rack="rackA")
        near = Peer("near", "h", 1, "rackA")
        far = Peer("far", "h", 2, "rackB")
        far.rtt = 0.0001
        near.rtt = 0.05
        self.assertEqual(m.by_proximity([far, near])[0].node_id, "near")

    def test_failures_deprioritise_a_peer(self):
        m = Membership("me", "127.0.0.1", 7000, rack="rackA")
        good = Peer("good", "h", 1, "rackA")
        bad = Peer("bad", "h", 2, "rackA")
        bad.failures = 3
        self.assertEqual(m.by_proximity([bad, good])[0].node_id, "good")


class TestConflictResolutionIsPure(unittest.TestCase):
    """Two nodes resolving the same conflict must reach the same manifest.

    Regression test. Conflict resolution used to stamp a fresh mtime, so each
    node produced a slightly different "winner", saw everyone else's winner as a
    brand-new conflict, and the cluster looped forever instead of converging.
    Order of arrival must not matter either.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.nodes = []

    def tearDown(self):
        for n in self.nodes:
            n.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _node(self, name):
        from ripple.node import RippleNode
        n = RippleNode(name, root=os.path.join(self.tmp, name))
        self.nodes.append(n)
        return n

    def test_same_result_regardless_of_arrival_order(self):
        import time as _t
        a = Manifest("/c.txt", 3, ["h1"], [3], {"na": 1}, "na", mtime=1000.0, created=1000.0)
        b = Manifest("/c.txt", 3, ["h2"], [3], {"nb": 1}, "nb", mtime=2000.0, created=2000.0)

        n1, n2 = self._node("n1"), self._node("n2")
        n1.accept_manifest(a)
        n1.accept_manifest(b)
        n2.accept_manifest(b)          # opposite order
        n2.accept_manifest(a)

        self.assertEqual(n1.manifests.current("/c.txt").digest(),
                         n2.manifests.current("/c.txt").digest(),
                         "conflict resolution depends on arrival order")
        # and re-applying the resolution must be a no-op, not a fresh conflict
        winner = n1.manifests.current("/c.txt")
        before = n2.metrics.get("conflicts")
        self.assertEqual(n2.accept_manifest(winner), "duplicate")
        self.assertEqual(n2.metrics.get("conflicts"), before,
                         "re-applying a resolved manifest re-triggered a conflict")

    def test_loser_is_preserved_not_dropped(self):
        a = Manifest("/c.txt", 3, ["h1"], [3], {"na": 1}, "na", mtime=1000.0, created=1000.0)
        b = Manifest("/c.txt", 3, ["h2"], [3], {"nb": 1}, "nb", mtime=2000.0, created=2000.0)
        n = self._node("n3")
        n.accept_manifest(a)
        n.accept_manifest(b)
        sidecars = [p for p in n.manifests.paths() if ".conflict-" in p]
        self.assertEqual(len(sidecars), 1, "the losing version was not preserved")


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestStructuredChunking(unittest.TestCase):
    """Grammar-aware splitting must never corrupt a file.

    The splitter is allowed to decline anything it does not understand, but if
    it returns pieces they must be an exact partition -- a chunker that drops or
    duplicates a byte would silently corrupt every config in the cluster.
    """

    YAML = (b"# database config\n"
            b"max_connections: 500\n"
            b"timeout: 30\n"
            b"replication:\n  factor: 3\n  mode: sync\n")
    INI = b"[server]\nport = 8080\nhost = 0.0.0.0\n\n[limits]\nmax_conn = 500\n"
    JSON = b'{"max_connections": 500, "timeout": 30, "nested": {"a": [1, 2]}}'

    def test_pieces_are_an_exact_partition(self):
        for name, data in (("/etc/db.yaml", self.YAML), ("/etc/app.ini", self.INI),
                           ("/etc/c.json", self.JSON)):
            pieces = structured.split(name, data)
            self.assertIsNotNone(pieces, "%s should have been split" % name)
            self.assertEqual(b"".join(b for _, b in pieces), data,
                             "%s did not round-trip byte-for-byte" % name)

    def test_labels_name_the_settings(self):
        labels = [l for l, _ in structured.split("/etc/db.yaml", self.YAML)]
        self.assertIn("max_connections", labels)
        self.assertIn("replication", labels)

    def test_one_setting_changes_one_chunk(self):
        """The whole point: a config edit must not disturb its neighbours."""
        import hashlib
        before = {l: hashlib.sha256(b).hexdigest()
                  for l, b in structured.split("/etc/db.yaml", self.YAML)}
        drifted = self.YAML.replace(b"max_connections: 500", b"max_connections: 200")
        after = {l: hashlib.sha256(b).hexdigest()
                 for l, b in structured.split("/etc/db.yaml", drifted)}
        changed = [k for k in before if before[k] != after.get(k)]
        self.assertEqual(changed, ["max_connections"])

    def test_declines_rather_than_guesses(self):
        self.assertIsNone(structured.split("/blob.bin", os.urandom(4096)))
        self.assertIsNone(structured.split("/etc/empty.yaml", b""))
        self.assertIsNone(structured.split("/etc/truncated.json", b'{"a": 1'))
        self.assertIsNone(structured.split("/etc/one.yaml", b"only_key: 1\n"))

    def test_drift_names_the_key_and_the_outlier(self):
        report = structured.drift({
            "n0": {"max_connections": "aaa", "timeout": "ttt"},
            "n1": {"max_connections": "aaa", "timeout": "ttt"},
            "n2": {"max_connections": "aaa", "timeout": "ttt"},
            "n3": {"max_connections": "bbb", "timeout": "ttt"},
        })
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]["key"], "max_connections")
        self.assertEqual(report[0]["majority_count"], 3)
        self.assertEqual(report[0]["outliers"][0]["nodes"], ["n3"])

    def test_no_drift_when_everyone_agrees(self):
        self.assertEqual(structured.drift({"n0": {"k": "h"}, "n1": {"k": "h"}}), [])
