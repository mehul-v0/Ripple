"""End-to-end tests over real sockets. No mocks: these nodes actually talk."""

import os
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ripple.manifest import PHANTOM, RESIDENT
from ripple.node import RippleNode


def wait_for(pred, timeout=20.0, interval=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(interval)
    return False


class ClusterCase(unittest.TestCase):
    n_nodes = 3
    fetch_policy = "lazy"

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ripple-test-")
        self.nodes = []
        for i in range(self.n_nodes):
            n = RippleNode("n%d" % i, root=os.path.join(self.tmp, "n%d" % i),
                           rack="r%d" % (i % 2), fetch_policy=self.fetch_policy)
            n.start()
            self.nodes.append(n)
        seed = self.nodes[0]
        for n in self.nodes[1:]:
            self.assertTrue(n.join(seed.host, seed.port), "join failed")
        self.assertTrue(
            wait_for(lambda: all(len(n.membership.alive()) == self.n_nodes - 1
                                 for n in self.nodes)),
            "cluster did not form")

    def tearDown(self):
        for n in self.nodes:
            n.stop()
        time.sleep(0.2)
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestManifestPropagation(ClusterCase):
    def test_manifest_reaches_all_nodes_without_bytes(self):
        payload = os.urandom(2 << 20)  # 2 MB
        self.nodes[0].publish("/data/blob.bin", payload)

        self.assertTrue(
            wait_for(lambda: all(n.manifests.current("/data/blob.bin") for n in self.nodes[1:])),
            "manifest did not propagate")

        # The defining property: the file exists everywhere, with correct size
        # and metadata, while holding none of its bytes.
        for n in self.nodes[1:]:
            m = n.manifests.current("/data/blob.bin")
            self.assertEqual(m.size, len(payload))
            self.assertEqual(n.state_of("/data/blob.bin"), PHANTOM)

    def test_read_materialises_only_what_is_touched(self):
        payload = os.urandom(4 << 20)
        self.nodes[0].publish("/data/big.bin", payload)
        n2 = self.nodes[2]
        self.assertTrue(wait_for(lambda: n2.manifests.current("/data/big.bin")))
        self.assertEqual(n2.state_of("/data/big.bin"), PHANTOM)

        got = n2.read("/data/big.bin", 1000, 4096)
        self.assertEqual(got, payload[1000:1000 + 4096])

        # A 4 KB read must not have dragged 4 MB across the network.
        fetched = n2.metrics.get("bytes_fetched")
        self.assertLess(fetched, len(payload) // 4,
                        "read pulled %d B for a 4 KB request" % fetched)
        self.assertEqual(n2.state_of("/data/big.bin"), "PARTIAL")

    def test_full_materialisation_reconstructs_exact_bytes(self):
        payload = os.urandom(3 << 20)
        self.nodes[0].publish("/data/exact.bin", payload)
        n1 = self.nodes[1]
        self.assertTrue(wait_for(lambda: n1.manifests.current("/data/exact.bin")))
        self.assertTrue(n1.materialise("/data/exact.bin", timeout=40))
        self.assertEqual(n1.state_of("/data/exact.bin"), RESIDENT)
        self.assertEqual(n1.read("/data/exact.bin"), payload)


class TestIncrementalEdit(ClusterCase):
    def test_small_edit_moves_few_bytes(self):
        payload = bytes(os.urandom(64) * 16384)  # 1 MB, compressible pattern
        src = self.nodes[0]
        src.publish("/cfg/app.dat", payload)
        n1 = self.nodes[1]
        self.assertTrue(wait_for(lambda: n1.manifests.current("/cfg/app.dat")))
        self.assertTrue(n1.materialise("/cfg/app.dat", timeout=40))
        before = n1.metrics.get("bytes_fetched")

        src.edit("/cfg/app.dat", 500_000, b"CHANGED-BYTES-HERE")
        self.assertTrue(wait_for(
            lambda: n1.manifests.current("/cfg/app.dat").note.startswith("edit")))
        self.assertTrue(n1.materialise("/cfg/app.dat", timeout=40))

        moved = n1.metrics.get("bytes_fetched") - before
        self.assertLess(moved, len(payload) // 8,
                        "an 18-byte edit moved %d B of a %d B file" % (moved, len(payload)))
        self.assertIn(b"CHANGED-BYTES-HERE", n1.read("/cfg/app.dat", 499_990, 60))


class TestFaultTolerance(ClusterCase):
    n_nodes = 4

    def test_transfer_survives_a_dead_peer(self):
        payload = os.urandom(2 << 20)
        self.nodes[0].publish("/ha/file.bin", payload)
        for n in self.nodes[1:3]:
            self.assertTrue(wait_for(lambda n=n: n.manifests.current("/ha/file.bin")))
            self.assertTrue(n.materialise("/ha/file.bin", timeout=40))

        # Kill the publisher. Its chunks live on two other nodes now, so the
        # last node must still be able to complete from the swarm.
        self.nodes[0].chaos.apply("kill")
        last = self.nodes[3]
        self.assertTrue(wait_for(lambda: last.manifests.current("/ha/file.bin")))
        self.assertTrue(last.materialise("/ha/file.bin", timeout=40),
                        "could not materialise after the source died")
        self.assertEqual(last.read("/ha/file.bin"), payload)

    def test_scrub_heals_silent_corruption(self):
        payload = os.urandom(1 << 20)
        self.nodes[0].publish("/heal/f.bin", payload)
        n1 = self.nodes[1]
        self.assertTrue(wait_for(lambda: n1.manifests.current("/heal/f.bin")))
        self.assertTrue(n1.materialise("/heal/f.bin", timeout=40))

        victim = n1.manifests.current("/heal/f.bin").chunks[0]
        self.assertTrue(n1.store.damage(victim))
        self.assertFalse(n1.store.verify(victim), "damage() should break the chunk")

        self.assertTrue(wait_for(lambda: n1.store.verify(victim), timeout=45),
                        "scrub did not detect and heal the corrupted chunk")
        self.assertGreater(n1.metrics.get("corruption_detected"), 0)
        self.assertEqual(n1.read("/heal/f.bin"), payload)


class TestConflicts(ClusterCase):
    def test_concurrent_edits_keep_both_versions(self):
        a, b = self.nodes[0], self.nodes[1]
        # Partition first, so neither edit can be seen by the other: this is a
        # genuine concurrent write, not a race we got lucky with.
        a.chaos.apply("partition", peers=[n.node_id for n in self.nodes if n is not a])
        b.chaos.apply("partition", peers=[n.node_id for n in self.nodes if n is not b])
        a.publish("/shared/conf.yaml", b"setting: from-A\n")
        b.publish("/shared/conf.yaml", b"setting: from-B\n")
        time.sleep(0.5)
        a.chaos.apply("heal")
        b.chaos.apply("heal")

        self.assertTrue(wait_for(lambda: a.metrics.get("conflicts") > 0
                                 or b.metrics.get("conflicts") > 0, timeout=25),
                        "concurrent writes were not flagged as a conflict")
        loser = [n for n in (a, b) if n.metrics.get("conflicts") > 0][0]
        sidecars = [p for p in loser.manifests.paths() if ".conflict-" in p]
        self.assertTrue(sidecars, "the losing version was silently dropped")

    def test_cluster_converges_on_one_winner(self):
        a, b = self.nodes[0], self.nodes[1]
        a.chaos.apply("partition", peers=[n.node_id for n in self.nodes if n is not a])
        b.chaos.apply("partition", peers=[n.node_id for n in self.nodes if n is not b])
        a.publish("/shared/x.txt", b"A" * 100)
        b.publish("/shared/x.txt", b"B" * 100)
        time.sleep(0.5)
        for n in self.nodes:
            n.chaos.apply("heal")

        def agreed():
            ds = {n.manifests.current("/shared/x.txt").digest()
                  for n in self.nodes if n.manifests.current("/shared/x.txt")}
            return len(ds) == 1 and len(
                [n for n in self.nodes if n.manifests.current("/shared/x.txt")]) == len(self.nodes)

        self.assertTrue(wait_for(agreed, timeout=30),
                        "nodes did not converge on a single winner")


class TestRollback(ClusterCase):
    def test_rollback_restores_previous_version_everywhere(self):
        src = self.nodes[0]
        good = b"max_connections: 500\n" * 100
        m1 = src.publish("/etc/db.conf", good)
        src.publish("/etc/db.conf", b"ENCRYPTED-BY-RANSOMWARE\n" * 100)

        n1 = self.nodes[1]
        self.assertTrue(wait_for(
            lambda: n1.manifests.current("/etc/db.conf")
            and n1.manifests.current("/etc/db.conf").size != len(good)))

        t0 = time.time()
        src.rollback("/etc/db.conf", m1.digest())
        self.assertTrue(wait_for(
            lambda: all(n.manifests.current("/etc/db.conf")
                        and n.manifests.current("/etc/db.conf").chunks == m1.chunks
                        for n in self.nodes), timeout=25),
            "rollback did not reach every node")
        elapsed = time.time() - t0
        self.assertLess(elapsed, 15)
        self.assertEqual(n1.read("/etc/db.conf"), good)


class TestDedup(ClusterCase):
    def test_identical_files_share_chunks(self):
        src = self.nodes[0]
        base = os.urandom(1 << 20)
        src.publish("/vm/a.img", base)
        physical_after_first = src.store.physical_bytes()
        # Same content under three more names: chunk names are derived from the
        # bytes, so nothing new should hit the disk.
        for name in ("/vm/b.img", "/vm/c.img", "/vm/d.img"):
            src.publish(name, base)
        self.assertEqual(src.store.physical_bytes(), physical_after_first)
        self.assertGreater(src.store.dedup_ratio(), 3.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestConfigDrift(ClusterCase):
    n_nodes = 5

    GOOD = (b"# cluster database config\n"
            b"max_connections: 500\n"
            b"timeout: 30\n"
            b"replication:\n  factor: 3\n")

    def test_drift_is_found_from_manifests_alone(self):
        """Name the drifted key without transferring a single file byte."""
        from ripple.structured import drift as drift_report
        src = self.nodes[0]
        src.publish("/etc/db.yaml", self.GOOD)
        self.assertTrue(wait_for(
            lambda: all(n.manifests.current("/etc/db.yaml") for n in self.nodes)))

        odd = self.nodes[3]
        odd.chaos.apply("partition",
                        peers=[n.node_id for n in self.nodes if n is not odd])
        odd.publish("/etc/db.yaml",
                    self.GOOD.replace(b"max_connections: 500", b"max_connections: 200"))
        time.sleep(1.0)

        labelled = {}
        for n in self.nodes:
            m = n.manifests.current("/etc/db.yaml")
            self.assertTrue(m.labels, "config file was not split on its grammar")
            labelled[n.node_id] = m.label_map()
        report = drift_report(labelled)

        self.assertEqual(len(report), 1, "expected exactly one drifted key")
        self.assertEqual(report[0]["key"], "max_connections")
        self.assertEqual(report[0]["outliers"][0]["nodes"], [odd.node_id])
        # The whole point: this cost no data-plane traffic at all.
        self.assertEqual(sum(n.metrics.get("bytes_fetched") for n in self.nodes), 0)

    def test_structured_file_still_reads_back_exactly(self):
        src = self.nodes[0]
        src.publish("/etc/db.yaml", self.GOOD)
        peer = self.nodes[1]
        self.assertTrue(wait_for(lambda: peer.manifests.current("/etc/db.yaml")))
        self.assertTrue(peer.materialise("/etc/db.yaml", timeout=30))
        self.assertEqual(peer.read("/etc/db.yaml"), self.GOOD)


class TestPartitionIsReal(ClusterCase):
    """Regression: a partition must block *every* message type.

    Only `hello` and `gossip` carried a sender id, so a partitioned node still
    answered manifest_get / merkle_get / bucket_get. Anti-entropy then tunnelled
    through the partition and the isolation under test was not real -- which
    would have quietly invalidated every partition-based result.
    """
    n_nodes = 4

    def test_isolated_node_neither_sends_nor_receives(self):
        odd = self.nodes[3]
        odd.chaos.apply("partition",
                        peers=[n.node_id for n in self.nodes if n is not odd])

        # inbound: a publish elsewhere must not reach it
        self.nodes[0].publish("/p/in.txt", b"published while isolated\n")
        # outbound: its own publish must not escape
        odd.publish("/p/out.txt", b"written while isolated\n")
        time.sleep(3.0)

        self.assertIsNone(odd.manifests.current("/p/in.txt"),
                          "partitioned node received data it should not have")
        for n in self.nodes[:3]:
            self.assertIsNone(n.manifests.current("/p/out.txt"),
                              "data escaped a partitioned node via %s" % n.node_id)

        # and it all converges once healed
        for n in self.nodes:
            n.chaos.apply("heal")
        self.assertTrue(wait_for(
            lambda: all(n.manifests.current("/p/in.txt")
                        and n.manifests.current("/p/out.txt") for n in self.nodes),
            timeout=30), "cluster did not converge after healing the partition")
