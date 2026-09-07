"""Tests for the simulator.

A simulator that agrees with whatever you hoped is worse than no simulator, so
these tests check that it reproduces the *qualitative* behaviours the protocol
is supposed to have -- and that it fails when it should.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ripple.sim import SimConfig, Simulation, sweep


def run(nodes, pieces=64, **kw):
    cfg = SimConfig(**kw.pop("cfg", {}))
    return Simulation(nodes, pieces, cfg, **kw).run()


class TestConvergence(unittest.TestCase):
    def test_everyone_converges(self):
        r = run(50)
        self.assertTrue(r["converged"])
        self.assertGreater(r["convergence_s"], 0)

    def test_manifest_arrives_long_before_bytes(self):
        r = run(100, pieces=256)
        self.assertLess(r["manifest_all_known_s"], r["convergence_s"],
                        "manifests should reach everyone before the bytes do")

    def test_convergence_grows_sublinearly(self):
        """20x the nodes must not cost anywhere near 20x the time."""
        small = run(25, pieces=128)["convergence_s"]
        large = run(500, pieces=128)["convergence_s"]
        self.assertLess(large / small, 5.0,
                        "convergence scaled %.1fx for 20x the nodes" % (large / small))

    def test_deterministic_for_a_fixed_seed(self):
        a, b = run(40), run(40)
        self.assertEqual(a["convergence_s"], b["convergence_s"])
        self.assertEqual(a["source_egress"], b["source_egress"])


class TestSwarmVsStar(unittest.TestCase):
    def test_star_egress_grows_with_cluster_size(self):
        small = run(20, pieces=64, source_policy="star")
        large = run(80, pieces=64, source_policy="star")
        self.assertGreater(large["source_egress"], small["source_egress"] * 3,
                           "a star topology must show linear source egress")

    def test_swarm_egress_stays_bounded(self):
        small = run(20, pieces=64)
        large = run(200, pieces=64)
        # 10x the nodes, nothing like 10x the egress
        self.assertLess(large["source_egress_ratio"], small["source_egress_ratio"] * 3)

    def test_swarm_beats_star_at_scale(self):
        sw = run(100, pieces=128)
        st = run(100, pieces=128, source_policy="star")
        self.assertLess(sw["source_egress"], st["source_egress"])
        self.assertLess(sw["convergence_s"], st["convergence_s"])


class TestFailure(unittest.TestCase):
    def test_survivors_converge_once_the_swarm_has_coverage(self):
        """The publisher stops being special the moment every piece exists elsewhere."""
        r = run(60, pieces=128, kill_source_after_coverage=True)
        self.assertTrue(r["converged"],
                        "the swarm should finish without the publisher")
        self.assertIsNotNone(r["coverage_s"])
        self.assertEqual(r["alive"], 59)

    def test_killing_the_source_too_early_loses_data(self):
        """The honest converse, asserted rather than glossed over.

        Before full coverage some pieces exist in exactly one place, and killing
        that place destroys them. No design beats that -- it is a statement about
        replication factor. We assert it so nobody mistakes our survival claim
        for a stronger one than it is.
        """
        r = run(60, pieces=128, kill_source_at=0.4)
        self.assertFalse(r["converged"])
        self.assertTrue(r["stalled"], "should stall, not spin to the horizon")

    def test_survives_losing_forty_percent_mid_transfer(self):
        r = run(100, pieces=128, kill_fraction=0.4, kill_at=0.8)
        self.assertTrue(r["converged"])
        self.assertLess(r["alive"], 100)
        self.assertGreater(r["alive"], 50)

    def test_star_cannot_use_the_swarm_it_is_sitting_in(self):
        """The control case, and a sharper one than we first wrote.

        Kill the source *after* the cluster collectively holds every piece. The
        swarm converges; the star does not -- not because the data is gone, but
        because a star topology forbids the peers from serving each other. The
        bytes are right there and it cannot reach them.
        """
        r = run(40, pieces=128, source_policy="star", kill_source_after_coverage=True)
        self.assertIsNotNone(r["coverage_s"], "the cluster did reach full coverage")
        self.assertFalse(r["converged"],
                         "a star topology has no business surviving its source")


class TestSweep(unittest.TestCase):
    def test_sweep_holds_file_size_constant(self):
        rows = sweep([10, 20], 32 << 20, pieces=64)
        self.assertEqual(len({r["file_bytes"] for r in rows}), 1)
        self.assertEqual([r["nodes"] for r in rows], [10, 20])


if __name__ == "__main__":
    unittest.main(verbosity=2)
