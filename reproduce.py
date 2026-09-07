"""Reproduce every number in the deck, from a clean checkout.

    python reproduce.py            # full sweep
    python reproduce.py --quick    # ~15 minutes

Runs the tests, benchmarks, simulator sweep, simulator validation, charts and
deck. Nothing is cached in the repo: if a number is in the deck it came out of
this script.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def run(label: str, args: list, optional: bool = False) -> bool:
    print("\n" + "=" * 74)
    print("  %s" % label)
    print("=" * 74, flush=True)
    t0 = time.time()
    rc = subprocess.call([PY] + args, cwd=ROOT)
    dt = time.time() - t0
    if rc != 0:
        msg = "  -> FAILED (rc=%d) after %.0fs" % (rc, dt)
        print(msg if optional else msg + "  <-- this one matters", flush=True)
        return False
    print("  -> ok (%.0fs)" % dt, flush=True)
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--skip-tests", action="store_true")
    ap.add_argument("--large", default="",
                    help="also run the large-file headline, e.g. --large 1,10 "
                         "(GB). Slow: ingest is ~1.6 MB/s, so 10 GB is ~100 min.")
    a = ap.parse_args()

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    ok = True
    t0 = time.time()

    if not a.skip_tests:
        ok &= run("Unit tests (pure components)", ["-m", "unittest", "tests.test_units"])
        ok &= run("Simulator tests", ["-m", "unittest", "tests.test_sim"])
        ok &= run("Integration tests (real nodes, real sockets)",
                  ["-m", "unittest", "tests.test_integration"])

    bench = ["-m", "bench.benchmark"] + (["--quick"] if a.quick else [])
    ok &= run("Benchmarks on a real cluster", bench)

    sim_nodes = "10,25,50,100,300" if a.quick else "10,25,50,100,300,600,1000"
    ok &= run("Simulator sweep",
              ["-m", "ripple.sim", "--nodes", sim_nodes, "--size", str(64 << 20),
               "--pieces", "256", "--json", "results/sim_swarm.json"])

    val_nodes = "8,16,24" if a.quick else "10,20,35,50"
    ok &= run("Simulator validation (model vs reality)",
              ["-m", "bench.validate", "--nodes", val_nodes,
               "--size", str(4 << 20)])

    if a.large:
        ok &= run("Headline at real file sizes (slow)",
                  ["-m", "bench.large_file", "--sizes", a.large, "--nodes", "50"])

    ok &= run("Charts", ["-m", "bench.charts"])
    ok &= run("Slide deck", [os.path.join("docs", "make_deck.py")])

    print("\n" + "=" * 74)
    print("  %s in %.0fs. Results in results/." %
          ("ALL GREEN" if ok else "COMPLETED WITH FAILURES", time.time() - t0))
    print("=" * 74)
    for f in sorted(os.listdir(os.path.join(ROOT, "results"))):
        print("    results/%s" % f)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
