"""The headline claim at file sizes that carry it.

    python -m bench.large_file --sizes 1,10 --nodes 50

Measures local ingest cost on the publisher, manifest size and how long until
every node knows the file exists, and time-to-first-byte for a node holding none
of it.

Full materialisation of 10 GB onto every node is deliberately not measured: on
this harness that is hours of loopback traffic and would be measuring the laptop.
See bench/validate.py.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ripple.chunker import Chunker
from ripple.cluster import LocalCluster
from ripple.manifest import PHANTOM
from ripple.metrics import human_bytes

RESULTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
BLOCK = 1 << 20


def generate(path: str, size: int, unique: bool = True) -> None:
    """Write `size` bytes of incompressible, non-duplicating data.

    Every 1 MB block comes from its own PRNG seed, so dedup cannot quietly shrink
    the work and flatter the ingest figures.
    """
    written = 0
    with open(path, "wb", buffering=1 << 22) as fh:
        i = 0
        while written < size:
            n = min(BLOCK, size - written)
            if unique:
                blk = random.Random(0xC0FFEE + i).randbytes(n)
            else:
                blk = b"\0" * n
            fh.write(blk)
            written += n
            i += 1
        fh.flush()
        os.fsync(fh.fileno())


def run_size(size: int, n_nodes: int, workdir: str) -> dict:
    label = "%d GB" % (size >> 30) if size >= (1 << 30) else "%d MB" % (size >> 20)
    prof = Chunker.for_file_size(size)
    print("\n=== %s across %d nodes  (profile: %s) ===" % (label, n_nodes, prof.describe()),
          flush=True)

    src_path = os.path.join(workdir, "payload.bin")
    t0 = time.perf_counter()
    generate(src_path, size)
    print("  generated %s of unique data in %.0fs" % (label, time.perf_counter() - t0),
          flush=True)

    cluster = LocalCluster(n_nodes, root=os.path.join(workdir, "cluster"),
                           racks=5, scrub_period=1e9)  # scrub off: irrelevant here
    cluster.start()
    print("  cluster of %d nodes formed" % n_nodes, flush=True)

    src = cluster.nodes[0]
    t0 = time.perf_counter()
    m = src.publish("/images/big.img", src_file=src_path)
    ingest_s = time.perf_counter() - t0
    print("  publisher ingested it in %.0fs (%.1f MB/s local chunk+hash+store)"
          % (ingest_s, size / ingest_s / 1e6), flush=True)

    # ---- the headline: how long until the file exists on every node ----------
    seen_s = cluster.await_manifest("/images/big.img", timeout=300)
    phantom = sum(1 for nd in cluster.nodes[1:]
                  if nd.state_of("/images/big.img") == PHANTOM)
    print("  MANIFEST ON ALL %d NODES IN %.3f s   (manifest %s for a %s file)"
          % (n_nodes, seen_s, human_bytes(m.nbytes()), label), flush=True)
    print("  %d of %d receivers are PHANTOM: the file exists, holding zero bytes"
          % (phantom, n_nodes - 1), flush=True)

    # ---- time to first byte on a node that holds nothing ---------------------
    reader = cluster.nodes[-1]
    offset = (size // 2) + 12345
    before = reader.metrics.get("bytes_fetched")
    t0 = time.perf_counter()
    got = reader.read("/images/big.img", offset, 4096, timeout=180)
    ttfb = time.perf_counter() - t0
    moved = reader.metrics.get("bytes_fetched") - before

    with open(src_path, "rb") as fh:            # verify against the real file
        fh.seek(offset)
        expect = fh.read(4096)
    assert got == expect, "read from a PHANTOM file returned wrong bytes"
    print("  TIME TO FIRST BYTE on a PHANTOM node: %.3f s, moved %s to serve a 4 KB read"
          % (ttfb, human_bytes(moved)), flush=True)
    print("  (bytes verified against the original file)", flush=True)

    row = {
        "label": label, "file_bytes": size, "nodes": n_nodes,
        "profile": prof.describe(), "chunks": len(m.chunks),
        "unique_chunks": len(m.unique_chunks()),
        "manifest_bytes": m.nbytes(),
        "manifest_ratio": round(size / m.nbytes(), 1),
        "ingest_s": round(ingest_s, 2),
        "ingest_mbs": round(size / ingest_s / 1e6, 2),
        "manifest_everywhere_s": round(seen_s, 4),
        "phantom_receivers": phantom,
        "ttfb_s": round(ttfb, 4),
        "ttfb_bytes": moved,
    }
    cluster.stop()
    os.remove(src_path)
    shutil.rmtree(os.path.join(workdir, "cluster"), ignore_errors=True)
    return row


def main() -> int:
    ap = argparse.ArgumentParser(description="headline claim at real file sizes")
    ap.add_argument("--sizes", default="1,10", help="comma-separated sizes in GB")
    ap.add_argument("--nodes", type=int, default=50)
    ap.add_argument("--workdir", default="")
    ap.add_argument("--out", default=os.path.join(RESULTS, "large_file.json"))
    a = ap.parse_args()

    sizes = [int(round(float(x) * (1 << 30))) for x in a.sizes.split(",")]
    need = max(sizes) * 2.2
    workdir = a.workdir or tempfile.mkdtemp(prefix="ripple-large-")
    os.makedirs(workdir, exist_ok=True)
    free = shutil.disk_usage(workdir)[2]
    if free < need:
        print("need ~%.0f GB free (file + chunk store), have %.0f GB"
              % (need / 1e9, free / 1e9))
        return 2

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    rows = []
    try:
        for s in sizes:
            rows.append(run_size(s, a.nodes, workdir))
            # Checkpoint after every size: the large cases take tens of minutes.
            with open(a.out, "w") as fh:
                json.dump({"nodes": a.nodes, "rows": rows}, fh, indent=2)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    print("\n%-8s %-12s %-11s %-13s %-15s %s"
          % ("size", "manifest", "vs file", "on all nodes", "time-to-1st-byte", "moved"))
    for r in rows:
        print("%-8s %-12s %-11s %-13s %-15s %s"
              % (r["label"], human_bytes(r["manifest_bytes"]),
                 "1/%d" % r["manifest_ratio"], "%.3f s" % r["manifest_everywhere_s"],
                 "%.3f s" % r["ttfb_s"], human_bytes(r["ttfb_bytes"])))
    print("\nwrote %s" % a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
