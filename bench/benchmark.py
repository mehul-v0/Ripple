"""Benchmarks on a real cluster.

Every experiment runs real nodes over real sockets. Baselines are this same
system configured to behave the naive way (--star, --eager), so comparisons are
like-for-like on identical hardware.

    python -m bench.benchmark            # full sweep
    python -m bench.benchmark --quick    # smaller sweep
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ripple.cluster import LocalCluster
from ripple.metrics import human_bytes

RESULTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")


def make_payload(size: int, seed: int = 7) -> bytes:
    """Semi-realistic data: repeated blocks with unique regions.

    Pure random understates dedup; pure zeros flatters it absurdly.
    """
    rng = random.Random(seed)
    block = bytes(rng.getrandbits(8) for _ in range(4096))
    out = bytearray()
    while len(out) < size:
        if rng.random() < 0.25:
            out.extend(bytes(rng.getrandbits(8) for _ in range(4096)))
        else:
            out.extend(block)
    return bytes(out[:size])


def bench_manifest_vs_bytes(sizes, n_nodes, results):
    """How long until the file exists everywhere, versus every byte everywhere."""
    print("\n=== 1. Manifest sync vs full transfer (N=%d) ===" % n_nodes)
    rows = []
    for size in sizes:
        payload = make_payload(size)
        with LocalCluster(n_nodes) as c:
            src = c.nodes[0]
            t0 = time.perf_counter()
            m = src.publish("/bench/f.bin", payload)
            manifest_t = c.await_manifest("/bench/f.bin", timeout=60)
            c.materialise_all("/bench/f.bin", skip=1)
            resident_t = c.await_resident("/bench/f.bin", timeout=300)
            row = {"file_bytes": size, "manifest_bytes": m.nbytes(),
                   "manifest_sync_s": round(manifest_t, 4),
                   "full_sync_s": round(resident_t, 4),
                   "speedup": round(resident_t / manifest_t, 1) if manifest_t else None,
                   "chunks": len(m.chunks)}
            rows.append(row)
            print("  %8s file -> manifest %6s | exists everywhere in %6.3fs | "
                  "fully resident in %6.3fs | %sx"
                  % (human_bytes(size), human_bytes(m.nbytes()), manifest_t,
                     resident_t, row["speedup"]))
    results["manifest_vs_bytes"] = rows


def bench_source_egress(node_counts, size, results):
    """Bytes leaving the publisher as the cluster grows.

    A star sends the transferable content once per receiver, so its egress is
    (N-1) x content by construction; the swarm sends roughly the content once.

    The payload is unique data deliberately. With a semi-repetitive payload only
    67% of the file is distinct bytes, and reporting egress as a multiple of file
    size while only distinct bytes move made the star read 6.0x at N=10 instead of
    9.0x. `unique_ratio` is recorded either way so the arithmetic can be checked.
    """
    print("\n=== 2. Source egress vs cluster size (%s file, unique data) ==="
          % human_bytes(size))
    payload = os.urandom(size)      # unique: file size == transferable content
    rows = []
    for n in node_counts:
        entry = {"nodes": n, "file_bytes": size, "receivers": n - 1}
        for label, kw in (("ripple", dict(source_policy="swarm")),
                          ("star", dict(source_policy="star"))):
            with LocalCluster(n, **kw) as c:
                src = c.nodes[0]
                m = src.publish("/bench/e.bin", payload)
                # Transferable content = distinct bytes; equal to file size for unique data.
                unique = sum(m.sizes[m.chunks.index(h)] for h in m.unique_chunks())
                c.await_manifest("/bench/e.bin", timeout=60)
                c.materialise_all("/bench/e.bin", skip=1)
                ok = c.await_resident("/bench/e.bin", timeout=400)
                egress = src.metrics.get("bytes_served")
                entry["unique_bytes"] = unique
                entry["unique_ratio"] = round(unique / size, 4)
                entry[label + "_egress"] = egress
                entry[label + "_convergence_s"] = round(ok, 3)
                entry[label + "_egress_ratio"] = round(egress / size, 2)
                # Per receiver per unit of distinct content: a true star must read 1.00.
                entry[label + "_egress_per_receiver"] = round(
                    egress / max(1, unique) / max(1, n - 1), 3)
        rows.append(entry)
        print("  N=%3d (%d receivers)  ripple %9s (%4.1fx file, %5.2fs)   "
              "star %9s (%4.1fx file, %5.2fs)   star per-receiver %.2f"
              % (n, n - 1, human_bytes(entry["ripple_egress"]),
                 entry["ripple_egress_ratio"], entry["ripple_convergence_s"],
                 human_bytes(entry["star_egress"]), entry["star_egress_ratio"],
                 entry["star_convergence_s"], entry["star_egress_per_receiver"]))
    results["source_egress"] = rows


def bench_edit(size, n_nodes, results):
    """Bytes actually moved when a large file changes slightly."""
    print("\n=== 3. Incremental edit cost (N=%d) ===" % n_nodes)
    payload = make_payload(size)
    with LocalCluster(n_nodes) as c:
        src, peer = c.nodes[0], c.nodes[1]
        src.publish("/bench/cfg.bin", payload)
        c.await_manifest("/bench/cfg.bin", timeout=60)
        c.materialise_all("/bench/cfg.bin", skip=1)
        c.await_resident("/bench/cfg.bin", timeout=300)
        before = peer.metrics.get("bytes_fetched")

        src.edit("/bench/cfg.bin", size // 2, b"# a one-line configuration change\n")
        time.sleep(1.5)
        c.materialise_all("/bench/cfg.bin", skip=1)
        c.await_resident("/bench/cfg.bin", timeout=300)
        moved = peer.metrics.get("bytes_fetched") - before

        row = {"file_bytes": size, "bytes_moved": moved,
               "percent_of_file": round(100 * moved / size, 3),
               "changed_chunk_bytes": src.metrics.get("last_edit_new_bytes")}
        print("  %s file, 34-byte edit -> %s moved per node (%.2f%% of the file)"
              % (human_bytes(size), human_bytes(moved), row["percent_of_file"]))
        results["incremental_edit"] = row


def bench_dedup(results):
    """Cross-file, cluster-wide deduplication."""
    print("\n=== 4. Deduplication across similar files ===")
    with LocalCluster(3) as c:
        src = c.nodes[0]
        base = make_payload(4 << 20, seed=11)
        src.publish("/vm/golden.img", base)
        # Ten VMs derived from one golden image, each with a small delta.
        for i in range(10):
            variant = bytearray(base)
            for _ in range(6):
                off = random.Random(i).randrange(0, len(variant) - 2048)
                variant[off:off + 512] = os.urandom(512)
            src.publish("/vm/vm%02d.img" % i, bytes(variant))
        row = {"logical_bytes": src.store.logical_bytes(),
               "physical_bytes": src.store.physical_bytes(),
               "ratio": round(src.store.dedup_ratio(), 2),
               "chunks": len(src.store)}
        print("  11 images: %s logical stored in %s physical -> %.2fx dedup"
              % (human_bytes(row["logical_bytes"]), human_bytes(row["physical_bytes"]),
                 row["ratio"]))
        results["dedup"] = row


def bench_failure(size, n_nodes, results):
    """Kill the publisher and time convergence anyway.

    The publisher stops being special once every chunk exists somewhere else, so we
    measure the time to full swarm coverage and kill it after. Before that point
    some bytes exist in one place and no design recovers them.
    """
    print("\n=== 5. Recovery from losing the source mid-transfer (N=%d) ===" % n_nodes)
    payload = make_payload(size)
    with LocalCluster(n_nodes) as c:
        src = c.nodes[0]
        m = src.publish("/bench/ha.bin", payload)
        uniq = set(m.unique_chunks())
        c.await_manifest("/bench/ha.bin", timeout=60)
        c.materialise_all("/bench/ha.bin", skip=1)

        # Wait until every chunk is held by at least one non-publisher node.
        t0 = time.perf_counter()
        covered_s = None
        deadline = time.time() + 240
        while time.time() < deadline:
            held = set()
            for nd in c.nodes[1:]:
                held.update(h for h in uniq if nd.store.has(h))
            if held >= uniq:
                covered_s = time.perf_counter() - t0
                break
            time.sleep(0.05)

        if covered_s is None:
            print("  swarm never reached full coverage; skipping the kill")
            results["failure_recovery"] = {"nodes": n_nodes, "file_bytes": size,
                                           "covered": False}
            return

        remaining = sum(1 for nd in c.nodes[1:]
                        if nd.state_of("/bench/ha.bin") != "RESIDENT")
        print("  swarm held every chunk after %.2fs; killing the publisher with "
              "%d node(s) still incomplete" % (covered_s, remaining))
        t1 = time.perf_counter()
        src.chaos.apply("kill")
        conv = c.await_resident("/bench/ha.bin", timeout=300, nodes=c.nodes[1:])
        recovery = time.perf_counter() - t1
        ok = conv == conv
        print("  survivors fully converged %s after the source died"
              % ("in %.2fs" % recovery if ok else "FAILED"))
        results["failure_recovery"] = {
            "nodes": n_nodes, "file_bytes": size, "covered": True,
            "coverage_s": round(covered_s, 3),
            "incomplete_at_kill": remaining,
            "recovery_s": round(recovery, 3) if ok else None,
            "converged": ok}


def bench_ablation(size, n_nodes, results, repeats=3):
    """What each scheduling fix is worth, measured side by side.

    Three configurations of the same code on the same hardware in the same run,
    repeated, with the range reported so a difference smaller than the range can be
    recognised as noise.
    """
    print("\n=== 7. Scheduling ablation (N=%d, %s unique, %d runs each) ==="
          % (n_nodes, human_bytes(size), repeats))
    payload = os.urandom(size)
    configs = (("naive rarest-first", False, False),
               ("+ per-node salt", False, True),
               ("+ salt + super-seed", True, True))
    acc = {label: {"share": [], "egress": [], "conv": []} for label, _, _ in configs}

    for _ in range(max(1, repeats)):
        for label, ss, salt in configs:
            with LocalCluster(n_nodes, super_seed=ss, salt=salt) as c:
                src = c.nodes[0]
                src.publish("/abl/f.bin", payload)
                c.await_manifest("/abl/f.bin", timeout=60)
                c.materialise_all("/abl/f.bin", skip=1)
                conv = c.await_resident("/abl/f.bin", timeout=600)
                served = [nd.metrics.get("chunks_served") for nd in c.nodes]
                acc[label]["share"].append(100 * served[0] / max(1, sum(served)))
                acc[label]["egress"].append(src.metrics.get("bytes_served") / size)
                if conv == conv:
                    acc[label]["conv"].append(conv)

    rows = []
    for label, ss, salt in configs:
        d = acc[label]
        row = {"config": label, "super_seed": ss, "salt": salt, "runs": len(d["share"])}
        for key, name in (("share", "publisher_share_pct"), ("egress", "source_egress_ratio"),
                          ("conv", "convergence_s")):
            vals = d[key]
            row[name] = round(statistics.median(vals), 3) if vals else None
            row[name + "_min"] = round(min(vals), 3) if vals else None
            row[name + "_max"] = round(max(vals), 3) if vals else None
        rows.append(row)
        print("  %-21s publisher %5.1f%% (%.0f-%.0f) | egress %4.2fx (%.2f-%.2f) | "
              "converged %5.1fs"
              % (label, row["publisher_share_pct"], row["publisher_share_pct_min"],
                 row["publisher_share_pct_max"], row["source_egress_ratio"],
                 row["source_egress_ratio_min"], row["source_egress_ratio_max"],
                 row["convergence_s"] or float("nan")))

    best = min(rows, key=lambda r: r["publisher_share_pct"])
    print("  -> best configuration: %s" % best["config"])
    if best["config"] != rows[-1]["config"]:
        print("     (super-seeding did not help here; it is off by default)")
    results["ablation"] = rows
    results["ablation_nodes"] = n_nodes
    results["ablation_best"] = best["config"]


def bench_metadata_scaling(node_counts, results):
    """Gossip metadata per round: Bloom summary vs raw chunk-ID list."""
    print("\n=== 6. Availability metadata cost ===")
    rows = []
    with LocalCluster(3) as c:
        src = c.nodes[0]
        for chunks in (100, 1000, 10000, 50000):
            payload = make_payload(chunks * 8192 // 10)
            src.publish("/meta/f%d.bin" % chunks, payload)
            bf = src.have_filter()
            raw = len(src.store) * 64
            rows.append({"chunks": len(src.store), "bloom_bytes": bf.nbytes(),
                         "raw_id_bytes": raw,
                         "reduction": round(raw / max(1, bf.nbytes()), 1)})
            print("  %6d chunks -> bloom %8s vs raw ID list %8s (%.0fx smaller)"
                  % (rows[-1]["chunks"], human_bytes(bf.nbytes()),
                     human_bytes(raw), rows[-1]["reduction"]))
            if len(src.store) > 20000:
                break
    results["metadata_scaling"] = rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Ripple benchmark sweep")
    ap.add_argument("--quick", action="store_true", help="smaller, faster sweep")
    ap.add_argument("--out", default=os.path.join(RESULTS, "benchmarks.json"))
    ap.add_argument("--only", default="", help="comma-separated experiment numbers")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    results = {"generated": time.time(), "quick": args.quick}
    want = set(args.only.split(",")) if args.only else None

    def run(num, fn, *a):
        if want is None or num in want:
            fn(*a, results)

    if args.quick:
        run("1", bench_manifest_vs_bytes, [1 << 20, 8 << 20], 6)
        run("2", bench_source_egress, [3, 6, 10], 2 << 20)
        run("3", bench_edit, 4 << 20, 5)
        run("4", bench_dedup)
        run("5", bench_failure, 2 << 20, 6)
        run("6", bench_metadata_scaling, [])
        run("7", bench_ablation, 2 << 20, 8)
    else:
        run("1", bench_manifest_vs_bytes, [1 << 20, 8 << 20, 32 << 20], 10)
        run("2", bench_source_egress, [3, 5, 10, 15, 25], 4 << 20)
        run("3", bench_edit, 16 << 20, 10)
        run("4", bench_dedup)
        run("5", bench_failure, 8 << 20, 12)
        run("6", bench_metadata_scaling, [])
        run("7", bench_ablation, 4 << 20, 12)

    with open(args.out, "w") as fh:
        json.dump(results, fh, indent=2)
    print("\nwrote %s" % args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
