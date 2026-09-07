from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ripple.cluster import LocalCluster
from ripple.dashboard import ClusterView, Dashboard
from ripple.metrics import human_bytes

DEMO_PATH = "/cluster/vm-image.qcow2"
CONF_PATH = "/etc/cluster-db.yaml"
EC_PATH = "/critical/durable.bin"

BASE_CONF = b"""# cluster database configuration
max_connections: 500
timeout_seconds: 30
replication:
  factor: 3
  mode: synchronous
logging:
  level: info
"""


def make_payload(mb: int, seed: int = 5) -> bytes:
    rng = random.Random(seed)
    block = bytes(rng.getrandbits(8) for _ in range(8192))
    out = bytearray()
    target = mb << 20
    while len(out) < target:
        out.extend(block if rng.random() > 0.3
                   else bytes(rng.getrandbits(8) for _ in range(8192)))
    return bytes(out[:target])


def main() -> int:
    ap = argparse.ArgumentParser(description="Ripple live demo")
    ap.add_argument("--nodes", type=int, default=24)
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--racks", type=int, default=4)
    ap.add_argument("--root", default=os.path.join(".ripple-demo"))
    ap.add_argument("--scrub-period", type=float, default=10.0,
                    help="seconds within which every chunk is re-verified")
    ap.add_argument("--publish", type=int, default=0,
                    help="publish an N-MB file at startup (0 = wait for a button)")
    a = ap.parse_args()

    shutil.rmtree(a.root, ignore_errors=True)
    print("starting %d nodes across %d racks ..." % (a.nodes, a.racks))
    cluster = LocalCluster(a.nodes, root=a.root, racks=a.racks,
                           scrub_period=a.scrub_period).start()
    print("cluster formed: every node knows %d peers"
          % len(cluster.nodes[0].membership.alive()))

    view = ClusterView(nodes=cluster.nodes)
    dash = Dashboard(view, port=a.port)

    def live(path=None):
        for nd in cluster.nodes:
            if nd.chaos.killed:
                continue
            if path is None or nd.manifests.current(path):
                return nd
        for nd in cluster.nodes:
            if not nd.chaos.killed:
                return nd
        raise ValueError("every node is killed -- revive one first")

    def reachable():
        return [nd for nd in cluster.nodes
                if not nd.chaos.killed and not nd.chaos.partitioned]

    def await_on(nodes, path, timeout=20.0):
        end = time.time() + timeout
        t0 = time.perf_counter()
        while time.time() < end:
            if all(nd.manifests.current(path) for nd in nodes):
                return round(time.perf_counter() - t0, 3)
            time.sleep(0.02)
        return None

    def do_publish(body):
        mb = int(body.get("size_mb") or 8)
        data = make_payload(mb)
        t0 = time.perf_counter()
        targets = reachable()
        m = live().publish(DEMO_PATH, data)
        seen = await_on(targets, DEMO_PATH, timeout=30)
        return {"path": DEMO_PATH, "size": len(data), "manifest_bytes": m.nbytes(),
                "chunks": len(m.chunks), "nodes": len(targets),
                "manifest_everywhere_s": seen,
                "published_s": round(time.perf_counter() - t0, 3)}

    def do_edit(body):
        path = body.get("path") or DEMO_PATH
        src = live(path)
        m = src.manifests.current(path)
        if not m:
            raise ValueError("nothing published yet")
        new = src.edit(path, m.size // 2, b"# changed by the operator\n")
        return {"path": path, "new_bytes": src.metrics.get("last_edit_new_bytes"),
                "file_bytes": new.size}

    def do_materialise(body):
        path = body.get("path") or DEMO_PATH
        targets = [nd for nd in reachable() if nd.state_of(path) != "RESIDENT"]
        for nd in targets:
            nd.materialise(path, block=False)
        return {"path": path, "requested": len(targets)}

    def do_rollback(body):
        path = body.get("path") or DEMO_PATH
        src = live(path)
        versions = src.manifests.version_list(path)
        if len(versions) < 2:
            raise ValueError("need at least two versions to roll back")
        target = versions[-2]
        t0 = time.perf_counter()
        src.rollback(path, target.digest())
        alive = reachable()
        deadline = time.time() + 20
        converged = False
        while time.time() < deadline:
            cur = [nd.manifests.current(path) for nd in alive]
            if all(c and c.chunks == target.chunks for c in cur):
                converged = True
                break
            time.sleep(0.02)
        return {"path": path, "to": target.digest()[:12], "nodes": len(alive),
                "converged": converged,
                "cluster_wide_s": round(time.perf_counter() - t0, 3)}

    def do_publish_conf(body):
        targets = reachable()
        m = live().publish(CONF_PATH, BASE_CONF)
        seen = await_on(targets, CONF_PATH, timeout=20)
        return {"path": CONF_PATH, "keys": [l for l in (m.labels or [])
                                            if not l.startswith("<")],
                "chunks": len(m.chunks), "nodes": len(targets),
                "manifest_everywhere_s": seen}

    def do_drift(body):
        target = body.get("node") or cluster.nodes[-1].node_id
        node = view.find(target)
        if node is None or node.manifests.current(CONF_PATH) is None:
            raise ValueError("publish the config first")
        node.chaos.apply("partition",
                         peers=[n.node_id for n in cluster.nodes if n is not node])
        node.publish(CONF_PATH,
                     BASE_CONF.replace(b"max_connections: 500",
                                       b"max_connections: 200"))
        time.sleep(0.3)
        return {"node": target, "changed": "max_connections: 500 -> 200",
                "note": "left partitioned so the disagreement persists"}

    def do_check_drift(body):
        before = cluster.total("bytes_fetched")
        report = cluster.drift(CONF_PATH)
        out = []
        for item in report:
            for grp in item["outliers"]:
                value = None
                for nd in cluster.nodes:
                    blob = nd.store.get(grp["hash"])
                    if blob:
                        value = blob.decode("utf-8", "replace").strip()
                        break
                out.append({"key": item["key"], "nodes": grp["nodes"],
                            "value": value, "agree": item["majority_count"]})
        for nd in cluster.nodes:
            nd.metrics.event("drift", "checked %s: %d key(s) drifted, 0 bytes moved"
                             % (CONF_PATH, len(out)))
        return {"drifted": out,
                "bytes_moved_to_find_it": cluster.total("bytes_fetched") - before}

    def do_corrupt(body):
        node = view.find(body.get("node", ""))
        path = body.get("path") or DEMO_PATH
        if node is None:
            raise ValueError("no such node")
        m = node.manifests.current(path)
        if not m:
            raise ValueError("that node has no manifest for %s" % path)
        held = [c for c in m.unique_chunks() if node.store.has(c)]
        if not held:
            raise ValueError("that node holds no chunks yet -- materialise first")
        victim = random.choice(held)
        node.store.damage(victim)
        node.metrics.event("chaos", "operator corrupted chunk %s on disk" % victim[:12])
        return {"node": node.node_id, "chunk": victim[:12]}

    def do_publish_ec(body):
        k = int(body.get("k") or 4)
        m = int(body.get("m") or 2)
        size = int(body.get("size_bytes") or 1536)
        if body.get("size_mb"):
            size = int(body["size_mb"]) << 20
        data = make_payload(max(1, size // 8192) or 1, seed=99)[:size] if size >= 8192 \
            else os.urandom(size)
        publisher = live()
        t0 = time.perf_counter()
        man = publisher.publish(EC_PATH, data, erasure_k=k, erasure_m=m)
        seen = await_on(reachable(), EC_PATH, timeout=20)
        frag_count = sum(len(g["frags"]) for g in (man.ec or {}).values())
        return {"path": EC_PATH, "k": k, "m": m, "size": len(data),
                "chunks": len(man.chunks), "fragments": frag_count,
                "overhead": round((k + m) / k, 2), "tolerates": m,
                "manifest_everywhere_s": seen,
                "published_s": round(time.perf_counter() - t0, 3)}

    def do_ec_spread(body):
        n = int(body.get("n") or 8)
        pub = live(EC_PATH)
        targets = [nd for nd in reachable() if nd is not pub][:n]
        for nd in targets:
            nd.materialise(EC_PATH, block=False)
        return {"requested": len(targets)}

    def do_ec_kill_and_prove(body):
        man = live(EC_PATH).manifests.current(EC_PATH)
        if not man or not man.ec:
            raise ValueError("publish the erasure-coded file first")
        grp = next(iter(man.ec.values()))
        k, m, frags = grp["k"], grp["m"], grp["frags"]
        alive = [nd for nd in reachable() if nd.node_id != man.origin]
        holders = {nd.node_id: [i for i, f in enumerate(frags) if nd.store.has(f)]
                   for nd in alive}
        ranked = sorted((n for n, fs in holders.items() if fs),
                        key=lambda n: -len(holders[n]))
        to_kill = ranked[:m]
        for nid in to_kill:
            view.find(nid).chaos.apply("kill")
        time.sleep(0.05)
        remaining = [nd for nd in reachable()
                     if nd.state_of(EC_PATH) != "RESIDENT" and nd.node_id != man.origin]
        if not remaining:
            remaining = [nd for nd in reachable() if nd.node_id != man.origin]
        reader = remaining[0] if remaining else reachable()[0]
        t0 = time.perf_counter()
        got = reader.read(EC_PATH, 0, min(4096, man.size))
        return {"killed": to_kill, "reader": reader.node_id,
                "read_s": round(time.perf_counter() - t0, 3),
                "bytes_read": len(got), "k": k, "m_tolerated": m,
                "note": "reconstructed from surviving fragments; killed nodes "
                        "held %d fragment(s) between them" % sum(
                            len(holders[n]) for n in to_kill)}

    def do_overcommit(body):
        cap = int(body.get("cap_bytes") or 512 * 1024)
        n_files = int(body.get("files") or 5)
        size = int(body.get("size_bytes") or 300 * 1024)

        pub = live()
        others = [nd for nd in reachable() if nd is not pub]
        if not others:
            raise ValueError("need at least two live nodes")
        victim = min(others, key=lambda nd: nd.store.physical_bytes())

        payloads = {}
        for i in range(n_files):
            path = "/overcommit/f%d.bin" % i
            data = make_payload(1, seed=200 + i)[:size]
            pub.publish(path, data)
            payloads[path] = data
        await_on(reachable(), list(payloads)[-1], timeout=20)

        victim.capacity_bytes = cap
        t0 = time.perf_counter()
        for path in payloads:
            victim.materialise(path, timeout=30)
        for _ in range(4):
            victim._evict()

        verified = 0
        for path, expected in payloads.items():
            got = victim.read(path, 0, len(expected), timeout=30)
            if got != expected:
                raise ValueError("%s did not read back exactly after eviction" % path)
            verified += len(got)
        oc = victim.overcommit()
        return {"node": victim.node_id, "cap": cap,
                "stored_bytes": oc["physical_bytes"],
                "presented_bytes": oc["presented_bytes"],
                "files_present": oc["files"], "ratio": oc["ratio"],
                "evicted_chunks": int(victim.metrics.get("chunks_evicted")),
                "evicted_bytes": int(victim.metrics.get("bytes_evicted")),
                "bytes_verified": verified,
                "elapsed_s": round(time.perf_counter() - t0, 3),
                "note": "every file read back byte-exact while the node stored "
                        "a fraction of them"}

    snapshots: dict = {}

    def do_snapshot(body):
        nd = live()
        snap = nd.snapshot(note=body.get("note") or "manual snapshot")
        label = time.strftime("%H:%M:%S", time.localtime(snap["at"]))
        snapshots[label] = snap
        cons = view.consistency()
        return {"label": label, "root": snap["root"][:16],
                "files": len(snap["entries"]),
                "bytes_to_keep": len(json.dumps(snap["entries"])),
                "cluster_agrees": cons["agreed"], "outliers": cons["outliers"]}

    def do_vandalise(body):
        nd = live()
        touched, skipped = [], []
        for path in list(nd.manifests.paths()):
            cur = nd.manifests.current(path)
            if not cur:
                continue
            if cur.ec:
                skipped.append(path)
                continue
            nd.publish(path, b"ENCRYPTED-BY-RANSOMWARE " * 64, note="vandalised")
            touched.append(path)
        if not touched:
            raise ValueError(
                "nothing to vandalise -- publish a file first"
                + (" (skipped %d erasure-coded file(s))" % len(skipped) if skipped else ""))
        await_on(reachable(), touched[-1], timeout=15)
        return {"vandalised": touched, "skipped_coded": skipped,
                "note": "every file above now holds garbage"}

    def do_restore(body):
        label = body.get("label") or (sorted(snapshots)[0] if snapshots else "")
        snap = snapshots.get(label)
        if not snap:
            raise ValueError("take a snapshot first")
        nd = live()
        t0 = time.perf_counter()
        res = nd.restore(snap)
        deadline = time.time() + 15
        target = snap["root"]
        while time.time() < deadline:
            if all(x.namespace_root() == target for x in reachable()):
                break
            time.sleep(0.05)
        cons = view.consistency()
        return {"label": label, "restored": len(res["restored"]),
                "already_correct": len(res["already"]),
                "created_since": res["created_since"],
                "root_target": snap["root"][:16],
                "root_now": nd.namespace_root()[:16],
                "matches": nd.namespace_root() == snap["root"],
                "cluster_agrees": cons["agreed"],
                "elapsed_s": round(time.perf_counter() - t0, 3)}

    def do_consistency(body):
        return view.consistency()

    for name, fn in (("publish", do_publish), ("edit", do_edit),
                     ("materialise", do_materialise), ("rollback", do_rollback),
                     ("corrupt", do_corrupt), ("publish_conf", do_publish_conf),
                     ("drift", do_drift), ("check_drift", do_check_drift),
                     ("publish_ec", do_publish_ec), ("ec_spread", do_ec_spread),
                     ("ec_kill_and_prove", do_ec_kill_and_prove),
                     ("overcommit", do_overcommit),
                     ("snapshot", do_snapshot), ("vandalise", do_vandalise),
                     ("restore", do_restore), ("consistency", do_consistency)):
        dash.action(name, fn)

    dash.start()
    if dash.port != a.port:
        print("\n  note: port %d was unavailable, using %d instead" % (a.port, dash.port))
    print("\n  dashboard:  http://localhost:%d\n" % dash.port)

    if a.publish:
        r = do_publish({"size_mb": a.publish})
        print("published %s (%s) -- manifest reached all %d nodes in %.3fs"
              % (DEMO_PATH, human_bytes(r["size"]), a.nodes, r["manifest_everywhere_s"]))
        print("bytes moved so far: %s  (the file exists everywhere; "
              "almost none of it has been transferred)"
              % human_bytes(cluster.total("bytes_fetched")))

    print("Ctrl-C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nstopping ...")
    finally:
        dash.stop()
        cluster.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
