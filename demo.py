"""Live cluster plus dashboard, in one command.

    python demo.py --nodes 24
    open http://localhost:8080

Every button acts on a real cluster of real nodes over real sockets. The chaos
controls are the same hooks the test suite uses.
"""

from __future__ import annotations

import argparse
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
    """Mostly repeated structure with unique regions."""
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
    # Faster scrub than a production default so self-healing is visible live.
    cluster = LocalCluster(a.nodes, root=a.root, racks=a.racks,
                           scrub_period=a.scrub_period).start()
    print("cluster formed: every node knows %d peers"
          % len(cluster.nodes[0].membership.alive()))

    view = ClusterView(nodes=cluster.nodes)
    dash = Dashboard(view, port=a.port)

    # --- demo actions, wired to the dashboard buttons ----------------------

    def do_publish(body):
        mb = int(body.get("size_mb") or 8)
        data = make_payload(mb)
        t0 = time.perf_counter()
        m = cluster.nodes[0].publish(DEMO_PATH, data)
        seen = cluster.await_manifest(DEMO_PATH, timeout=30)
        return {"path": DEMO_PATH, "size": len(data), "manifest_bytes": m.nbytes(),
                "chunks": len(m.chunks),
                "manifest_everywhere_s": round(seen, 3),
                "published_s": round(time.perf_counter() - t0, 3)}

    def do_edit(body):
        path = body.get("path") or DEMO_PATH
        src = cluster.nodes[0]
        m = src.manifests.current(path)
        if not m:
            raise ValueError("nothing published yet")
        new = src.edit(path, m.size // 2, b"# changed by the operator\n")
        return {"path": path, "new_bytes": src.metrics.get("last_edit_new_bytes"),
                "file_bytes": new.size}

    def do_materialise(body):
        path = body.get("path") or DEMO_PATH
        for nd in cluster.nodes[1:]:
            nd.materialise(path, block=False)
        return {"path": path, "requested": len(cluster.nodes) - 1}

    def do_rollback(body):
        path = body.get("path") or DEMO_PATH
        src = cluster.nodes[0]
        versions = src.manifests.version_list(path)
        if len(versions) < 2:
            raise ValueError("need at least two versions to roll back")
        target = versions[-2]
        t0 = time.perf_counter()
        src.rollback(path, target.digest())
        deadline = time.time() + 20
        while time.time() < deadline:
            cur = [nd.manifests.current(path) for nd in cluster.nodes]
            if all(c and c.chunks == target.chunks for c in cur):
                break
            time.sleep(0.02)
        return {"path": path, "to": target.digest()[:12],
                "cluster_wide_s": round(time.perf_counter() - t0, 3)}

    def do_publish_conf(body):
        """Push a config file to every node, chunked on its grammar."""
        m = cluster.nodes[0].publish(CONF_PATH, BASE_CONF)
        seen = cluster.await_manifest(CONF_PATH, timeout=20)
        return {"path": CONF_PATH, "keys": [l for l in (m.labels or [])
                                            if not l.startswith("<")],
                "chunks": len(m.chunks),
                "manifest_everywhere_s": round(seen, 3)}

    def do_drift(body):
        """Make one node disagree, in a partition so it is genuine drift."""
        target = body.get("node") or cluster.nodes[-1].node_id
        node = view.find(target)
        if node is None or node.manifests.current(CONF_PATH) is None:
            raise ValueError("publish the config first")
        # Stay partitioned. Healing here would let the edit propagate and become the
        # majority, so the check would report whichever node had not caught up.
        node.chaos.apply("partition",
                         peers=[n.node_id for n in cluster.nodes if n is not node])
        node.publish(CONF_PATH,
                     BASE_CONF.replace(b"max_connections: 500",
                                       b"max_connections: 200"))
        time.sleep(0.3)
        return {"node": target, "changed": "max_connections: 500 -> 200",
                "note": "left partitioned so the disagreement persists"}

    def do_check_drift(body):
        """Answer 'who has drifted' from manifests alone."""
        before = cluster.total("bytes_fetched")
        report = cluster.drift(CONF_PATH)
        out = []
        for item in report:
            for grp in item["outliers"]:
                # The stanza may live on any node; the outliers are usually PHANTOM.
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
        """Flip a byte on disk under a node, without telling it."""
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

    for name, fn in (("publish", do_publish), ("edit", do_edit),
                     ("materialise", do_materialise), ("rollback", do_rollback),
                     ("corrupt", do_corrupt), ("publish_conf", do_publish_conf),
                     ("drift", do_drift), ("check_drift", do_check_drift)):
        dash.action(name, fn)

    dash.start()
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
