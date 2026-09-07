from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from typing import List, Tuple

from . import protocol as P
from .dashboard import ClusterView, Dashboard
from .metrics import human_bytes
from .node import RippleNode


def parse_addr(s: str) -> Tuple[str, int]:
    host, _, port = s.rpartition(":")
    return (host or "127.0.0.1", int(port))


def parse_addrs(s: str) -> List[Tuple[str, int]]:
    return [parse_addr(x) for x in s.split(",") if x.strip()]


def ask(addr: Tuple[str, int], msg: dict, timeout: float = 10.0):
    client = P.PeerClient(addr, timeout=timeout)
    try:
        head, body, _, _ = client.request(msg)
        return head, body
    finally:
        client.close()


def cmd_node(a) -> int:
    node = RippleNode(a.id, host=a.host, port=a.port, root=a.root, rack=a.rack,
                      fetch_policy=a.policy, source_policy=a.source,
                      fanout=a.fanout, fsync=a.fsync)
    node.start()
    print("[%s] listening on %s:%d (rack=%s, policy=%s/%s)"
          % (node.node_id, node.host, node.port, node.rack, a.policy, a.source),
          flush=True)

    for seed in parse_addrs(a.seed or ""):
        for attempt in range(60):
            if node.join(*seed):
                print("[%s] joined via %s:%d" % (node.node_id, *seed), flush=True)
                break
            time.sleep(1)
        else:
            print("[%s] WARNING could not reach seed %s:%d" % (node.node_id, *seed),
                  flush=True)

    if a.publish and a.publish_path:
        node.publish(a.publish_path, src_file=a.publish)
        print("[%s] published %s" % (node.node_id, a.publish_path), flush=True)

    stop = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("now", True))
    try:
        last = 0.0
        while not stop["now"]:
            time.sleep(1)
            if a.verbose and time.time() - last > 10:
                last = time.time()
                st = node.state()
                print("[%s] peers=%d files=%d chunks=%d fetched=%s"
                      % (node.node_id, st["alive"], len(st["files"]), st["chunks"],
                         human_bytes(st["counters"].get("bytes_fetched", 0))), flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
    return 0


def cmd_publish(a) -> int:
    addr = parse_addr(a.seed)
    head, _ = ask(addr, {"type": P.STATE_GET})
    node_id = head.get("state", {}).get("node", "?")
    print("target node %s" % node_id)
    print("note: run `publish` on the node that holds the file, or use the demo "
          "harness. Use `--local-root` to publish directly into a node's store.")
    if not a.local_root:
        return 2
    node = RippleNode(a.id or "publisher", root=a.local_root, port=0)
    m = node.publish(a.path, src_file=a.file)
    node.join(*addr)
    time.sleep(2.0)
    print("published %s: %s in %d chunks, manifest %s"
          % (a.path, human_bytes(m.size), len(m.chunks), human_bytes(m.nbytes())))
    node.stop()
    return 0


def cmd_ls(a) -> int:
    head, _ = ask(parse_addr(a.seed), {"type": P.STATE_GET})
    st = head.get("state", {})
    files = st.get("files", {})
    if not files:
        print("no files known to %s" % st.get("node"))
        return 0
    print("%-40s %10s %8s %-9s %s" % ("path", "size", "chunks", "state", "origin"))
    for path, f in sorted(files.items()):
        print("%-40s %10s %8d %-9s %s"
              % (path, human_bytes(f["size"]), f["chunks"], f["state"], f["origin"]))
    return 0


def cmd_stat(a) -> int:
    head, _ = ask(parse_addr(a.seed), {"type": P.STATE_GET})
    print(json.dumps(head.get("state", {}), indent=2)[:8000])
    return 0


def cmd_dash(a) -> int:
    view = ClusterView(peers=parse_addrs(a.peers))
    dash = Dashboard(view, port=a.port).start()
    print("dashboard on http://localhost:%d watching %d peers"
          % (dash.port, len(view.peers)), flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        dash.stop()
    return 0


def cmd_sim(a) -> int:
    from .sim import main as sim_main
    sys.argv = ["sim"] + a.rest
    return sim_main()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ripple", description="Ripple cluster file sync")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("node", help="run a node (blocks)")
    p.add_argument("--id", required=True)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=7000)
    p.add_argument("--root", default=None)
    p.add_argument("--rack", default="rack0")
    p.add_argument("--seed", default="", help="comma-separated host:port seeds")
    p.add_argument("--policy", default="lazy", choices=["lazy", "eager"])
    p.add_argument("--source", default="swarm", choices=["swarm", "star"])
    p.add_argument("--fanout", type=int, default=3)
    p.add_argument("--fsync", action="store_true")
    p.add_argument("--publish", default="", help="local file to publish on start")
    p.add_argument("--publish-path", default="", help="cluster path for --publish")
    p.add_argument("--verbose", action="store_true")
    p.set_defaults(fn=cmd_node)

    p = sub.add_parser("publish", help="publish a file into the cluster")
    p.add_argument("--seed", required=True)
    p.add_argument("--path", required=True, help="path within the cluster namespace")
    p.add_argument("--file", required=True, help="local file to read")
    p.add_argument("--local-root", default="", help="storage root for the publishing node")
    p.add_argument("--id", default="")
    p.set_defaults(fn=cmd_publish)

    p = sub.add_parser("ls", help="list files a node knows about")
    p.add_argument("--seed", required=True)
    p.set_defaults(fn=cmd_ls)

    p = sub.add_parser("stat", help="dump a node's full state as JSON")
    p.add_argument("--seed", required=True)
    p.set_defaults(fn=cmd_stat)

    p = sub.add_parser("dash", help="serve the dashboard against remote nodes")
    p.add_argument("--peers", required=True, help="comma-separated host:port list")
    p.add_argument("--port", type=int, default=8080)
    p.set_defaults(fn=cmd_dash)

    p = sub.add_parser("sim", help="run the protocol simulator")
    p.add_argument("rest", nargs=argparse.REMAINDER)
    p.set_defaults(fn=cmd_sim)

    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
