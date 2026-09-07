"""Live cluster dashboard and chaos control surface.

Serves one page that polls a JSON endpoint, either reading in-process
RippleNode objects directly or asking remote peers over the Ripple protocol.
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from . import protocol as P

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class ClusterView:
    """Whatever the dashboard is looking at: local objects or remote peers."""

    def __init__(self, nodes: Optional[List] = None,
                 peers: Optional[List[Tuple[str, int]]] = None):
        self.nodes = nodes or []
        self.peers = peers or []
        self._clients: Dict[Tuple[str, int], P.PeerClient] = {}
        self._lock = threading.Lock()

    def _remote_state(self, addr: Tuple[str, int]) -> Optional[dict]:
        with self._lock:
            c = self._clients.get(addr)
            if c is None:
                c = self._clients[addr] = P.PeerClient(addr, timeout=2.0)
        try:
            head, _, _, _ = c.request({"type": P.STATE_GET})
            return head.get("state")
        except Exception:
            with self._lock:
                self._clients.pop(addr, None)
            return {"node": "%s:%d" % addr, "unreachable": True,
                    "files": {}, "peers": [], "counters": {}, "events": []}

    def states(self) -> List[dict]:
        if self.nodes:
            out = []
            for n in self.nodes:
                try:
                    out.append(n.state())
                except Exception as e:
                    out.append({"node": n.node_id, "error": str(e), "files": {},
                                "peers": [], "counters": {}, "events": []})
            return out
        return [s for s in (self._remote_state(a) for a in self.peers) if s]

    def find(self, node_id: str):
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        return None

    def chaos(self, node_id: str, action: str, args: dict) -> dict:
        node = self.find(node_id)
        if node is not None:
            return node.chaos.apply(action, **args)
        for addr in self.peers:
            st = self._remote_state(addr)
            if st and st.get("node") == node_id:
                with self._lock:
                    c = self._clients.get(addr)
                if c:
                    head, _, _, _ = c.request({"type": P.CHAOS, "action": action,
                                               "args": args})
                    return head
        return {"ok": False, "reason": "no such node %s" % node_id}


def aggregate(states: List[dict]) -> dict:
    """Roll per-node state up into the numbers the header shows."""
    files: Dict[str, dict] = {}
    totals = {"bytes_served": 0, "bytes_fetched": 0, "chunks_served": 0,
              "chunks_fetched": 0, "conflicts": 0, "corruption_detected": 0,
              "gossip_rounds": 0, "manifests_received": 0, "reads_faulted": 0,
              "antientropy_repairs": 0, "upload_rejected": 0, "chunk_busy": 0}
    events = []
    alive = 0
    for st in states:
        if st.get("unreachable") or st.get("chaos", {}).get("killed"):
            pass
        else:
            alive += 1
        for k in totals:
            totals[k] += st.get("counters", {}).get(k, 0)
        events.extend(st.get("events", []))
        for path, info in st.get("files", {}).items():
            f = files.setdefault(path, {"path": path, "size": info["size"],
                                        "origin": info.get("origin", "?"),
                                        "chunks": info.get("chunks", 0),
                                        "manifest_bytes": info.get("manifest_bytes", 0),
                                        "digest": info.get("digest", ""),
                                        "PHANTOM": 0, "PARTIAL": 0, "RESIDENT": 0,
                                        "have": 0, "want": 0})
            f[info["state"]] = f.get(info["state"], 0) + 1
            f["have"] += info.get("have", 0)
            f["want"] += info.get("chunks", 0)
    events.sort(key=lambda e: e.get("t", 0), reverse=True)
    return {"files": sorted(files.values(), key=lambda f: f["path"]),
            "totals": totals, "alive": alive, "events": events[:60]}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # keep the console readable during a demo
        pass

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code: int = 200) -> None:
        self._send(code, json.dumps(obj).encode(), "application/json")

    def do_GET(self) -> None:
        u = urlparse(self.path)
        view: ClusterView = self.server.view
        if u.path in ("/", "/index.html"):
            try:
                with open(os.path.join(STATIC, "dashboard.html"), "rb") as fh:
                    return self._send(200, fh.read(), "text/html; charset=utf-8")
            except OSError:
                return self._send(500, b"dashboard.html missing", "text/plain")
        if u.path == "/api/state":
            states = view.states()
            return self._json({"t": time.time(), "nodes": states,
                               "summary": aggregate(states)})
        if u.path == "/api/versions":
            path = parse_qs(u.query).get("path", [""])[0]
            node = view.nodes[0] if view.nodes else None
            if node is None:
                return self._json({"versions": []})
            return self._json({"versions": [
                {"digest": m.digest(), "created": m.created, "size": m.size,
                 "origin": m.origin, "note": m.note}
                for m in node.manifests.version_list(path)]})
        return self._send(404, b"not found", "text/plain")

    def do_POST(self) -> None:
        u = urlparse(self.path)
        view: ClusterView = self.server.view
        n = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return self._json({"ok": False, "reason": "bad json"}, 400)

        if u.path == "/api/chaos":
            return self._json(view.chaos(body.get("node", ""), body.get("action", ""),
                                         body.get("args", {})))
        if u.path == "/api/action":
            handler = self.server.actions.get(body.get("action", ""))
            if handler is None:
                return self._json({"ok": False, "reason": "unknown action"}, 400)
            try:
                return self._json({"ok": True, "result": handler(body)})
            except Exception as e:
                return self._json({"ok": False, "reason": str(e)}, 500)
        return self._send(404, b"not found", "text/plain")


class Dashboard:
    def __init__(self, view: ClusterView, port: int = 8080, host: str = "0.0.0.0"):
        self.view = view
        self.server = ThreadingHTTPServer((host, port), _Handler)
        self.server.view = view
        self.server.actions: Dict[str, Callable] = {}
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]

    def action(self, name: str, fn: Callable) -> None:
        """Register a demo control (publish, edit, rollback, materialise...)."""
        self.server.actions[name] = fn

    def start(self) -> "Dashboard":
        threading.Thread(target=self.server.serve_forever, daemon=True,
                         name="dashboard").start()
        return self

    def stop(self) -> None:
        try:
            self.server.shutdown()
            self.server.server_close()
        except Exception:
            pass
