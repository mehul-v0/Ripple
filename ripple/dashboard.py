from __future__ import annotations

import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from . import protocol as P

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class ClusterView:

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

    def durability(self) -> Dict[str, dict]:
        if not self.nodes:
            return {}
        live = [n for n in self.nodes if not n.chaos.killed]
        out: Dict[str, dict] = {}
        seen = set()
        for n in live:
            for m in n.manifests.all_current():
                if m.path in seen:
                    continue
                seen.add(m.path)
                out[m.path] = _durability_of(m, live)
        return out

    def consistency(self) -> dict:
        roots: Dict[str, List[str]] = {}
        for n in self.nodes:
            if n.chaos.killed:
                continue
            roots.setdefault(n.namespace_root(), []).append(n.node_id)
        if not roots:
            return {"agreed": False, "roots": {}, "majority": "", "outliers": []}
        majority = max(roots, key=lambda r: len(roots[r]))
        return {"agreed": len(roots) == 1,
                "roots": {r[:16]: sorted(v) for r, v in roots.items()},
                "majority": majority[:16],
                "outliers": sorted(n for r, v in roots.items()
                                   if r != majority for n in v)}

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


def _durability_of(m, live) -> dict:
    weakest = None
    for chunk in m.chunks:
        grp = (m.ec or {}).get(chunk)
        if grp is None:
            tolerated = sum(1 for n in live if n.store.has(chunk)) - 1
        else:
            k, frags = grp["k"], grp["frags"]
            holders = [n for n in live if n.store.has(chunk)]
            contrib = sorted(
                (len([f for f in frags if n.store.has(f)]) for n in live
                 if n not in holders), reverse=True)
            killed = len(holders)
            remaining = len([f for f in frags
                             if any(n.store.has(f) for n in live
                                    if n not in holders)])
            for c in contrib:
                if remaining - c < k:
                    break
                remaining -= c
                killed += 1
            tolerated = killed - 1
        if weakest is None or tolerated < weakest:
            weakest = tolerated
    return {"survives": max(-1, weakest if weakest is not None else -1),
            "chunks": len(m.chunks),
            "coded": bool(m.ec)}


_PRIORITY_COUNTERS = [
    "bytes_served", "bytes_fetched", "chunks_served", "chunks_fetched",
    "gossip_rounds", "gossip_ok", "msgs_sent", "msgs_recv",
    "bytes_sent", "bytes_recv", "manifests_received", "reads_faulted",
    "conflicts", "corruption_detected", "chunks_scrubbed",
    "antientropy_clean", "antientropy_repairs",
    "ec_files_published", "ec_reconstructions", "ec_fallback_triggered",
    "ec_reconstruct_failed",
    "upload_rejected", "chunk_busy", "chunk_misses", "fetch_errors",
    "corrupt_received", "served_misses", "prefetch_queued", "cross_rack_bytes",
]


def aggregate(states: List[dict], durability: Optional[Dict[str, dict]] = None) -> dict:
    files: Dict[str, dict] = {}
    seen_counters: Dict[str, int] = {}
    events = []
    alive = 0
    nodes_summary = []
    for st in states:
        is_up = not (st.get("unreachable") or st.get("chaos", {}).get("killed"))
        if is_up:
            alive += 1
        counters = st.get("counters", {})
        for k, v in counters.items():
            if isinstance(v, (int, float)):
                seen_counters[k] = seen_counters.get(k, 0) + v
        events.extend(st.get("events", []))
        nodes_summary.append({
            "node": st.get("node"), "rack": st.get("rack"), "policy": st.get("policy"),
            "up": is_up, "alive_peers": st.get("alive", 0),
            "chunks": st.get("chunks", 0), "store_bytes": st.get("store_bytes", 0),
            "logical_bytes": st.get("logical_bytes", 0),
            "dedup_ratio": st.get("dedup_ratio", 1.0),
            "merkle_root": st.get("merkle_root", ""),
            "scheduler": st.get("scheduler", {}), "chaos": st.get("chaos", {}),
            "uptime": counters.get("uptime", 0),
            "overcommit": st.get("overcommit", {}),
        })
        for path, info in st.get("files", {}).items():
            f = files.setdefault(path, {"path": path, "size": info["size"],
                                        "origin": info.get("origin", "?"),
                                        "chunks": info.get("chunks", 0),
                                        "manifest_bytes": info.get("manifest_bytes", 0),
                                        "digest": info.get("digest", ""),
                                        "labels": info.get("labels", False),
                                        "PHANTOM": 0, "PARTIAL": 0, "RESIDENT": 0,
                                        "have": 0, "want": 0, "ec": None,
                                        "ec_fragments_on_origin": 0,
                                        "ec_fragments_elsewhere": 0,
                                        "ec_fragments_total": 0})
            f[info["state"]] = f.get(info["state"], 0) + 1
            f["have"] += info.get("have", 0)
            f["want"] += info.get("chunks", 0)
            if info.get("ec") and f["ec"] is None:
                f["ec"] = {k: info["ec"][k] for k in ("k", "m", "overhead", "tolerates")}
            if info.get("ec"):
                present = info["ec"].get("fragments_present", 0)
                if st.get("node") == info.get("origin"):
                    f["ec_fragments_on_origin"] = present
                else:
                    f["ec_fragments_elsewhere"] += present
                f["ec_fragments_total"] = max(f["ec_fragments_total"],
                                              info["ec"].get("fragments_total", 0))
    for path, d in (durability or {}).items():
        if path in files:
            files[path]["durability"] = d
    events.sort(key=lambda e: e.get("t", 0), reverse=True)
    ordered = {k: seen_counters[k] for k in _PRIORITY_COUNTERS if k in seen_counters}
    ordered.update({k: v for k, v in sorted(seen_counters.items()) if k not in ordered})
    return {"files": sorted(files.values(), key=lambda f: f["path"]),
            "totals": ordered, "alive": alive, "events": events[:600],
            "nodes_summary": nodes_summary}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
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
            try:
                dur = view.durability()
            except Exception:
                dur = {}
            return self._json({"t": time.time(), "nodes": states,
                               "summary": aggregate(states, dur)})
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
        self.server = self._bind(host, port)
        self.server.view = view
        self.server.actions: Dict[str, Callable] = {}
        self.server.daemon_threads = True
        self.host, self.port = self.server.server_address[:2]

    @staticmethod
    def _in_use(host: str, port: int) -> bool:
        probe = socket.socket()
        probe.settimeout(0.25)
        try:
            return probe.connect_ex(("127.0.0.1" if host in ("0.0.0.0", "") else host,
                                     port)) == 0
        except OSError:
            return False
        finally:
            probe.close()

    @classmethod
    def _bind(cls, host: str, port: int) -> ThreadingHTTPServer:
        attempts = []
        hosts = [host, "127.0.0.1"] if host != "127.0.0.1" else [host]
        for h in hosts:
            for p in range(port, port + 12):
                if cls._in_use(h, p):
                    attempts.append("%s:%d (in use)" % (h, p))
                    continue
                try:
                    return ThreadingHTTPServer((h, p), _Handler)
                except OSError as e:
                    attempts.append("%s:%d (%s)" % (h, p, e.__class__.__name__))
        raise OSError("dashboard could not bind a port; tried %s" % ", ".join(attempts))

    def action(self, name: str, fn: Callable) -> None:
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
