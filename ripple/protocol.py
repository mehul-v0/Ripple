"""Wire protocol: length-prefixed frames over TCP.

    magic(4) | header_len(4) | payload_len(4) | JSON header | raw payload

Two lengths so chunk bytes are not base64'd into JSON, which would cost 33% on
the largest category of traffic. Connections are long-lived and pooled.
"""

from __future__ import annotations

import json
import socket
import struct
import threading
from typing import Dict, List, Optional, Tuple

MAGIC = b"RPL1"
HDR = struct.Struct("!4sII")
MAX_HEADER = 8 << 20
MAX_PAYLOAD = 128 << 20

# message types
HELLO = "hello"                    # join: introduce self, receive peer table
GOSSIP = "gossip"                  # membership + manifest advertisements
MANIFEST_GET = "manifest_get"      # pull a manifest by path or digest
HAVE_GET = "have_get"              # request a Bloom summary of held chunks
CHUNK_GET = "chunk_get"            # request one chunk by hash
CHUNK_PROBE = "chunk_probe"        # exact "do you hold these?" (Bloom refinement)
PING = "ping"                      # RTT probe for latency-aware peer selection
STATE_GET = "state_get"            # dashboard aggregation
MERKLE_GET = "merkle_get"          # anti-entropy: level hashes
BUCKET_GET = "bucket_get"          # anti-entropy: keys inside a differing bucket
CHAOS = "chaos"                    # fault injection control


class ProtocolError(Exception):
    pass


def encode(msg: dict, payload: bytes = b"") -> bytes:
    head = json.dumps(msg, separators=(",", ":")).encode()
    return HDR.pack(MAGIC, len(head), len(payload)) + head + payload


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    if n == 0:
        return b""
    parts: List[bytes] = []
    got = 0
    while got < n:
        b = sock.recv(min(n - got, 1 << 20))
        if not b:
            raise ConnectionError("peer closed mid-frame")
        parts.append(b)
        got += len(b)
    return b"".join(parts)


def read_frame(sock: socket.socket) -> Tuple[dict, bytes]:
    magic, hlen, plen = HDR.unpack(_recv_exact(sock, HDR.size))
    if magic != MAGIC:
        raise ProtocolError("bad magic %r" % (magic,))
    # Bound both lengths before allocating: an oversized frame from a confused or
    # hostile peer must not exhaust our memory.
    if hlen > MAX_HEADER or plen > MAX_PAYLOAD:
        raise ProtocolError("frame too large (%d/%d)" % (hlen, plen))
    head = json.loads(_recv_exact(sock, hlen))
    return head, _recv_exact(sock, plen)


def send_frame(sock: socket.socket, msg: dict, payload: bytes = b"") -> int:
    buf = encode(msg, payload)
    sock.sendall(buf)
    return len(buf)


class PeerClient:
    """Pooled client for one remote node.

    One connection per in-flight request, so a chunk fetch and a gossip round
    do not interleave frames on the same socket.
    """

    def __init__(self, addr: Tuple[str, int], timeout: float = 5.0, max_conns: int = 4):
        self.addr = addr
        self.timeout = timeout
        self.max_conns = max_conns
        self._pool: List[socket.socket] = []
        self._lock = threading.Lock()

    def _acquire(self) -> socket.socket:
        with self._lock:
            if self._pool:
                return self._pool.pop()
        s = socket.create_connection(self.addr, timeout=self.timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.settimeout(self.timeout)
        return s

    def _release(self, sock: socket.socket) -> None:
        with self._lock:
            if len(self._pool) < self.max_conns:
                self._pool.append(sock)
                return
        try:
            sock.close()
        except OSError:
            pass

    def request(self, msg: dict, payload: bytes = b"") -> Tuple[dict, bytes, int, int]:
        """Send one request; return (header, payload, bytes_sent, bytes_recv)."""
        sock = self._acquire()
        try:
            sent = send_frame(sock, msg, payload)
            head, body = read_frame(sock)
            self._release(sock)
            return head, body, sent, HDR.size + len(json.dumps(head)) + len(body)
        except Exception:
            try:
                sock.close()
            except OSError:
                pass
            raise

    def close(self) -> None:
        with self._lock:
            pool, self._pool = self._pool, []
        for s in pool:
            try:
                s.close()
            except OSError:
                pass


def error(reason: str) -> dict:
    return {"type": "error", "reason": reason}
