# Ripple

Cluster file sync that moves **manifests** first and file bytes only when
something actually reads them.

Push a small manifest — the ordered list of content hashes that defines a file —
to every node, and the file *exists* everywhere immediately: right name, right
size, it opens. The bytes follow on demand, fetched from the nearest peer that
has them, and every node that receives a chunk becomes a source for it.

Python 3.8+, **standard library only, no dependencies.**

## Run it

```bash
python demo.py --nodes 14
```

It prints the dashboard URL — open that. (If port 8080 is taken it picks the
next free one and says so.)

Everything is a button. Publish a file and watch the manifest reach every node
while the byte counter stays at zero, then materialise it and watch chunks
spread outward. The chaos panel kills nodes, partitions links, corrupts disks,
and makes a node serve deliberate garbage — all live.

## As a library

```python
from ripple.node import RippleNode

a = RippleNode("n1", port=7001).start()
b = RippleNode("n2", port=7002).start()
b.join("127.0.0.1", 7001)

a.publish("/etc/app.conf", b"max_connections: 500\n")

b.state_of("/etc/app.conf")     # 'PHANTOM' — exists, holds no bytes
b.read("/etc/app.conf")         # b'max_connections: 500\n' — fetched on read
b.materialise("/etc/app.conf")  # pull every chunk
```

## Real nodes, separate processes

```bash
python -m ripple serve --id n1 --port 7001
python -m ripple serve --id n2 --port 7002 --join 127.0.0.1:7001
```

## What's in here

```
ripple/
  chunker.py     content-defined chunking (gear rolling hash, FastCDC)
  store.py       content-addressed chunk store; eviction under a disk budget
  manifest.py    manifests, version history, content identity
  node.py        gossip, lazy reads, anti-entropy, scrub, snapshots
  peers.py       membership, failure detection, RTT/rack awareness
  scheduler.py   rarest-chunk-first, latency-aware peer selection
  bloom.py       compact availability summaries
  versions.py    version vectors for concurrent edits
  merkle.py      anti-entropy tree, and the cluster-wide namespace root
  erasure.py     Reed-Solomon over GF(256): any k of k+m fragments rebuild it
  structured.py  config files chunked on their own grammar
  dashboard.py   live dashboard, chaos API, durability coverage
  chaos.py       fault injection, including a byzantine peer
  protocol.py    length-prefixed framing, pooled connections
  cluster.py     spin up N real nodes in one process
  sim.py         discrete-event simulator
demo.py          live cluster + dashboard in one command
```

Tests, benchmarks, architecture notes and generated results are kept out of this
repo, so what's here is just what you need to run it.
