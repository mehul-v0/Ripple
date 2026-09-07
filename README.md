# Ripple

**Cluster file sync that moves meaning first and bytes only when needed.**

Nutanix Hackathon, IIT Guwahati — Project Area #2: *keep files in sync across
the machines of a cluster, scalable to hundreds of machines.*

---

## The idea

Every other approach assumes "sync" means moving bytes to N machines. Ripple
moves **manifests** — the ordered list of content hashes that defines a file,
a few KB regardless of file size — and materialises bytes lazily on first read.

Push the manifest to all N nodes and the file *exists* everywhere: correct name,
correct size, correct metadata, visible in the directory, openable. The bytes
arrive when something actually reads them, fetched from the nearest peer that
has them, and every node that receives a chunk immediately becomes a source for
it.

```
        publish 10 GB                          read() on node 47
              |                                        |
   chunk + hash + manifest                    fetch just the chunks
              |                                that this read touches
              v                                        v
     641 KB manifest gossips               PHANTOM ---> PARTIAL ---> RESIDENT
     to all 50 nodes in 4.3 s              exists      materialising   local
```

*Those are measured, not illustrative: a real 10 GB file of unique data across 50
real nodes. First byte on a node holding none of it: 0.25 s.*

Full reasoning, layer by layer, in **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

---

## Quick start

No dependencies. Python 3.8+, standard library only.

```bash
# live cluster + dashboard
python demo.py --nodes 24 --publish 8
# then open http://localhost:8080

# tests
python -m unittest discover -s tests -v

# every number in the deck, reproduced from scratch (~15 min)
python reproduce.py --quick

# the headline at real file sizes (slow: ~2 h, ingest is CPU-bound)
python reproduce.py --quick --large 1,10
```

The dashboard is the demo: publish a file, watch the manifest reach every node
almost instantly while the byte counter barely moves, then materialise it and
watch chunks ripple outward. The chaos panel is wired to the same fault
injection the test suite uses — **kill nodes, partition links, add latency,
corrupt a disk, and watch it heal, live.**

### A 50-node cluster in Docker

```bash
python docker/gen_compose.py --nodes 50 --racks 5 > docker/docker-compose.yml
docker compose -f docker/docker-compose.yml up --build
# dashboard on http://localhost:8080
```

Each container is one node. Node 1 is the seed; everyone else joins through it
and gossip does the rest — no node needs a complete peer list to start, which is
the property that makes this deployable at 300 nodes rather than merely runnable
at 3.

### As a library

```python
from ripple.node import RippleNode

a = RippleNode("n1", port=7001).start()
b = RippleNode("n2", port=7002).start()
b.join("127.0.0.1", 7001)

a.publish("/etc/app.conf", b"max_connections: 500\n")

# On b the file now exists with no bytes transferred...
b.state_of("/etc/app.conf")        # -> 'PHANTOM'
b.read("/etc/app.conf")            # -> b'max_connections: 500\n'  (materialises on read)
b.materialise("/etc/app.conf")     # -> pull every chunk
```

---

## Measured results

All numbers produced by `python reproduce.py`; raw JSON and SVG charts land in
`results/`. Nothing is cached in the repo.

| | result |
|---|---|
| **10 GB present on all 50 nodes** | **4.32 s** — 49 of 49 receivers holding zero bytes |
| **First byte on a node holding nothing** | **0.25 s**, verified against the original file |
| Manifest everywhere vs. every byte everywhere | **46 ms vs 14.3 s** for an 8 MB file — **307x** |
| Manifest size | **641 KB describes a 10 GB file** (1/16,352 of it); scales with chunk count, not file size |
| Source egress, N=10 (9 receivers) | **2.0x the file** vs **9.0x** for a star — and converged faster (8.1 s vs 11.5 s) |
| Source egress trend, N=3 -> 10 | Ripple **1.2x -> 2.0x**; star **2.0x -> 9.0x** (exactly N-1, per-receiver 1.00) |
| Scheduling ablation, 3 trials | publisher share **73% -> 23%**, egress **5.07x -> 1.63x** |
| Recovery after killing the publisher | **0.24 s** to full convergence (once the swarm held every chunk, at 2.9 s) |
| Chunks surviving a 1-byte prepend | **99.8%** (fixed blocks: ~0%) |
| A 34-byte edit to a 4 MB file | **8.4 KB moved — 0.20% of the file** |
| Cluster-wide dedup, 11 VM-image variants | **14.5x** (44 MB logical in 3.0 MB physical) |
| Availability metadata, 840 chunks | **0.8 KB Bloom vs 52 KB raw ID list — 63x smaller** |
| Merkle divergence search, 5,000 keys, 1 differing | **1 of 256 buckets inspected** |
| Simulated 10 -> 300 nodes | convergence **5.4 s -> 10.9 s**; source egress **1.6x -> 4.1x the file** |
| Simulator vs reality on unseen sizes | **9.1% egress error, below the 11.6% measurement noise floor** |

The headline: **the publisher's egress stays roughly flat as the cluster grows,
where a star topology grows linearly — and the swarm converges faster anyway.**

### Charts

| file | what it shows |
|---|---|
| `chart_egress.svg` | source egress vs cluster size, Ripple vs star — *the money chart* |
| `chart_harness_limit.svg` | why the real cluster's wall-clock is linear (the rig saturates) |
| `chart_sim_scale.svg` | projection to 1,000 nodes |
| `chart_convergence.svg` | convergence time vs cluster size, Ripple vs star |
| `chart_sim_egress.svg` | simulated publisher egress as a multiple of file size |
| `chart_validation.svg` | simulator vs reality, with the error stated |
| `chart_manifest_vs_bytes.svg` | the thesis, quantified |
| `chart_metadata.svg` | Bloom summary vs raw chunk-ID list |
| `chart_edit.svg` | bytes moved by a one-line change to a large file |

### Config drift, answered from metadata alone

Files with a grammar (YAML, JSON, INI) are chunked on the grammar instead of the
bytes — one chunk per top-level key, each carrying a label. Since manifests are
already gossiped everywhere, comparing label → chunk-hash maps names the drifted
key **without reading or transferring any file bytes**:

```
$ cluster.show_drift("/etc/db.yaml")
  max_connections: 4 node(s) agree
    max_connections          differs on n003 -> max_connections: 200

bytes fetched across whole cluster: 0
```

Cluster config drift is a real operational problem, and this answers it from
data every node already holds in memory.

---

## A claim we had wrong, and fixed

The docs used to say a 10 GB file has a ~90 KB manifest. **That was wrong by
1000×.** Manifest size is driven by chunk *count*: at a fixed 8 KB chunk, 10 GB
is 1.31 million chunks and a **92 MB** manifest — not something you can gossip
eagerly, and it would have broken the central claim at exactly the sizes the
claim matters most for.

The fix is that chunk size scales with file size (`chunker.PROFILES`), which is
what every production deduplicating store does. Measured: **1 GB → 127 KB**.
The tradeoff, stated: files in different profiles cannot dedup against each
other.

We found this by taking the headline claim to a real file size instead of
leaving it at 8 MB.

### And one we built, measured, and switched off

Rarest-first aims every node at the publisher, because the rarest chunks are the
ones only the publisher has. The textbook fix is **super-seeding**. We built it,
ran a three-trial ablation — and it made things slightly *worse*: publisher share
23% with the salt alone versus 32% with super-seeding added, ranges barely
overlapping. It fights rarest-first by leaving the publisher idle while peers
trade what they already have.

So it ships **off by default**, behind a flag. `bench/benchmark.py` experiment 7
re-measures all three configurations on demand, so this is a result rather than a
story.

---

## Is the 1,000-node number real?

It is **simulated, and labelled as such everywhere.** `bench/validate.py` does
the unflattering thing: it runs the real cluster at several sizes, runs the
simulator at exactly those sizes, calibrates on the **smallest run only**,
freezes it, and reports the error at every larger size as a prediction rather
than a fit. If the model doesn't track, the script says so in its verdict line
instead of printing the projection anyway.

**It said so.** The first run returned *"the model does NOT track reality"* — a
**55% convergence-time error**. Real convergence grew linearly with node count
(9.2 s, 17.6 s, 28.8 s at N = 8, 16, 24) where the model predicted the sublinear
curve gossip should give. So we measured why:

| nodes | total fetched | aggregate throughput |
|---:|---:|---:|
| 8 | 17.0 MB | 1.84 MB/s |
| 16 | 36.4 MB | 2.06 MB/s |
| 24 | 55.8 MB | 1.94 MB/s |

Aggregate throughput is **pinned** regardless of node count. Every "node" is a
thread in one Python process on one machine, so the whole cluster shares one
CPU. Total work grows linearly with N, the machine delivers a constant rate, and
so wall-clock convergence is linear *by arithmetic* — it was measuring the test
rig, not the protocol.

Rather than tune the model until it agreed, we gave it the rig's own constraint
(`host_slots` / `host_throughput`) and validated in two parts:

1. **Traffic** — source egress ratio, which is what the scaling claim actually
   rests on and doesn't care whose CPU the nodes run on.
2. **Time** — with the harness bottleneck modelled. Reproducing the rig once
   told about the rig means the mechanism is right.

The projection then runs with that cap removed, because a real cluster of 300
machines has 300 CPUs and 300 NICs. **That step is an argument from a validated
mechanism, not a measurement**, and it is labelled as one everywhere it appears.
See `results/chart_harness_limit.svg` for the evidence.

With the harness constraint modelled the verdict flips — but the more useful
question is *how accurate is accurate enough*, and that has a measurable answer.

Each node count is run **three times** and the median used, with the run-to-run
spread reported as a **noise floor**: 11.6%. That is how much the same
measurement moves between runs on a contended laptop, and no model can be shown
to predict this harness better than the harness predicts itself.

| | error | vs noise floor |
|---|---:|---|
| **Source egress** — what the scaling claim rests on | **9.1%** | **at/below the floor** |
| Convergence time | 27.6% | above it, and we say why |
| The same model without the harness modelled | 60.2% | — |

The convergence residual has a named cause: the harness's own throughput decays
as node count rises (thread and gossip overhead), while the model holds it
constant. **We report that decay rather than fitting it** — absorbing it would
make the model better at predicting this laptop and no better at predicting a
cluster.

---

## Repository layout

```
ripple/
  chunker.py     L0  gear rolling hash, FastCDC normalisation
  store.py       L1  content-addressed chunk store, atomic writes
  manifest.py    L1  manifests + full version history
  node.py        L2  the node: gossip, lazy reads, anti-entropy, scrub
  peers.py       L2  membership, failure detection, RTT/rack awareness
  scheduler.py   L3  rarest-chunk-first, latency-aware peer selection
  bloom.py       L3  compact availability summaries
  versions.py    L4  version vectors
  merkle.py      L4  anti-entropy over the manifest set
  protocol.py        length-prefixed framing, pooled connections
  chaos.py           fault injection, exposed as a control surface
  sim.py             discrete-event simulator
  dashboard.py       live dashboard + chaos API
  cluster.py         spin up N real nodes in one process
bench/
  benchmark.py       measurements on a real cluster
  validate.py        simulator vs reality
  charts.py          SVG rendering, no plotting library
tests/                31 unit + 11 simulator + 10 integration tests
docker/               Dockerfile + compose generator
demo.py               live cluster + dashboard in one command
reproduce.py          every number, from scratch, one command
```

## Design decisions worth defending

**Why content-defined chunking, not fixed blocks?** Insert one byte at the front
of a file and every fixed block shifts, so everything resends. Content-defined
boundaries move with the content: measured 99.8% chunk reuse after a 1-byte
prepend.

**Why gear hash, not Rabin?** One add and one shift per byte, no multiply or
modulo, ~15 lines. Bit *i* of the hash depends on the last *i* bytes, which is
all a boundary test needs.

**Why Bloom filters for availability?** A raw chunk-ID list makes gossip traffic
grow with (cluster size × file size). A Bloom summary is a fixed budget. False
positives cost one wasted request and are recorded so we never repeat them;
false negatives — which would be a correctness bug — are impossible.

**Why rarest-chunk-first?** Otherwise every node wants chunk 0 from the publisher
at the same instant, which is the star topology we set out to avoid, and the
endgame stalls on whichever chunk only the publisher holds.

**What happens if two nodes edit the same file simultaneously?** Version vectors
detect that neither version dominates. We pick a winner deterministically —
higher manifest digest, so every node picks the same one with no coordination —
and keep the loser at a `.conflict-<origin>-<digest>` path. Nothing is silently
lost, and the cluster still converges on one answer.

**What if a node lies, or a disk does?** A chunk's name is its SHA-256, so every
received chunk is verified against its claimed hash before it is stored, and the
background scrub re-verifies chunks already on disk. A corrupted chunk is
dropped and re-fetched from a peer without anyone asking.

**Why Python?** We are measuring bytes moved and convergence rounds, not raw
throughput. A faster language would move the absolute numbers and change none of
the curves.

## Limitations

Stated plainly in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#known-limitations):
the 1,000-node figure is simulated; `edit()` rewrites the file locally to
re-chunk it (only changed chunks cross the network, but the local pass is
O(file)); there is no access control or encryption.
