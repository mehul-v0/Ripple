# Ripple — Architecture

> Every other approach assumes "sync" means moving bytes to N machines.
> We move **manifests**, and materialise bytes lazily on first read.

---

## The problem, as a number

One source, 300 machines, one 10 GB file. A naive push sends 3 TB out of one
NIC. At a realistic 1 GB/s that is 50 minutes during which the source is
saturated and the file is unavailable on 299 machines.

But look at what is actually being asked for. In the overwhelming majority of
cases — a VM image, a container layer, a model checkpoint, a config bundle —
the machines need the file to *exist*: to appear in the directory, to have the
right size and metadata, to be openable. Most of them will never read most of
its bytes. Container-image research puts the unread fraction of a typical image
at roughly 80%.

So we separate the two things that "sync" conflates:

| | what it is | cost | when |
|---|---|---|---|
| **Manifest** | ordered list of chunk hashes + metadata | KB | pushed eagerly, immediately |
| **Bytes** | the chunks themselves | GB | pulled lazily, on first read |

Push the manifest to all N nodes and the file *exists* on every node. Bytes
follow on demand, swarm-distributed so the second reader is fast too.

**Measured, at a size that carries the claim:** a **10 GB** file of unique data is
described by a **641 KB** manifest — 1/16,352nd of it — which reaches **all 50
nodes in 4.32 s**. A node holding *none* of it then serves a 4 KB read from the
middle in **0.25 s**, verified byte-for-byte against the original. Forty-nine of
forty-nine receivers are `PHANTOM`: the file is present and readable while
holding zero bytes.

| file | manifest | vs file | on all 50 nodes | first byte |
|---|---:|---:|---:|---:|
| 1 GB | 127 KB | 1/8,257 | 1.93 s | 0.21 s |
| 10 GB | 641 KB | 1/16,352 | 4.32 s | 0.25 s |

Local ingest on the publisher (chunk, hash, store) ran at 1.6 MB/s, so the 10 GB
file took 111 minutes to take in. That is a one-time cost on one machine and is
not part of the sync — we report it rather than hide it, and it is the single
clearest place a Go implementation would pay off.

At 8 MB on 6 nodes the same comparison is 46 ms to exist everywhere versus
14.3 s to fully materialise — a **307×** gap.

### The obvious objection

*"That's cheating — you didn't really transfer it."*

This is exactly how modern container image distribution works (lazy pulls,
snapshotters, on-demand layer materialisation). Transferring bytes nobody reads
is wasted bandwidth, not thoroughness. And unlike a lazy container pull, we also
swarm-distribute the bytes that *are* read, so the cost is amortised across the
cluster rather than paid again by every reader.

When you genuinely need every byte on every node, ask for it — `materialise()`
does exactly that, and the swarm still beats a star topology by a wide margin
(see the egress chart). Lazy is the default, not the only option.

---

## Five layers

```
  publish(file)                                     read(path, off, len)
        |                                                     |
        v                                                     v
  +----------------------------------------------------------------------+
  | L0  Content-defined chunking          chunker.py                     |
  |     gear hash, FastCDC normalisation, size-scaled chunk profiles      |
  +----------------------------------------------------------------------+
  | L1  Content-addressed store           store.py, manifest.py          |
  |     chunks keyed by SHA-256; cluster-wide dedup; atomic writes        |
  +----------------------------------------------------------------------+
  | L2  Manifest propagation              node.py, peers.py              |
  |     gossip; PHANTOM -> PARTIAL -> RESIDENT; read-triggered prefetch   |
  +----------------------------------------------------------------------+
  | L3  Swarm distribution                scheduler.py, bloom.py         |
  |     rarest-chunk-first; rack- and RTT-aware peer selection            |
  +----------------------------------------------------------------------+
  | L4  Correctness and integrity         versions.py, merkle.py         |
  |     version vectors; atomic apply; Merkle anti-entropy; scrub         |
  +----------------------------------------------------------------------+
  | ++  Structure-aware chunking          structured.py                   |
  |     config files cut on their grammar; drift from manifests alone     |
  +----------------------------------------------------------------------+
```

### L0 — Content-defined chunking (`chunker.py`)

A gear rolling hash slides over the file:

```
h = ((h << 1) + GEAR[byte]) & 2**64-1
```

Bit *i* of `h` depends on the last *i* bytes, so masking bits 12..12+n gives a
fingerprint over a ~28-byte window with one add and one shift per byte — no
multiply or modulo, unlike a Rabin fingerprint. A boundary is declared where the
fingerprint hits a rare pattern.

**Why not fixed blocks?** Insert one byte at the start of a file and every fixed
block shifts; every block hashes differently; the whole file resends. With
content-defined boundaries, only the chunk containing the insertion changes.

> **Measured:** after a 1-byte prepend to a 4 MB file, **99.8%** of chunks are
> unchanged. Fixed blocks would give ~0%.

We use FastCDC-style *normalisation*: a strict mask before the average size and
a lenient one after, which tightens the size distribution around the target and
makes dedup ratios predictable.

#### Chunk-size profiles

Chunk size is chosen from file size, because manifest size is driven by chunk
count. This is not a refinement — it is what keeps the central claim true:

| file | profile | chunks | manifest |
|---|---|---:|---:|
| ≤ 8 MB | 2K / 8K / 64K | ~512 per 4 MB | ~40 KB |
| ≤ 256 MB | 24K / 64K / 512K | ~1k per 64 MB | ~70 KB |
| ≤ 4 GB | 192K / 512K / 4M | ~2k per 1 GB | **127 KB measured** |
| beyond | 384K / 1M / 8M | ~10k per 10 GB | ~0.7 MB |

Every production deduplicating store does the same (restic targets ~1 MB, borg
defaults to 2 MB). The cost is real and worth stating: two files chunked under
different profiles cannot dedup against each other, since their boundaries come
from different masks. Dedup matters most between *similar* files, which are
usually similar sizes and land in the same profile — but a 10 MB file and a 10 GB
file will not share chunks even if one contains the other.

#### Structure-aware chunking (`structured.py`)

For files with a grammar — YAML, JSON, INI/conf — we cut on the grammar instead:
one chunk per top-level key or stanza, each carrying a **label**. Splitting is
lexical rather than a full parse, so pieces concatenate back to the original file
byte-for-byte, and anything unrecognised is declined so the caller falls back to
content-defined chunking.

The payoff is not smaller diffs, it is *legible* ones. Because each chunk is
labelled and manifests are already gossiped everywhere, **config drift is a
manifest-only query**:

```
max_connections: 4 node(s) agree
  max_connections differs on n003 -> max_connections: 200
bytes fetched across whole cluster: 0
```

Comparing label → chunk-hash maps across nodes names the drifted key and the
outlier node without reading, transferring or materialising a single file byte.
Only the differing stanza is ever fetched, and only to display its value.

### L1 — Content-addressed store (`store.py`, `manifest.py`)

Chunks are filed under their own SHA-256. Two things fall out for free:

- **Dedup is cluster-wide and cross-file.** Fifty VM images from one golden
  image share chunks automatically — identical bytes produce identical names,
  and nobody has to declare the relationship. *Measured: 14.5× on 11 VM-image
  variants.*
- **Integrity is intrinsic.** A chunk's name *is* its checksum, so any node can
  verify anything it receives without trusting the sender, and corruption on
  disk is detectable by re-reading. This is what makes the background scrub
  possible at all.

Writes are atomic (temp → fsync → rename), so a crash mid-write leaves the old
state or the new one, never a torn chunk indistinguishable from corruption.

A **manifest** is `{path, size, mode, mtime, version vector, [chunk hashes]}` —
about 70 bytes per chunk. Manifest size therefore tracks chunk *count*, so chunk
size scales with file size (`chunker.PROFILES`): measured, **1 GB → 127 KB** and
**51 MB → 50 KB**. At a fixed 8 KB chunk a 10 GB file would need 1.31 M chunks and
a 92 MB manifest, which is not something you can push eagerly to 300 nodes — see
*Chunk-size profiles* below. Manifests are
immutable and content-addressed by their own digest, so editing a file produces
a new manifest and the old one stays valid forever at negligible cost. *That is
why rollback is nearly free rather than a feature we had to build.*

### L2 — Manifest propagation (`node.py`, `peers.py`)

Manifests gossip to every node. Per-file state on each node is one of:

- **`PHANTOM`** — manifest held, zero bytes. The file exists: correct name,
  size, metadata; it appears in listings and can be opened.
- **`PARTIAL`** — some chunks local, usually mid-read.
- **`RESIDENT`** — every chunk local; reads need no network at all.

Reading a `PHANTOM` file works. `chunks_for_range()` maps the requested byte
range to the minimal set of chunks, fetches those and nothing else, and returns
bytes. **A 4 KB read of a 10 GB file moves one chunk.**

Membership uses heartbeat-counter gossip rather than all-to-all probing —
detection cost per node stays constant as the cluster grows, where all-to-all at
300 nodes would mean 90,000 probes per interval. Merges are monotonic (a record
is accepted only if its counter is strictly higher), so a replayed or reordered
message can never resurrect a dead node.

**Read-triggered prefetch:** reading chunk *i* speculatively queues *i+1..i+8*.
Reads are overwhelmingly sequential, so materialisation feels instant rather
than stuttery.

### L3 — Swarm distribution (`scheduler.py`, `bloom.py`)

Any node holding a chunk serves it. Two decisions, made separately:

**Which chunk? Rarest first.** If everyone fetched in file order, they would all
want chunk 0 from the publisher at the same instant — the star topology we set
out to avoid. Worse is the endgame, where the last chunk only the publisher
holds is wanted by everyone at once and the swarm stalls. Rarest-first pushes
scarce chunks out early so replica count grows evenly.

#### One correction that mattered more than the rule, and one that did not

Applied naively, rarest-first made things *worse*, and only measurement caught
it: the publisher was serving **66% of all chunks** in a 10-node cluster — a star
topology wearing a swarm's clothing. Two candidate causes:

1. **Every node was choosing identically.** At the start of a transfer every
   chunk has exactly one holder, so every chunk *ties* on rarity. Our tiebreak
   was the content hash — identical on every node — so all N nodes fetched the
   same chunks in the same order and had nothing to trade. Measured mid-transfer,
   a node found **0 of its 127 missing chunks** available from any peer.
2. **Rarest-first aims everyone at the publisher**, since the rarest chunks are
   by definition the ones only it holds. The textbook answer is *super-seeding*:
   take from the swarm whatever the swarm can supply, ask the publisher only for
   what nothing else has.

We implemented both and ran an ablation, three trials each at N=10:

| config | publisher share | egress | convergence |
|---|---:|---:|---:|
| rarest-first alone | 66.4% (65–67) | 5.91× | 13.0 s |
| **+ per-node salt** | **24.0% (20–27)** | **2.12×** | **6.9 s** |
| + salt + super-seeding | 27.5% (26–28) | 2.47× | 7.9 s |

**The salt does all the work. Super-seeding did not help and slightly hurt** —
the ranges barely overlap, so it is not noise. The likely reason is that it
fights rarest-first: deferring origin-only chunks leaves the publisher idle while
peers trade what they already have, so scarce chunks reach the swarm *later*.
Rarest-first was already handling scarcity; super-seeding was solving a problem
the salt had removed.

So super-seeding is **off by default**. The implementation stays behind a flag,
because we expect it to matter where the publisher's uplink is genuinely the
binding constraint — which at N=10 on this harness it is not. We are not going to
ship a mechanism on the strength of it appearing in the literature when our own
measurement says it costs three points of publisher share.

*(This is why the ablation exists: `bench/benchmark.py` experiment 7 re-measures
all three configurations on the spot rather than asking anyone to take the story
on trust.)*

**From whom? The nearest peer that has it**From whom? The nearest peer that has it and isn't saturated.** Ranked by rack
locality, then RTT, then recent failures, with a per-peer concurrency cap (and
optionally, off by default, the publisher deprioritised — see above).
Without the cap, "prefer the nearest peer" degenerates into "everybody asks the
same peer" — the star again, with extra steps.

Availability is exchanged as **Bloom filters**, not chunk-ID lists. A raw list
costs 64 bytes per chunk per peer per round, so gossip traffic would grow with
(cluster size × file size). A Bloom summary is a fixed budget.

> **Measured:** 10,000 chunks summarised in **12 KB** instead of **640 KB** — 53×
> smaller, at a 0.9% false-positive rate.

A false positive costs one wasted request that the peer answers with
`chunk_miss`; the scheduler records the exact answer and never asks again. A
false *negative* would be a correctness bug — and Bloom filters cannot produce
one.

### L4 — Correctness and integrity (`versions.py`, `merkle.py`)

**Version vectors.** A timestamp cannot distinguish "older, discard it" from "two
nodes edited independently"; last-writer-wins silently destroys one edit, and
with skewed clocks may destroy the newer one. A version vector makes the
distinction structural. On genuine concurrency we pick a winner
*deterministically* (higher digest, so every node picks the same one without
coordinating) and keep the loser at a `.conflict-<origin>-<digest>` sidecar path.
**Nothing is ever silently discarded, and the cluster still converges to one
answer.**

**Merkle anti-entropy.** Gossip is best-effort; a message dropped at the wrong
moment leaves a node missing a manifest and nothing notices. Each node keeps a
Merkle tree over its manifest set. Two identical nodes compare one 32-byte root
and stop. Two that differ descend only into disagreeing subtrees, so cost is
proportional to the divergence, not the dataset. *Measured: 5,000 keys with one
difference → 1 of 256 buckets inspected.* This is what Cassandra and DynamoDB
use for replica repair.

**Self-healing scrub.** Nodes continuously re-verify sampled chunks against their
own hashes. A failure drops the chunk and re-queues it, and the swarm heals it
from any peer, unprompted. Because the name *is* the checksum, detection needs
no extra metadata.

---

## Scaling to hundreds of nodes

A laptop hosts perhaps 50 real nodes before thread scheduling — rather than the
protocol — becomes what you are measuring. Rather than hand-wave the rest, we
built a discrete-event simulator of the same protocol (`sim.py`) and then
**checked it** (`bench/validate.py`):

1. Run the real cluster at several node counts.
2. Run the simulator at exactly those counts.
3. Calibrate the model on the **smallest** run only, then freeze it.
4. Report the error at every larger size — those are predictions, not fits.

### The validation failed first, and that turned out to be the useful part

The first run's verdict was *"the model does NOT track reality"*: 55% mean
convergence-time error. Real convergence grew **linearly** with node count
(8.4 s → 17.5 s → 29.0 s at N = 8, 16, 24) where the model predicted the
sublinear curve gossip should produce.

The tempting move is to tune the model until it agrees. Instead we measured what
the harness was actually doing:

| nodes | total fetched | aggregate throughput |
|---:|---:|---:|
| 8 | 17.0 MB | 2.02 MB/s |
| 16 | 36.4 MB | 2.08 MB/s |
| 24 | 55.8 MB | 1.92 MB/s |

Aggregate throughput across the *entire cluster* is constant regardless of how
many nodes are in it. That is the signature of a saturated single machine — and
of course it is, because every "node" is a set of threads in one Python process
sharing one GIL. Total work grows linearly with N, the machine delivers a fixed
rate, so wall-clock convergence grows linearly *by arithmetic*. The measurement
was characterising the test rig, not the protocol.

So validation was split in two:

- **Traffic** — source egress ratio. A protocol property, indifferent to whose
  CPU the nodes run on, and the quantity the scaling claim actually rests on.
  Directly comparable.
- **Time** — with the model given the harness's own constraint
  (`host_slots`, `host_throughput`). If the model reproduces the rig once told
  about the rig, the mechanism is right even though the environment is not the
  deployment environment.

The projection then removes that cap, because a real cluster of 300 machines has
300 CPUs and 300 NICs. **That final step is an argument from a validated
mechanism, not a measurement**, and every place it appears says so.

One parameter is fitted (`host_throughput`), and unlike an abstract fudge factor
it has an independently measurable counterpart — the table above — so the fitted
value can be checked against reality rather than taken on faith. Gossip
interval, fanout, slot counts and RTTs all come from the implementation's own
constants and are not tuned.

The evidence chart is `results/chart_harness_limit.svg`.

The simulator is written to be pessimistic about its own knowledge:

- **Belief is snapshot-based.** A node knows what a peer held when they last
  gossiped, never what it holds now (implemented with an acquisition sequence
  number per piece — exact, O(1), and *not* a shortcut through shared state).
- **Bounded peer working set.** Nobody has a global view, here or in production.
- **Bounded planning window**, matching the real scheduler.

Simulated projection to 1,000 nodes appears in `results/chart_sim_scale.svg`;
the model-vs-reality comparison is in `results/chart_validation.svg`. If the
model does not track reality, `validate.py` says so in its verdict line rather
than quietly reporting the projection anyway.

---

## Wire protocol

Length-prefixed frames over TCP:

```
magic(4) | header_len(4) | payload_len(4) | JSON header | raw payload
```

Two lengths because chunk bytes must not be base64'd into JSON — that would cost
33% on the single largest category of traffic in a project judged on bytes
moved. Connections are long-lived and pooled; at a few hundred nodes a handshake
per gossip exchange would put connection setup on the critical path of
everything.

| message | purpose |
|---|---|
| `hello` | join, exchange peer tables |
| `gossip` | membership + manifest adverts + Bloom summary |
| `manifest_get` | pull a manifest by path or digest |
| `chunk_get` | fetch one chunk |
| `chunk_probe` | exact availability query (Bloom refinement) |
| `merkle_get` / `bucket_get` | anti-entropy |
| `state_get` | dashboard aggregation |
| `chaos` | fault injection |

There is no privileged control channel: the CLI and dashboard speak the same
protocol any peer speaks.

---

## Failure handling

| failure | response |
|---|---|
| Node dies mid-transfer | Its queued chunks are released and re-planned against other holders on the next tick. Costs a round, not a restart. |
| Publisher dies | Irrelevant **once every chunk exists somewhere else** — from that moment every holder is a source and losing the publisher costs a scheduling round. *(Tested; the star-topology control case fails here, as it should.)* Before that moment, chunks that exist in one place only are unrecoverable — see the limitation below. |
| Network partition | Both sides keep serving reads from local chunks. On heal, version vectors sort out what happened; concurrent edits are preserved, not resolved by luck. |
| Silent disk corruption | Scrub detects it (name = checksum), drops the chunk, re-fetches from a peer. |
| Dropped gossip | Merkle anti-entropy repairs it at log cost. |
| Slow / distant peer | Deprioritised by RTT and failure count; the concurrency cap stops it soaking up requests. |

All of these are exposed as buttons on the dashboard, which is the point: the
demo hands over the controls rather than asserting fault tolerance.

---

## What we would build next

- **Erasure-coded fragments.** Encode chunks into *n* fragments where any *k*
  reconstruct, so nodes ask for "any fragments" rather than "chunk 47" — this
  deletes the scheduling problem instead of optimising it.
- **Similarity-based delta for unseen files.** Keep a sketch (a handful of the
  smallest chunk hashes) per file to estimate overlap, and delta a *new* file
  against the most similar existing one before transferring anything.
- **Structure-aware chunking.** For YAML/JSON/INI, chunk on the file's own
  grammar so diffs become semantic — "node 43 has `max_connections: 200`,
  everyone else has 500" instead of "differs at byte offset 8,214".
- **Signed Merkle attestation.** Each node signs its Merkle root; collected
  signatures form an unforgeable proof that all N machines hold byte-identical
  data at a given moment.

## Known limitations

- **Python, and honest about it.** We measure bytes moved and convergence
  rounds, not raw throughput. A Go or Rust implementation would move the
  absolute numbers; it would not change the shape of any curve here, which is
  what the argument rests on.
- **The 1,000-node figure is simulated**, and labelled as such everywhere. Its
  credibility rests entirely on the validation step, which is why that step
  reports its own error rather than being quietly omitted.
- **Whole-file republish on edit.** `edit()` reconstructs the file to re-chunk
  it. Only changed chunks move across the network, but the local rewrite is
  O(file). A production version would chunk incrementally around the edit.
- **No access control or encryption.** Out of scope for the brief; chunk-level
  integrity is present, confidentiality is not.
- **No redundancy guarantee during the seeding window.** Until every chunk has
  left the publisher at least once, some bytes exist in exactly one place, and
  killing the publisher destroys them. This is a statement about replication
  factor, not about this design -- no system recovers data that exists once --
  but it does mean "survives losing the source" is only true after the swarm has
  full coverage. `bench/benchmark.py` measures the time to reach that point and
  kills the publisher only afterwards, rather than quietly killing it early and
  reporting the data loss as a protocol failure. The planned erasure-coding work
  is the real answer: with any k of n fragments sufficient, coverage is reached
  far earlier and survives more simultaneous loss.
