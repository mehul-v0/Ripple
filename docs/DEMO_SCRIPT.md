# Demo video — 5 minutes, shot list

One idea, three proofs:

> Sync does not mean moving bytes → so we move manifests and materialise lazily
> → here is the measurement showing it holds at scale → now break it yourself.

Four minutes of one idea beats ten features. The config-drift beat at 2:15 is the
one exception, and it earns its twenty seconds by being about something other
than bandwidth.

**Before recording:** `python reproduce.py` so `results/` is populated, then
`python demo.py --nodes 24` in a clean terminal. Have the dashboard open at
1920×1080, browser zoom 100%, terminal font large enough to read on a laptop.

---

## 0:00–0:30 — The problem, as a number

**Screen:** title card, then a single arithmetic line.

> "One source. Three hundred machines. One ten-gigabyte file.
> The naive approach transfers three terabytes out of one network card. At a
> gigabyte a second that's fifty minutes, during which the source is saturated
> and the file doesn't exist on two hundred and ninety-nine machines.
> Every byte of that is transferred whether anyone reads it or not."

Show: `300 × 10 GB = 3 TB` on screen. Let it sit for a beat.

## 0:30–1:15 — The reframe

*(If you have the 1 GB or 10 GB figures to hand, lead with them — they are far
stronger than the live 64 MB demo and they are in `results/large_file.json`.
"A one-gigabyte file, present and readable on all fifty machines in under two
seconds, with forty-nine of them holding none of it.")*

**Screen:** dashboard, empty cluster of 24 nodes. Click **publish 64 MB**.

> "A file is two things: what it *is*, and what it *contains*. We separate them.
> A manifest is the ordered list of content hashes that defines the file — a few
> kilobytes no matter how big the file is. We push the manifest everywhere
> immediately."

Ring goes grey-blue (PHANTOM) across all 24 nodes within a second.

> "The file now exists on every node. Right name, right size, right metadata,
> shows up in the directory. Bytes moved so far: essentially none."

Point at the manifest column in the file table, and the near-zero byte counter.

> "Now watch what happens when someone actually reads it."

Click a node, trigger a read. It flips to amber (PARTIAL).

> "It materialised the chunks that read touched. Not the file. The chunks."

## 1:15–2:15 — The swarm, and a failure

**Screen:** click **materialise everywhere**.

> "When you genuinely want every byte on every node, ask for it. Every node that
> receives a chunk becomes a source for that chunk, so this spreads outward
> rather than radiating from one machine."

Ring turns green outward from the publisher. Let it run 5–8 seconds.

> "Rarest chunk first — otherwise every node wants chunk zero from the publisher
> at the same instant, and you've rebuilt the bottleneck you were trying to
> avoid."

**Now kill the publisher, on camera.** Click node n000 → **kill**.

> "That was the source. Watch."

Ring keeps turning green. Do not narrate over the pause — let it finish.

> "Nobody noticed. Every node that had a chunk was already a source for it."

## 2:15–2:45 — Config drift, and it isn't about bandwidth

**Screen:** dashboard, config row. Click **publish config**, then **drift one node**,
then **find drift (0 bytes)**.

> "Content-defined chunking would tell you byte offset 8,214 differs, which helps
> nobody. So for files with a grammar — YAML, JSON, INI — we cut on the grammar
> instead. One chunk per setting, each one labelled."

The drift line appears naming the key, the node, and the value.

> "Every node already holds every manifest. So finding which node drifted, on
> which key, to what value, is a comparison of metadata that's already in memory.
> Zero file bytes moved. We only fetch the one stanza that differs, and only to
> print it."

*This is the second demo beat and it costs 20 seconds. Cluster config drift is
something Nutanix sells against.*

## 2:45–3:00 — A small edit to a large file

**Screen:** click **edit 1 line**.

> "Now the part that decides whether this is usable day to day. One line changes
> in a sixty-four megabyte file."

Point at the event log line showing changed chunks and bytes.

> "Content-defined chunking: boundaries are chosen by the *content*, using a
> rolling hash, so inserting a byte doesn't shift every boundary after it. We
> measured ninety-nine point eight percent chunk reuse after a one-byte insertion
> at the front of a file. Fixed-size blocks give you zero."

## 3:00–4:00 — Scale, and whether to believe it

**Screen:** `results/chart_egress.svg` — flat line vs diagonal.

> "Measured on a real cluster: source egress against cluster size. A star topology
> is the diagonal. Ripple is the flat line."

**Screen:** `results/chart_sim_scale.svg`.

> "The brief says hundreds of machines. A laptop runs about fifty real nodes
> before you're measuring thread scheduling instead of the protocol. So we built
> a discrete-event simulator of the same protocol — and then checked it."

**Screen:** `results/chart_validation.svg`.

> "We run the real cluster and the simulator at the same sizes, three runs each.
> We calibrate on the *smallest* run only, then freeze it — everything larger is a
> prediction, not a fit. Nine percent error on source egress, which is what the
> scaling claim rests on, against a measured noise floor of eleven point six.
> In other words it's already inside the range you'd get from just re-running the
> benchmark."

*Check the live numbers in `results/validation.json` before recording — never
quote a figure the script didn't produce.*

## 4:00–4:40 — Chaos: hand over the controls

**Screen:** dashboard, chaos panel.

> "Claiming fault tolerance is easy. So — pick something."

Do two of these, live, without cuts:

- **Corrupt a chunk on disk** on a node holding data. Wait. Event log shows
  `heal — chunk … failed verification, re-fetching`. *"Nobody asked it to. A
  chunk's name is its checksum, so a disk that lies gets caught on the next
  scrub."*
- **Kill a third of the cluster** mid-transfer. It converges anyway.
- **Rollback:** publish garbage over the file across all 24 nodes, then click
  **rollback**. *"Everything is content-addressed, so old manifests cost almost
  nothing to keep. Restoring the whole cluster to a previous version is a
  pointer swap — independent of file size."* Show the sub-second timing.

## 4:40–5:00 — What's next

> "Next: erasure-coded fragments, so nodes stop asking 'do you have chunk
> forty-seven' and ask 'give me any k fragments' — that deletes the scheduling
> problem instead of optimising it, and it shortens the window where losing the
> publisher can still lose data. And similarity-based delta, so a file the
> cluster has never seen can be sent as a diff against the closest one it has."

> "Ripple. Move meaning first. Bytes when you need them."

---

## Recording notes

- **Do not speed up the materialisation footage.** Real timing is the point; a
  sped-up clip reads as a cheat even when it isn't.
- **Kill the publisher on camera, in one take.** A cut there destroys the claim.
- Keep the event log visible throughout — the `heal` and `conflict` lines are
  what make it look alive rather than staged.
- If something genuinely breaks during recording, keep it and say what happened.
  A recovered failure is better footage than a clean run.
- Rehearse the 3:00–4:00 block hardest. It's the one where a judge decides
  whether the whole thing is credible.

## Anticipated questions

**"You didn't really transfer the file."**
> Correct, and deliberately. This is how modern container image distribution
> works — roughly 80% of a typical image is never read, so transferring it
> eagerly is wasted bandwidth. And unlike a lazy container pull, we also
> swarm-distribute the bytes that *are* read, so the second reader is fast too.
> When you want everything everywhere, `materialise()` does that, and it still
> beats a star topology.

**"Why content-defined chunking instead of fixed blocks?"**
> Insert one byte at the front of a file and every fixed block shifts, so
> everything resends. Ours: 99.8% chunk reuse, measured.

**"What if two nodes gossip the same chunk simultaneously?"**
> Chunks are immutable and named by their own hash, so a duplicate arrival is a
> no-op — the store already has those exact bytes under that exact name. For
> *manifests*, concurrent edits are caught by version vectors: we keep both
> versions and pick a winner deterministically by digest, so every node picks
> the same one without coordinating.

**"What's your bottleneck?"**
> Python, and we say so. Local ingest runs at about 1.6 MB/s — chunking a gigabyte
> takes eleven minutes on the publisher. That's a one-time cost on one machine,
> not the sync, and it would be seconds in Go. We're measuring bytes moved and
> convergence rounds, not throughput.

**"How accurate is your simulator, really?"**
> Nine percent on source egress, which is the number the scaling claim rests on —
> against a measured noise floor of eleven point six percent, so it's below the
> point where re-running the benchmark would tell you anything different.
> Convergence time is still 28% off and we know exactly why: the harness's own
> throughput decays as node count rises and the model holds it constant. We
> report that instead of fitting it.

**"What did you build and then throw away?"**
> Super-seeding. Textbook answer to the problem we had, we implemented it, the
> ablation says it makes things slightly worse here, so it's off by default.
