# Live panel prep

Each answer is written to be said **out loud in under 30 seconds**. Say the
answer first, then the reason. Do not narrate the architecture before answering.

The strongest thing you can do in this room is volunteer a limitation before it
is asked for. An infra panel has heard a hundred teams claim their system is
fault-tolerant. Almost none of them open with what it cannot do.

---

## Own your layer

| layer | owner answers for |
|---|---|
| L0 chunking | gear hash, FastCDC normalisation, chunk-size profiles |
| L1 store | content addressing, dedup, atomic writes, integrity |
| L2 propagation | gossip, membership, PHANTOM/PARTIAL/RESIDENT, prefetch |
| L3 swarm | rarest-first, super-seeding, the salt, Bloom availability |
| L4 correctness | version vectors, conflicts, Merkle anti-entropy, scrub |
| measurement | benchmarks, simulator, validation, what we got wrong |

---

## The six they will ask

### 1. Why content-defined chunking instead of fixed blocks?

> Insert one byte at the front of a file and every fixed block shifts, so every
> block hashes differently and you resend the whole file. We cut boundaries
> using a rolling hash over the content, so the boundaries move *with* the
> content. Measured: after a one-byte prepend, **99.8% of chunks are unchanged**.
> Fixed blocks give you essentially zero.

If pushed on cost: it's a gear hash — one shift and one add per byte, no
multiply or modulo like Rabin. And we skip the first `min_size` bytes of every
chunk entirely, so we don't even hash 25–40% of the file.

### 2. What is super-seeding, and why didn't it help you?

This is the strongest answer in the set, because it is a negative result.

> Rarest-first says fetch the chunk with the fewest replicas. But at the start,
> the chunks with the fewest replicas are *precisely the ones only the publisher
> has* — so the rule aims every node at the publisher. Super-seeding is the
> textbook fix: take from the swarm whatever the swarm can supply, and ask the
> publisher only for what nothing else has. The seed's job is to get each piece
> *into* the swarm, not to serve it.
>
> We built it, ran an ablation — three trials, same hardware — and **it didn't
> help.** Publisher share went 24% with the salt alone to 27.5% with
> super-seeding added, and convergence got a second slower. The ranges barely
> overlap, so it isn't noise.
>
> Our read is that it fights rarest-first: deferring the origin-only chunks
> leaves the publisher idle while peers trade what they already have, so the
> scarce chunks reach the swarm later. Rarest-first was already handling
> scarcity. So it's off by default, behind a flag, and we expect it to earn its
> place only where the publisher's uplink is the binding constraint — which at
> ten nodes on a loopback harness it isn't.

### 3. What does the per-node salt fix, specifically?

> At the very start, *every* chunk has exactly one holder, so every chunk ties on
> rarity. Our tiebreak was the content hash — which is identical on every node.
> So all N nodes walked the same order, requested the same chunks at the same
> time, ended up with near-identical sets, and had **nothing to trade**.
>
> We measured it directly: mid-transfer, a node found **0 of its 127 missing
> chunks** available from any peer. Salting the tiebreak per node makes each one
> walk a different order, so chunk diversity appears within the first second.
> It's why BitTorrent picks its first pieces at random rather than rarest-first.

The salt alone took the publisher from **66% of chunks served to 24%**, halved
source egress, and made convergence *faster*. `bench/benchmark.py` experiment 7
re-measures all three configurations on the spot, three trials each, and reports
the range so a difference smaller than the noise can be seen as noise.

### 4. Why must conflict resolution be a pure function?

> Because every node resolves the conflict independently and they have to land on
> the same answer without talking to each other. Ours stamps the winner by
> comparing manifest digests, which is deterministic everywhere.
>
> We got this wrong first: the resolved manifest took a fresh `mtime`, so two
> nodes resolving the same conflict produced *different* manifests, each saw the
> other's as a brand-new conflict, and the cluster never converged. It looked
> like a flaky test; it was an unbounded conflict loop. The resolved manifest now
> inherits the winner's metadata, so it's a pure function of its two inputs.
> There's a regression test that feeds two nodes the same pair in opposite orders
> and asserts they agree.

### 5. What breaks first at 1,000 nodes?

Answer honestly and specifically — this is where vague answers get punished.

> Three things, in this order.
>
> **Gossip metadata**, first. Adverts are per-file, so a cluster with a large
> file *count* grows the per-round message. Bloom summaries keep the availability
> half flat, but the advert list doesn't; we'd move to advertising a Merkle root
> and letting anti-entropy pull detail.
>
> **Membership tables** second — every node holds a record per peer, so the
> table is O(N) and the gossip digest with it. Real systems cap this with a
> partial view; we don't yet.
>
> **Not the data plane.** That's the part that actually gets better with scale,
> because every new node is another source.

### 6. What happens if the publisher dies before any chunk has left it?

**Volunteer this one.** Do not wait to be asked.

> Then you lose the file, and no system could do otherwise — those bytes existed
> in exactly one place. The publisher stops being special the moment every chunk
> exists somewhere else; before that, this is a replication-factor question, not
> a protocol question.
>
> Our benchmark measures the time to reach full swarm coverage and kills the
> publisher *after* it — **0.30 seconds to full convergence** once coverage is
> reached. An earlier version killed it early, destroyed chunks nobody else had,
> and reported the data loss as a protocol failure, which was us marking our own
> homework wrong. Erasure coding is what would shorten that window: with any *k*
> of *n* fragments sufficient, coverage arrives much earlier.

---

## Also likely

**"Isn't lazy materialisation just cheating?"**
> It's how container image distribution works — roughly 80% of a typical image is
> never read. And when you *do* want every byte everywhere, you call
> `materialise()`, and the swarm still beats a star: at N=10 the publisher sends
> 2.5× the file instead of 9×, and finishes sooner (7.7 s vs 11.2 s).

**"Is your star baseline actually a star?"**
> Yes, and you can check it in one number: we report **egress per receiver per
> unit of distinct content**, and the star reads exactly **1.00 at every cluster
> size**. It also serves 100% of chunks from the publisher.
>
> It didn't always read cleanly. With a semi-repetitive payload it came out at
> 6.0× instead of 9× at N=10, because only 67% of that file was distinct bytes
> and we were dividing by file size while only distinct bytes ever transfer —
> 9 × 0.672 = 6.04, exactly what we measured. The baseline was right and the
> denominator was misleading, so the benchmark now uses unique data and records
> `unique_ratio` either way.

**"How is rollback that fast?"**
> It isn't a restore. Manifests are immutable and content-addressed and chunks
> are never overwritten, so rolling back is a pointer swap plus a gossip round —
> independent of file size.

**"What stops a bad node poisoning the cluster?"**
> Every chunk is verified against its claimed hash before it is stored, so a peer
> cannot hand you bytes that aren't what they claim to be. Authentication is out
> of scope; integrity is not.

**"Bloom filter false positives?"**
> Cost one wasted request, answered `chunk_miss`, and we record the exact answer
> so we never ask again. False negatives would be a correctness bug and Bloom
> filters cannot produce them.

**"Why Python?"**
> We're measuring bytes moved and convergence rounds, not throughput. A faster
> language moves the absolute numbers and changes none of the curves. We can show
> you exactly where it binds — the harness saturates at ~2 MB/s aggregate, which
> is why we modelled that constraint explicitly instead of pretending it wasn't
> there.

**"Did anything surprise you?"**
> Four things, all of them us being wrong.
>
> The swarm silently behaving like a star until we measured *who served what*.
>
> The simulator agreeing with us for the wrong reason.
>
> A "partition" in our chaos harness that wasn't one — anti-entropy messages
> didn't carry a sender id, so a supposedly isolated node still answered them.
> That would have invalidated every partition result we had.
>
> And a claim in our own README that a 10 GB file has a 90 KB manifest. It's 92
> *megabytes* at a fixed chunk size — wrong by a factor of a thousand. We found
> it by taking the headline to a real file size instead of leaving it at 8 MB.

**"What did you build that you then threw away?"**
> Super-seeding. It's the textbook answer to the problem we had, we implemented
> it, and the ablation says it makes things slightly worse here. It's off by
> default. We'd rather ship the thing we measured than the thing we read about.

---

## Rehearsal drill

Run this twice. Once reading, once cold.

1. Someone picks a question at random; the layer owner answers **without
   slides**, in under 30 seconds.
2. The rest of the team scores it: did they answer in the first sentence?
3. Any answer that starts with "so basically the architecture..." gets redone.

Then the harder drill: each person names **one thing their own layer does
badly**. If anybody can't, they don't understand their layer yet.
