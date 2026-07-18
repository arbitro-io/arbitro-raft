# arbitro-raft

> A transport-agnostic, storage-agnostic **Raft consensus primitive** for the
> control plane of distributed systems.

`arbitro-raft` is a low-level building block, not a database and not a message
bus. It gives you one thing: a set of nodes that **agree on an ordered,
replicated log** and hand each committed entry to a state machine you provide.
You bring the storage (`RaftStorage`), the transport (`RaftTransport`), and the
state machine (`StateMachine`); the crate runs elections, replication, log
matching, membership changes, and snapshots on top of them.

---

## What it is for (and what it is *not* for)

Consensus is **deliberately expensive**: every committed entry costs a quorum
round-trip and, when durable, an `fsync` on each quorum node — milliseconds, not
microseconds. So the rule every serious system follows is:

> **Put the control plane through consensus. Keep the data hot path out of it.**

How the well-known systems apply this:

| System | What goes through consensus | What does *not* |
| :--- | :--- | :--- |
| **Kafka (KRaft)** | cluster metadata: topics, partition assignment, broker registration, leadership | the messages — replicated per-partition by the ISR fetch protocol, **not** Raft |
| **etcd** | *everything* — but it is a small, low-frequency config store (this is why it tops out around tens of thousands of writes/s) | n/a — nobody uses etcd as a high-throughput bus |
| **TiKV / CockroachDB** | every write, but the keyspace is sharded into thousands of ranges, **each its own Raft group** (multi-raft), so the *aggregate* scales | — |

`arbitro-raft` is the primitive you reach for to build the **Kafka-style
metadata plane**: stream/consumer/cron creation, membership, config — things
that must be linearizable across the cluster and happen rarely. A message hot
path should ride a lighter per-partition replication scheme, with consensus used
only for that partition's metadata and leadership.

---

## Status (honest)

- **Protocol correctness & safety** — complete and audited: leader election
  (with pre-vote / check-quorum), log matching, joint-consensus membership
  changes, learners (non-voting members), ReadIndex linearizable reads,
  leadership transfer, snapshots + log compaction.
- **Soundness** — no `unsafe` in the wire path beyond `zerocopy`'s checked
  views; Miri-clean over the wire/scratch modules; overflow/panic audited.
- **Durability** — persist-before-ack contract on `save_hard_state` /
  `append_entries`; disk-full degrades read-only instead of dying.
- **Multi-raft** — a single-core `MultiRaftDriver` can drive N groups
  share-nothing (library capability; see `docs/MULTI_RAFT.md`). **Not yet wired
  into a server deployment.**
- **Open work** — the verification harness (fuzz / loom / DST / model checking),
  performance levers (pipelining, group-commit, async apply), and a wire-version
  / `cluster_id` decision. None are correctness blockers. See `MASTER_TODO.md`.

---

## Performance (measured, with conditions attached)

All numbers below were measured on **WSL2 (Ubuntu), 3-node loopback, single Raft
group**. Each is labelled with its load model — they are **different quantities**
and are not interchangeable:

- `wait, one-in-flight` — one proposal at a time, blocking until it commits. A
  **latency** probe; its throughput is just `1/latency`, not capacity.
- `batch-wait, single proposer` — one proposer coalescing B entries per round. A
  per-round **bandwidth** ceiling, not a concurrent-client number.

### Non-durable (in-memory storage, no `fsync`)

| Transport | one commit (`wait`) | batch ×1024 (`batch-wait`, empty entries) |
| :--- | :--- | :--- |
| in-memory channels | **~27.6 µs** | **~13.5 M entries/s** |
| TCP loopback | **~204 µs** | **~3.3 M entries/s** |

The per-commit cost lives in the async-runtime hot path (task wakeups, channel,
syscalls) and the network round-trip — **not** in Raft logic: 64 entries commit
in the *same* ~205 µs as one over TCP.

### Durable (real WAL + `fdatasync` on every quorum node)

| Transport | one commit (`wait`, `fsync=data`, scope=quorum) |
| :--- | :--- |
| TCP loopback | **~2.04 ms** |

A single `fdatasync` on the WSL2 VHD measures **~730 µs**; a durable commit pays
two on the critical path (leader + one follower), which is exactly the ~1.8 ms
delta over the non-durable number. **Caveat:** WSL2-VHD `fsync` is a virtualized
barrier — real, but not evidence of bare-metal power-loss durability, and not
comparable to bare-metal NVMe numbers. Methodology: `docs/BENCH_METHODOLOGY_K3.md`.

> A like-for-like comparison table against etcd / Dragonboat / openraft is **not
> published yet**: it requires measuring each system on the same host with its
> own tool (the concurrent-client "sustained" bench and the cross-system runs are
> in progress). We will not print competitor numbers measured under different
> conditions.

---

## Design properties

- **Zero-copy inbound decode** — wire frames are reinterpreted in place as typed
  `zerocopy` views; no parse-time payload copy.
- **Zero-copy vectored send** — `encode_message_vectored` assembles a frame from
  borrowed slices for `writev(2)`, so entry payloads are never copied on the
  send path. (The default *contiguous* encoder does allocate once per frame —
  the zero-alloc guarantee holds on the vectored path, not universally.)
- **Lock-free commit notifications** — a preallocated slot arena with an atomic
  cursor; no `Arc<Mutex>` per proposal.
- **Share-nothing shape** — groups are owned by value, the hot path takes
  `&mut self`, and the `MultiRaftDriver` lends per-core scratch so idle groups
  cost ~KB, making O(cores) big-buffer memory instead of O(groups).
- **Opaque payloads** — user entries are handed to `StateMachine::apply(&[u8])`
  without decoding; the engine only peeks the first byte to tell its own control
  entries (config change / read-index no-op) from user data.

---

## Usage

```rust
use arbitro_raft::{ArbitroRaft, RaftNode};

// You provide: config, an impl RaftStorage, an impl RaftTransport,
// and an impl StateMachine.
let node = RaftNode::new(config, storage, transport)?;
let mut raft = ArbitroRaft::new(node, state_machine);

// A clonable handle lets many tasks propose concurrently.
let handle = raft.client_handle();

// Drive the consensus loop.
tokio::spawn(async move { raft.run().await });

// Propose an entry; the future resolves at the committed log index.
let index = handle.write(b"create-stream:orders").await?;

// bytes::Bytes moves by refcount into the engine — no copy across the boundary.
let index = handle.write_bytes(payload_bytes).await?;
```

---

## Architecture

```
protocol/       wire encode/decode (zerocopy)
  codec/        encode/ (vectored, contiguous), decode.rs, wire.rs, view.rs
  message.rs    outbound message types + entry iterators
api/
  arbitro_raft/ execution loop, batching, timers, client API
    slot.rs     lock-free commit-notification arena
    client.rs   ClientHandle, bounded mailbox (Overloaded fail-fast)
    run.rs      leader/follower event loop, apply
  node/         Raft state machine
    replication/  propose/, handler.rs, heartbeat.rs, snapshot install
    election.rs   RequestVote, campaign, pre-vote
    membership.rs joint consensus, learners, config-change entries
  registry/     RaftGroupRegistry, MultiRaftDriver, CoreScratch (multi-raft)
  transport/    MultiplexDemux (per-group inbox routing)
dispatch/       optional custom-RPC layer with quorum ack policies
traits/         RaftStorage, RaftTransport, StateMachine, Clock
```

---

## Tuning

The vectored-I/O path picks between contiguous and `writev(2)` per batch;
thresholds are overridable at process start:

| Env var | Default | Purpose |
| :--- | :--- | :--- |
| `ARBITRO_RAFT_VEC_IOV_MAX` | `4096` | Max iovecs before falling back to contiguous |
| `ARBITRO_RAFT_VEC_MIN_ENTRY` | `4096` | Min per-entry payload to qualify for vectored |
| `ARBITRO_RAFT_VEC_MIN_TOTAL` | `65536` | Min aggregate batch size to qualify for vectored |
| `ARBITRO_RAFT_FORCE_CONTIGUOUS` | — | Force the contiguous path (A/B testing) |
| `ARBITRO_RAFT_FORCE_VECTORED` | — | Force the vectored path (A/B testing) |

The decision is cached in a `OnceLock` on first use.

Benchmark knobs (durable mode, in-memory vs TCP, sample sizes, WAL directory)
are documented at the top of `benches/raft_transport_bench.rs`.

---

*Part of the [arbitro](https://github.com/automatizadovip) project.*
