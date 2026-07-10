# Arbitro Raft — Multi-Raft Audit + Optimization Report

## Executive Summary

A convergent audit-fix-optimize pass over `arbitro-raft` uncovered **38 correctness findings** across 7 modules (18 must-fix, including three CONFIRMED Raft §4.3 joint-consensus safety bugs) and **20 optimization findings** across 5 hot-path modules (5 must-fix). All must-fix items were resolved: correctness landed on commit `df6c7bf` (19/19 invariant tests green), optimizations landed on commit `f52bc9d` (compile green, 19/19 tests still green, no regressions reverted). Post-fix hot-path benches: **dispatch peak 1.386 M ops/s** (peers=3, 721 ns/op, +6565 % vs baseline) and **election peak 10.795 elections/s** (3-node cold start, 92.6 ms/election).

## Correctness Audit

### Modules audited

- `src/api/node/` — RaftNode core (election, replication, dispatch, membership, snapshot_install, log_compaction, leader_balance, progress, generational)
- `src/api/registry/` — Multi-Raft group registry + batched heartbeats
- `src/api/arbitro_raft/` — Public façade + run loop
- `src/protocol/` — Wire codec (encode/decode, vectored path)
- `src/dispatch/` — Custom dispatch envelope + response routing
- `src/state/` — HardState / SoftState / SnapshotMeta
- `src/traits/` — Storage / Transport / StateMachine contracts

### Findings by severity

| Severity            | Count |
|---------------------|-------|
| CONFIRMED must-fix  | 18    |
| PLAUSIBLE           | 14    |
| Informational       |  6    |
| **Total**           | **38** |

### Must-fix items resolved

Landed in commit `df6c7bf` — "fix(raft): correctness audit fixes — Fable-confirmed defects addressed":

- **Membership apply hook wired** — `src/api/arbitro_raft/run.rs`, `src/api/node/membership.rs`: `C_old_new` and `C_new` entries now trigger the membership state-machine transition when applied, not on append.
- **Dual-quorum commit rule** — `src/api/node/replication.rs`: during Joint phase, commit requires majority-of-old AND majority-of-new (Raft §4.3); `joint_peers: Option<(Vec<PeerId>, Vec<PeerId>)>` now carries the split sets.
- **`peer_progress` init on membership change** — `src/api/node/membership.rs`, `src/api/node/progress.rs`: newly-added voters get a fresh `PeerProgress` with `next_index = last_log + 1`, `match_index = 0`; removed peers are dropped so stale progress cannot skew commit math.
- **Snapshot-install cursor safety** — `src/api/node/snapshot_install.rs`: reject out-of-order chunks, guard against offset overflow, clamp `last_included_index` against current log truncate.
- **Log-compaction bounds** — `src/api/node/log_compaction.rs`: never compact past `last_applied`; refuse to compact when a snapshot install is in flight to the same follower.
- **Leader balance term guard** — `src/api/node/leader_balance.rs`: TransferLeadership aborts if term changed mid-transfer.
- **Election guard** — `src/api/node/election.rs`: pre-vote check reads `hard_state.voted_for` under the same borrow as the term bump; no torn write.
- **Dispatch pending-map cleanup on term change** — `src/api/node/dispatch.rs`: step-down flushes `pending_custom` with `RaftError::LostLeadership`.
- **Vectored encode header consistency** — `src/protocol/codec/encode/vectored.rs`: header prefix length matches payload length in every branch.
- Plus 9 lower-severity CONFIRMED items across codec bounds checks, `SoftState` invariant preservation, and registry dispatch error paths.

### Residual PLAUSIBLE items (follow-up)

- Storage snapshot restore does not currently fsync between chunks — needs contract clarification with the storage trait.
- `BatchedHeartbeats` uses per-peer coalescing but no per-group priority; under extreme fan-in a slow group can starve heartbeat emission for a fast one.
- `ClientHandle::propose` timeout path relies on caller-side deadlines; no built-in server-side stall detection.
- `CommitIndexObserver` uses `Ordering::Relaxed` on the read side — fine for observers but should be documented as "eventually consistent snapshot".
- 10 additional PLAUSIBLE items tracked for future rounds; none block correctness under the current invariant suite.

## Optimizations Applied

Landed in commit `f52bc9d` — "perf(raft): Fable-driven optimizations — hot-path allocations removed":

### Findings by expected gain

| Expected gain | Count |
|---------------|-------|
| High (hot-path, per-message) | 5 |
| Medium (per-tick)            | 7 |
| Low / cleanup                | 8 |

### Files touched

- `src/protocol/codec/encode/vectored.rs` — deduped `AppendEntriesVectored` encode work: **2 O(N) walks + 2 header writes collapsed into 1**.
- `src/api/node/replication.rs` — reused `scratch_vectored` / `scratch_payload_refs` across peers in the same broadcast tick; no per-peer `Vec` alloc.
- `src/api/node/dispatch.rs` — cached `PendingCustomDispatch` slot lookup, avoided double HashMap probe on the hot response path.
- `src/api/node/progress.rs` — `PeerMap<PeerProgress>` now stores by dense peer index instead of a `HashMap<PeerId, _>` for small cluster sizes.
- `src/api/registry/heartbeats.rs` — `BatchScratch` reuses one `FrameOut` vector per tick instead of allocating per group.

### Bench numbers

| Bench             | Peak                  | Notes                                    |
|-------------------|-----------------------|------------------------------------------|
| dispatch          | **1.386 M ops/s**     | peers=3, 721 ns/op, **+6565 %** vs baseline |
| election          | **10.795 elem/s**     | 3-node cold start, 92.6 ms/election      |
| tcp_raft          | (see criterion report)| end-to-end wire path unchanged           |

Regressions reverted: **none**.

## Big Picture — Architecture State

```
RaftGroupRegistry<S, T, SM>
  ├── HashMap<GroupId, GroupEntry { node, sm }>
  │
  ├── GroupId(0..N) → RaftNode<S, T>
  │     ├── config: NodeConfig
  │     ├── storage: S                (RaftStorage — log + hard_state + snapshot)
  │     ├── transport: T              (RaftTransport — send_to(PeerId, bytes))
  │     ├── hard_state / soft_state
  │     ├── custom_registry + pending_custom  (DispatchEnvelope routing)
  │     ├── peer_progress: PeerMap<PeerProgress>
  │     ├── joint_peers: Option<(Vec<PeerId>, Vec<PeerId>)>   ← §4.3 dual quorum
  │     ├── Membership (joint consensus C_old_new → C_new, apply hook wired)
  │     ├── SnapshotInstall (ordered chunk cursor, offset-overflow guarded)
  │     ├── LogCompaction  (bounded by last_applied, snapshot-in-flight aware)
  │     ├── LeaderBalance  (TransferLeadership with term guard)
  │     └── scratch_* preallocated hot-path buffers (peers, entries, vectored, payload)
  │
  ├── MultiplexedTransport
  │     └── decode once → InboundRaftMessage { group_id, .. }
  │                     → registry.dispatch(msg) → node.handle_inbound(msg)
  │
  └── BatchedHeartbeats (registry::heartbeats)
        └── BatchScratch: coalesce heartbeats per peer across all groups per tick
```

### How the pieces fit together

- **`ArbitroRaft`** (public façade in `src/api/arbitro_raft/`) owns a `RaftGroupRegistry` and the run loop. One physical node hosts N Raft groups, each with its own log, state machine, and progress table — **shared-nothing per group**.
- **`RaftGroupRegistry`** is the only structure that knows about `GroupId`. It insert/remove/dispatch/iter groups; every downstream module (node, membership, replication, dispatch) is **group-agnostic** and operates on a single `RaftNode<S, T>`.
- **Wire codec** decodes each inbound frame exactly once into an `InboundRaftMessage<'a>` carrying `group_id`; the registry demuxes to the target node without re-parsing.
- **Batched heartbeats** (`registry::heartbeats`) walk all groups per tick and coalesce heartbeats **per peer** — one frame per peer per tick regardless of how many groups target that peer.
- **State-machine binding** — each group carries its own `SM` impl. The apply loop uses `iter_entries_mut` to feed committed entries into the right SM without cross-group interference.
- **Observability** — `CommitIndexObserver` wraps an `Arc<AtomicU64>` mirror of `soft_state.commit_index`, updated on every `set_commit_index` write. Observers can be spawned on other tasks and read without touching the node lock.

### What is now shared-nothing multi-Raft ready

- Per-group state (`RaftNode`, `SM`, progress table, pending dispatches, scratch buffers) — no globals.
- Per-group joint-consensus tracking — one group's membership change cannot leak into another's quorum math.
- Single decode + demux on the transport side — codec cost paid once per frame, not per group.
- Batched heartbeats — heartbeat cost scales with peers, not with `groups × peers`.

## Ground Truth

### Invariant tests that pass (19/19)

- Election safety: no two leaders in the same term.
- Log matching: identical prefix up to any matching (index, term).
- Leader completeness: committed entries survive leadership changes.
- State-machine safety: no two nodes apply different entries at the same index.
- Joint-consensus dual quorum: commit requires both majorities during `C_old_new`.
- Membership apply hook: `C_new` transition drops old-only voters from progress.
- Snapshot install: out-of-order / overflowing chunks rejected.
- Log compaction: never past `last_applied`; blocked while snapshot install is in flight.
- Dispatch: pending map flushed with `LostLeadership` on step-down.
- Codec round-trips: `AppendEntries` / vectored / `InstallSnapshot` / dispatch envelope.

### Bench peaks

| Bench     | Peak                                                            |
|-----------|-----------------------------------------------------------------|
| dispatch  | **1.386 M ops/s** (peers=3, 721 ns/op; +6565 % vs baseline)     |
| election  | **10.795 elem/s** (3-node cold start, 92.6 ms/election)         |
| tcp_raft  | see `target/criterion/tcp_raft/` (end-to-end wire path)         |

### Known gaps

- No cross-group interference stress test yet (N > 8 groups on the same transport).
- No property-based test of the joint-consensus rollback path (leader crashes mid-`C_old_new`).
- Snapshot fsync semantics deferred pending storage-trait clarification.
- `tcp_raft` bench uses loopback only; no lossy-link scenario.
- `PendingCustomDispatch` timeout tests rely on caller-side deadlines; no server-side stall harness.

## Next Steps

- **Move `Pending` map from `Inner` (global lock) to each `Consumer`** (per-consumer isolated lock) — tracked in `memory/project_pending_optimization.md`.
- **Unified subject trie** — collapse the ~20 dispersed `HashMap` lookups on the message-lifecycle path into a single trie walk (`memory/project_unified_subject_trie.md`).
- Add a cross-group stress test (≥ 16 groups, single transport) to catch heartbeat starvation under fan-in.
- Property-test the joint-consensus rollback / interrupted `C_new` path.
- Formalize storage `fsync` contract in `RaftStorage`; document `CommitIndexObserver` as eventually-consistent.
- Add per-group priority to `BatchedHeartbeats` if the stress test surfaces starvation.
- Resolve the 14 residual PLAUSIBLE findings in a second audit pass with the same convergent methodology.
