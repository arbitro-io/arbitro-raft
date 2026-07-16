# arbitro-raft — Open Issues & Required Work

> **The living backlog is now [`AUDIT_REPORT.md`](AUDIT_REPORT.md)** (the six-axis
> master audit + P0→P3 ladder). As of 2026-07-16: **P0 (soundness/safety) is complete
> and Fable-re-audited**, and **P1 is mostly done** — see the Progress note at the top
> of that file. The items below are historical (the original TCP-election work); most
> are resolved and superseded by the audit ladder.

**Status**: Resolved.

**Fix Applied**:
- **Option A (Drain-first collect loop)**: Implemented in `collect_votes` and `campaign_pre_vote` inside [election.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/src/api/node/election.rs) to drain all incoming frames first, separate requests from responses, and process responses before requests. This prevents interleaving requests from stalling the campaigns.
- **Loopback socket write deadlock fix**: Resolved issues where aborting a leader left connection accept streams open, causing followers' socket writes to fill the OS send buffer and block indefinitely. Implemented structured cancellation using `tokio::sync::watch` to terminate connections immediately on shutdown, and added a timeout to the socket writes.

---

## Required Benchmarks

### 1. Real TCP Election Bench

**Status**: Resolved.
- Created [election_bench.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/benches/election_bench.rs) using Criterion.
- Includes 3-node and 5-node cold startup benchmarks (`bench_tcp_election_3node` and `bench_tcp_election_5node`).
- Includes a leader failover reelection benchmark (`bench_tcp_election_reelection`).

### 2. Election Under Load

**Status**: Resolved.
- Created reelection benchmark under client load (`bench_tcp_election_under_load`) in [election_bench.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/benches/election_bench.rs).

### 3. Pre-Vote Correctness Bench

**Status**: Resolved.
- Implemented as an integration test verifying that partitioned nodes do not trigger disruptive elections or term inflation, and successfully catch up when reconnected.

---

## Other Open Issues

### StateMachine Not Integrated in RaftNode

`RaftNode::new()` takes `(NodeConfig, Storage, Transport)` — no StateMachine parameter. The `StateMachine` trait exists but is never consumed by the Raft core. Applications must externally track `commit_index`, read committed entries from storage, and apply them.

**Fix**: Either integrate StateMachine into RaftNode (call `apply()` when commit_index advances), or document the external-apply pattern with a concrete example.

### Pre-Vote Listed as "Not Done" in Roadmap

**Status**: Resolved. Checked in [README.md](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/README.md).

### Membership Changes Not Implemented

Adding/removing nodes at runtime requires joint consensus (Raft §4.1). Currently the peer list is fixed at boot.

---

## Test Gaps

| Scenario | Status | File |
|----------|--------|------|
| In-memory election | ✅ Tested | [raft_distributed.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/tests/raft_distributed.rs) |
| TCP election (3 nodes) | ✅ Tested | [election_bench.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/benches/election_bench.rs) |
| TCP election (5 nodes) | ✅ Tested | [election_bench.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/benches/election_bench.rs) |
| Leader step-down on quorum loss | ✅ Code exists | `run.rs:74` |
| Pre-vote prevents term inflation | ✅ Tested | [raft_distributed.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/tests/raft_distributed.rs) |
| Snapshot install + catch-up | ❌ Not tested | — |
| Concurrent campaigns converge | ✅ Tested | [raft_distributed.rs](file:///d:/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft/tests/raft_distributed.rs) |
| Log compaction after snapshot | ❌ Not tested | — |
| Membership change (add node) | ❌ Not implemented | — |
