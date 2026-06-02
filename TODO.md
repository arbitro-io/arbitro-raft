# arbitro-raft — Open Issues & Required Work

## Critical: Election Does Not Converge Over TCP

**Status**: Broken. Real TCP election with 3 nodes never elects a leader.

**Symptoms**: `campaign_pre_vote` sends PreVote to all peers (send OK), peers receive and process them (`handle_pre_vote` fires, sends `PreVoteResp granted=true ok=true`), but the requesting node never sees the PreVoteResp — it times out every round. After 16+ rounds in 8 seconds, no node reaches `campaign_once`.

**Root Cause**: All 3 nodes enter `campaign_pre_vote` simultaneously. Each node's collect loop receives PreVote REQUESTS from the other two nodes (processed via `handle_inbound`), which delays or displaces the PreVoteResp it's waiting for. The responses arrive in the mpsc channel but the node has already consumed the deadline processing the interleaved requests.

**Evidence** (from arbitro-server 3-node cluster test):
```
[RAFT] node 2 handle_pre_vote from 1 term=1
[RAFT] node 2 sent PreVoteResp to 1 granted=true ok=true
[RAFT] node 3 handle_pre_vote from 1 term=1
[RAFT] node 3 sent PreVoteResp to 1 granted=true ok=true
// ... repeats 16 times, never reaches campaign_once
```

**Partial Fix Applied** (commit 8db4330): Changed `campaign_pre_vote` and `collect_votes` to use absolute deadline (`Instant::now() + election_timeout`) instead of per-recv timeout. This prevents non-response messages from extending the window indefinitely, but doesn't solve the core problem.

**What Works**: The bench (`tcp_raft_bench.rs`) bypasses election entirely with `node.become_leader_for_benchmark(Term(1))` + simulated followers (`run_follower_sim`). The in-memory tests (`raft_correctness.rs`) use `TestTransport` (channels, no TCP). Neither tests real TCP election.

### Fix Options (pick one)

**Option A — Drain-first collect loop**:
```rust
// Instead of recv-one-at-a-time, drain ALL available frames first,
// then separate responses from requests.
while votes < votes_needed {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() { return Ok(false); }

    // Drain all available frames from channel
    let mut frames = Vec::new();
    while let Some(n) = transport.recv_frame_timeout(Duration::ZERO, buf).await? {
        frames.push(buf[..n].to_vec());
    }
    if frames.is_empty() {
        // Nothing available — wait with remaining budget
        if let Some(n) = transport.recv_frame_timeout(remaining, buf).await? {
            frames.push(buf[..n].to_vec());
        }
    }

    // Process responses FIRST, then requests
    let mut responses = Vec::new();
    let mut requests = Vec::new();
    for f in frames {
        let msg = decode_message(&f)?;
        if matches!(msg.message, RaftMessage::PreVoteResp(_)) {
            responses.push(msg);
        } else {
            requests.push(msg);
        }
    }
    for r in responses { /* count votes */ }
    for r in requests { handle_inbound(r).await?; }
}
```

**Option B — Separate response channel**:
Add a second mpsc channel in the transport exclusively for responses. `send_message` tags outbound frames as response vs request. The accept_loop routes them to different channels. `campaign_pre_vote` reads only from the response channel.

**Option C — Staggered start**:
Don't start all 3 nodes simultaneously. Use `become_leader_for_benchmark` on the first node (like the bench does), then let the other 2 join as followers. Election only needed on leader failure. This is the pragmatic approach for v1.

### Recommended: Option A for correctness, Option C for immediate usability.

---

## Required Benchmarks

### 1. Real TCP Election Bench (does not exist)

**Goal**: Measure election convergence time with 3/5/7 real Raft nodes over TCP.

```
bench_tcp_election_3node  — 3 nodes, all start as followers, measure time to leader
bench_tcp_election_5node  — 5 nodes
bench_tcp_election_reelection — kill leader, measure re-election time
```

**Why**: The current bench (`tcp_raft_bench.rs`) forces the leader with `become_leader_for_benchmark`. No benchmark measures real election latency.

### 2. Election Under Load

**Goal**: Election convergence while publish traffic is in-flight.

```
bench_tcp_election_under_load — leader dies mid-publish, measure:
  - time to new leader
  - messages lost during transition
  - client write latency during re-election
```

### 3. Pre-Vote Correctness Bench

**Goal**: Verify that pre-vote prevents disruptive elections from partitioned nodes.

```
bench_prevote_partitioned — partition 1 node, let it rejoin, verify:
  - no term inflation on the partitioned node
  - cluster doesn't step down when partitioned node rejoins
```

### 4. Concurrent Campaign Stress Test

**Goal**: Force all nodes to campaign simultaneously (the exact scenario that fails today).

```
test_concurrent_campaign_converges — 3 nodes start campaign at exact same instant
  - must elect exactly 1 leader within 5 seconds
  - no panic, no infinite loop
```

---

## Other Open Issues

### StateMachine Not Integrated in RaftNode

`RaftNode::new()` takes `(NodeConfig, Storage, Transport)` — no StateMachine parameter. The `StateMachine` trait exists but is never consumed by the Raft core. Applications must externally track `commit_index`, read committed entries from storage, and apply them.

**Impact**: Followers in a cluster don't automatically apply committed entries to their local state. The application must poll commit_index and apply manually.

**Fix**: Either integrate StateMachine into RaftNode (call `apply()` when commit_index advances), or document the external-apply pattern with a concrete example.

### Pre-Vote Listed as "Not Done" in Roadmap

The README roadmap (Phase 4) shows `Pre-Vote / Check-Quorum` as unchecked, but `campaign_pre_vote` and `handle_pre_vote` are implemented in `election.rs`. Check-Quorum is implemented in `run_leader_once` (line 74: `check_quorum_active()`). Update the roadmap.

### Membership Changes Not Implemented

Adding/removing nodes at runtime requires joint consensus (Raft §4.1). Currently the peer list is fixed at boot.

---

## Test Gaps

| Scenario | Status | File |
|----------|--------|------|
| In-memory election | ❌ Not tested | — |
| TCP election (3 nodes) | ❌ Broken | — |
| TCP election (5 nodes) | ❌ Not tested | — |
| Leader step-down on quorum loss | ✅ Code exists | `run.rs:74` |
| Pre-vote prevents term inflation | ❌ Not tested | — |
| Snapshot install + catch-up | ❌ Not tested | — |
| Concurrent campaigns converge | ❌ Broken | — |
| Log compaction after snapshot | ❌ Not tested | — |
| Membership change (add node) | ❌ Not implemented | — |
