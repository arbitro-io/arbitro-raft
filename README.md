# arbitro-raft

> **Status: active development — not yet stable. First release target: `v0.1.0`.**

Transport-agnostic, storage-agnostic Raft consensus core for Rust.

---

## Benchmark Results

> Windows 11, loopback, `cargo bench --release`.
> Values for **v0.2.0 (Total Zerocopy)**.

### Latency — `propose_once`

| Transport | Payload | Latency  | Throughput |
|-----------|---------|----------|------------|
| Memory    | empty   | 2.03 µs  | 494 K/s    |
| Memory    | 1 KB    | 2.37 µs  | 422 K/s    |

### Batch throughput — `propose_batch_once(N)`

| Transport | Batch | Latency  | Throughput      |
|-----------|-------|----------|-----------------|
| Memory    | 64    | 4.97 µs  | 12.87 M ops/s   |
| Memory    | 256   | 12.72 µs | 20.12 M ops/s   |
| Memory    | 1024  | 44.76 µs | 22.88 M ops/s   |

### Concurrent writes — `ClientHandle::write()` (N tasks)

| Transport | Clients | Throughput      |
|-----------|---------|-----------------|
| Memory    | 64      | 4.01 M ops/s    |
| Memory    | 256     | 4.85 M ops/s    |
| Memory    | 1024    | 5.73 M ops/s    |
| TCP       | 16      | 448 K ops/s     |
| TCP       | 64      | 1.32 M ops/s    |

---

## Usage

### Writing entries

```rust
use arbitro_raft::{ArbitroRaft, NodeConfig, RaftNode};

let mut node = RaftNode::new(config, storage, transport).unwrap();
let mut raft = ArbitroRaft::new(node);

// Single entry — blocks until quorum commits it.
let index = raft.propose_once(b"payload").await?;

// Batch — one network round-trip for N entries.
let indexes = raft.propose_batch_once(&[
    b"entry-a",
    b"entry-b",
]).await?;

// Concurrent writes from many tasks.
let handle = raft.client_handle();
tokio::spawn(async move { raft.run().await });
let index = handle.write(b"concurrent").await?;
```

### Custom RPCs via Dispatch

```rust
use arbitro_raft::{DispatchAckPolicy, DispatchPeerState, DispatchScope, DispatchSpec};

fn ping() -> DispatchSpec<String, String> {
    DispatchSpec::new(
        0x01,
        |s| Ok(s.as_bytes().to_vec()),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
        |s| Ok(s.as_bytes().to_vec()),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
    )
    .with_scope(DispatchScope::All)
    .with_ack_policy(DispatchAckPolicy::Quorum)
}

// Register handler on every node.
node.on_with(ping(), |question: String, ctx| Box::pin(async move {
    ctx.accept_bytes(format!("pong: {question}").into_bytes()).await
}))?;

// Dispatch from the leader and await responses.
let result = node.dispatch(ping(), "hello".to_string()).await?.wait().await?;
for peer in &result.peers {
    if let DispatchPeerState::Accepted(reply) = &peer.state {
        println!("{:?} → {reply}", peer.peer);
    }
}
```

See [`examples/basic_raft.rs`](examples/basic_raft.rs) and [`examples/dispatch_rpc.rs`](examples/dispatch_rpc.rs).

---

## Roadmap

### `v0.1.0` — Core consensus ✅ (current)
- [x] Leader election (term-based voting, randomised timeouts)
- [x] Log replication (`AppendEntries`, quorum commit)
- [x] Heartbeat / follower timeout
- [x] Transport abstraction (`RaftTransport` trait)
- [x] Storage abstraction (`RaftStorage` trait)
- [x] `MemStorage` reference implementation
- [x] `TcpTransport` reference implementation
- [x] `propose_once` / `propose_batch_once` single-node API
- [x] `ClientHandle` concurrent-write API
- [x] Custom RPC layer (`DispatchSpec`, `on_with`, `dispatch`)
- [x] 32-byte cache-aligned wire header

### `v0.2.0` — Robustness
- [ ] **Log compaction / snapshotting** — install-snapshot RPC, truncate log prefix
- [ ] **Learner nodes** — catch-up members that do not vote until fully replicated
- [ ] **Single-server membership changes** — add/remove one peer at a time (§4.1)
- [ ] **Leader transfer** — graceful leadership handoff without election timeout
- [ ] **Check-quorum** — leader steps down if it cannot hear from a quorum

### `v0.3.0` — Performance & linearisability
- [ ] **Pre-vote** — candidate asks peers before incrementing term, prevents disruptive elections
- [ ] **ReadIndex** — linearisable reads without writing to the log
- [ ] **Lease-based reads** — bounded-clock leader-lease for lower-latency reads
- [ ] **Pipeline replication** — overlap multiple `AppendEntries` RPCs per follower
- [ ] **Joint consensus** — safe arbitrary membership changes (§6)
- [ ] **Witness / non-voting replicas** — quorum participation without full log storage

---

*Built by [@automatizadovip](https://github.com/automatizadovip).*
