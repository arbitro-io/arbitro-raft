# arbitro-raft

> **Status: active development — not yet stable. First release target: `v0.1.0`.**

Transport-agnostic, storage-agnostic Raft consensus core for Rust.

---

## Benchmark Results

> Windows 11, loopback TCP, `cargo bench --release`.

### Latency — `propose_once`

| Transport | Payload | Latency  | Throughput |
|-----------|---------|----------|------------|
| Memory    | empty   | 1.05 µs  | 954 K/s    |
| Memory    | 1 KB    | 1.33 µs  | 754 K/s    |
| TCP       | empty   | 21.8 µs  | 45.9 K/s   |
| TCP       | 1 KB    | 21.7 µs  | 46.0 K/s   |

### Batch throughput — `propose_batch_once(N)`

| Transport | Batch | Latency  | Throughput      |
|-----------|-------|----------|-----------------|
| Memory    | 1     | 1.00 µs  | 1.00 M ops/s    |
| Memory    | 64    | 8.70 µs  | 7.35 M ops/s    |
| Memory    | 256   | 31.7 µs  | 8.08 M ops/s    |
| Memory    | 1024  | 126 µs   | 8.12 M ops/s    |
| TCP       | 64    | 38.1 µs  | 1.68 M ops/s    |
| TCP       | 256   | 50.5 µs  | 5.07 M ops/s    |

### Concurrent writes — `ClientHandle::write()` (N tasks)

| Transport | Clients | Throughput      |
|-----------|---------|-----------------|
| Memory    | 64      | 2.61 M ops/s    |
| Memory    | 256     | 3.26 M ops/s    |
| Memory    | 1024    | 3.61 M ops/s    |
| TCP       | 16      | 448 K ops/s     |
| TCP       | 64      | 1.32 M ops/s    |

---

## Usage

### Writing entries

```rust
use arbitro_raft::{ArbitroRaft, NodeConfig, RaftNode};
use bytes::Bytes;

let mut node = RaftNode::new(config, storage, transport).unwrap();
let mut raft = ArbitroRaft::new(node);

// Single entry — blocks until quorum commits it.
let index = raft.propose_once(Bytes::from("payload")).await?;

// Batch — one network round-trip for N entries.
let indexes = raft.propose_batch_once(vec![
    Bytes::from("a"),
    Bytes::from("b"),
]).await?;

// Concurrent writes from many tasks.
let handle = raft.client_handle();
tokio::spawn(async move { raft.run().await });
let index = handle.write(Bytes::from("concurrent")).await?;
```

### Custom RPCs via Dispatch

```rust
use arbitro_raft::{DispatchAckPolicy, DispatchPeerState, DispatchScope, DispatchSpec};
use bytes::Bytes;

fn ping() -> DispatchSpec<String, String> {
    DispatchSpec::new(
        0x01,
        |s| Ok(Bytes::copy_from_slice(s.as_bytes())),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
        |s| Ok(Bytes::copy_from_slice(s.as_bytes())),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
    )
    .with_scope(DispatchScope::All)
    .with_ack_policy(DispatchAckPolicy::Quorum)
}

// Register handler on every node.
node.on_with(ping(), |question: String, ctx| Box::pin(async move {
    ctx.accept_bytes(Bytes::copy_from_slice(
        format!("pong: {question}").as_bytes()
    )).await
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

*Built by [@automatizadovip](https://github.com/automatizadovip).*
