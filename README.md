# arbitro-raft

> **The ultra-fast, zero-allocation Raft consensus core for premium distributed systems.**

`arbitro-raft` is a transport-agnostic, storage-agnostic Raft implementation designed for sub-microsecond consensus latency and extreme throughput.

---

## ⚡ Performance Profile

> [!IMPORTANT]
> **Total Zero-Copy / Zero-Allocation Architecture**
> All hot paths operate without heap allocations or data copies. Metadata is handled via zero-copy views and internal futures are stack-allocated using native AFIT (Async Functions in Traits).

| Scenario | Mode | Persistence | Latency (P50) | Throughput (Peak) |
| :--- | :--- | :--- | :--- | :--- |
| **Direct Proposal** | In-Memory | No-Op | **1.50 µs** | 680 K ops/s |
| **Pipelined Batch** | In-Memory | No-Op | 44.76 µs | **20.12 M ops/s** |
| **TCP Loopback** | Network | No-Op | 43.80 µs | 820 K ops/s |

*Benchmarks executed on WSL2 (Ubuntu 22.04), CPU: High-frequency x86_64.*

---

## ✨ Features

- **Zero-Allocation Host**: No `Box`, No `Vec`, No `String` in the replication hot path.
- **Zero-Copy Protocol**: Direct pointer-mapping of wire frames to Raft views.
- **AFIT Native**: Leverages native async trait implementation for maximum compiler optimization.
- **Lock-Free Consensus Loop**: Minimized internal synchronization for massive concurrency support.
- **Dispatch Engine**: Transparent custom RPC layer with quorum-based acknowledgement policies.

---

## 🚀 Usage

### Initializing the Node

```rust
use arbitro_raft::{ArbitroRaft, NodeConfig, RaftNode};

let mut node = RaftNode::new(config, storage, transport).unwrap();
let mut raft = ArbitroRaft::new(node);

// Continuous consensus loop
tokio::spawn(async move { raft.run().await });
```

### Proposing Entries

```rust
// Concurrent writes from many tasks using a clonable handle
let handle = raft.client_handle();
let index = handle.write(b"premium payload").await?;

// Batch proposals for maximum throughput
let indexes = raft.propose_batch_once(&[
    b"entry-a",
    b"entry-b",
]).await?;
```

---

## 🗺️ Roadmap

### Phase 1: Core Hardening ✅
- [x] Zero-Copy wire protocol v1
- [x] AFIT (Async Fn in Traits) migration (Eliminated `async_trait` overhead)
- [x] RAII-based scratchpad management
- [x] Quorum-based batch replication

### Phase 2: Memory Optimization ⏳ (current)
- [ ] **Slot Arena**: Pre-allocated circular buffer for commit notifications (remove `Arc<Slot>`).
- [ ] **Generational Metadata**: Recyclable log index buffers.
- [ ] **Zero-Copy Snapshots**: DMA-friendly state transfer.

### Phase 3: Distributed Resilience
- [ ] **Log Compaction**: Install-snapshot RPC support.
- [ ] **Membership Changes**: Single-server configuration updates (§4.1).
- [ ] **Pre-Vote / Check-Quorum**: Leadership stability improvements.

---

*Built for high-performance distributed infrastructure by [@automatizadovip](https://github.com/automatizadovip).*
