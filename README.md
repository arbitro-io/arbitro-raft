# arbitro-raft

> **The ultra-fast, zero-allocation Raft consensus core for premium distributed systems.**

`arbitro-raft` is a transport-agnostic, storage-agnostic Raft implementation designed for sub-microsecond consensus latency and extreme throughput.

---

## ⚡ Performance Profile

> [!IMPORTANT]
> **Total Zero-Copy / Zero-Allocation Architecture**
> All hot paths operate without heap allocations or data copies. Metadata is handled via zero-copy views and internal futures are stack-allocated using native AFIT (Async Functions in Traits).

### In-Memory Transport (No-Op Storage)

| Scenario | Mode | Latency (P50) | Throughput (Peak) |
| :--- | :--- | :--- | :--- |
| **Direct Proposal** | Single client | **1.49 µs** | 670 K ops/s |
| **Pipelined Batch** | 128-entry batch | 49.62 µs | **20.63 M ops/s** |
| **High Concurrency** | 1024 clients | 232.38 µs | **4.40 M ops/s** |

### TCP Transport (Loopback, Real Sockets)

| Scenario | Clients | Latency (P50) | Throughput |
| :--- | :--- | :--- | :--- |
| `propose_once` / empty | 1 | **40.50 µs** | 24.7 K ops/s |
| `propose_once` / 1 KB payload | 1 | **42.82 µs** | 23.4 K ops/s |
| Concurrent writes | 1 | 32.01 µs | 31.2 K ops/s |
| Concurrent writes | 4 | 35.19 µs | 113.7 K ops/s |
| Concurrent writes | 16 | 42.24 µs | 378.8 K ops/s |
| Concurrent writes | 64 | 73.17 µs | **874.7 K ops/s** |
| Batch throughput | 64 | 1.34 ms | 47.6 K ops/s |
| Batch throughput | 1024 | 29.20 ms | 35.1 K ops/s |

*Benchmarks executed on WSL2 (Ubuntu 22.04), tmpfs, CPU: High-frequency x86_64.*
*TCP transport uses `TCP_NODELAY` and real loopback sockets (127.0.0.1).*

---

## ✨ Features

- **Zero-Allocation Hot Path**: No `Box`, No `Vec`, No `String` in the replication critical path.
- **Zero-Copy Protocol**: Direct pointer-mapping of wire frames to Raft views via `zerocopy`.
- **Lock-Free Slot Arena**: 65k pre-allocated commit-notification slots — no `Arc<Mutex>` per proposal.
- **AFIT Native**: Leverages native async trait implementation for maximum compiler optimization.
- **Vectored I/O**: `encode_message_vectored` assembles frames from pre-allocated scratchpads, never copying.
- **Dispatch Engine**: Transparent custom RPC layer with quorum-based acknowledgement policies.
- **False-Sharing Prevention**: `#[repr(C, align(64))]` on all hot concurrent data structures.

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

## 🏗️ Architecture

```
protocol/       → wire encode/decode (zero-copy, zerocopy crate)
  codec/        → encode.rs, decode.rs, wire.rs, view.rs
  view.rs       → zero-copy inbound message views
  message.rs    → owned outbound message types

api/
  arbitro_raft/ → execution loop, batching, timers, client API
    slot.rs     → Slot, SlotId, SlotRegistry (lock-free arena)
    client.rs   → ClientHandle, WriteFuture (zero-alloc write path)
    run.rs      → run_leader_once, run_follower_once, event loop
    timers.rs   → election/heartbeat deadline management
  node/         → Raft state machine
    replication/→ propose.rs, handler.rs, heartbeat.rs, shared.rs
    election.rs → RequestVote, campaign_once
    snapshot.rs → InstallSnapshot
    dispatch.rs → custom RPC dispatch
  custom_registry.rs → handler registration

dispatch/       → DispatchSpec, DispatchHandle, DispatchContext
state/          → HardState, SoftState
traits/         → RaftStorage, RaftTransport (AFIT)
```

---

## 🗺️ Roadmap

### Phase 1: Core Hardening ✅
- [x] Zero-Copy wire protocol v1
- [x] AFIT (Async Fn in Traits) migration — eliminated `async_trait` overhead
- [x] RAII-based scratchpad management
- [x] Quorum-based batch replication

### Phase 2: Memory & Transport Optimization ✅
- [x] **Slot Arena**: Pre-allocated circular buffer for commit notifications — no `Arc<Slot>`
- [x] **Lock-Free Leasing**: Atomic cursor with `compare_exchange` — no `Mutex`
- [x] **Module Split**: `arbitro_raft` split by responsibility (slot / client / run / timers)
- [x] **TCP_NODELAY**: Enabled on all transport connections — -28% latency on loopback
- [ ] **Generational Metadata**: Recyclable log index buffers
- [ ] **Zero-Copy Snapshots**: DMA-friendly state transfer

### Phase 3: Distributed Resilience
- [ ] **Log Compaction**: Install-snapshot RPC support
- [ ] **Membership Changes**: Single-server configuration updates (§4.1)
- [ ] **Pre-Vote / Check-Quorum**: Leadership stability improvements
- [ ] **Learner Nodes**: Non-voting members for catch-up replication

---

*Built for high-performance distributed infrastructure by [@automatizadovip](https://github.com/automatizadovip).*
