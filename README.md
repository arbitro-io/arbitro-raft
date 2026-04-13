# arbitro-raft

> **The ultra-fast, zero-allocation Raft consensus core for premium distributed systems.**

`arbitro-raft` is a transport-agnostic, storage-agnostic Raft implementation designed for sub-microsecond consensus latency and extreme throughput.

---

## ⚡ Performance Profile

> [!IMPORTANT]
> **Total Zero-Copy / Zero-Allocation Architecture**
> All hot paths operate without heap allocations or data copies. Metadata is handled via zero-copy views and internal futures are stack-allocated using native AFIT (Async Functions in Traits).

### In-Memory Transport (No-Op Storage, WSL2)

| Scenario | Mode | Latency (P50) | Throughput (Peak) |
| :--- | :--- | :--- | :--- |
| **Direct Proposal** | Single client (empty) | **1.43 µs** | 699 K ops/s |
| **Direct Proposal** | Single client (1KB) | **1.72 µs** | 580 K ops/s |
| **Pipelined Batch** | 256-entry batch | 9.72 µs | **26.33 M ops/s** |
| **Extreme Batch** | 4096-entry batch | 129.25 µs | **31.69 M ops/s** |

### Verified Scalability Profile

| Batch Size | Latency | Time per Message | Throughput |
| :--- | :--- | :--- | :--- |
| 64 | 3.46 µs | 54 ns | 18.45 M ops/s |
| 256 | 9.72 µs | 38 ns | 26.33 M ops/s |
| 1024 | 33.58 µs | 32 ns | 30.49 M ops/s |
| 4096 | 129.25 µs | **31 ns** | **31.69 M ops/s** |

*Benchmarks executed on WSL2 (Ubuntu 22.04), pinned high-frequency x86_64.*
*Zero-copy validation: 1KB payload adds only ~290ns overhead vs empty payload.*

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
