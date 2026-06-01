# arbitro-raft

> **The ultra-fast, zero-allocation Raft consensus core for premium distributed systems.**

`arbitro-raft` is a transport-agnostic, storage-agnostic Raft implementation designed for sub-microsecond consensus latency and extreme throughput.

---

## ⚡ Performance Profile

> [!IMPORTANT]
> **Total Zero-Copy / Zero-Allocation Architecture**
> All hot paths operate without heap allocations or data copies. Metadata is handled via zero-copy views and internal futures are stack-allocated using native AFIT (Async Functions in Traits).

### 🚀 Performance Tiers

#### Tier 1: In-Memory Transport (Engine Baseline)
*No-Op Storage, No-Op Transport, zero-copy loopback.*

| Scenario | Mode | Latency (P50) | Throughput (Peak) |
| :--- | :--- | :--- | :--- |
| **Direct Proposal** | Single client (empty) | **548.84 ns** | **1.82 M ops/s** |
| **Direct Proposal** | Single client (1KB) | **948.44 ns** | **1.05 M ops/s** |
| **Extreme Batch** | 4096-entry batch | 132.54 µs | **30.90 M ops/s** |

#### Tier 2: TCP Transport (Network Reality)
*Loopback TCP sockets, TCP_NODELAY, real-world serialization, hybrid vectored I/O.*

| Scenario | Mode | Latency (P50) | Throughput (Peak) |
| :--- | :--- | :--- | :--- |
| **Direct Proposal** | Single client (empty) | **38.13 µs** | **26.2 K ops/s** |
| **Direct Proposal** | Single client (1KB) | **37.56 µs** | **26.6 K ops/s** |
| **Extreme Batch** | 1024 clients (no-op transport) | 81.41 µs | **12.58 M ops/s** |
| **Replicated Batch** | 1024-entry batch w/ follower | 2.65 ms | **387 K ops/s** |

#### Tier 3: Parallel Dispatch & Orchestration Latency (`dispatch_bench`)
*No-network, parallel Dispatch API RPC fan-out.*

| Cluster Size | Latency (P50) | Throughput (Peak) |
| :--- | :--- | :--- |
| **3 Nodes (`peers_3`)** | **58.74 µs** | 17.0 K ops/s |
| **10 Nodes (`peers_10`)** | **25.25 µs** | **39.6 K ops/s** |
| **100 Nodes (`peers_100`)** | **45.61 µs** | 21.9 K ops/s |

*Benchmarks executed on WSL2 (Ubuntu 22.04), CPU: High-frequency x86_64.*
*Zero-copy validation: 1KB network latency is only ~5.2 µs overhead over empty, confirming zero-copy processing.*
*Orchestration latency for 10 nodes fell by **-64.8%** after implementing the zero-allocation target collection path.*

---

## ✨ Features

- **Zero-Allocation Hot Path**: No `Box`, No `Vec`, No `String` in the replication critical path.
- **Zero-Copy Protocol**: Direct pointer-mapping of wire frames to Raft views via `zerocopy`.
- **Lock-Free Slot Arena**: 65k pre-allocated commit-notification slots — no `Arc<Mutex>` per proposal.
- **AFIT Native**: Leverages native async trait implementation for maximum compiler optimization.
- **Hybrid Vectored I/O**: `encode_message_vectored` assembles frames from pre-allocated scratchpads without copying; an auto-threshold picks between contiguous and `writev(2)` per batch, tunable at runtime via env vars.
- **Dispatch Engine**: Transparent custom RPC layer with quorum-based acknowledgement policies.
- **Election Optimization**: Concurrent parallel broadcast of RequestVote messages combined with a zero-allocation linear-scan responders bitset.
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
  codec/        → encode/, decode.rs, wire.rs, view.rs
    encode/     → mod.rs, vectored.rs, contiguous.rs (zero-copy encoders)
  view.rs       → zero-copy inbound message views
  message.rs    → owned outbound message types

api/
  arbitro_raft/ → execution loop, batching, timers, client API
    slot.rs     → Slot, SlotId, SlotRegistry (lock-free arena)
    client.rs   → ClientHandle, WriteFuture (zero-alloc write path)
    run.rs      → run_leader_once, run_follower_once, event loop
    timers.rs   → election/heartbeat deadline management
  node/         → Raft state machine
    replication/→ propose/, handler.rs, heartbeat.rs, shared.rs
      propose/  → mod.rs, initial.rs, quorum.rs (proposal engine)
    election.rs → RequestVote, campaign_once (election logic)
    snapshot.rs → InstallSnapshot (snapshot engine)
    dispatch.rs → custom RPC dispatch
  custom_registry.rs → handler registration

dispatch/       → DispatchSpec, DispatchHandle, DispatchContext
state/          → HardState, SoftState
traits/         → RaftStorage, RaftTransport (AFIT)
```

---

## 🎛️ Tuning

The hybrid vectored I/O path ships with conservative defaults chosen empirically via A/B benchmarks. Thresholds can be overridden at process start without rebuilding:

| Env var | Default | Purpose |
| :--- | :--- | :--- |
| `ARBITRO_RAFT_VEC_IOV_MAX` | `4096` | Max iovecs before falling back to contiguous encoding |
| `ARBITRO_RAFT_VEC_MIN_ENTRY` | `4096` | Min per-entry payload size (1 page) to qualify for vectored |
| `ARBITRO_RAFT_VEC_MIN_TOTAL` | `65536` | Min aggregate batch size to qualify for vectored |
| `ARBITRO_RAFT_FORCE_CONTIGUOUS` | — | Force legacy contig path (A/B testing) |
| `ARBITRO_RAFT_FORCE_VECTORED` | — | Force vectored path (A/B testing) |

The auto-mode decision is cached in a `OnceLock` on first use — zero overhead on the hot path.

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
- [x] **Hybrid Vectored I/O**: Auto-threshold contig vs `writev(2)` — +21% throughput on large replicated batches

### Phase 3: SOTA Latency & Zero-Alloc Despacho ✅
- [x] **Zero-Alloc Target Collection**: Reuse `scratch_peers` in `dispatch()` to avoid target vector allocations
- [x] **Preallocated Capacities**: Preallocate `pending_custom`, `peer_progress`, `pending_snapshots` and internal scratchpads in constructor
- [x] **Vectored/Propose Splits**: Split large `encode.rs` and `propose.rs` files into highly cohesive submodules under 250 lines
- [x] **Election Latency Improvement**: Concurrent RequestVote broadcast + preallocated linear scan responders (no `HashSet` allocations)

### Phase 4: Distributed Resilience & Compactness
- [x] **Log Compaction**: Install-snapshot RPC support
- [ ] **Membership Changes**: Single-server configuration updates (§4.1)
- [ ] **Pre-Vote / Check-Quorum**: Leadership stability improvements
- [ ] **Learner Nodes**: Non-voting members for catch-up replication
- [ ] **Generational Metadata**: Recyclable log index buffers
- [ ] **Zero-Copy Snapshots**: DMA-friendly state transfer

---

*Built for high-performance distributed infrastructure by [@automatizadovip](https://github.com/automatizadovip).*
