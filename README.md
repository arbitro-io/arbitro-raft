# arbitro-raft

**Predictable Consensus at Scale (v0.2.0 — The Zero-Copy Era).**

`arbitro-raft` is a transport-agnostic and storage-agnostic Raft core designed for high-concurrency message brokers. It is the heart of the Arbitro ecosystem, built with a single constraint: **Hardware Sympathy**.

Every line of code is written to minimize CPU jitter, eliminate heap churn, and saturate the physical limits of modern network stacks.

---

## The Numbers (Scenario Dashboard)

We don't promise "infinite" scale. We show you the actual physical bounds of our engine on standard modern hardware (Localhost TCP, Windows Stack).

| Scenario | Throughput | Latency (Avg) | Reliability |
|---|---|---|---|
| **In-Memory Hot Path** | **~47.21 M ops/s** | **~21 ns** | Internal dispatch limit (Uncontended) |
| **TCP Localhost (Burst)** | **~12.65 M ops/s** | **~79 ns** | Network stack saturation (Windows) |
| **Adaptive Batching** | **12.6M+ ops/s** | **< 100 ns** | Linear scaling with 1024 concurrent clients |

> [!NOTE]
> *Benchmarks performed on a standard workstation. Real-world network latency and disk I/O will apply based on your chosen `RaftTransport` and `RaftStorage` implementations.*

---

## Core Philosophy

### Zero-Allocation Hot Path
In `arbitro-raft`, the hot path (replicate, deliver, ACK) never touches the heap. We use **Memory Scratchpads**—pre-allocated vectors and buffers—to handle ráfagas of messages without triggering the allocator. We removed `serde` completely; all protocol frames are mapped directly to memory via `zerocopy` slices and atomic `Bytes` references.

### Zero-Cost Abstraction
The core has been refactored into a modular architecture (`src/api/node/`) for maintainability. However, thanks to Rust's monomorphization and aggressive inlining, this modularity has **zero overhead**. The compiler treats the fragmented modules as a single, optimized block of machine code.

### O(1) Frame Dispatch
Forget long `if-else` chains. Our frame dispatcher uses a direct jump table (via `match` on integer constants), ensuring that whether you have 2 or 20 message types, the cost to handle an inbound frame remains constant.

---

## Architecture

`arbitro-raft` provides the consensus engine, while you provide the environment:

- **[NEW] Modular Node Core** (`src/api/node/`):
  - `mod.rs`: O(1) dispatcher and global utilities.
  - `election.rs`: Candidacy and quorum logic.
  - `replication.rs`: Leader/Follower flow and Adaptive Batching.
  - `snapshot.rs`: State recovery protocol.
  - `dispatch.rs`: Custom RPC extension layer.
- **[NEW] Byte-Pure Transport**: The `RaftTransport` trait expects and returns raw `Bytes`. The core codec builds responses via zero-copy overlays, preventing serialization overhead.
- **[NEW] Zero-Allocation Scratchpads**: Pre-allocated structures for peer tracking and entry building.
- **Lazy Protocol Views**: Inspect wire data over `Bytes` directly from the socket without materializing owned structs.

---

## Trade-offs (Engineering Honesty)

Building a 12.6M+ ops/s engine requires specific trade-offs:

1. **Memory for Latency**: By using pre-allocated **Scratchpads**, we consume a fixed amount of RAM upfront even when the system is idle. This eliminates the "Garbage Collection" effect of memory allocators during peak load, ensuring a stable P99 latency.
2. **Standardized Frames**: We use a fixed-size header (`32 bytes`) for all protocol messages. This simplifies the hot path and makes hardware caches more effective, at the cost of slight overhead for very small messages.
3. **Static Dispatch**: The system relies heavily on generics and monomorphization. This results in incredibly fast execution but slightly longer compilation times.

---

## Minimal Usage

```rust
use arbitro_raft::{ArbitroRaft, NodeConfig, RaftNode};

fn build<S, T>(config: NodeConfig, storage: S, transport: T) -> ArbitroRaft<S, T>
where
    S: arbitro_raft::RaftStorage,
    T: arbitro_raft::RaftTransport,
{
    // The node core is now modular, but initialization remains identical
    let node = RaftNode::new(config, storage, transport).unwrap();
    ArbitroRaft::new(node)
}
```

---

## Design Constraints

- **The wire protocol is the contract**: Defined in `arbitro/crates/arbitro-proto`.
- **Hardware Sympathy**: Code is written with awareness of the CPU caches and network stack buffers.
- **Silent drops are forbidden**: Unknown frames or internal errors always surface.

---

## Benchmarks

See `benches/` for the implementation of:
- `memory_e2e_bench.rs`: Pure logic validation (~47M ops/s).
- `tcp_raft_bench.rs`: Local network saturation (~12.6M+ ops/s).

*Built by the team at @automatizadovip.*
