# arbitro-raft

**Predictable Consensus at Scale (v0.2.0 — The Zero-Copy Era).**

`arbitro-raft` is a transport-agnostic and storage-agnostic Raft core designed for high-concurrency message brokers. It is the heart of the Arbitro ecosystem, built with a single constraint: **Hardware Sympathy**.

Every line of code is written to minimize CPU jitter, eliminate heap churn, and saturate the physical limits of modern network stacks.

---

## Benchmark Results

> Measured on Windows 11 — all numbers from `cargo bench --release`.
> Memory transport = in-process channels. TCP transport = loopback 127.0.0.1.

### Latency — `propose_once` (single entry, quorum required)

| Transport | Payload | Latency (avg) | Throughput |
|-----------|---------|--------------|------------|
| Memory    | empty   | **1.05 µs**  | 954 K/s    |
| Memory    | 1 KB    | **1.33 µs**  | 754 K/s    |
| TCP       | empty   | **21.8 µs**  | 45.9 K/s   |
| TCP       | 1 KB    | **21.7 µs**  | 46.0 K/s   |

### Batch Throughput — `propose_batch_once(N entries)`

| Transport | Batch size | Latency (avg) | Throughput      |
|-----------|-----------|--------------|-----------------|
| Memory    | 1         | 1.00 µs      | 1.00 M ops/s    |
| Memory    | 64        | 8.70 µs      | **7.35 M ops/s**|
| Memory    | 256       | 31.7 µs      | **8.08 M ops/s**|
| Memory    | 1024      | 126 µs       | **8.12 M ops/s**|
| Memory    | 4096      | 588 µs       | 6.97 M ops/s    |
| Memory    | 1 (1KB)   | 1.07 µs      | 934 K ops/s     |
| Memory    | 4096 (1KB)| 2.62 ms      | 1.56 M ops/s    |
| TCP       | 1         | 23.1 µs      | 43.4 K/s        |
| TCP       | 64        | 38.1 µs      | **1.68 M ops/s**|
| TCP       | 256       | 50.5 µs      | **5.07 M ops/s**|
| TCP       | 1024      | 249 µs       | **4.12 M ops/s**|

### Concurrent Writes — `ClientHandle::write()` (N tasks, each awaits commit)

| Transport | Clients | Latency (avg) | Throughput      |
|-----------|---------|--------------|-----------------|
| Memory    | 1       | 2.23 µs      | 447 K ops/s     |
| Memory    | 4       | 4.83 µs      | 827 K ops/s     |
| Memory    | 16      | 8.63 µs      | 1.85 M ops/s    |
| Memory    | 64      | 24.6 µs      | 2.61 M ops/s    |
| Memory    | 256     | 78.6 µs      | 3.26 M ops/s    |
| Memory    | 1024    | 284 µs       | **3.61 M ops/s**|
| TCP       | 1       | 23.5 µs      | 41.3 K ops/s    |
| TCP       | 4       | 30.5 µs      | 131 K ops/s     |
| TCP       | 16      | 35.7 µs      | 448 K ops/s     |
| TCP       | 64      | 48.3 µs      | **1.32 M ops/s**|

> [!NOTE]
> *Benchmarks performed on a standard workstation. Real-world network latency and disk I/O will apply based on your chosen `RaftTransport` and `RaftStorage` implementations.*

---

## Core Philosophy

### Zero-Allocation Hot Path
In `arbitro-raft`, the hot path (replicate, deliver, ACK) never touches the heap. We use **Memory Scratchpads**—pre-allocated vectors and buffers—to handle bursts of messages without triggering the allocator. We removed `serde` completely; all protocol frames are mapped directly to memory via `zerocopy` slices and atomic `Bytes` references.

### Zero-Cost Abstraction
The core has been refactored into a modular architecture (`src/api/node/`) for maintainability. However, thanks to Rust's monomorphization and aggressive inlining, this modularity has **zero overhead**. The compiler treats the fragmented modules as a single, optimized block of machine code.

### O(1) Frame Dispatch
Forget long `if-else` chains. Our frame dispatcher uses a direct jump table (via `match` on integer constants), ensuring that whether you have 2 or 20 message types, the cost to handle an inbound frame remains constant.

---

## Architecture

`arbitro-raft` provides the consensus engine, while you provide the environment:

- **Modular Node Core** (`src/api/node/`):
  - `mod.rs`: O(1) dispatcher and global utilities.
  - `election.rs`: Candidacy and quorum logic.
  - `replication/`: Leader/Follower flow and Adaptive Batching.
  - `snapshot.rs`: State recovery protocol.
  - `dispatch.rs`: Custom RPC extension layer.
- **Byte-Pure Transport**: The `RaftTransport` trait expects and returns raw `Bytes`. The core codec builds responses via zero-copy overlays, preventing serialization overhead.
- **Zero-Allocation Scratchpads**: Pre-allocated structures for peer tracking and entry building.
- **Lazy Protocol Views**: Inspect wire data over `Bytes` directly from the socket without materializing owned structs.

---

## Trade-offs (Engineering Honesty)

Building a high-throughput consensus engine requires specific trade-offs:

1. **Memory for Latency**: By using pre-allocated **Scratchpads**, we consume a fixed amount of RAM upfront even when the system is idle. This eliminates the "Garbage Collection" effect of memory allocators during peak load, ensuring a stable P99 latency.
2. **Standardized Frames**: We use a fixed-size header (**32 bytes**, power-of-2 aligned) for all protocol messages. This places each header on a half-cache-line boundary, eliminating cross-boundary reads in the decode path. Cost: 8 bytes of wire overhead vs a 24-byte header.
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
- `memory_e2e_bench.rs`: Pure in-process consensus (~8 M ops/s peak batch throughput).
- `tcp_raft_bench.rs`: Loopback TCP consensus (~5 M ops/s peak batch throughput).

*Built by the team at @automatizadovip.*
