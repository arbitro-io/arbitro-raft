# ARBITRO RAFT PERFORMANCE RULES

These rules are MANDATORY for all code in this repository. 
Violations will be treated as bugs.

## UNIVERSAL CONSTANTS

- **Hot Path**: Anything involving `AppendEntries`, `propose`, `dispatch`, and `commit`.
- **Zero-Allocation**: No `Box`, No `String`, No `Vec::new()` during hot path execution.
- **Hardware Sympathy**: Think in bytes, cache lines (64 bytes), and instruction pipelining.

## THE RULES

### 1. Reference over Clone
**Priority**: Maximum.
**Description**: Never clone a structure or a buffer if a reference (`&T`) can fulfill the requirement. 

**Why?**
- **Zero Allocations**: Clones often trigger heap allocations (especially for `Vec` and `String`).
- **Memory Bandwidth**: Copying bytes is slow compared to passing a 64-bit pointer.
- **Cache Locality**: Accessing the same memory location multiple times is orders of magnitude faster than jumping between scattered copies.
- **Deterministic Latency**: Allocators introduce non-deterministic pauses. References are zero-cost at runtime.

### 2. Zero-Copy Seeds
Entries must be stored and transmitted using their wire-ready headers (Seeds). 
See `arbitro-store` for the established pattern.

### 3. O(1) Everything
Storage lookups, frame dispatching, and slot notifications must be O(1). 
Sequential scanning is forbidden on the hot path.

### 4. Cache Line Alignment
Hot concurrent structures (like slots or atomic counters) must use `#[repr(align(64))]` to prevent false sharing.