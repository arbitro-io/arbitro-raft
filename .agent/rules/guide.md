---
trigger: always_on
---

# ARBITRO-RAFT — Contribution Rules for Agents

You are working inside `arbitro-raft`, the Raft core for Arbitro. This crate inherits all root `AGENTS.md` rules. The rules below are crate-specific and mandatory.

## 1) Layering

The crate has 3 ordered layers:

```txt
protocol/   -> wire serialization only, no Raft logic
state/ types/ error/ entry/ config/ validation/ traits/ -> pure types
api/        -> Raft logic and orchestration
```

A higher layer must never import from an equal or higher layer in another branch.

### Module responsibilities

- `protocol/codec.rs` -> zero-copy wire encode/decode
- `protocol/view.rs` -> zero-copy views over received frames
- `protocol/message.rs` -> owned outbound message types
- `dispatch/` -> custom dispatch protocol over Raft
- `api/node/` -> Raft state machine: election, replication, snapshot, dispatch
- `api/arbitro_raft.rs` -> execution loop, batching, timers
- `api/custom_registry.rs` -> dispatch handler registry by command byte

## 2) HardState vs SoftState

This is a correctness invariant, not style.

### HardState

Must be durably persisted on every relevant transition:

- `current_term` -> persist before any send
- `voted_for` -> persist before voting

### SoftState

Rebuilt in memory on restart:

- `role` -> starts as `Follower`
- `leader_id` -> starts as `None`
- `is_leader` -> starts as `false`
- `commit_index` -> starts at `0`; leader propagates it via AppendEntries

### Critical rule

`commit_index` is **not** HardState. Never persist or restore it from disk. Restoring it can violate linearizability if the log was truncated.

## 3) Wire Protocol Rules

### Constants

All protocol constants must live in `protocol/codec.rs`. Never use raw hex literals directly. Use named constants only.

- `RAFT_MAGIC = 0x5241_4654`
- `RAFT_VERSION = 0x01`
- `RAFT_FRAME_HEADER_SIZE = size_of::<RaftFrameHeader>()`
- `RAFT_DISPATCH_MAGIC = 0x4453_5054`
- `RAFT_DISPATCH_RESPONSE_MAGIC = 0x4452_5350`

### Endianness

All wire fields are **little-endian**. Use `zerocopy::byteorder::little_endian::{U16, U32, U64}` for all multi-byte fields in `#[repr(C)]` structs.

### Padding

Every wire struct must remain **8-byte aligned**. Explicit `_pad` fields are part of the wire contract. Do not remove or resize them without a protocol version bump.

### Receive validation

Before reading any field:

1. validate `magic`
2. validate `version`
3. validate `body_len` against remaining bytes
4. for `AppendEntries`, validate every `EntryHeader` so `payload_end <= body.len()`

Never access a received frame before validation. `parse_*_view` functions are the only valid entry points for inbound frames.

## 4) Hot Path Rules

These refine the root performance rules.

### No allocations in hot paths

Preallocated scratchpads in `RaftNode` are the only valid temporary buffers:

```rust
self.scratch_entries.clear();
self.storage.read_entries(from, to, &mut self.scratch_entries)?;
```

Do **not** allocate new `Vec` or `HashMap` in per-frame paths.

Scratchpads:

- `scratch_entries: Vec<LogEntry>`
- `scratch_indexes: Vec<LogIndex>`
- `scratch_peers: Vec<PeerId>`
- `scratch_pending: HashMap<PeerId, AppendAttemptState>`
- `scratch_started: HashMap<PeerId, Instant>`

Always call `.clear()` before reuse.

### Views vs owned

- received frame, used only locally -> `*View`
- frame stored or sent across channels -> owned via `.to_owned()`
- same bytes forwarded unchanged -> `Bytes::clone()` only

`Bytes::clone()` is O(1) and must be preferred over copying.

### Dispatch must use `match`, not if-chains

```rust
match inbound.message {
    RaftMessageView::RequestVote(msg)         => self.handle_request_vote(msg).await,
    RaftMessageView::RequestVoteResp(msg)     => self.handle_request_vote_response(msg).await,
    RaftMessageView::AppendEntries(msg)       => self.handle_append_entries(msg).await,
    RaftMessageView::AppendEntriesResp(msg)   => self.handle_append_entries_response(msg).await,
    RaftMessageView::InstallSnapshot(msg)     => self.handle_install_snapshot(msg).await,
    RaftMessageView::InstallSnapshotResp(msg) => self.handle_install_snapshot_response(msg).await,
    RaftMessageView::Custom(msg)              => self.handle_custom_message(msg).await,
    RaftMessageView::CustomResponse(msg)      => self.handle_custom_response(msg).await,
}
```

Do not use a silent wildcard arm. Every new enum variant must be handled explicitly.

## 5) Raft Correctness Invariants

### Persistence order before send

Before any network send:

1. if term changed -> `save_hard_state` with new term and `voted_for = None`
2. if `voted_for` changed -> `save_hard_state`
3. if entries were appended -> `append_entries` and `fsync` if supported
4. only then -> `transport.send(...)`

Never invert this order.

### Quorum

Use this as the single source of truth:

```rust
pub(crate) fn quorum(nodes: usize) -> usize { (nodes / 2) + 1 }
```

Never inline quorum math elsewhere.

### Election termination

Vote collection must continue until:

- `votes >= votes_needed`, or
- all possible responders replied, or
- timeout expires

Correct:

```rust
while votes < votes_needed && responders.len() < possible_votes {
    ...
}
```

Never subtract 1 from `possible_votes`.

### Replication errors

Network failures during `AppendEntries` are not fatal, but must be recorded in peer progress state. Never silence them with `let _ = ...` in quorum-related paths.

Correct pattern:

```rust
async fn send_best_effort(&self, peer: PeerId, msg: RaftMessage, _phase: &str) -> bool {
    self.transport.send(peer, msg).await.is_ok()
}
```

### Snapshot offsets

Followers receiving snapshot chunks must verify:

```txt
pending.bytes.len() as u64 == msg.offset()
```

If mismatched, reply with:

- `accepted: false`
- `next_offset = pending.bytes.len()`

Leaders must retry from `next_offset`. Never assume ordered or gap-free chunk delivery.

## 6) Dispatch System Rules

### Handler registration

- one `command: u8` -> exactly one handler
- duplicate registration -> `Err`
- register handlers before the main loop starts
- never register handlers from inside the execution loop

### `DispatchSpec` is the only encode/decode source of truth

Always encode/decode through the spec:

```rust
let body = spec.encode_params(&params)?;
let response = spec.decode_response(bytes)?;
```

Never encode/decode ad hoc. JSON is forbidden in hot paths.

### Dispatch scopes

- `All` -> all nodes, including self
- `Others` -> all except self
- `Followers` -> followers only
- `Leader` -> known leader only
- `LocalOnly` -> self only, no network

Scope is part of the wire frame and cannot change after `build()`.

### ACK / failure policy

- default ACK policy -> `DispatchAckPolicy::Quorum`
- default failure policy -> `DispatchFailPolicy::AllowFailures`
- a `DispatchHandle` is ready when `completion.is_some()`
- never call `.wait()` inside the main node loop
- use `.try_result()` for non-blocking polling

## 7) Trait Contracts

### `RaftStorage`

- `load_hard_state` -> called once at init, must be idempotent
- `save_hard_state` -> synchronous and durable before return
- `append_entries` -> entries durable before return
- `read_entries(from, to, out)` -> range is `[from, to)`, `out` is extended, not cleared
- `truncate_suffix(from)` -> removes `[from, ∞)`, durable before return
- `last_log_position` -> must be overridden; default is O(N)
- `entry_at` -> must be overridden; default allocates per call
- `save_snapshot` -> atomic: full snapshot or nothing

### `RaftTransport`

- `send` -> best-effort, not fatal to node correctness
- `recv` -> blocks until a valid frame arrives
- `recv_timeout(d)` -> `None` on timeout, `Some` on frame

`recv` and `recv_timeout` must return already parsed `InboundRaftMessageView`. Parsing belongs in the transport, not in the node.

### `StateMachine`

Reserved for future extensions. In v0.1 it has no methods. Log application is the responsibility of the crate user, not `arbitro-raft`.

## 8) Observability

### Allowed

```rust
tracing::info!(...);
tracing::debug!(...);

if super::trace_enabled() {
    tracing::trace!(...);
}
```

### Forbidden

```rust
println!(...);
eprintln!(...);
```

### `ARBITRO_RAFT_TRACE`

`trace_enabled()` must read `ARBITRO_RAFT_TRACE` only once via `OnceLock`. Tracing output must go through `tracing`, never `eprintln!`.

Any timing instrumentation using `Instant::now()` must exist only inside `if trace_enabled()` blocks. Never measure unconditionally in hot paths.

## 9) Size Limits

- `.rs` file -> max 400 lines
- function / method -> max 60 lines
- `impl` block -> max 200 lines

If a file exceeds the limit, split it into focused submodules, for example:

- `replication/heartbeat.rs`
- `replication/propose.rs`
- `replication/handler.rs`
- `codec/encode.rs`
- `codec/decode.rs`
- `codec/validate.rs`

## 10) Naming Conventions

- `*View` -> zero-copy type over `Bytes`
- `*Resp` -> owned response message
- `*RespView` -> zero-copy response view
- `handle_*` -> inbound frame handler
- `build_*` -> outbound message builder
- `send_*_once` -> single send, no internal loop
- `*_once` -> one iteration, no external retry loop implied
- `scratch_*` -> preallocated scratch buffer in `RaftNode`
- `pending_*` -> in-flight state awaiting completion

## 11) PR Must-Not Checklist

Before proposing changes, verify all of the following:

- no `println!` / `eprintln!` in production code
- no `format!` in hot paths; only allowed in errors or tracing
- no `Instant::now()` outside `if trace_enabled()`
- no `Vec::new()` / `HashMap::new()` in per-frame logic
- `commit_index` is not in `HardState`
- election loop uses `responders.len() < possible_votes`
- replication paths do not hide send failures with `let _ = ...`
- `save_hard_state` happens before `transport.send` on any term/vote transition
- new protocol constants live in `protocol/codec.rs` or `dispatch/view.rs`
- no raw protocol hex literals outside constant definition files
- `RaftStorage` implementations used in tests/benchmarks override `last_log_position` and `entry_at`
- no silent `_ => {}` in frame dispatch; unknown cases must fail explicitly
- no file exceeds 400 lines

## Final Rule

If you change or add code in this crate, preserve:

- Raft safety
- durability ordering
- zero-copy receive paths
- zero-allocation hot paths
- explicit validation
- explicit dispatch
- strict trait semantics
- trace-only observability overhead

Do not trade correctness for convenience.