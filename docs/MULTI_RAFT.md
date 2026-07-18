# Multi-Raft: the per-core blueprint

How to run many Raft groups per process with `MultiRaftDriver`
(`arbitro_raft::api::registry::MultiRaftDriver`).

## Deployment model

```
process
├── core 0: MultiRaftDriver ── groups {1, 4, 7, ...}   one shared transport
├── core 1: MultiRaftDriver ── groups {2, 5, 8, ...}   one shared transport
└── core N: MultiRaftDriver ── groups {3, 6, 9, ...}   one shared transport
```

- **N cores → N drivers → many groups per driver.** Each driver is owned by
  exactly one task/thread and runs share-nothing: a group's node state,
  storage handle, state machine, and timers all live on its driver. There is
  no lock shared between drivers and no lock on the route→step path.
- **One transport per driver.** All groups on a driver multiplex over that
  driver's transport. Every frame carries its `group_id` at a fixed header
  offset; the driver routes each inbound frame with one O(1) map lookup —
  no allocation, no decode before routing.
- **The loop is `run_once(max_wait)`: demux → step → apply.** One tick
  drains parked frames, burst-drains the shared transport, routes each frame
  to its group, steps that group, applies newly committed entries to the
  group's state machine, then services per-group heartbeat/election timers.
  Idle ticks block on the transport bounded by `max_wait` and the earliest
  timer deadline — no spinning.
- **Frames for unhosted groups are safe.** A frame whose `group_id` names no
  group on the driver is counted (`unknown_group_frames()`) and dropped
  before decode. It is never mis-routed to another group and never panics.

## Memory model

Big buffers are **O(cores), not O(groups)**:

- The driver owns ONE set of MB-class scratch buffers (~33 MiB per core:
  inbound frame buffer, 16 MiB payload scratch, 1 MiB outbound scratch,
  64 KiB quorum scratch, 16 MiB apply scratch).
- A group borrows them only while it is being stepped: the driver
  `mem::swap`s the shared buffers into the node before each operation and
  back out after — pointer swaps, zero copy, zero allocation.
- `add_group` frees the node's own MB-class buffers, so an **idle group
  costs ~100 KiB** (capacity docks, index/peer vecs, small maps), not ~17 MiB.
  `group_idle_scratch_bytes(gid)` reports the MB-class bytes a group holds
  while idle — it is `0` for every group on the diet, before and after
  traffic. 1000 idle groups on one core cost one ~33 MiB scratch set plus
  ~100 KiB each, instead of ~17 GiB.

## Adding and removing groups at runtime

```rust
let mut driver: MultiRaftDriver<S, T, SM> = MultiRaftDriver::new(transport);

// Register: builds the node over a demux-backed group transport and puts it
// on the memory diet. Fails on a duplicate group id.
driver.add_group(GroupId(7), config, storage, state_machine)?;

// Operate.
driver.campaign(GroupId(7)).await?;          // real election, skips pre-vote
let idx = driver.propose(GroupId(7), b"op").await?;  // replicate + commit + apply
driver.group(GroupId(7)).unwrap().commit_index();

// Deregister: returns (node, state_machine) with standalone MB-class buffers
// restored, so the group can run outside the driver or migrate to another
// driver/core.
let (node, sm) = driver.remove_group(GroupId(7)).unwrap();
```

`campaign`, `propose`, and `run_once` must be polled to completion from the
owning task's loop (do not race them inside `select!`). Cancellation
mid-flight never affects correctness — the driver self-heals its scratch on
the next operation — but costs a transient cold-path allocation.

## v1 scope limits (additive follow-ups)

- **No client handle / commit waiter on the driver yet.** `propose` is the
  driver-owned, awaited path (replicate → quorum ack → apply, then return).
  A concurrent client handle that parks waiters per commit index is an
  additive follow-up; nothing in the current API changes for it.
- **No D3 abuse-jail on the driver dispatch path.** Malformed frames are
  already dropped safely (counted for unknown groups, non-fatal decode
  errors logged and dropped for known groups), but per-peer offender
  scoring/jailing does not run on the multi-group dispatch path yet — also
  an additive follow-up.

## Worked references

- `examples/multiraft_driver.rs` — runnable blueprint: one driver pair, 3
  groups over one shared transport per side, independent commit counts
  (2/4/6), idle scratch 0. `cargo run --release --example multiraft_driver`
- `tests/multi_driver.rs` — the full contract under assertion: routing,
  independent election/commit, no cross-talk, unknown-group frame counting,
  garbage-frame tolerance, the idle memory diet, and `remove_group`
  restoring standalone buffers.
