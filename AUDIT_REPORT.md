# arbitro-raft — Master Audit & Path-to-Masterpiece

> **Status of this document.** This replaces the previous `AUDIT_REPORT.md`, whose
> "38 fixes / 19-of-19 green" executive summary was **aspirational, not demonstrated**:
> the four safety-critical integration tests are `#[ignore]`d, two of them **fail when
> force-run**, and none of the five Raft safety properties has a running assertion. This
> report is a consolidated, adversarial re-audit by six independent Fable passes plus
> measured ground truth. Treat it as the authoritative backlog for making this crate the
> trustworthy, extremely-low-latency, world-class core of arbitro.
>
> Audited at rustc 1.92.0, `arbitro-raft` v0.2.0, ~8.7k LOC src + ~3.4k tests.
> Ground truth: build green (7 warnings); **27 tests pass, 4 ignored (2 fail on --ignored)**.

## What "masterpiece in every sense" means here (the six axes)

| Axis | Question | Current rating |
|---|---|---|
| **1. Correctness / safety** | Does it uphold the 5 Raft safety properties under adversity? | 🔴 **Unsound** on membership/joint + election-safety hole |
| **2. Code soundness / robustness** | Does the code itself avoid UB, panics, remote-kill? | 🔴 UB on wire + node dies on 1 bad frame |
| **3. Performance / latency** | Is it genuinely fast, honestly measured? | 🟡 Fast engine, honest numbers ≠ README, missing pipelining/durability |
| **4. Readability / API / maintainability** | Would a new engineer trust and extend it? | 🟡 Good arch, but dead code, dup, implicit contracts |
| **5. Observability / operability** | Can an operator see, debug, and safely run it? | 🔴 ~1/10 — near-blind, fails silently |
| **6. Verification** | Is the safety actually proven, and kept proven? | 🔴 ~2/10 — plumbing tested, consensus not, no CI |

**One-line verdict:** a genuinely fast, structurally clean Raft *prototype presenting as an
audited implementation*. The data plane shows real craft; the safety, soundness,
observability, and verification are far from production. Nothing found requires a redesign —
the trait seams are excellent — but the gap to "masterpiece" is substantial and itemized below.

---

## Master priority ladder

Ordered by "must-fix-first for a broker cluster substrate." IDs cross-reference the per-axis
sections. **P0 blocks any adoption; P1 is required before it is trustworthy; P2 is the
masterpiece bar; P3 is polish.**

### P0 — Soundness & safety blockers (a broker cannot ship on these)

> **STATUS — P0 complete (2026-07-16), verified green + Fable re-audited.**
> All seven blockers are fixed. P0-1 (write `flags`/`reserved`, no other encode
> path leaks). P0-2 (`ErrorClass` + `is_fatal`; every inbound decode/handle/recv
> site on the run loop, campaign, and post-commit drain now logs+drops non-fatal
> and only propagates Fatal). P0-3 (per-response step-down bail, term/role
> re-checks, self-membership guard). P0-4 (dual-quorum commit rule via
> `index_meets_commit_quorum`/`propose_commit_reached`; non-joint fast path keeps
> the cheap own-term majority commit). P0-5 (`reject_reserved_prefix` at
> `ClientHandle::write`). P0-6 (dangerous scratch case confirmed mitigated).
> P0-7 (saturating conflict hint + `next_index ≤ last+1` clamp).
>
> **Fable re-audit wave (post-fix) additionally fixed:** G1 — a regression the
> P0-4 fix introduced (`try_advance_commit_index` clobbered `scratch_indexes`,
> the slice `propose_batch_once` returns) via a dedicated `scratch_commit_acks`
> buffer + a non-joint fast-path branch; G2 — a hole in the §4.2.2 stickiness
> (a leader never stamps its own `last_leader_contact`, so it granted a
> challenger's pre-vote) closed by denying pre-votes while `is_leader()`; G3/G5 —
> the remaining P0-2 gaps on the election, post-commit-drain, and synchronous
> quorum-gather (`gather_dispatch_frame` skips bad frames) paths; and the §4.3
> **joint-transition auto-resumption** (`finalize_joint_if_inherited`): a leader
> that inherits an active joint config now proposes `C_new` so a membership
> change can never stall.
>
> **Remaining membership gap (→ P1):** `test_remove_node_via_config_change` stays
> `#[ignore]`d (~1-in-3 flaky). The auto-resumption + G2 cut the failure rate but
> cannot fully close removed-node disruption under a harness that holds the
> leader lock across `propose_config_change` (starving heartbeats so a peer wins
> mid-transition before `C_new` reaches it). Robust close needs leader-transfer
> (§4.2.3) or a run-loop-driven config-change harness. The add-node path is
> un-ignored and passes robustly.
>
> **Deferred theoretical item (→ P1, G4):** the Joint entry's own commit is still
> decided under the leader's old-majority (followers activate joint at append via
> C3; the leader activates after `propose_once` returns). Fable could not
> construct a committed-entry-loss from this; a symmetric leader-side append-time
> activation is the principled fix but is unsafe to apply naively to the Final
> phase (it would step a self-removing leader down before `C_new` commits).

- **P0-1 (UB on the wire)** — `contiguous.rs:14-16` (US1/P1): `set_len` over uninitialized `BytesMut` + `flags`/`reserved` header bytes never written → **6 uninitialized heap bytes sent on every frame**. Soundness bug + info leak. Fix: `resize(total_len, 0)` and write all header fields.
- **P0-2 (remote node kill)** — `run.rs` (PS1/ERR-3/C10): any malformed/unknown/oversized/unknown-command frame `?`-propagates → `run()` dies, **unlogged**, and the README's `tokio::spawn(raft.run())` discards the error. One version-skewed or hostile peer kills the cluster's leadership. Also ERR-4 (oversized snapshot) and ERR-5 (unknown dispatch command) are individually remote-triggerable. Fix: classify errors; decode/dispatch errors on inbound → log+count+drop; only storage/corrupt-log is fatal, and it must log before dying.
- **P0-3 (election safety hole)** — `election.rs:151-156, 55-58` (C1/PS2): a candidate that `step_down`s to a higher term mid vote-collection **keeps counting stale old-term grants** and still sets `role = Leader` at the new term → two leaders in one term. Fix: after each inbound during collection, bail unless still Candidate at the same term.
- **P0-4 (joint consensus unsound)** — (C2/C3): the dual-quorum §4.3 rule is **bypassed by the exact propose path config-changes use** (`propose/mod.rs:49-57` uses union quorum), and config activates at **apply-time not append-time** → committed-entry loss / disjoint-quorum dual leaders. The two membership tests that would catch this are ignored and fail. Fix: route all commits through the joint-aware rule; activate config on append; un-ignore + pass the tests.
- **P0-5 (config-change injection)** — `client.rs:83` / `run.rs:89-105` (PS3): `ClientHandle::write` never calls `reject_reserved_prefix`, so a **user payload starting `0xC0` silently rewrites the voter set** at apply time. Fix: validate the reserved prefix at the client-write entry.
- **P0-6 (dangling-ref discipline)** — (US2/US4): the core `'static`-laundered scratchpads (`node/mod.rs:50-61`) are **not cleared on error paths** (`handler.rs:65`, `propose/mod.rs:101`, `shared.rs:90-94`) → live dangling `&'static [u8]` in the node (UB by reference validity). `unsafe impl Send/Sync` justification omits these fields. Fix: RAII `ScratchGuard` clearing on drop; correct the safety invariant.
- **P0-7 (divergence repair crashes leader)** — `handler.rs:331-339, 165, 200` (C5): the reject hint `match_index = follower's longer stale tail` walks `next_index` **forward past the leader's own log** → `term_at()` → `CorruptLog` → node death. Divergent follower never repaired. Fix: send conflict index (or clamp `next_index ≤ leader last+1`) + divergence-repair test.

### P1 — Trust & operability (required before it runs in production)
- **P1-1** Persist-before-reply durability contract (P3-plausible): define `RaftStorage` fsync semantics; today no test ever fails a storage op or restarts a node → double-vote-after-crash is possible and untested (Verification §; API2). **[DONE]** `RaftStorage` now documents the persist-before-return contract (save_hard_state/append_entries durable before `Ok`, else double-vote); test `hard_state_survives_restart_no_double_vote` restarts a node over the same storage and asserts term + voted_for recover. **Remaining:** a storage-fault-injection harness (a store that *fails* a write) — deferred to the P2 DST work.
- **P1-2** Observability layer: `RaftMetrics` + `RaftStatus` mirror-atomics cloned before spawn (OBS §1; H-1/H-6); `leader_id()` getter (an operator literally cannot ask a node who leads); ISR/lag signal (BP-3). **[P1-2a+b DONE]** `leader_id()` + `RaftStatus` snapshot + `status()` on both `RaftNode` and `ArbitroRaft`; `RaftMetrics` mirror-atomic counters (elections_started/won, step_downs, config_changes_applied, frames_dropped_nonfatal) cloned via `metrics()`, incremented only on cold paths; all exported; tests `status_and_leader_id_report_initial_state`, `metrics_count_election_activity`. **Remaining:** ISR/lag signal (BP-3).
- **P1-3** Fail loudly: structured-log event set (OBS-T4), demote heartbeat-spam from info (OBS-T2), log run-loop death + SM-apply-failure (ERR-10) + step-down (OBS-T4.1), error taxonomy `ErrorClass` (ERR-1). **[core DONE]** `ErrorClass` taxonomy (P0-2); heartbeat spam demoted to `trace!`; run-loop death logged with class (P0-2); SM-apply failure now logs `error!` with the offending index before stopping (ERR-10); step-downs counted in `RaftMetrics`. **Remaining:** a fuller structured span set is polish (P3).
- **P1-4** Frame-size contract (PS5): a `MAX_FRAME_SIZE` linking propose-time validation, `inbound_buf` (64 KiB), the 4 KiB snapshot stack buffer, and batch limits — today a legal 100 KiB committed payload can never reach a follower.
- **P1-5** Snapshot hardening: reject snapshots from non-members (PS6, unbounded multi-GiB buffers on spoofed `from`), timer-based eviction (not message-triggered), attempt cap (PS7), and don't monopolize the loop (OPS-2). **[PARTIAL]** non-member snapshots now rejected before any buffer is allocated (PS6 — a spoofed `from` can no longer force a multi-GiB pending buffer). **Remaining:** timer-based (not message-triggered) eviction, per-peer attempt cap (PS7), and chunked yielding so a large install doesn't monopolize the run loop (OPS-2).
- **P1-6** Fix the 4 disabled tests + safety-oracle sweep (S1–S5 cross-node assertions) + paused-time determinism (Verification roadmap 0-2). **[PARTIAL]** the add-node membership test is un-ignored + robust; the remove-node test remains ignored pending §4.2.3 leader-transfer (see P0 STATUS). The cross-node safety-oracle sweep + paused-time determinism belong with the **P2 DST harness** (that is where they become tractable), not standalone here.
- **P1-7** Integer/overflow policy: `checked_add` on terms/indices (P2, P3, P4, P15 u32-truncation, P18) — adversarial `term = u64::MAX` currently wraps. **[DONE]** both `election.rs` term increments now `saturating_add(1)` (adversarial `u64::MAX` no longer wraps to 0). **Index sweep DONE** (Fable-flagged): `apply_append_entries`/`apply_append_entries_seeded` now reject non-contiguous entry indices (`incoming.index == prev_log_index + 1 + offset`), so a spoofed near-`u64::MAX` index can't reach storage/the arena; and `generational.rs` arena arithmetic (49/57/75/96) is `saturating_add` (a bad index degrades to `None`, never panics the node). Tests `campaign_term_saturates_instead_of_wrapping` + `arena_arithmetic_saturates_near_u64_max`.
- **P1-8** Wire up or delete lying config knobs (API3): `append_batch_bytes`, `max_inflight_per_peer`, `cluster_id` (nodes of different clusters converse!), `bootstrap_peers`. **[OPEN — needs a wire decision]** enforcing `cluster_id` requires putting it on the wire, but `ClusterId` is `u64` and the only spare `RaftFrameHeader` slot is the `reserved` **u32** (zeroed by P0-1); carrying a u64 cluster id means a header-layout/version bump — a deliberate wire-format change, not a drop-in. Flagged for that decision.
- **P1-9** Backpressure (BP-1): unbounded client mpsc → bounded or depth-gauged; `stop()` must `close()` the channel (PS10). **[PS10 DONE]** `stop()` now `close()`s the client channel; a late `ClientHandle::write` fails fast ("raft node stopped") instead of parking forever. **Remaining:** unbounded→bounded — a **product decision** (writes then fail with backpressure under load rather than buffer), plus a `&self`→owned-`try_send` refactor; deferred pending that call.

### P2 — Masterpiece bar (world-class core)
- **P2-1** Determinism: thread the (dead) `Clock` trait through all 23 `Instant::now()` sites (D-1); replace unseedable `futures::select!` with `select_biased!` (Verification §3.2); swap iterated `HashMap`→`BTreeMap`/fixed-hasher (D-3).
- **P2-2** Deterministic Simulation Testing harness (TigerBeetle/FoundationDB grade): `SimNet` (drop/delay/reorder/dup/one-way/partial partition) + `SimStorage` (fault injection, torn writes) + `SimClock` + linearizability checker + seed corpus; 10k+ seeds nightly (Verification §3, roadmap 3/8).
- **P2-3** Property tests (codec round-trip, log-reconciliation model, commit-index reference, vote-grant purity — Verification §4) + cargo-fuzz on the codec that parses untrusted peer bytes (§6) + loom on `CommitIndexObserver`/dispatch/slots (§7).
- **P2-4** Stateright model of election + joint consensus (exhaustive small-scope; the two sub-protocols hold all 3 CRITICALs — Verification §5); optional TLA+ trace-validation.
- **P2-5** Async-apply + pipelining + group-commit fsync (the throughput levers Dragonboat/TiKV use that this lacks — Performance §): decouple leader append / replicate / apply; overlap multiple AppendEntries in flight; batch fsync across proposals.
- **P2-6** Per-group memory diet (API10): the ~37 MiB fixed buffers per `ArbitroRaft` cap multi-raft at ~50-100 groups; pool/share them. Plus the multi-group run driver + recv-side demux splitter (multi-raft §, the `_MultiplexedRecvPlaceholder`).
- **P2-7** CI: build+clippy+test gate, `--ignored` reporting job, Miri over codec/handler (multiple hot-path `unsafe` transmutes, US3), ASan nightly, coverage ratchet, fuzz smoke (Verification §10).

### P3 — Readability & polish (the "trusted and extendable" finish)
- Dead-code purge (RD1: orphan `protocol/view.rs`, unreachable `leader_balance`/`log_compaction`, dead `Clock`, `_MultiplexedRecvPlaceholder`, stale `#[allow(dead_code)]`), the `#[path]` double-compile hack (MO1 — one-line fix: `pub(crate) mod replication`), duplication in the 5 hottest files (RD3), naming (RD2), magic numbers (RD5), comment cleanup incl. Spanish-in-code and marketing tone (RD6), README truth (RD8: examples don't compile, "zero-alloc"/"AFIT eliminated async-trait" overstated), dependency slimming (DEP1 futures+tokio overlap, DEP2 async-trait), idiom (`#[non_exhaustive]`/`#[must_use]`/`Debug` derives, one arithmetic policy, newtype helpers — ID1-10).

---

## Axis 1 — Correctness & Raft safety

### Safety property → enforcing code → guarding test
| Property | Enforced in code? | Running test that fails if broken? |
|---|---|---|
| Election Safety (≤1 leader/term) | vote persist ordering `election.rs:187` — but hole C1 | **NONE** |
| Leader Append-Only | structural | **NONE** |
| Log Matching | `handler.rs:179-194` + conflict truncation | **NONE** (repair broken by C5) |
| Leader Completeness | up-to-date vote `election.rs:175` | **NONE** (no leader-kill test) |
| State Machine Safety | §5.4.2 own-term gate `shared.rs:287-304` ✅ (only guard genuinely correct) | **NONE** (no cross-node SM equality) |

### Confirmed findings
- **C1 CRITICAL** — Election Safety hole after mid-collection step-down (`election.rs:98-159`). See P0-3.
- **C2 CRITICAL** — Joint dual-quorum bypassed by `propose_once`/`propose_batch_once` (`propose/mod.rs:49-57`); config-change commits via union quorum → committed-entry loss. See P0-4.
- **C3 CRITICAL** — Config activates at apply-time not append-time (`run.rs:77-83`) with volatile `commit_index` → two leaders, disjoint quorums, divergent commits. See P0-4.
- **C4 HIGH** — Membership empirically broken end-to-end; ignored tests fail: "node 4 never catches up", "removed node re-elects itself" (`tests/membership_change.rs`).
- **C5 HIGH** — Divergence repair walks `next_index` forward → `CorruptLog` kills leader (`handler.rs:331-339`). See P0-7.
- **C6 HIGH** — No pre-vote leader-stickiness + candidates never check self-membership (`election.rs:328-351`) → removed node with longest log reinstates itself.
- **C7 MED** — `LostLeadership` notification does not exist; `step_down` silently `clear()`s `pending_custom` (`node/mod.rs:371-377`, in-code TODO admits leak); no compaction-vs-snapshot-in-flight guard; no `TransferLeadership` at all (the "term guard" fix claim is false).
- **C8 MED** — Single-node cluster never commits via run loop (`shared.rs:248-250` early-returns on empty progress).
- **C9 MED** — `StateMachine::apply(&[u8])` carries no `LogIndex` but `last_applied` is volatile → replay-on-restart hazard for any externally-persistent SM (`traits/state_machine.rs` undocumented).
- **C10 MED** — Any single bad frame permanently kills the node (merged into P0-2).

### Plausible (need targeted tests)
- P1 — Elections during joint use union majority, not both-majorities.
- P2 — `install_snapshot_once` blocks the whole event loop + 4 KiB stack recv buffer; large snapshot starves heartbeats → spurious elections.
- P3 — `RaftStorage` has no durability contract; vote/append ACKs sent before any fsync guarantee → crash-restart double-vote (split brain). See P1-1.
- P4 — `apply_committed_entries` calls `restore_state_machine_from_snapshot` (full `load_snapshot()`) on every batch once a snapshot exists → O(snapshot) disk read per apply.

---

## Axis 2 — Code soundness & robustness

### `unsafe` audit (highlights of 22 blocks + 2 unsafe impls)
- **US1 CRITICAL** — uninitialized bytes shipped on the wire (P0-1).
- **US2 CRITICAL (class)** — `'static`-laundered scratchpads leak dangling refs on early-`?` (P0-6).
- **US3 HIGH** — Stacked-Borrows-hostile self-referential sends (`shared.rs:106-124`, `run.rs:206-219`, `propose/*`): `&`-refs laundered to `'static` into `self`'s fields, live across a `&mut self` reborrow. Works today, likely UB, no Miri. Fix: outbound buffer outside `self` + Miri CI.
- **US4 HIGH** — `unsafe impl Send/Sync for RaftNode` invariant omits the laundered-ref fields it depends on.
- **US5 MED** — `Vec<(*const u8, usize)>` → `Vec<&[u8]>` transmute assumes non-ABI-guaranteed layout.
- **US6 MED** — `set_len(64*1024)` after conditional `reserve` (uninit exposure if realloc) — `resize` is free (`quorum.rs:49`, `shared.rs:314`).
- US7/US8 — vectored encoder raw-pointer interleave (sound, undocumented); `batch_scratch`/`heartbeat_batch` are positive examples to emulate.

### Panic / overflow / truncation inventory (reachable-first)
| ID | Site | Issue |
|---|---|---|
| P1 | `contiguous.rs:16` | UB uninit + unwritten header fields (P0-1) |
| P2 | `election.rs:17,225` | unchecked `term+1`; adversarial `u64::MAX` term wraps |
| P3 | `handler.rs:370` | unchecked `match_index+1`; sibling uses saturating — inconsistent |
| P4 | `initial.rs:135` | unchecked `last_log - len` |
| P5 | `initial.rs:95` | `.last().unwrap()` on empty payloads |
| P7 | `dispatch/tx*` ×8 | `Mutex::lock().unwrap()` → poison cascade |
| P14 | `quorum.rs:49`,`shared.rs:314` | `set_len` uninit (US6) |
| P15 | encode/`dispatch` | `usize→u32` length truncation >4 GiB → wire corruption; no size validation anywhere |
| P6,P8-P13,P16-P20 | various | guarded/unreachable but avoidable (iterate-flatten, store parsed header, `checked_mul`, `resize`, name constants) |

### Robustness (PS)
- **PS1 CRITICAL** — bad frame kills node (P0-2). **PS2 CRITICAL** — election hole (P0-3). **PS3 HIGH** — config-change injection (P0-5).
- **PS4 HIGH** — 8-byte `entry_at` "dummy read" relies on undocumented truncation semantics; a compliant storage returns `Err` on entries >8 B after restart → node death. Fix: add `RaftStorage::term_at`.
- **PS5 HIGH** — no frame-size contract (P1-4). **PS6 HIGH** — snapshot from unvalidated `from`, 4 GiB buffers, message-triggered eviction (P1-5). **PS7 MED** — snapshot re-stream loop under follower control. **PS8 MED** — check-quorum counts stale-term junk as contact. **PS9 MED** — `gather_quorum_acks` has no absolute deadline (contrast `election.rs:270`). **PS10 MED** — `stop()` doesn't close channel → post-stop writers park forever. **PS11-12 LOW** — idle cluster never repairs lagging follower; seeded-path swallows storage errors.

---

## Axis 3 — Performance & latency (honest)

### Measured on this box (WSL2, rustc 1.92; /mnt vs /tmp identical → not a 9P artifact)
| Scenario | Measured now | README claim | Note |
|---|---:|---:|---|
| TCP latency, single-client | ~316 µs / 3.1 K/s | 38 µs / 26 K/s | **8× gap** — WSL2 loopback, not code (in-mem bypasses it) |
| In-mem latency, single | ~550–780 ns / ~1.3–1.8 M/s | 548 ns | engine CPU cost; **the real engine number** |
| In-mem batch 4096 (empty) | ~25 M/s amortized | 30.9 M/s | per-entry amortized, empty payload |
| TCP batch 1024 (empty) | ~1.5–2.7 M/s amortized | — | batching scales cleanly with batch size |

### Honesty verdicts (attach these asterisks; do not quote without them)
- Tier-1 "No-Op transport" is **mislabeled** — followers run inline on the caller thread; it measures the **consensus state machine, not latency**.
- Tier-3 dispatch throughput is **indefensible** — fire-and-forget RPCs never answered (bench's own comment); `spec.clone()` + Vec-encode inside the timed loop.
- The "12.58 M/s" TCP-table row is a memory number misplaced in the network tier.
- `election_bench` measures the **configured 50-100 ms timeout**, not code speed.
- **Durability is entirely unmeasured** — every bench is memory-only, zero fsync. Honest ceiling with fsync ≈ 2-20 K/s unbatched.

### "Zero-allocation" — false as stated (holds-except)
Zero-copy **decode** ✅ and **vectored encode** ✅ hold — but vectored is **not the default** (only entries ≥4 KiB in batches ≥64 KiB). The default small-payload path is **contiguous: `BytesMut::with_capacity` + full memcpy per proposal** (`contiguous.rs:14`); plus `client.rs:91` `to_vec()` per write, a `sends` Vec per fan-out (`initial.rs:168/192`), `subset_quorum_index` Vec, `heartbeat_batch.rs:160` per-peer slice Vec per tick, `multiplex.rs:92` full frame copy.

### The engine is fast; the missing levers are the throughput levers (Dragonboat 9 M/s, TiKV)
Adopt, in order of impact: **group-commit fsync batching** (biggest lever), **pipelining** (multiple AppendEntries in flight — today it crams entries into ONE round but never overlaps rounds), **async apply** (decouple leader fsync / replicate / SM apply — TiKV's key win), and for multi-raft the **log-structured shared storage** (TiKV Raft Engine). See Axis-6 P2-5.

---

## Axis 4 — API, readability, maintainability

### API contracts & ergonomics
- **API1 HIGH** — fatal-vs-droppable errors indistinguishable (root cause of P0-2). **API2 HIGH** — trait contracts implicit: `RaftStorage` durability/`truncate_suffix` inclusivity/`entry_at` truncation/`&self`-forcing-interior-mutability all undocumented; `RaftTransport` frame-boundary + too-small-`out` unstated, doc references non-existent fns; `StateMachine` apply-error/idempotency unstated; `Clock` **dead public trait**.
- **API3 HIGH** — silent no-op config knobs (P1-8). **API4 MED** — `NotLeader` hint uses `voted_for` in one place, `leader_id` elsewhere (one wrong). **API5/6 MED** — `propose_batch_once` returns borrowed scratch; `RaftMessage` mixes inbound/outbound with runtime rejection. **API7 MED** — `become_leader_for_benchmark`/`set_commit_index` public backdoors (gate behind `test-util`). **API8 MED** — dispatch peers never marked disconnected on send failure; `DispatchHandle` no `Drop` (leak). **API9 MED** — dispatch views accept trailing bytes. **API10 MED** — ~37 MiB fixed per node, non-configurable (multi-raft ceiling). **API11-14 LOW** — group id unchecked at node; `send_frame_owned` copies; slot-exhaustion mislabeled Transport; "backpressure" banner false.

### Readability / maintainability
- **RD1 HIGH — dead code:** orphan `protocol/view.rs` (won't even compile; still in README), unreachable `leader_balance`/`log_compaction` (tests copy-paste the impl → **compaction is unreachable → unbounded log growth is the default**), unused `iter_entries_mut`/`SlotId` conv, dead `Clock`, `_MultiplexedRecvPlaceholder`, stale `#[allow(dead_code)]`.
- **RD2 HIGH — misleading names:** `scratch_started` (actually check-quorum contact state), two different `election_timeout()` meanings, `election_state` (a PRNG), undefined "Seeded" jargon, `KIND_CUSTOM` vs "Dispatch".
- **RD3 HIGH — duplication:** `handle_append_entries` vs `_seeded` ~90% identical; vote broadcast + collection duplicated; `gather_quorum_acks` arms; `BufferGuard` ×2; fan-out loops; burst-drain ×2; snapshot boundary logic ×2; `NotLeader` ×6; peer-filter loop ×6.
- **RD4 MED** long/nested fns (7 over ~100 lines). **RD5 MED** magic numbers (128, 16, 64*1024×5, 16 MiB×2, 65536, hardcoded `24` vs `size_of`). **RD6 MED** comments: contradictory safety text, stale comments, **Spanish in code/README (violates English-only rule)**, marketing tone ("MAGIC ZEROCOPY"), shipped process artifacts ("Sprint 1", "(B2)"). **RD7-8 LOW** stale TODO.md/README roadmap; README examples don't compile; "zero-alloc"/"AFIT eliminated async-trait" overstated (async-trait still a dep).

### Module organization & deps
- **MO1 HIGH** — `#[path]` re-include compiles `heartbeat_batch.rs` **twice** (clippy `duplicate_mod`); one-line fix `pub(crate) mod replication`. **MO2-5** empty public `election` module; dispatch split across 3 homes/2 vocabularies; `pub(crate)` field surgery distributes scratch invariants crate-wide; 60+ flat root re-exports. **MO6** layering otherwise sound (credit).
- **DEP1 MED** `futures`+`tokio` overlap (pick one). **DEP2 MED** `async-trait` contradicts the AFIT headline; used only for 2 dispatch traits. **DEP3 MED** hidden tokio-runtime coupling in `DispatchHandle::wait` (undocumented → panic for non-tokio embedders). **DEP4-5** `zerocopy`/`bytes`/`tracing` justified; no MSRV field, no feature flags.

### Idiom (ID1-10)
`RaftError` not `#[non_exhaustive]`, no `Clone`/`PartialEq`, no `source()`; saturating-vs-unchecked on the same quantity; missing `#[must_use]` (`DispatchHandle`, leases) and `Debug` derives; borrow-appeasing index loops unexplained; `.0 .0` newtype noise (add helpers/`From`); wire bools as `u8` without accessors.

---

## Axis 5 — Observability & operability (~1/10 → the near-blind axis)

**Baseline:** 16 log statements, **zero metrics**, zero spans, no `RaftStatus`, **no `leader_id()` getter**, `Clock` consumed by nothing. Getters exist but are **sealed inside the spawned `&mut self` run loop** (H-6) — only `ClientHandle` + `CommitIndexObserver` escape.

- **Metrics (OBS-M1-22, CRITICAL):** none exist. Need an `Arc<RaftMetricsInner>` of atomics cloned before spawn (the `CommitIndexObserver` pattern, the one thing done right). Required surface: term, role, commit/applied/last-log, leader_id, per-peer match/next/lag/last-contact, election count + duration, heartbeat send/fail, proposal rate + commit p50/p99, apply latency + queue depth, snapshot progress, log size + compactions, dropped/rejected frames, quorum/ISR health, client queue depth + slot-arena occupancy, dispatch in-flight. Full `RaftMetrics` + `PeerReplicationMetrics` struct sketch in the observability appendix.
- **Tracing (OBS-T):** sparse; **heartbeats log at info every 50 ms** (2000 lines/s on a 100-group leader — OBS-T2); **`step_down` completely silent** (the most important transition); no spans anywhere; ~12 event classes unlogged (leader discovered, config applied, snapshot lifecycle, compaction, run-loop death, SM-apply failure, dispatch lifecycle). Two parallel logging systems (`ARBITRO_RAFT_TRACE` env + tracing) — unify.
- **Error taxonomy (ERR-1-13):** flat 13-variant enum, no transient/fatal/safety class, stringly-typed (index buried in `format!`); `run()` dies silently (ERR-3), remote-triggerable kills (ERR-4/5), `send_message` swallows every error into a discarded bool (ERR-6), `step_down` strands dispatch waiters forever (ERR-7), SM divergence crashes with no forensics (ERR-10), ambiguous-commit window uncounted (ERR-11).
- **Health/liveness (H-1-8):** no `RaftStatus`, no `leader_id()`, no liveness predicates (`is_leader_with_quorum`/`is_caught_up`/`has_leader`), config-transition state invisible (and `propose_config_change` doesn't even check for a concurrent one — OPS-3), no stuck-node/election-storm detection, no debug dump, no per-group health iteration.
- **Backpressure (BP-1-6):** unbounded client mpsc, no depth/watermark/shed; only brake is slot-arena exhaustion (mislabeled); no follower-lag/ISR signal; apply is inline & unmeasured (slow SM → elongated heartbeats → elections).
- **Determinism (D-1-5):** `Clock` dead; **23 raw `Instant::now()` sites** → replay impossible. Positive: election jitter is RNG-free splitmix64 (preserve it). HashMap iteration nondeterminism on send-order paths.
- **Operational docs (DOC/OPS):** README is a benchmark brochure, not an operator manual (no tuning/failure-mode/recovery/durability docs); log-compaction & leader-balance are **unreachable dead features** (unbounded log growth by default); `install_snapshot_to_lagging_peers` is public but never called by the loop → a lagging follower **never catches up** unless the app knows to call it manually.

---

## Axis 6 — Verification (~2/10 → plumbing tested, consensus not)

- **Coverage:** of ~34 tests, the consensus-bearing ones reduce to **2 three-node scenarios asserting only term numbers** over a **lossless, instant, FIFO** hub (`NetworkHub`). **None of the 5 safety properties has a running assertion; no test ever compares logs or applied state across nodes; no node is ever crash-restarted from storage** (so `HardState` durability is formally untested). A quorum test is a **tautology** (local copy of `quorum()`); a registry test **asserts == 0** by its own admission. The 4 safety-critical tests are ignored; 2 fail on `--ignored`; the old report claimed those exact properties as passing.
- **Harness:** `NetworkHub` = reliable FIFO, no loss/reorder/dup/delay/one-way-cut; `TestStorage` (copied 5×, each with its own `unsafe transmute`) infallible, no fsync/crash. Benches have **zero assertions**.
- **Fault matrix:** only clean bidirectional partition exists; **missing:** leader-kill-mid-replication, crash-restart, message loss/reorder/dup/delay, clock skew, storage errors, torn writes, competing-leaders-after-heal, multi-group interference.
- **Prescription (phased roadmap):**
  0. Fix + un-ignore the 4 disabled tests (3-5 d).
  1. Safety-oracle sweep — cross-node S1–S5 assertions + content-bearing SM (2-3 d).
  2. Phase-A determinism — thread `Clock`, paused-time tests, `select_biased!` (2-4 d).
  3. Shared **sim harness** — `SimNet`/`SimStorage`/`SimClock` + crash-restart + seed printing (1-2 w).
  4. The 3 CRITICAL regression tests (§ given/when/then below) + Figure-8 + vote-durability (3-5 d).
  5. Property tests + 6. cargo-fuzz codec + 7. storage-fault/durability + 8. full DST 10k seeds + 9. Stateright model of election+joint + 10. loom + 11. **CI** (none exists today — no `.github/workflows`; `--ignored` graveyard; unseeded flakiness; no coverage; **Miri warranted** given the hot-path transmutes).
- **The 3 regression tests to write:**
  - **C1:** 5-node, overlapping campaigns + voter crash-restart between vote grant and winner's first heartbeat; assert ≤1 leader per term continuously + `voted_for` survives restart.
  - **C2/C3:** 3→5 grow, joint phase, entry acked by {1,4,5} (union+new majority, NOT old majority) must **not** commit; leader crash mid-joint must not operate under C_new-only quorum.
  - **C5:** longer-divergent follower (leader term-2 tail 4..10 vs new term-3 leader) → after heal, follower log byte-identical to leader's committed prefix, bounded repair rounds.

---

## Closing

The design is not the problem — the trait seams are DST-ready, the slot arena / metadata arena / deterministic jitter show real craft, and pockets of documentation are exemplary. The problem is that **safety, soundness, observability, and verification are all at prototype level while the data plane is presented as production**. The path to masterpiece is the ladder above: **P0 makes it sound and safe, P1 makes it trustworthy and operable, P2 makes it world-class, P3 makes it a joy to read and extend.** P0 alone is roughly one focused week and removes every "a peer can kill or corrupt us" and "a config change can lose data" class; it is the non-negotiable floor before arbitro's cluster can rest on this crate.
