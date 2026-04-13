---
trigger: always_on
---

# TESTING & BENCHMARK RULES

---

## BUILD & TEST

All compilation and testing happens in WSL:

```bash
wsl bash -lc "cd /mnt/d/.../arbitro-raft && cargo test --workspace 2>&1"
wsl bash -lc "cd /mnt/d/.../arbitro-raft && cargo clippy --workspace 2>&1"
```

Build and run **in place** from `/mnt/d/...`. **DO NOT** copy/sync/mirror the
source tree into `/tmp` for builds — Cargo handles incremental compilation,
re-copying the tree every run is wasted I/O and produces duplicate targets.

### ABSOLUTELY FORBIDDEN

- **NEVER** use `rsync` for ANY purpose in this project. Not for mirroring
  the source tree, not for copying artifacts, not for anything. If you think
  you need `rsync`, you don't — you need to `cd` into the real path and run
  `cargo` there. This rule is inviolable.
- **NEVER** copy the whole source tree into `/tmp`. Only **compiled binaries**
  may be copied into `/tmp` (see bench rules below), and only with `cp`.

---

## BENCHMARK EXECUTION — MANDATORY SAFETY RULES

1. **WSL only** — never run benchmarks on Windows directly
2. **Compile first, run separately:**
   ```bash
   wsl bash -lc "cd /mnt/d/.../arbitro-raft && cargo bench --bench <name> --no-run 2>&1"
   ```
3. **Copy to /tmp before running:**
   ```bash
   wsl bash -lc "mkdir -p /tmp/arbitro-raft && cp -a target/<bench>-* /tmp/arbitro-raft/"
   ```
4. **Run with timeout + tee:**
   ```bash
   wsl bash -lc "cd /tmp/arbitro-raft && timeout 120 ./<bench>-* --bench '<pattern>' 2>&1 | tee /tmp/bench.log"
   ```
5. **One bench at a time** — never run multiple benchmarks concurrently
6. **Foreground only** — never run benchmarks in background
7. **Max 1000 messages** for smoke tests — never use 1M in ad-hoc testing

---

## BENCHMARK LOCATION

| Location | Content |
|---|---|
| `benches/` | Criterion benchmarks only |
| `examples/` | Runnable usage examples |
| `tests/` | Integration tests (public API only) |
| `src/` | `#[cfg(test)]` unit tests at module bottom |

---

## TEST STRUCTURE

### Unit tests — per module, in isolation

Each module has `#[cfg(test)] mod tests` at the bottom testing:
- Happy path
- Edge cases (empty, single, boundary)
- `size_of` assertions for hot structs
- Generation/ABA protection for slabs

### Integration tests — in `tests/`

- `publish_ack_cycle` — publish N, deliver, ack all, verify counters zero
- `credit_exhaustion` — publish when credits exhausted, verify backpressure
- `disconnect_cleanup` — connect, subscribe, publish, disconnect, verify edges clean
- `idempotency_window` — publish same key twice, second rejected
- `config_replay` — create streams/consumers, shutdown, replay, verify state
- `nack_redelivery` — deliver, nack, verify redelivery
- `deadline_expiry` — deliver, wait ack_wait, verify timeout

### Benchmarks — in `benches/`

Each benchmark states what it measures in its module doc:

- `publish_batch` — ns/entry for 1/10/100/1000 entries
- `ack_release` — ns/ack for single and batch
- `match_table` — ns/match for 1/10/100 consumers
- `slab_operations` — ns/op for insert/get/remove
- `idempotency` — ns/check at 0%/50%/100% duplicate rate