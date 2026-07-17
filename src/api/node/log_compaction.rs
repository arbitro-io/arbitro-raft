// Log compaction wired into the run loop (C2; formerly the dead B3 helper).
//
// The run loop calls [`maybe_compact`] after every apply batch (leader AND
// follower — both accumulate log). When the applied-since-last-snapshot debt
// crosses the configured threshold, the state machine is snapshotted, the
// snapshot is persisted, and the log prefix is truncated up to a
// CONSERVATIVE horizon that can never strand a live voter.

use super::RaftNode;
use crate::{LogIndex, RaftError, RaftStorage, RaftTransport, SnapshotMeta, StateMachine};

/// Policy trigger + compaction step (C2).
///
/// Fires when either `limits.compaction_threshold_entries` or
/// `limits.compaction_threshold_bytes` of applied-entry debt has accumulated
/// since the last snapshot (a `0` threshold disables that trigger). On fire:
///
/// 1. `state_machine.snapshot()` at the `last_applied` boundary,
/// 2. `storage.save_snapshot(meta, bytes)`,
/// 3. `storage.truncate_before(up_to)` where `up_to` is the conservative
///    compaction horizon (see below).
///
/// # The conservative horizon (never strand a live follower)
///
/// C3 (leader-driven snapshot catch-up for lagging peers) is NOT wired yet,
/// so a follower that falls behind the log start could never recover unless
/// the application manually streams it a snapshot. Until C3 lands, the
/// horizon only discards entries that EVERY current voter already has AND
/// that are applied locally:
///
/// ```text
/// up_to = min(last_applied, min over current voters of match_index)
///         − limits.compaction_min_retain
/// ```
///
/// * `last_applied <= commit_index` always (protocol invariant), so this can
///   never compact past `commit_index`.
/// * On a leader, a permanently-lagging follower clamps the horizon and
///   therefore BLOCKS further truncation — that is CORRECT and safe for now.
///   C3 (snapshot catch-up) is what will let such a follower advance again
///   so compaction can proceed; snapshots keep being taken meanwhile, so the
///   moment C3 repairs the peer the very next threshold crossing reclaims
///   the space.
/// * On a follower there is no `peer_progress`; it compacts strictly against
///   its own applied state (`up_to <= last_applied <= commit_index`), and the
///   `compaction_min_retain` margin keeps a tail the leader can still probe
///   with plain `AppendEntries`.
/// * The entry AT the snapshot boundary is always retained (`truncate_before`
///   removes strictly-below `up_to <= last_applied`), so `term_at` and the
///   leader's `prev_log` checks keep working across the boundary.
///
/// # In-flight install guard (C4 hand-off)
///
/// While `pending_snapshots` is non-empty an inbound snapshot transfer is in
/// flight whose boundary supersedes anything this node would compact (a
/// transfer only ever targets a node that is BEHIND the sender's boundary).
/// Compacting mid-transfer would race the install's own truncation, so the
/// whole step is skipped; C4's timer sweep evicts a stalled transfer and the
/// next apply tick compacts normally. Leader-side outbound installs are
/// synchronous (`install_snapshot_once` completes before the run loop
/// ticks), and the voter `match_index` clamp already guarantees no entry a
/// lagging install-target still needs is ever discarded.
///
/// Returns the persisted [`SnapshotMeta`] when a compaction ran, `None` when
/// the trigger did not fire or a guard deferred it. The applied-debt
/// counters reset only on a successful snapshot save.
pub(crate) fn maybe_compact<S, T, SM>(
    node: &mut RaftNode<S, T>,
    sm: &SM,
) -> Result<Option<SnapshotMeta>, RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    let limits = &node.config.limits;
    let by_entries = limits.compaction_threshold_entries != 0
        && node.compaction_debt_entries >= limits.compaction_threshold_entries;
    let by_bytes = limits.compaction_threshold_bytes != 0
        && node.compaction_debt_bytes >= limits.compaction_threshold_bytes;
    if !by_entries && !by_bytes {
        return Ok(None);
    }

    // C4 guard: never compact while an inbound snapshot transfer is pending.
    if !node.pending_snapshots.is_empty() {
        return Ok(None);
    }

    let last_applied = node.last_applied();
    if last_applied.0 == 0 {
        return Ok(None);
    }

    // A snapshot already at or beyond last_applied means there is nothing new
    // to snapshot (post-InstallSnapshot case); settle the debt and bail. This
    // also keeps term_at below safe: it is only consulted when the log still
    // holds entries above the on-disk snapshot boundary.
    // C5/P4: meta-only probe — the existence/boundary check must not read
    // the full snapshot payload.
    if let Some(existing) = node.storage.load_snapshot_meta()? {
        if existing.last_included_index >= last_applied {
            node.compaction_debt_entries = 0;
            node.compaction_debt_bytes = 0;
            return Ok(None);
        }
    }

    // Conservative horizon — see the doc comment. min over current voters'
    // match_index applies on the leader only; `config.peers` holds the joint
    // union during a §4.3 membership transition, so BOTH voter sets clamp.
    let mut horizon = last_applied;
    if node.is_leader() {
        let self_id = node.config.node_id;
        for &peer in &node.config.peers {
            if peer == self_id {
                continue;
            }
            let matched = node
                .peer_progress
                .get(&peer)
                .map(|p| p.match_index)
                .unwrap_or(LogIndex(0));
            if matched < horizon {
                horizon = matched;
            }
        }
    }
    let up_to = LogIndex(horizon.0.saturating_sub(limits.compaction_min_retain));

    compact_with_horizon(node, sm, up_to).map(Some)
}

/// The compaction step itself: snapshot the state machine at the current
/// `last_applied` boundary, persist it via `storage.save_snapshot`, and
/// truncate all log entries strictly below `up_to`.
///
/// Callers must ensure `up_to <= last_applied` (the [`maybe_compact`]
/// horizon guarantees it). A `up_to` of 0 or 1 persists the snapshot but
/// leaves the log untouched — used when a lagging voter blocks truncation.
///
/// Returns the `SnapshotMeta` that was saved.
pub(crate) fn compact_with_horizon<S, T, SM>(
    node: &mut RaftNode<S, T>,
    sm: &SM,
    up_to: LogIndex,
) -> Result<SnapshotMeta, RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    let last_applied = node.last_applied();
    debug_assert!(
        up_to <= last_applied,
        "compaction horizon ({}) must not exceed last_applied ({})",
        up_to.0,
        last_applied.0,
    );

    // term_at consults the log_metadata arena first and only falls back to
    // the node's own pre-sized scratch_payload buffer, so it works for
    // entries of any size.
    let last_included_term = node.term_at(last_applied)?;
    let meta = SnapshotMeta {
        last_included_index: last_applied,
        last_included_term,
    };

    let bytes = sm.snapshot()?;
    node.storage.save_snapshot(&meta, &bytes)?;
    // §7: remember the boundary so its term stays answerable once the prefix
    // (and possibly the boundary entry itself) leaves the log (C3).
    node.snapshot_boundary = (meta.last_included_index, meta.last_included_term);
    // Debt is settled by the snapshot (the SM state through last_applied is
    // durable), even when a lagging voter clamps truncation to a no-op.
    node.compaction_debt_entries = 0;
    node.compaction_debt_bytes = 0;

    if up_to.0 > 0 {
        node.storage.truncate_before(up_to)?;
    }
    node.metrics.inc_log_compactions();
    tracing::debug!(
        node_id = node.config.node_id.0,
        snapshot_index = meta.last_included_index.0,
        truncated_below = up_to.0,
        "log compaction completed"
    );
    Ok(meta)
}
