// Snapshot-install leader trigger + SM restore glue (B2).

use super::RaftNode;
use crate::{LogIndex, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta, StateMachine};

/// Notification hook invoked from `handle_install_snapshot` when a snapshot
/// transfer completes and has been persisted via `storage.save_snapshot`.
///
/// Today the outer apply loop discovers a freshly-installed snapshot by
/// calling `storage.load_snapshot_meta()` and comparing it against
/// `node.last_applied()`. This function is intentionally a no-op so the
/// wiring is discoverable and future sprints can flip to an in-memory
/// dirty flag without changing `snapshot.rs`.
pub(crate) fn mark_snapshot_installed<S, T>(_node: &mut RaftNode<S, T>, _meta: &SnapshotMeta)
where
    S: RaftStorage,
    T: RaftTransport,
{
}

/// If `peer`'s `next_index` has fallen below the on-disk snapshot boundary,
/// stream the snapshot to it via `install_snapshot_once`. Otherwise no-op.
///
/// Returns `Ok(true)` when a snapshot was actually sent, `Ok(false)` when
/// the peer is not tracked, no snapshot exists on disk, or the peer can
/// still be caught up via `AppendEntries`.
pub(crate) async fn maybe_install_snapshot_to_lagging_peer<S, T>(
    node: &mut RaftNode<S, T>,
    peer: PeerId,
) -> Result<bool, RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
{
    let Some((next_index, _match_index)) = node.peer_progress(peer) else {
        return Ok(false);
    };
    // C5/P4: probe the snapshot META only for the bail checks — this helper
    // runs on heartbeat-driven escalation probes, so the common "peer does
    // not need a snapshot" outcome must not pay an O(snapshot) payload read.
    let Some(meta) = node.storage.load_snapshot_meta()? else {
        return Ok(false);
    };
    // Peer can still be caught up via AppendEntries — no snapshot needed.
    if next_index.0 > meta.last_included_index.0 {
        return Ok(false);
    }

    // PS7 per-peer attempt cap: a follower that keeps failing (or gaming)
    // installs must not drive an unbounded re-stream loop. After
    // `limits.snapshot_max_attempts_per_peer` consecutive failures the peer
    // is refused for `limits.snapshot_attempt_cooldown_ms`, then the counter
    // resets and installs are allowed again.
    let max_attempts = node.config.limits.snapshot_max_attempts_per_peer.max(1);
    let cooldown =
        std::time::Duration::from_millis(node.config.limits.snapshot_attempt_cooldown_ms);
    let now = std::time::Instant::now(); // F3
    {
        let state = node.snapshot_attempts.entry(peer).or_default();
        if let Some(until) = state.cooldown_until {
            if now < until {
                node.metrics.inc_snapshot_installs_refused();
                tracing::debug!(
                    node_id = node.config.node_id.0,
                    peer = peer.0,
                    "refusing snapshot install: peer in attempt-cap cooldown"
                );
                return Ok(false);
            }
            // Cooldown elapsed — forgive and start a fresh attempt budget.
            state.attempts = 0;
            state.cooldown_until = None;
        }
        if state.attempts >= max_attempts {
            state.cooldown_until = Some(now + cooldown);
            node.metrics.inc_snapshot_installs_refused();
            tracing::warn!(
                node_id = node.config.node_id.0,
                peer = peer.0,
                attempts = state.attempts,
                cooldown_ms = node.config.limits.snapshot_attempt_cooldown_ms,
                "snapshot install attempt cap reached; backing off peer"
            );
            return Ok(false);
        }
        // Count the attempt up front; a success below clears the entry.
        state.attempts += 1;
    }

    // C5/P4: an install is actually going to be attempted — only NOW pay the
    // full snapshot payload read. The run loop is single-threaded, so the
    // snapshot cannot change between the meta probe above and this load.
    let Some((meta, bytes)) = node.storage.load_snapshot()? else {
        return Ok(false);
    };

    let boundary = meta.last_included_index;
    match node.install_snapshot_once(peer, meta, &bytes).await {
        Ok(()) => {
            // Successful install — reset the peer's attempt budget.
            node.snapshot_attempts.remove(&peer);
            // Raft §7 hand-off (C3): the follower's state now matches the
            // leader through the snapshot boundary, so re-anchor its progress
            // there. Without this the leader keeps probing compacted indexes
            // below the boundary — the probe read fails, the C3 trigger fires
            // again, and the same snapshot is re-streamed until the C4
            // attempt cap kicks in. With it, the next heartbeat tick resumes
            // plain AppendEntries at `boundary + 1` (A9 takes over the tail),
            // and the advanced `match_index` is what un-clamps C2's
            // conservative compaction horizon. Both values are capped by the
            // leader's own last log (same rationale as the `next_index_cap`
            // clamp in `replication/handler.rs`) so a snapshot whose boundary
            // is ahead of the log can never inflate progress past the tip.
            let last_log = node.cached_last_log.0;
            let matched = LogIndex(boundary.0.min(last_log.0));
            let next = LogIndex(
                boundary
                    .0
                    .saturating_add(1)
                    .min(last_log.0.saturating_add(1)),
            );
            if let Some(progress) = node.peer_progress.get_mut(&peer) {
                if matched > progress.match_index {
                    progress.match_index = matched;
                }
                if next > progress.next_index {
                    progress.next_index = next;
                }
            }
            Ok(true)
        }
        Err(e) => Err(e),
    }
}

/// If the on-disk snapshot is more recent than the state machine's
/// `last_applied`, restore the state machine from it and jump
/// `last_applied` to the snapshot boundary.
///
/// Idempotent: after the first successful restore, subsequent calls with
/// the same on-disk snapshot are no-ops because
/// `meta.last_included_index <= node.last_applied()`.
///
/// Returns `true` iff a restore actually happened.
pub(crate) fn restore_state_machine_from_snapshot<S, T, SM>(
    node: &mut RaftNode<S, T>,
    sm: &mut SM,
) -> Result<bool, RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    // C5/P4: probe the snapshot META only for the idempotence bail. This
    // helper runs at the top of EVERY apply batch, and once any snapshot
    // exists the overwhelmingly common steady-state outcome is "already
    // applied" — which must not pay an O(snapshot) payload read per batch.
    let Some(meta) = node.storage.load_snapshot_meta()? else {
        return Ok(false);
    };
    if meta.last_included_index.0 <= node.last_applied().0 {
        return Ok(false);
    }
    // A restore is actually warranted — only NOW load the full snapshot
    // bytes. Single-threaded run loop: the snapshot cannot change between
    // the meta probe and this load.
    let Some((meta, bytes)) = node.storage.load_snapshot()? else {
        return Ok(false);
    };
    sm.restore(&bytes)?;
    // `set_last_applied` debug-asserts `idx <= commit_index`; the
    // snapshot-install handler already advanced commit_index, but a
    // caller invoking this helper out-of-band may not have. Bump
    // commit_index first to preserve the protocol invariant
    // `last_applied <= commit_index`.
    if node.commit_index().0 < meta.last_included_index.0 {
        node.set_commit_index(meta.last_included_index);
    }
    node.set_last_applied(meta.last_included_index);

    // Same discard rule as the in-band snapshot handler: any local entry that
    // disagrees with the snapshot boundary is from a divergent branch and
    // must go, otherwise the prefix is safe to drop. Without this the
    // restarted follower keeps a stale log that AppendEntries §7 rejects.
    let last_idx = meta.last_included_index;
    let last_term = meta.last_included_term;
    // B10: term-only probe — no payload read, no undersized-buffer reliance.
    let boundary_conflict = node
        .storage
        .term_at(last_idx)?
        .map(|t| t != last_term)
        .unwrap_or(false);
    if boundary_conflict {
        node.storage.truncate_suffix(LogIndex(1))?;
    } else {
        node.storage
            .truncate_before(LogIndex(last_idx.0.saturating_add(1)))?;
    }
    node.log_metadata
        .clear(LogIndex(last_idx.0.saturating_add(1)));
    node.cached_last_log = (last_idx, last_term);
    // §7: the boundary entry was just discarded from the log — remember its
    // (index, term) so prev_log/term_at reads at the boundary keep working.
    node.snapshot_boundary = (last_idx, last_term);
    // The snapshot replaced everything applied so far — the compaction debt
    // it represented is settled (C2).
    node.compaction_debt_entries = 0;
    node.compaction_debt_bytes = 0;
    Ok(true)
}
