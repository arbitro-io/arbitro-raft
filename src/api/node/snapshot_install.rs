// Snapshot-install leader trigger + SM restore glue (B2).

use super::RaftNode;
use crate::{PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta, StateMachine};

/// Notification hook invoked from `handle_install_snapshot` when a snapshot
/// transfer completes and has been persisted via `storage.save_snapshot`.
///
/// Today the outer apply loop discovers a freshly-installed snapshot by
/// calling `storage.load_snapshot()` and comparing its meta against
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
    let Some((meta, bytes)) = node.storage.load_snapshot()? else {
        return Ok(false);
    };
    // Peer can still be caught up via AppendEntries — no snapshot needed.
    if next_index.0 > meta.last_included_index.0 {
        return Ok(false);
    }
    node.install_snapshot_once(peer, meta, &bytes).await?;
    Ok(true)
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
    let Some((meta, bytes)) = node.storage.load_snapshot()? else {
        return Ok(false);
    };
    if meta.last_included_index.0 <= node.last_applied().0 {
        return Ok(false);
    }
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
    Ok(true)
}
