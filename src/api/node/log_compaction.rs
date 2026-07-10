// Log compaction after snapshot (B3).

use super::RaftNode;
use crate::{RaftError, RaftStorage, RaftTransport, SnapshotMeta, StateMachine};

/// Take a snapshot of `sm` at the current `last_applied` boundary, persist
/// it via `storage.save_snapshot`, and truncate all log entries strictly
/// below `last_applied`.
///
/// Returns the SnapshotMeta that was saved.
///
/// Fails with `RaftError::Snapshot` if `last_applied == 0` (nothing to snapshot).
pub fn compact_up_to_last_applied<S, T, SM>(
    node: &mut RaftNode<S, T>,
    sm: &SM,
) -> Result<SnapshotMeta, RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    let last_applied = node.last_applied();
    if last_applied.0 == 0 {
        return Err(RaftError::Snapshot("cannot compact: last_applied is 0".into()));
    }

    // A snapshot already at or beyond last_applied means there is nothing left
    // to do here. This also covers the post-InstallSnapshot case where
    // last_applied points past whatever the log has ever held locally, so the
    // log/term_at path below must not be consulted.
    if let Some((existing, _bytes)) = node.storage.load_snapshot()? {
        if existing.last_included_index >= last_applied {
            return Ok(existing);
        }
    }

    // term_at consults the log_metadata arena first and only falls back to the
    // node's own pre-sized scratch_payload buffer, so it works for entries of
    // any size (unlike a fixed 8-byte scratch read).
    let last_included_term = node.term_at(last_applied)?;
    let meta = SnapshotMeta {
        last_included_index: last_applied,
        last_included_term,
    };

    let bytes = sm.snapshot()?;
    node.storage.save_snapshot(&meta, &bytes)?;

    // Don't truncate past what the slowest known peer has actually replicated.
    // maybe_install_snapshot_to_lagging_peer would be the proper remedy for a
    // peer whose next_index has fallen behind last_applied, but it is async and
    // this function is not, so instead we cap truncation at the minimum
    // match_index among such peers to avoid handing them a CorruptLog error on
    // their next AppendEntries attempt.
    let mut truncate_up_to = last_applied;
    for progress in node.peer_progress.values() {
        if progress.next_index <= last_applied && progress.match_index < truncate_up_to {
            truncate_up_to = progress.match_index;
        }
    }
    node.storage.truncate_before(truncate_up_to)?;
    Ok(meta)
}
