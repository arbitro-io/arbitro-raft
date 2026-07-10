// Log compaction after snapshot (B3).

use super::RaftNode;
use crate::{LogIndex, RaftError, RaftStorage, RaftTransport, SnapshotMeta, StateMachine};

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
    // Snapshot semantics: the term at last_applied must be known.
    let mut scratch = [0u8; 8];
    let entry = node
        .read_entry_payload_into(last_applied, &mut scratch)?
        .ok_or_else(|| {
            RaftError::Snapshot(format!(
                "cannot compact: log missing entry at last_applied={}",
                last_applied.0,
            ))
        })?;
    let last_included_term = entry.term;
    let meta = SnapshotMeta {
        last_included_index: last_applied,
        last_included_term,
    };

    let bytes = sm.snapshot()?;
    node.storage.save_snapshot(&meta, &bytes)?;
    node.storage.truncate_before(last_applied)?;
    Ok(meta)
}
