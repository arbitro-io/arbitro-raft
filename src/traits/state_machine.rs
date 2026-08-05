use crate::{LogIndex, RaftError};

/// User state machine the engine applies committed log entries to.
///
/// # Apply-cursor volatility and restart idempotency (A6)
///
/// The engine's applied cursor (`RaftNode::last_applied`) is **volatile**: it
/// is reset to `LogIndex(0)` on every process restart (see the `last_applied`
/// field and its initialization in `RaftNode::new`, `src/api/node/mod.rs`),
/// after which committed entries are re-applied from the last snapshot
/// boundary (or from the start of the log when no snapshot exists). A state
/// machine may therefore observe the SAME committed entry again after a
/// crash-restart — apply is **at-least-once per index**, never exactly-once.
///
/// * A state machine whose only persistence is the engine-driven
///   [`snapshot`](StateMachine::snapshot) / [`restore`](StateMachine::restore)
///   cycle needs no dedupe: replay after `restore` reconstructs exactly the
///   pre-crash state. Implement [`apply`](StateMachine::apply) and ignore
///   indexes.
/// * A state machine that persists its effects EXTERNALLY (its own database,
///   files, or any side effect that survives the process) MUST override
///   [`apply_at`](StateMachine::apply_at), durably record the `index` it
///   received as its own applied cursor (atomically with the applied effect),
///   and treat a later `apply_at` call with `index <=` its durable cursor as
///   an idempotent no-op. Relying on the engine's `last_applied` for this is
///   incorrect — it does not survive restarts.
pub trait StateMachine: Send + Sync + 'static {
    /// Apply a committed entry's payload. See the trait docs for the
    /// at-least-once/restart contract. Implementations that need the entry's
    /// log index must override [`apply_at`](StateMachine::apply_at) instead.
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError>;

    /// Apply a committed entry's payload together with its committed
    /// [`LogIndex`] (A6). This is the method the engine actually invokes for
    /// every committed entry, in strict log order.
    ///
    /// The default implementation forwards to [`apply`](StateMachine::apply),
    /// dropping the index — existing implementations keep today's behavior
    /// without any change.
    ///
    /// # Idempotency contract
    ///
    /// Because the engine's applied cursor is volatile (reset to 0 on
    /// restart — see the trait docs), `apply_at` may be called again for an
    /// index that was already applied before a crash. Externally-persistent
    /// state machines MUST use `index` to persist their own applied cursor
    /// and deduplicate such replays; see the trait docs for the full rule.
    fn apply_at(&mut self, index: LogIndex, entry: &[u8]) -> Result<(), RaftError> {
        let _ = index;
        self.apply(entry)
    }

    fn snapshot(&self) -> Result<Vec<u8>, RaftError>;
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError>;
}

/// Trivial StateMachine that ignores everything.
///
/// Useful for tests, benches, and any Raft group that only needs the
/// replicated log without state-machine semantics on top.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopStateMachine;

impl StateMachine for NoopStateMachine {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
}
