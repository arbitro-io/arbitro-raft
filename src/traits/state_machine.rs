use crate::RaftError;

pub trait StateMachine: Send + Sync + 'static {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError>;
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
