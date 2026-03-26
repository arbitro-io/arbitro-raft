use crate::RaftError;

pub trait StateMachine: Send + Sync + 'static {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError>;
    fn snapshot(&self) -> Result<Vec<u8>, RaftError>;
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError>;
}
