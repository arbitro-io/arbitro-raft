use std::time::Duration;

use async_trait::async_trait;

use crate::{InboundRaftMessageView, PeerId, RaftError, RaftMessage};

#[async_trait]
pub trait RaftTransport: Send + Sync {
    async fn send(&self, peer: PeerId, msg: RaftMessage) -> Result<(), RaftError>;
    async fn recv(&self) -> Result<InboundRaftMessageView, RaftError>;
    async fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<InboundRaftMessageView>, RaftError>;
}
