use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::channel::mpsc;

use crate::{
    DispatchContextView, DispatchHandle, DispatchSpec, LogIndex, PeerId, RaftError, RaftNode,
    RaftStorage, RaftTransport, Role,
};

mod client;
mod run;
mod slot;
mod timers;

use client::{ClientProposal, CommitWaiter};
use slot::{SlotId, SlotRegistry};
use timers::seed;

// Re-export the public types from this module.
pub use client::ClientHandle;

// ---------------------------------------------------------------------------
// ArbitroRaft — execution loop, batching, timers, client backpressure.
// ---------------------------------------------------------------------------

pub struct ArbitroRaft<S, T> {
    pub(crate) node: RaftNode<S, T>,
    stopped: bool,
    pub(crate) next_election_at: Instant,
    pub(crate) next_heartbeat_at: Instant,
    pub(crate) election_state: u64,
    client_tx: mpsc::UnboundedSender<ClientProposal>,
    pub(crate) client_rx: mpsc::UnboundedReceiver<ClientProposal>,
    /// Shared registry for commit notifications.
    pub(crate) registry: Arc<SlotRegistry>,
    /// Scratch — payloads drained from client_rx this tick, cleared before each use.
    pub(crate) pending_batch: Vec<Vec<u8>>,
    /// Scratch — slots parallel to pending_batch, drained together.
    pub(crate) pending_slots: Vec<SlotId>,
    /// Entries replicated but not yet committed; resolved as commit_index advances.
    pub(crate) commit_waiters: Vec<CommitWaiter>,
    /// Long-lived inbound buffer to avoid per-frame allocations.
    pub(crate) inbound_buf: Box<[u8]>,
}

// --- Public API --------------------------------------------------------------

impl<S, T> ArbitroRaft<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    pub fn new(node: RaftNode<S, T>) -> Self {
        let (client_tx, client_rx) = mpsc::unbounded();
        // 65k slots = 4MB RAM — absorbs extreme concurrent bursts without backpressure.
        let registry_cap = 65536;
        let mut raft = Self {
            election_state: seed(node.node_id()),
            node,
            stopped: false,
            next_election_at: Instant::now(),
            next_heartbeat_at: Instant::now(),
            client_tx,
            client_rx,
            registry: SlotRegistry::new(registry_cap),
            pending_batch: Vec::with_capacity(4096),
            pending_slots: Vec::with_capacity(4096),
            commit_waiters: Vec::with_capacity(4096),
            inbound_buf: vec![0u8; 64 * 1024].into_boxed_slice(),
        };
        raft.reset_election_deadline();
        raft.reset_heartbeat_deadline();
        raft
    }

    #[inline]
    pub fn node(&self) -> &RaftNode<S, T> {
        &self.node
    }
    #[inline]
    pub fn node_mut(&mut self) -> &mut RaftNode<S, T> {
        &mut self.node
    }
    #[inline]
    pub fn role(&self) -> Role {
        self.node.role()
    }
    #[inline]
    pub fn commit_index(&self) -> LogIndex {
        self.node.commit_index()
    }

    /// Cheaply-clonable read-only observer over the committed log index.
    ///
    /// Clone this before moving `self` into a background task so external
    /// consumers (for example an apply loop that lives in another task)
    /// can safely poll the current commit boundary without holding the
    /// node.
    #[inline]
    pub fn commit_index_observer(&self) -> crate::CommitIndexObserver {
        self.node.commit_index_observer()
    }
    #[inline]
    pub fn node_id(&self) -> PeerId {
        self.node.node_id()
    }
    #[inline]
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    /// Returns a clonable [`ClientHandle`] for concurrent writes from multiple tasks.
    #[inline]
    pub fn client_handle(&self) -> ClientHandle {
        ClientHandle {
            tx: self.client_tx.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Direct single-entry propose — caller holds `&mut self` (e.g. benchmarks, tests).
    #[inline]
    pub async fn propose_once(&mut self, payload: &[u8]) -> Result<LogIndex, RaftError> {
        self.node.propose_once(payload).await
    }

    /// Direct batch propose — caller holds `&mut self`.
    #[inline]
    pub async fn propose_batch_once(
        &mut self,
        payloads: &[&[u8]],
    ) -> Result<&[LogIndex], RaftError> {
        self.node.propose_batch_once(payloads).await
    }

    #[inline]
    pub async fn campaign_once(&mut self) -> Result<bool, RaftError> {
        let elected = self.node.campaign_once(&mut self.inbound_buf).await?;
        self.reset_election_deadline();
        if elected {
            self.reset_heartbeat_deadline();
        }
        Ok(elected)
    }

    #[inline]
    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        let result = self.node.send_heartbeat_once().await;
        self.reset_heartbeat_deadline();
        result
    }

    #[inline]
    pub fn on_with<P, R, F>(&self, spec: DispatchSpec<P, R>, handler: F) -> Result<(), RaftError>
    where
        P: Send + 'static,
        R: 'static,
        F: for<'a> Fn(
                P,
                DispatchContextView<'a>,
            )
                -> Pin<Box<dyn Future<Output = Result<(), RaftError>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        self.node.on_with(spec, handler)
    }

    #[inline]
    pub async fn dispatch<P, R>(
        &mut self,
        spec: DispatchSpec<P, R>,
        params: P,
    ) -> Result<DispatchHandle<R>, RaftError>
    where
        P: Send + 'static,
        R: Clone + Send + 'static,
    {
        self.node.dispatch(spec, params).await
    }

    pub async fn run_once(&mut self) -> Result<bool, RaftError> {
        if self.stopped {
            return Ok(false);
        }
        if self.node.is_leader() {
            self.run_leader_once().await?;
        } else {
            self.run_follower_once().await?;
        }
        Ok(!self.stopped)
    }

    pub async fn run(&mut self) -> Result<(), RaftError> {
        while self.run_once().await? {}
        Ok(())
    }
}
