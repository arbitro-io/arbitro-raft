use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::{
    DispatchContextView, DispatchHandle, DispatchSpec, LogIndex, PeerId, RaftError, RaftNode,
    RaftStorage, RaftTransport, Role,
};

pub struct ArbitroRaft<S, T> {
    node: RaftNode<S, T>,
    stopped: bool,
    next_election_at: Instant,
    next_heartbeat_at: Instant,
    election_state: u64,
    proposal_tx: futures::channel::mpsc::UnboundedSender<Bytes>,
    proposal_rx: futures::channel::mpsc::UnboundedReceiver<Bytes>,
    pending_batch: Vec<Bytes>,
}

impl<S, T> ArbitroRaft<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    pub fn new(node: RaftNode<S, T>) -> Self {
        let (proposal_tx, proposal_rx) = futures::channel::mpsc::unbounded();
        let mut raft = Self {
            election_state: seed(node.node_id()),
            node,
            stopped: false,
            next_election_at: Instant::now(),
            next_heartbeat_at: Instant::now(),
            proposal_tx,
            proposal_rx,
            pending_batch: Vec::with_capacity(4096),
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
    pub fn node_id(&self) -> PeerId {
        self.node.node_id()
    }

    #[inline]
    pub fn handle(&self) -> futures::channel::mpsc::UnboundedSender<Bytes> {
        self.proposal_tx.clone()
    }

    #[inline]
    pub fn stop(&mut self) {
        self.stopped = true;
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

    #[inline]
    pub async fn campaign_once(&mut self) -> Result<bool, RaftError> {
        let elected = self.node.campaign_once().await?;
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
    pub async fn propose_once(&mut self, payload: Bytes) -> Result<LogIndex, RaftError> {
        self.node.propose_once(payload).await
    }

    #[inline]
    pub async fn propose_batch_once(
        &mut self,
        payloads: Vec<Bytes>,
    ) -> Result<Vec<LogIndex>, RaftError> {
        self.node.propose_batch_once(payloads).await
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

    async fn run_leader_once(&mut self) -> Result<(), RaftError> {
        self.pending_batch.clear();
        while let Ok(payload) = self.proposal_rx.try_recv() {
            self.pending_batch.push(payload);
            // Bound the batch to avoid starving the heartbeat loop
            if self.pending_batch.len() >= 4096 { break; }
        }

        if !self.pending_batch.is_empty() {
            self.node.replicate_batch_async(&self.pending_batch).await?;
            self.pending_batch.clear();
        }

        let now = Instant::now();
        if now >= self.next_heartbeat_at {
            self.node.send_heartbeat_once().await?;
            self.reset_heartbeat_deadline();
            return Ok(());
        }

        let timeout = self.next_heartbeat_at.saturating_duration_since(now);
        match self.node.transport().recv_frame_timeout(timeout).await? {
            Some(raw) => {
                let inbound = crate::decode_message_view(raw)?;
                self.node.handle_inbound(inbound).await?;
            }
            None => {
                self.node.send_heartbeat_once().await?;
                self.reset_heartbeat_deadline();
            }
        }

        Ok(())
    }

    async fn run_follower_once(&mut self) -> Result<(), RaftError> {
        let now = Instant::now();
        let timeout = self.next_election_at.saturating_duration_since(now);
        match self.node.transport().recv_frame_timeout(timeout).await? {
            Some(raw) => {
                let inbound = crate::decode_message_view(raw)?;
                self.node.handle_inbound(inbound).await?;
                self.reset_election_deadline();
                if self.node.is_leader() {
                    self.reset_heartbeat_deadline();
                }
            }
            None => match self.node.campaign_once().await {
                Ok(elected) => {
                    self.reset_election_deadline();
                    if elected {
                        self.reset_heartbeat_deadline();
                        self.node.send_heartbeat_once().await?;
                        self.reset_heartbeat_deadline();
                    }
                }
                Err(RaftError::NoQuorum) => {
                    self.reset_election_deadline();
                }
                Err(err) => return Err(err),
            },
        }

        Ok(())
    }

    fn reset_heartbeat_deadline(&mut self) {
        self.next_heartbeat_at = Instant::now() + self.heartbeat_interval();
    }

    fn reset_election_deadline(&mut self) {
        self.next_election_at = Instant::now() + self.next_election_timeout();
    }

    fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.node.timing().heartbeat_ms.max(1))
    }

    fn next_election_timeout(&mut self) -> Duration {
        let timing = self.node.timing();
        let min_ms = timing.election_min_ms.max(1);
        let max_ms = timing.election_max_ms.max(min_ms);
        if max_ms == min_ms {
            return Duration::from_millis(min_ms);
        }

        self.election_state = mix64(self.election_state);
        let span = max_ms - min_ms + 1;
        let jitter = self.election_state % span;
        Duration::from_millis(min_ms + jitter)
    }
}

#[inline]
fn seed(node_id: PeerId) -> u64 {
    mix64(node_id.0.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
