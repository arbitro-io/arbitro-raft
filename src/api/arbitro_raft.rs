use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::channel::{mpsc, oneshot};
use futures::{FutureExt, StreamExt};

use crate::{
    DispatchContextView, DispatchHandle, DispatchSpec, LogIndex, PeerId, RaftError, RaftNode,
    RaftStorage, RaftTransport, Role,
};

// ---------------------------------------------------------------------------
// Internal types — not exposed directly; ClientHandle is the public surface.
// ---------------------------------------------------------------------------

struct ClientProposal {
    payload: Bytes,
    tx:      oneshot::Sender<Result<LogIndex, RaftError>>,
}

struct CommitWaiter {
    index: LogIndex,
    tx:    oneshot::Sender<Result<LogIndex, RaftError>>,
}

// ---------------------------------------------------------------------------
// ClientHandle — clonable, Send + Sync handle for concurrent client_write calls
// ---------------------------------------------------------------------------

/// Clonable handle returned by [`ArbitroRaft::client_handle`].
///
/// Any number of tasks can hold a `ClientHandle` and call [`write`] concurrently.
/// The leader batches all in-flight writes naturally as they arrive from the channel.
/// Each call blocks until the entry reaches quorum.
///
/// [`write`]: ClientHandle::write
#[derive(Clone)]
pub struct ClientHandle {
    tx: mpsc::UnboundedSender<ClientProposal>,
}

impl ClientHandle {
    /// Submit `payload` and wait until it is committed by a quorum.
    /// Returns the [`LogIndex`] assigned to the entry.
    pub async fn write(&self, payload: Bytes) -> Result<LogIndex, RaftError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .unbounded_send(ClientProposal { payload, tx })
            .map_err(|_| RaftError::Transport("raft node stopped".into()))?;
        rx.await
            .map_err(|_| RaftError::Transport("raft node stopped".into()))?
    }
}

// ---------------------------------------------------------------------------
// ArbitroRaft — execution loop, batching, timers, client backpressure
// ---------------------------------------------------------------------------

pub struct ArbitroRaft<S, T> {
    node:              RaftNode<S, T>,
    stopped:           bool,
    next_election_at:  Instant,
    next_heartbeat_at: Instant,
    election_state:    u64,
    client_tx:         mpsc::UnboundedSender<ClientProposal>,
    client_rx:         mpsc::UnboundedReceiver<ClientProposal>,
    /// Scratch — payloads drained from client_rx this tick, cleared before each use.
    pending_batch:     Vec<Bytes>,
    /// Scratch — oneshot senders parallel to pending_batch, drained together.
    pending_senders:   Vec<oneshot::Sender<Result<LogIndex, RaftError>>>,
    /// Entries replicated but not yet committed; resolved as commit_index advances.
    commit_waiters:    Vec<CommitWaiter>,
}

// --- Public API --------------------------------------------------------------

impl<S, T> ArbitroRaft<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    pub fn new(node: RaftNode<S, T>) -> Self {
        let (client_tx, client_rx) = mpsc::unbounded();
        let mut raft = Self {
            election_state:    seed(node.node_id()),
            node,
            stopped:           false,
            next_election_at:  Instant::now(),
            next_heartbeat_at: Instant::now(),
            client_tx,
            client_rx,
            pending_batch:     Vec::with_capacity(4096),
            pending_senders:   Vec::with_capacity(4096),
            commit_waiters:    Vec::with_capacity(4096),
        };
        raft.reset_election_deadline();
        raft.reset_heartbeat_deadline();
        raft
    }

    #[inline] pub fn node(&self) -> &RaftNode<S, T> { &self.node }
    #[inline] pub fn node_mut(&mut self) -> &mut RaftNode<S, T> { &mut self.node }
    #[inline] pub fn role(&self) -> Role { self.node.role() }
    #[inline] pub fn commit_index(&self) -> LogIndex { self.node.commit_index() }
    #[inline] pub fn node_id(&self) -> PeerId { self.node.node_id() }
    #[inline] pub fn stop(&mut self) { self.stopped = true; }

    /// Returns a clonable [`ClientHandle`] for concurrent writes from multiple tasks.
    #[inline]
    pub fn client_handle(&self) -> ClientHandle {
        ClientHandle { tx: self.client_tx.clone() }
    }

    /// Direct single-entry propose — caller holds `&mut self` (e.g. benchmarks, tests).
    #[inline]
    pub async fn propose_once(&mut self, payload: Bytes) -> Result<LogIndex, RaftError> {
        self.node.propose_once(payload).await
    }

    /// Direct batch propose — caller holds `&mut self`.
    #[inline]
    pub async fn propose_batch_once(
        &mut self,
        payloads: Vec<Bytes>,
    ) -> Result<Vec<LogIndex>, RaftError> {
        self.node.propose_batch_once(payloads).await
    }

    #[inline]
    pub async fn campaign_once(&mut self) -> Result<bool, RaftError> {
        let elected = self.node.campaign_once().await?;
        self.reset_election_deadline();
        if elected { self.reset_heartbeat_deadline(); }
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
            ) -> Pin<Box<dyn Future<Output = Result<(), RaftError>> + Send + 'a>>
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
        if self.stopped { return Ok(false); }
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

// --- Private event-loop impl -------------------------------------------------

impl<S, T> ArbitroRaft<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    async fn run_leader_once(&mut self) -> Result<(), RaftError> {
        // 1. Drain inbound client proposals → pending_batch + pending_senders.
        //    NOTE: do NOT clear first — items may have been pushed by the idle-path select
        //    arm on the previous tick; clearing would drop their oneshot senders uncompleted.
        while let Ok(proposal) = self.client_rx.try_recv() {
            self.pending_batch.push(proposal.payload);
            self.pending_senders.push(proposal.tx);
            if self.pending_batch.len() >= self.node.config.limits.append_batch_entries { break; }
        }

        if !self.pending_batch.is_empty() {
            match self.node.replicate_batch_async(&self.pending_batch).await {
                Ok(indices) => {
                    for (idx, tx) in indices.into_iter().zip(self.pending_senders.drain(..)) {
                        self.commit_waiters.push(CommitWaiter { index: idx, tx });
                    }
                }
                Err(e) => {
                    for tx in self.pending_senders.drain(..) {
                        let _ = tx.send(Err(RaftError::NotLeader { leader_hint: None }));
                    }
                    self.pending_batch.clear();
                    return Err(e);
                }
            }
            self.pending_batch.clear();
        }

        // 2. Burst-drain available inbound frames.
        let mut processed = 0;
        while let Some(raw) = self.node.transport().recv_frame_timeout(Duration::ZERO).await? {
            let inbound = crate::decode_message_view(raw)?;
            self.node.handle_inbound(inbound).await?;
            processed += 1;
            if processed >= 128 { break; }
        }

        if processed > 0 {
            self.node.try_advance_commit_index()?;
            self.drain_commit_waiters();
            if !self.node.is_leader() { self.fail_commit_waiters(); }
            return Ok(());
        }

        // 3. Idle — wait until next heartbeat or next inbound frame.
        let now = Instant::now();
        if now >= self.next_heartbeat_at {
            self.node.send_heartbeat_once().await?;
            self.reset_heartbeat_deadline();
            return Ok(());
        }

        let timeout = self.next_heartbeat_at.saturating_duration_since(now);
        futures::select! {
            frame_result = self.node.transport().recv_frame_timeout(timeout).fuse() => {
                if let Some(raw) = frame_result? {
                    let inbound = crate::decode_message_view(raw)?;
                    self.node.handle_inbound(inbound).await?;
                    self.node.try_advance_commit_index()?;
                    self.drain_commit_waiters();
                    if !self.node.is_leader() { self.fail_commit_waiters(); }
                }
                // else: timeout, heartbeat sent on next tick
            }
            proposal = self.client_rx.next() => {
                if let Some(p) = proposal {
                    self.pending_batch.push(p.payload);
                    self.pending_senders.push(p.tx);
                }
            }
        }

        Ok(())
    }

    async fn run_follower_once(&mut self) -> Result<(), RaftError> {
        let mut processed = 0;
        while let Some(raw) = self.node.transport().recv_frame_timeout(Duration::ZERO).await? {
            let inbound = crate::decode_message_view(raw)?;
            self.node.handle_inbound(inbound).await?;
            processed += 1;
            if processed >= 128 { break; }
        }

        if processed > 0 {
            self.reset_election_deadline();
            return Ok(());
        }

        let now = Instant::now();
        let timeout = self.next_election_at.saturating_duration_since(now);
        match self.node.transport().recv_frame_timeout(timeout).await? {
            Some(raw) => {
                let inbound = crate::decode_message_view(raw)?;
                self.node.handle_inbound(inbound).await?;
                self.reset_election_deadline();
                if self.node.is_leader() { self.reset_heartbeat_deadline(); }
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
                Err(RaftError::NoQuorum) => { self.reset_election_deadline(); }
                Err(err) => return Err(err),
            },
        }

        Ok(())
    }

    /// Resolve all commit_waiters whose index ≤ current commit_index.
    /// Uses swap_remove for O(1) removal without allocation.
    fn drain_commit_waiters(&mut self) {
        let commit_index = self.node.commit_index();
        let mut i = 0;
        while i < self.commit_waiters.len() {
            if self.commit_waiters[i].index <= commit_index {
                let w = self.commit_waiters.swap_remove(i);
                let _ = w.tx.send(Ok(w.index));
            } else {
                i += 1;
            }
        }
    }

    /// Fail all pending commit_waiters — called on step-down.
    fn fail_commit_waiters(&mut self) {
        for w in self.commit_waiters.drain(..) {
            let _ = w.tx.send(Err(RaftError::NotLeader { leader_hint: None }));
        }
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
        if max_ms == min_ms { return Duration::from_millis(min_ms); }
        self.election_state = mix64(self.election_state);
        let span   = max_ms - min_ms + 1;
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
