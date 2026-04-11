use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::channel::mpsc;
use futures::task::AtomicWaker;
use futures::{FutureExt, StreamExt};

use crate::{
    DispatchContextView, DispatchHandle, DispatchSpec, LogIndex, PeerId, RaftError, RaftNode,
    RaftStorage, RaftTransport, Role,
};

// ---------------------------------------------------------------------------
// Slot — lock-free single-use notification primitive.
//
// Replaces oneshot::channel for commit notifications. The raft task stores
// the committed LogIndex via an atomic and wakes the waiting client task.
// No mutex, no Arc<Mutex<Option<T>>> — just two word-sized fields.
//
// Sentinel values:
//   SLOT_PENDING    (0)        — not yet committed
//   SLOT_NOT_LEADER (u64::MAX) — leader stepped down before commit
//   any other value            — committed LogIndex
// ---------------------------------------------------------------------------

const SLOT_PENDING: u64 = 0;
const SLOT_NOT_LEADER: u64 = u64::MAX;

#[repr(C, align(64))]
struct Slot {
    state: AtomicU64,
    waker: AtomicWaker,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            state: AtomicU64::new(SLOT_PENDING),
            waker: AtomicWaker::new(),
        }
    }
}

impl Slot {
    #[inline]
    fn notify_committed(&self, index: LogIndex) {
        // Non-zero state marks it as "not pending" (free for lease after waker is done)
        self.state.store(index.0, Ordering::Release);
        self.waker.wake();
    }

    #[inline]
    fn notify_error(&self) {
        self.state.store(SLOT_NOT_LEADER, Ordering::Release);
        self.waker.wake();
    }

    #[inline]
    fn decode(v: u64) -> Result<LogIndex, RaftError> {
        if v == SLOT_NOT_LEADER {
            Err(RaftError::NotLeader { leader_hint: None })
        } else {
            Ok(LogIndex(v))
        }
    }
}

/// Opaque identifier for a leased notification slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotId(u32);

pub(crate) struct SlotRegistry {
    arena: Box<[Slot]>,
    cursor: AtomicU64,
}

impl SlotRegistry {
    pub fn new(capacity: usize) -> Arc<Self> {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(Slot {
                state: AtomicU64::new(SLOT_NOT_LEADER),
                waker: AtomicWaker::new(),
            });
        }
        Arc::new(Self {
            arena: slots.into_boxed_slice(),
            cursor: AtomicU64::new(0),
        })
    }

    #[inline]
    pub fn lease(&self) -> Option<SlotId> {
        let cap = self.arena.len() as u64;
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);

        for i in 0..cap {
            let idx = ((start + i) % cap) as usize;
            let slot = &self.arena[idx];

            // A slot is free if its state is NOT SLOT_PENDING.
            // When we lease it, we atomically move it to SLOT_PENDING.
            let current = slot.state.load(Ordering::Acquire);
            if current != SLOT_PENDING {
                if slot
                    .state
                    .compare_exchange(current, SLOT_PENDING, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Some(SlotId(idx as u32));
                }
            }
        }
        None
    }

    #[inline]
    pub fn release(&self, id: SlotId) {
        // Technically, notify_committed/notify_error already moves state away from SLOT_PENDING.
        // Release is mainly for cleanup if a proposal never reaches the raft task.
        let slot = &self.arena[id.0 as usize];
        if slot.state.load(Ordering::Acquire) == SLOT_PENDING {
            slot.state.store(SLOT_NOT_LEADER, Ordering::Release);
        }
    }

    #[inline]
    fn get(&self, id: SlotId) -> &Slot {
        &self.arena[id.0 as usize]
    }
}

// ---------------------------------------------------------------------------
// WriteFuture — returned internally by ClientHandle::write.
// ---------------------------------------------------------------------------

struct WriteFuture {
    registry: Arc<SlotRegistry>,
    id: SlotId,
}

impl Future for WriteFuture {
    type Output = Result<LogIndex, RaftError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slot = self.registry.get(self.id);
        let v = slot.state.load(Ordering::Acquire);
        if v != SLOT_PENDING {
            return Poll::Ready(Slot::decode(v));
        }
        slot.waker.register(cx.waker());
        // Re-check after registration to close the register → store race.
        let v = slot.state.load(Ordering::Acquire);
        if v != SLOT_PENDING {
            return Poll::Ready(Slot::decode(v));
        }
        Poll::Pending
    }
}

impl Drop for WriteFuture {
    fn drop(&mut self) {
        self.registry.release(self.id);
    }
}

// ---------------------------------------------------------------------------
// Internal types — not exposed directly; ClientHandle is the public surface.
// ---------------------------------------------------------------------------

struct ClientProposal {
    payload: Vec<u8>,
    slot_id: SlotId,
}

struct CommitWaiter {
    index: LogIndex,
    slot_id: SlotId,
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
    registry: Arc<SlotRegistry>,
}

impl ClientHandle {
    /// Submit `payload` and wait until it is committed by a quorum.
    /// Returns the [`LogIndex`] assigned to the entry.
    pub async fn write(&self, payload: &[u8]) -> Result<LogIndex, RaftError> {
        let id = self
            .registry
            .lease()
            .ok_or_else(|| RaftError::Transport("no free notification slots".into()))?;

        self.tx
            .unbounded_send(ClientProposal {
                payload: payload.to_vec(),
                slot_id: id,
            })
            .map_err(|_| {
                self.registry.release(id);
                RaftError::Transport("raft node stopped".into())
            })?;

        WriteFuture {
            registry: self.registry.clone(),
            id,
        }
        .await
    }
}

// ---------------------------------------------------------------------------
// ArbitroRaft — execution loop, batching, timers, client backpressure
// ---------------------------------------------------------------------------

pub struct ArbitroRaft<S, T> {
    node: RaftNode<S, T>,
    stopped: bool,
    next_election_at: Instant,
    next_heartbeat_at: Instant,
    election_state: u64,
    client_tx: mpsc::UnboundedSender<ClientProposal>,
    client_rx: mpsc::UnboundedReceiver<ClientProposal>,
    /// Shared registry for commit notifications.
    registry: Arc<SlotRegistry>,
    /// Scratch — payloads drained from client_rx this tick, cleared before each use.
    pending_batch: Vec<Vec<u8>>,
    /// Scratch — slots parallel to pending_batch, drained together.
    pending_slots: Vec<SlotId>,
    /// Entries replicated but not yet committed; resolved as commit_index advances.
    commit_waiters: Vec<CommitWaiter>,
    /// Long-lived inbound buffer to avoid per-frame allocations.
    inbound_buf: Box<[u8]>,
}

// --- Public API --------------------------------------------------------------

impl<S, T> ArbitroRaft<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    pub fn new(node: RaftNode<S, T>) -> Self {
        let (client_tx, client_rx) = mpsc::unbounded();
        let registry_cap = node.config.limits.append_batch_entries * 2;
        let mut raft = Self {
            election_state: seed(node.node_id()),
            node,
            stopped: false,
            next_election_at: Instant::now(),
            next_heartbeat_at: Instant::now(),
            client_tx,
            client_rx,
            registry: SlotRegistry::new(registry_cap.max(4096)),
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

// --- Private event-loop impl -------------------------------------------------

impl<S, T> ArbitroRaft<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    async fn run_leader_once(&mut self) -> Result<(), RaftError> {
        // 1. Drain inbound client proposals → pending_batch + pending_slots.
        //    NOTE: do NOT clear first — items may have been pushed by the idle-path select
        //    arm on the previous tick; clearing would drop slots without notifying clients.
        let limit = self.node.config.limits.append_batch_entries;
        while self.pending_batch.len() < limit {
            match self.client_rx.try_recv() {
                Ok(p) => {
                    self.pending_batch.push(p.payload);
                    self.pending_slots.push(p.slot_id);
                }
                Err(_) => break,
            }
        }

        if !self.pending_batch.is_empty() {
            self.replicate_pending().await?;
        }

        // 2. Burst-drain available inbound frames.
        let mut processed = 0;
        while let Some(n) = self
            .node
            .transport()
            .recv_frame_timeout(Duration::ZERO, &mut self.inbound_buf)
            .await?
        {
            let inbound = crate::decode_message(&self.inbound_buf[..n])?;
            self.node.handle_inbound(inbound).await?;
            processed += 1;
            if processed >= 128 {
                break;
            }
        }

        if processed > 0 {
            self.node.try_advance_commit_index()?;
            self.drain_commit_waiters();
            if !self.node.is_leader() {
                self.fail_commit_waiters();
            }
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
            frame_result = self.node.transport().recv_frame_timeout(timeout, &mut self.inbound_buf).fuse() => {
                if let Some(n) = frame_result? {
                    let inbound = crate::decode_message(&self.inbound_buf[..n])?;
                    self.node.handle_inbound(inbound).await?;
                    self.node.try_advance_commit_index()?;
                    self.drain_commit_waiters();
                    if !self.node.is_leader() { self.fail_commit_waiters(); }
                }
                // else: timeout, heartbeat sent on next tick
                return Ok(());
            }
            proposal = self.client_rx.next() => {
                if let Some(p) = proposal {
                    self.pending_batch.push(p.payload);
                    self.pending_slots.push(p.slot_id);
                    // Drain all remaining proposals in one shot — form the full batch
                    // immediately so we replicate below without waiting for the next tick.
                    while self.pending_batch.len() < limit {
                        match self.client_rx.try_recv() {
                            Ok(p2) => { self.pending_batch.push(p2.payload); self.pending_slots.push(p2.slot_id); }
                            Err(_) => break,
                        }
                    }
                }
                // Fall through — replicate the batch formed above.
            }
        }

        // Replicate batch accumulated by the proposal arm (skipped if frame arm returned).
        if !self.pending_batch.is_empty() {
            self.replicate_pending().await?;
        }

        Ok(())
    }

    /// Replicate `pending_batch`, pair results with `pending_slots` → `commit_waiters`.
    async fn replicate_pending(&mut self) -> Result<(), RaftError> {
        // Form a slice of slices for the scratchpad
        let mut payloads = Vec::with_capacity(self.pending_batch.len());
        for p in &self.pending_batch {
            payloads.push(p.as_slice());
        }

        match self.node.replicate_batch_async(&payloads).await {
            Ok((first_index, _)) => {
                for (i, slot_id) in self.pending_slots.drain(..).enumerate() {
                    self.commit_waiters.push(CommitWaiter {
                        index: LogIndex(first_index.0 + i as u64),
                        slot_id,
                    });
                }
            }
            Err(e) => {
                for slot_id in self.pending_slots.drain(..) {
                    self.registry.get(slot_id).notify_error();
                }
                self.pending_batch.clear();
                return Err(e);
            }
        }
        self.pending_batch.clear();
        Ok(())
    }

    async fn run_follower_once(&mut self) -> Result<(), RaftError> {
        let mut processed = 0;
        while let Some(n) = self
            .node
            .transport()
            .recv_frame_timeout(Duration::ZERO, &mut self.inbound_buf)
            .await?
        {
            let inbound = crate::decode_message(&self.inbound_buf[..n])?;
            self.node.handle_inbound(inbound).await?;
            processed += 1;
            if processed >= 128 {
                break;
            }
        }

        if processed > 0 {
            self.reset_election_deadline();
            return Ok(());
        }

        let now = Instant::now();
        let timeout = self.next_election_at.saturating_duration_since(now);
        match self
            .node
            .transport()
            .recv_frame_timeout(timeout, &mut self.inbound_buf)
            .await?
        {
            Some(n) => {
                let inbound = crate::decode_message(&self.inbound_buf[..n])?;
                self.node.handle_inbound(inbound).await?;
                self.reset_election_deadline();
                if self.node.is_leader() {
                    self.reset_heartbeat_deadline();
                }
            }
            None => match self.node.campaign_once(&mut self.inbound_buf).await {
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

    /// Resolve all commit_waiters whose index ≤ current commit_index.
    ///
    /// Fast path: when the entire batch commits at once (normal case), drain
    /// in insertion order via a single pass with no swap_remove overhead.
    fn drain_commit_waiters(&mut self) {
        let commit_index = self.node.commit_index();
        if self.commit_waiters.is_empty() {
            return;
        }

        // Fast path — full batch committed (common in bench + low-contention).
        if self
            .commit_waiters
            .last()
            .map_or(false, |w| w.index <= commit_index)
        {
            for w in self.commit_waiters.drain(..) {
                self.registry.get(w.slot_id).notify_committed(w.index);
            }
            return;
        }

        // Slow path — partial commit, swap_remove to avoid shifting.
        let mut i = 0;
        while i < self.commit_waiters.len() {
            if self.commit_waiters[i].index <= commit_index {
                let w = self.commit_waiters.swap_remove(i);
                self.registry.get(w.slot_id).notify_committed(w.index);
            } else {
                i += 1;
            }
        }
    }

    /// Fail all pending commit_waiters — called on step-down.
    fn fail_commit_waiters(&mut self) {
        for w in self.commit_waiters.drain(..) {
            self.registry.get(w.slot_id).notify_error();
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
