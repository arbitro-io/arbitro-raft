//! Group multiplexing over one shared transport.
//!
//! Send side: [`MultiplexedTransport`] stamps every outbound frame with its
//! [`GroupId`] (header bytes `[24..32]`) and delegates I/O to the shared
//! inner transport.
//!
//! Recv side (H2): all wrappers over one [`MultiplexDemux`] share a
//! [`DemuxShared`] routing table. When a wrapper receives a frame that
//! belongs to a *sibling* group it PARKS the frame in that group's inbox
//! (recycled fixed-size buffers — no per-frame allocation in steady state)
//! instead of dropping it; the sibling consumes it from its inbox on its next
//! recv, and a driver can drain every parked frame via
//! [`MultiplexDemux::pop_parked_into`]. A frame whose `group_id` names no
//! registered group is counted and dropped (H9) — it is never routed to the
//! wrong group and never panics.
//!
//! Locking: the inbox table sits behind one `Mutex` per demux instance
//! (never global). The fast path of every recv checks an atomic pending
//! counter first, so a frame addressed to the receiving group itself —
//! the steady-state case — touches no lock at all.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::protocol::codec::wire::MAX_FRAME_SIZE;
use crate::{GroupId, PeerId, RaftError, RaftTransport, RAFT_FRAME_HEADER_SIZE};

/// Frames parked per group before the oldest surplus is shed. Raft tolerates
/// frame loss; unbounded buffering for a group its driver never drains does
/// not.
const MAX_INBOX_FRAMES: usize = 1024;
/// Recycled frame buffers kept when idle (`64 KiB` each). Bursts beyond this
/// allocate (cold) and the surplus is freed on recycle.
const MAX_POOL_BUFS: usize = 64;
/// Outbound iovec lists up to this many slices are assembled on the stack.
const STACK_IOVECS: usize = 128;

/// One parked frame: a recycled `MAX_FRAME_SIZE` buffer plus its live length.
struct ParkedFrame {
    data: Box<[u8]>,
    len: usize,
}

struct DemuxState {
    /// Per-group inbox of parked frames, keyed by raw `GroupId`.
    inboxes: HashMap<u64, VecDeque<ParkedFrame>>,
    /// Groups whose inbox went empty→nonempty; may hold stale (now-empty)
    /// entries, which `pop_parked_into` skips and `try_pop_into` lazily pops
    /// from the front (so the queue stays bounded even when no driver ever
    /// drains it). Invariant: every nonempty inbox has at least one entry
    /// here.
    ready: VecDeque<u64>,
    /// Recycled frame buffers.
    pool: Vec<Box<[u8]>>,
}

/// Routing table shared by every wrapper cloned from one [`MultiplexDemux`].
pub(crate) struct DemuxShared {
    /// Total parked frames — lock-free emptiness probe for the fast path.
    pending: AtomicUsize,
    /// H9: frames whose `group_id` named no registered group (dropped), plus
    /// frames too short to carry a header.
    unknown_group: AtomicU64,
    /// Frames shed because a group's inbox hit [`MAX_INBOX_FRAMES`].
    overflow_dropped: AtomicU64,
    state: Mutex<DemuxState>,
}

impl DemuxShared {
    fn new() -> Self {
        Self {
            pending: AtomicUsize::new(0),
            unknown_group: AtomicU64::new(0),
            overflow_dropped: AtomicU64::new(0),
            state: Mutex::new(DemuxState {
                inboxes: HashMap::new(),
                ready: VecDeque::new(),
                pool: Vec::new(),
            }),
        }
    }

    /// Poison-tolerant lock: the state is a frame queue — worst case after a
    /// panicked holder is stale frames, never a broken invariant we must halt
    /// on.
    fn lock(&self) -> MutexGuard<'_, DemuxState> {
        match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn register(&self, gid: u64) {
        self.lock().inboxes.entry(gid).or_default();
    }

    fn unregister(&self, gid: u64) {
        let mut st = self.lock();
        if let Some(inbox) = st.inboxes.remove(&gid) {
            self.pending.fetch_sub(inbox.len(), Ordering::Release);
        }
    }

    /// Pop the next parked frame for `gid` into `out`. Lock-free when nothing
    /// is parked anywhere.
    fn try_pop_into(&self, gid: u64, out: &mut [u8]) -> Result<Option<usize>, RaftError> {
        if self.pending.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }
        let mut guard = self.lock();
        let st = &mut *guard;
        // Lazy `ready` hygiene: this path drains frames WITHOUT going through
        // `pop_parked_into`, so without a driver the stale (drained or
        // unregistered) heads it leaves behind would accumulate unbounded.
        // Popping stale heads here bounds the queue in the driverless case;
        // each entry is popped at most once, so the amortized cost is O(1)
        // per park, all inside the lock we already hold.
        while let Some(head) = st.ready.front() {
            if st.inboxes.get(head).is_some_and(|q| !q.is_empty()) {
                break;
            }
            st.ready.pop_front();
        }
        let Some(inbox) = st.inboxes.get_mut(&gid) else {
            return Ok(None);
        };
        let Some(frame) = inbox.pop_front() else {
            return Ok(None);
        };
        self.pending.fetch_sub(1, Ordering::Release);
        if out.len() < frame.len {
            recycle(&mut st.pool, frame.data);
            return Err(RaftError::Transport(
                "multiplex: recv buffer smaller than parked frame".into(),
            ));
        }
        out[..frame.len].copy_from_slice(&frame.data[..frame.len]);
        recycle(&mut st.pool, frame.data);
        Ok(Some(frame.len))
    }

    /// Park a frame owned by group `gid` (which is NOT the caller's group).
    /// Unregistered group ⇒ count + drop (H9). Full inbox ⇒ count + drop.
    fn park(&self, gid: u64, frame: &[u8]) {
        if frame.len() > MAX_FRAME_SIZE {
            self.unknown_group.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut guard = self.lock();
        let st = &mut *guard;
        if !st.inboxes.contains_key(&gid) {
            drop(guard);
            self.unknown_group.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut data = st
            .pool
            .pop()
            .unwrap_or_else(|| vec![0u8; MAX_FRAME_SIZE].into_boxed_slice());
        data[..frame.len()].copy_from_slice(frame);
        if let Some(inbox) = st.inboxes.get_mut(&gid) {
            if inbox.len() >= MAX_INBOX_FRAMES {
                recycle(&mut st.pool, data);
                drop(guard);
                self.overflow_dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
            let was_empty = inbox.is_empty();
            inbox.push_back(ParkedFrame {
                data,
                len: frame.len(),
            });
            if was_empty {
                st.ready.push_back(gid);
            }
            self.pending.fetch_add(1, Ordering::Release);
        }
    }

    /// Driver drain: pop the oldest parked frame of ANY group into `out`.
    fn pop_parked_into(&self, out: &mut [u8]) -> Result<Option<(u64, usize)>, RaftError> {
        if self.pending.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }
        let mut guard = self.lock();
        let st = &mut *guard;
        while let Some(gid) = st.ready.pop_front() {
            let Some(inbox) = st.inboxes.get_mut(&gid) else {
                continue; // group unregistered since it was queued
            };
            let Some(frame) = inbox.pop_front() else {
                continue; // stale ready entry — inbox drained by its own recv
            };
            if !inbox.is_empty() {
                st.ready.push_back(gid);
            }
            self.pending.fetch_sub(1, Ordering::Release);
            if out.len() < frame.len {
                recycle(&mut st.pool, frame.data);
                return Err(RaftError::Transport(
                    "multiplex: recv buffer smaller than parked frame".into(),
                ));
            }
            out[..frame.len].copy_from_slice(&frame.data[..frame.len]);
            recycle(&mut st.pool, frame.data);
            return Ok(Some((gid, frame.len)));
        }
        Ok(None)
    }
}

fn recycle(pool: &mut Vec<Box<[u8]>>, buf: Box<[u8]>) {
    if pool.len() < MAX_POOL_BUFS {
        pool.push(buf);
    }
}

/// Shared recv-side demultiplexer over one inner transport. Create one per
/// core/driver, then mint one [`MultiplexedTransport`] per group with
/// [`transport`](Self::transport) — all wrappers cooperate through the same
/// routing table, so no group's recv can starve or drop a sibling's frames.
pub struct MultiplexDemux<T> {
    inner: Arc<T>,
    shared: Arc<DemuxShared>,
}

impl<T> MultiplexDemux<T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            shared: Arc::new(DemuxShared::new()),
        }
    }

    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }

    /// Register `group_id` and return its send-stamping, demux-backed
    /// transport.
    pub fn transport(&self, group_id: GroupId) -> MultiplexedTransport<T> {
        self.shared.register(group_id.0);
        MultiplexedTransport {
            inner: self.inner.clone(),
            group_id,
            shared: self.shared.clone(),
        }
    }

    /// Drop `group_id`'s inbox; frames still parked in it are discarded.
    pub fn unregister(&self, group_id: GroupId) {
        self.shared.unregister(group_id.0);
    }

    /// Pop the oldest frame parked for ANY registered group into `out`,
    /// returning its raw group id and length. Lock-free `None` when nothing
    /// is parked.
    pub fn pop_parked_into(&self, out: &mut [u8]) -> Result<Option<(u64, usize)>, RaftError> {
        self.shared.pop_parked_into(out)
    }

    /// H9 counter: frames whose `group_id` named no registered group (or that
    /// were too short to carry a header) — counted, then dropped.
    pub fn unknown_group_frames(&self) -> u64 {
        self.shared.unknown_group.load(Ordering::Relaxed)
    }

    /// Frames shed because a group's inbox was full.
    pub fn overflow_dropped_frames(&self) -> u64 {
        self.shared.overflow_dropped.load(Ordering::Relaxed)
    }
}

impl<T> Clone for MultiplexDemux<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shared: self.shared.clone(),
        }
    }
}

/// Wrapper that stamps outbound frames with a fixed [`GroupId`] and, on recv,
/// demultiplexes the shared inner transport cooperatively: its own frames are
/// returned in place (zero copy, no lock on the fast path), sibling groups'
/// frames are parked in their inboxes, unknown groups' frames are counted and
/// dropped (H9). Mint instances via [`MultiplexDemux::transport`] so all
/// groups share one routing table.
pub struct MultiplexedTransport<T> {
    inner: Arc<T>,
    group_id: GroupId,
    shared: Arc<DemuxShared>,
}

impl<T> MultiplexedTransport<T> {
    /// Standalone wrapper with a private routing table containing only its
    /// own group. Frames for other groups are counted and dropped — with no
    /// sibling registered there is nowhere to route them. Prefer
    /// [`MultiplexDemux::transport`] whenever one inner transport carries
    /// more than one group.
    pub fn new(inner: Arc<T>, group_id: GroupId) -> Self {
        let shared = Arc::new(DemuxShared::new());
        shared.register(group_id.0);
        Self {
            inner,
            group_id,
            shared,
        }
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T> Clone for MultiplexedTransport<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            group_id: self.group_id,
            shared: self.shared.clone(),
        }
    }
}

impl<T> RaftTransport for MultiplexedTransport<T>
where
    T: RaftTransport,
{
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        // The first slice is the RaftFrameHeader (per `encode_message_vectored`),
        // which hard-codes group_id=0. Re-stamp by copying just the 32-byte
        // header onto the stack and rewriting bytes [24..32]; the payload
        // slices are reborrowed as-is. The iovec list itself is assembled on
        // the stack for every realistic slice count — no heap allocation.
        let inner = self.inner.clone();
        let group_id = self.group_id;
        async move {
            if slices.is_empty() || slices[0].len() < RAFT_FRAME_HEADER_SIZE {
                return Err(RaftError::Protocol(
                    "multiplex: frame too small to stamp group_id".into(),
                ));
            }

            // group_id lives at header[24..32], little-endian — matches
            // RaftFrameHeader::group_id in protocol/codec/wire.rs.
            // B13: length checked above — a HEADER_SIZE prefix always converts.
            #[allow(clippy::unwrap_used)]
            let mut header: [u8; RAFT_FRAME_HEADER_SIZE] =
                slices[0][..RAFT_FRAME_HEADER_SIZE].try_into().unwrap();
            header[24..32].copy_from_slice(&group_id.0.to_le_bytes());

            let total = slices.len() + 1;
            if total <= STACK_IOVECS {
                let mut stamped: [&[u8]; STACK_IOVECS] = [&[]; STACK_IOVECS];
                stamped[0] = &header[..];
                stamped[1] = &slices[0][RAFT_FRAME_HEADER_SIZE..];
                stamped[2..total].copy_from_slice(&slices[1..]);
                inner.send_vectored(peer, &stamped[..total]).await
            } else {
                let mut stamped: Vec<&[u8]> = Vec::with_capacity(total);
                stamped.push(&header[..]);
                stamped.push(&slices[0][RAFT_FRAME_HEADER_SIZE..]);
                stamped.extend_from_slice(&slices[1..]);
                inner.send_vectored(peer, &stamped).await
            }
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        // Rewrite the 8-byte group_id at [24..32] in the frame header before
        // send. `try_into_mut` avoids the copy when the Bytes is uniquely
        // owned (the common case for freshly-encoded frames).
        let inner = self.inner.clone();
        let gid = self.group_id;
        async move {
            if frame.len() < RAFT_FRAME_HEADER_SIZE {
                return Err(RaftError::Protocol(
                    "multiplex: frame too short to stamp group_id".into(),
                ));
            }
            let mut buf = match frame.try_into_mut() {
                Ok(unique) => unique,
                Err(shared) => bytes::BytesMut::from(&shared[..]),
            };
            buf[24..32].copy_from_slice(&gid.0.to_le_bytes());
            inner.send_frame_owned(peer, buf.freeze()).await
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let inner = self.inner.clone();
        let shared = self.shared.clone();
        let gid = self.group_id.0;
        async move {
            loop {
                if let Some(n) = shared.try_pop_into(gid, out)? {
                    return Ok(n);
                }
                let n = inner.recv_frame(out).await?;
                match frame_group(&out[..n]) {
                    Some(g) if g == gid => return Ok(n),
                    Some(g) => shared.park(g, &out[..n]),
                    None => {
                        shared.unknown_group.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let inner = self.inner.clone();
        let shared = self.shared.clone();
        let gid = self.group_id.0;
        async move {
            let deadline = std::time::Instant::now() + timeout;
            loop {
                if let Some(n) = shared.try_pop_into(gid, out)? {
                    return Ok(Some(n));
                }
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Ok(None);
                }
                match inner.recv_frame_timeout(remaining, out).await? {
                    None => return Ok(None),
                    Some(n) => match frame_group(&out[..n]) {
                        Some(g) if g == gid => return Ok(Some(n)),
                        Some(g) => shared.park(g, &out[..n]),
                        None => {
                            shared.unknown_group.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                }
            }
        }
    }

    fn send_file_dma(
        &self,
        peer: PeerId,
        file: std::fs::File,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.inner.send_file_dma(peer, file)
    }
}

/// Raw group id at header bytes `[24..32]`; `None` for a buffer too short to
/// carry a frame header.
#[inline]
pub(crate) fn frame_group(buf: &[u8]) -> Option<u64> {
    buf.get(24..32)
        .and_then(|b| <[u8; 8]>::try_from(b).ok())
        .map(u64::from_le_bytes)
}
