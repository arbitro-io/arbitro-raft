use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{LogIndex, RaftError};

use super::slot::{Slot, SlotId, SlotRegistry, SLOT_PENDING};

// ---------------------------------------------------------------------------
// Internal wire types — not exposed; ClientHandle is the public surface.
// ---------------------------------------------------------------------------

pub(crate) struct ClientProposal {
    /// Refcounted payload (H5): a caller that already owns a `Bytes` (or a
    /// `Vec<u8>`, convertible for free) crosses the mailbox with zero copies.
    pub(super) payload: Bytes,
    pub(super) slot_id: SlotId,
}

pub(crate) struct CommitWaiter {
    pub(super) index: LogIndex,
    pub(super) slot_id: SlotId,
}

// ---------------------------------------------------------------------------
// WriteFuture — returned internally by ClientHandle::write.
//
// Polls the Slot's AtomicU64 state. Registers the waker before the second
// load to close the register → store race without any mutex.
// ---------------------------------------------------------------------------

pub(super) struct WriteFuture {
    pub(super) registry: Arc<SlotRegistry>,
    pub(super) id: SlotId,
}

impl Future for WriteFuture {
    type Output = Result<LogIndex, RaftError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slot = self.registry.get(self.id);
        let v = slot.state.load(std::sync::atomic::Ordering::Acquire);
        if v != SLOT_PENDING {
            return Poll::Ready(Slot::decode(v));
        }
        slot.waker.register(cx.waker());
        // Re-check after registration to close the register → store race.
        let v = slot.state.load(std::sync::atomic::Ordering::Acquire);
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
// ClientHandle — clonable, Send + Sync handle for concurrent write calls.
// ---------------------------------------------------------------------------

/// Clonable handle returned by [`ArbitroRaft::client_handle`].
///
/// Any number of tasks can hold a `ClientHandle` and call [`write`] /
/// [`write_bytes`] concurrently. The leader batches all in-flight writes
/// naturally as they arrive from the channel. Each call blocks until the
/// entry reaches quorum.
///
/// # Backpressure (H5)
///
/// The mailbox behind this handle is BOUNDED
/// ([`LimitsConfig::client_mailbox_capacity`](crate::LimitsConfig::client_mailbox_capacity)).
/// The enqueue is a non-blocking `try_send`: when the run loop lags behind a
/// client flood the mailbox fills and the write fails fast with the retryable
/// [`RaftError::Overloaded`] — it never buffers without limit (OOM), never
/// parks the caller on a full queue, and never drops a proposal silently.
///
/// [`write`]: ClientHandle::write
/// [`write_bytes`]: ClientHandle::write_bytes
#[derive(Clone)]
pub struct ClientHandle {
    pub(super) tx: mpsc::Sender<ClientProposal>,
    pub(super) registry: Arc<SlotRegistry>,
}

impl ClientHandle {
    /// Submit `payload` and wait until it is committed by a quorum.
    /// Returns the [`LogIndex`] assigned to the entry.
    ///
    /// A borrowed slice must be copied ONCE into an owned buffer to cross the
    /// mailbox; callers that already own the payload should use
    /// [`write_bytes`](Self::write_bytes), which crosses with zero copies.
    ///
    /// Errors: [`RaftError::Overloaded`] when the bounded mailbox is full
    /// (retryable backpressure, nothing enqueued); a `Transport` error once
    /// the node has stopped.
    #[inline]
    pub async fn write(&self, payload: &[u8]) -> Result<LogIndex, RaftError> {
        self.write_bytes(Bytes::copy_from_slice(payload)).await
    }

    /// Zero-copy variant of [`write`](Self::write): `payload` crosses the
    /// mailbox as a refcounted [`Bytes`] — no copy, no allocation on the send
    /// path. `Vec<u8>` and `&'static [u8]` convert into [`Bytes`] for free
    /// (`Bytes::from` / `Bytes::from_static`).
    pub async fn write_bytes(&self, payload: Bytes) -> Result<LogIndex, RaftError> {
        // Reject payloads whose first byte collides with the config-change
        // magic (0xC0): the apply path decodes such entries as control entries
        // and would silently rewrite the voter set. This is the same guard the
        // `propose*` entry points enforce; this method feeds the same
        // replication path and must enforce it too.
        super::reject_reserved_prefix(&payload)?;

        // Slot-pool exhaustion is the same condition as a full mailbox — more
        // in-flight writes than the node can track — and gets the same
        // retryable signal.
        let id = self.registry.lease().ok_or(RaftError::Overloaded)?;

        if let Err(e) = self.tx.try_send(ClientProposal {
            payload,
            slot_id: id,
        }) {
            self.registry.release(id);
            return Err(match e {
                // Bounded mailbox full (H5): fail fast, retryable — the run
                // loop drains the mailbox every tick, so backing off briefly
                // and retrying is the correct client response.
                mpsc::error::TrySendError::Full(_) => RaftError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => {
                    RaftError::Transport("raft node stopped".into())
                }
            });
        }

        WriteFuture {
            registry: self.registry.clone(),
            id,
        }
        .await
    }
}
