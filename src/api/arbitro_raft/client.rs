use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc;

use crate::{LogIndex, RaftError};

use super::slot::{Slot, SlotId, SlotRegistry, SLOT_PENDING};

// ---------------------------------------------------------------------------
// Internal wire types — not exposed; ClientHandle is the public surface.
// ---------------------------------------------------------------------------

pub(crate) struct ClientProposal {
    pub(super) payload: Vec<u8>,
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
/// Any number of tasks can hold a `ClientHandle` and call [`write`] concurrently.
/// The leader batches all in-flight writes naturally as they arrive from the channel.
/// Each call blocks until the entry reaches quorum.
///
/// [`write`]: ClientHandle::write
#[derive(Clone)]
pub struct ClientHandle {
    pub(super) tx: mpsc::UnboundedSender<ClientProposal>,
    pub(super) registry: Arc<SlotRegistry>,
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
