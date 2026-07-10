use std::sync::Arc;
use std::time::Duration;

use crate::{GroupId, PeerId, RaftError, RaftTransport};

/// Wrapper that stamps outbound frames with a fixed [`GroupId`] and delegates
/// I/O to an inner transport `T`. On recv it returns frames as-is; the outer
/// registry inspects the decoded header to route by `group_id`.
///
/// Sprint-1 scope: single-group tagging on send. Multi-group demux on recv is
/// handled by [`crate::RaftGroupRegistry::dispatch`], which reads the decoded
/// `InboundRaftMessage::group_id` field. This wrapper therefore does not need
/// its own recv-side routing table for Sprint 1 — a future sprint can add a
/// per-group inbox channel here.
pub struct MultiplexedTransport<T> {
    inner: Arc<T>,
    group_id: GroupId,
}

impl<T> MultiplexedTransport<T> {
    pub fn new(inner: Arc<T>, group_id: GroupId) -> Self {
        Self { inner, group_id }
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
        // The first slice of `slices` is the RaftFrameHeader (per
        // `encode_message_vectored`). We cannot rewrite its `group_id` field
        // in-place here because mutating a `&[u8]` we do not own is UB — the
        // correct integration point is the encoder. Hot-path callers should
        // call `encode_message_vectored_with_group(from, self.group_id(), ...)`
        // directly and bypass this wrapper's `send_vectored`, so the frame is
        // already stamped before it ever reaches us. This impl exists only to
        // satisfy the `RaftTransport` trait and passes slices through as-is.
        let inner = self.inner.clone();
        // Copy slice metadata into an owned vector so we can move into async.
        let owned: Vec<Vec<u8>> = slices.iter().map(|s| s.to_vec()).collect();
        async move {
            let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
            inner.send_vectored(peer, &refs).await
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        // Rewrite the 8-byte group_id at [24..32] in the frame header before
        // send. This is the primary correctness path for group-tagged sends.
        let inner = self.inner.clone();
        let gid = self.group_id;
        async move {
            let mut buf = frame.to_vec();
            if buf.len() >= 32 {
                let bytes = gid.0.to_le_bytes();
                buf[24..32].copy_from_slice(&bytes);
            }
            inner.send_frame_owned(peer, buf.into()).await
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        // Pass-through. Demux happens at RaftGroupRegistry::dispatch.
        self.inner.recv_frame(out)
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        self.inner.recv_frame_timeout(timeout, out)
    }

    fn send_file_dma(
        &self,
        peer: PeerId,
        file: std::fs::File,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.inner.send_file_dma(peer, file)
    }
}

/// Reserved for a future sprint: a demultiplexing recv-side splitter that
/// hands each group its own inbox. Sprint 1 keeps the design surface minimal
/// by having [`crate::RaftGroupRegistry::dispatch`] do the routing directly
/// off the decoded header.
#[doc(hidden)]
pub struct _MultiplexedRecvPlaceholder;
