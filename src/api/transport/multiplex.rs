use std::sync::Arc;
use std::time::Duration;

use crate::{GroupId, PeerId, RaftError, RaftTransport, RAFT_FRAME_HEADER_SIZE};

/// Wrapper that stamps outbound frames with a fixed [`GroupId`] and delegates
/// I/O to an inner transport `T`.
///
/// This wrapper does not implement recv-side demultiplexing on its own: if
/// `inner` is shared by multiple `MultiplexedTransport`s (one per group), each
/// wrapper's `recv_frame`/`recv_frame_timeout` filters out frames that were
/// not addressed to its `group_id`, dropping them and waiting for the next
/// one. **This requires per-group inner queues** (or callers that never share
/// one `inner` across groups) — otherwise a busy group can starve another
/// group's recv loop by producing a stream of frames it keeps rejecting.
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
        // The first slice is the RaftFrameHeader (per `encode_message_vectored`),
        // which hard-codes group_id=0. We re-stamp it here by copying just the
        // 32-byte header onto the stack and rewriting bytes [24..32] — the
        // remaining payload slices are reborrowed as-is, so no body bytes are
        // ever copied.
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

            let mut stamped: Vec<&[u8]> = Vec::with_capacity(slices.len() + 1);
            stamped.push(&header[..]);
            stamped.push(&slices[0][RAFT_FRAME_HEADER_SIZE..]);
            stamped.extend_from_slice(&slices[1..]);

            inner.send_vectored(peer, &stamped).await
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
            if buf.len() < RAFT_FRAME_HEADER_SIZE {
                return Err(RaftError::Protocol(
                    "multiplex: frame too short to stamp group_id".into(),
                ));
            }
            buf[24..32].copy_from_slice(&gid.0.to_le_bytes());
            inner.send_frame_owned(peer, buf.into()).await
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        // No shared demux table exists at this layer, so a mismatched frame
        // is dropped and we keep waiting — see the struct-level doc comment
        // for why this requires per-group inner queues.
        let inner = self.inner.clone();
        let group_id = self.group_id;
        async move {
            loop {
                let n = inner.recv_frame(out).await?;
                if frame_group_matches(&out[..n], group_id) {
                    return Ok(n);
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
        let group_id = self.group_id;
        async move {
            let deadline = std::time::Instant::now() + timeout;
            loop {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Ok(None);
                }
                match inner.recv_frame_timeout(remaining, out).await? {
                    Some(n) if frame_group_matches(&out[..n], group_id) => return Ok(Some(n)),
                    Some(_) => continue,
                    None => return Ok(None),
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

/// Returns whether `buf` decodes to a frame header tagged with `group_id`.
/// Short buffers (below `RAFT_FRAME_HEADER_SIZE`) never match.
fn frame_group_matches(buf: &[u8], group_id: GroupId) -> bool {
    buf.len() >= RAFT_FRAME_HEADER_SIZE
        && buf
            .get(24..32)
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .is_some_and(|b| u64::from_le_bytes(b) == group_id.0)
}

/// Reserved for a future sprint: a proper per-group inbox splitter (e.g. a
/// channel fed by a single background reader) so `recv_frame` on a shared
/// `inner` never has to drop-and-retry frames belonging to other groups.
#[doc(hidden)]
pub struct _MultiplexedRecvPlaceholder;
