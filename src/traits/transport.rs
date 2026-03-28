use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{PeerId, RaftError};

/// Transport layer for the Raft protocol.
///
/// Implementations move pre-encoded frames between peers.
/// Encoding and decoding is performed by the node, not the transport.
/// The transport is protocol-agnostic: it only moves raw `Bytes`.
///
/// # Frame flow
///
/// **Send:** `RaftNode` encodes a `RaftMessage` into `Bytes` via
/// `encode_message`, then calls `send_frame`. The transport writes the bytes
/// to the wire without inspecting them.
///
/// **Recv:** The transport reads raw bytes from the wire and returns them via
/// `recv_frame`. `RaftNode` decodes them with `decode_message_view` and
/// dispatches to the appropriate handler.
#[async_trait]
pub trait RaftTransport: Send + Sync {
    /// Send a pre-encoded Raft frame to a peer.
    ///
    /// Best-effort: a transport error does not imply the peer is permanently
    /// unreachable. The node decides whether to retry based on Raft timeout logic.
    async fn send_frame(&self, peer: PeerId, frame: Bytes) -> Result<(), RaftError>;

    /// Receive the next incoming raw frame from any peer.
    ///
    /// Returns raw bytes exactly as received. The caller decodes with
    /// `decode_message_view`. Blocks until a frame arrives.
    async fn recv_frame(&self) -> Result<Bytes, RaftError>;

    /// Receive the next incoming raw frame, returning `None` if `timeout` elapses.
    async fn recv_frame_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<Bytes>, RaftError>;
}
