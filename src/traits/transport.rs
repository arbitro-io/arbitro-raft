use std::time::Duration;

use async_trait::async_trait;

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
pub trait RaftTransport: Send + Sync {
    /// Send a Raft message split into multiple slices (Vectored I/O).
    ///
    /// This eliminates copies of large payloads by allowing the transport
    /// to pass multiple buffers (e.g. headers in one, payload in another) 
    /// directly to the OS.
    fn send_vectored(&self, peer: PeerId, slices: &[&[u8]]) -> impl std::future::Future<Output = Result<(), RaftError>> + Send;

    /// Send a Raft message using an owned buffer (Zero-copy sharing).
    ///
    /// The transport takes ownership of the `Bytes` object, allowing the 
    /// sender to dispatch multiple parallel sends without lifetime issues.
    fn send_frame_owned(&self, peer: PeerId, frame: bytes::Bytes) -> impl std::future::Future<Output = Result<(), RaftError>> + Send;


    /// Receive the next incoming raw frame from any peer into the provided buffer.
    ///
    /// The transport writes exactly the frame bytes into `out` and returns the length.
    /// Blocks until a frame arrives.
    fn recv_frame(&self, out: &mut [u8]) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send;

    /// Receive the next incoming raw frame into the provided buffer, returning `None` if `timeout` elapses.
    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send;
}
