use std::time::Duration;

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
///
/// # Security contract (production transports)
///
/// The Raft node **trusts the transport** for peer authentication. The wire
/// frame carries a claimed sender id (`RaftFrameHeader.from`), and the node
/// uses it directly: votes are granted to it, append/snapshot acks are
/// credited to `PeerId(from)`, and membership checks only verify that the
/// *claimed* id is a member. The node cannot detect impersonation on its own,
/// so a production transport MUST:
///
/// 1. **Confidentiality + mutual peer authentication.** Consensus traffic
///    must run over a mutually-authenticated, encrypted channel (e.g. mTLS
///    with a cluster CA, or an equivalent authenticated transport). An
///    unauthenticated plaintext transport lets any process that can reach
///    the port vote, replicate, and forge quorums.
/// 2. **Identity binding (anti-spoofing).** Bind each connection to the
///    authenticated identity's `PeerId` (e.g. derived from the peer
///    certificate's SAN/CN) and **reject any received frame whose header
///    `from` field does not equal the connection's authenticated `PeerId`**.
///    Rejected frames must be dropped before they reach `recv_frame` /
///    `recv_frame_timeout`; counting them for observability is recommended.
///    Without this binding, any authenticated peer can still impersonate any
///    other member and forge acknowledgements toward a false quorum.
///
/// In-process/test transports (loopback channels, benches) are exempt: they
/// are only reachable from the test harness itself.
pub trait RaftTransport: Send + Sync {
    /// Send a Raft message split into multiple slices (Vectored I/O).
    ///
    /// This eliminates copies of large payloads by allowing the transport
    /// to pass multiple buffers (e.g. headers in one, payload in another)
    /// directly to the OS.
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send;

    /// Send a Raft message using an owned buffer (Zero-copy sharing).
    ///
    /// The transport takes ownership of the `Bytes` object, allowing the
    /// sender to dispatch multiple parallel sends without lifetime issues.
    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send;

    /// Receive the next incoming raw frame from any peer into the provided buffer.
    ///
    /// The transport writes exactly the frame bytes into `out` and returns the length.
    /// Blocks until a frame arrives.
    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send;

    /// Receive the next incoming raw frame into the provided buffer, returning `None` if `timeout` elapses.
    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send;

    /// Send a file using DMA (Direct Memory Access / sendfile / io_uring).
    ///
    /// Falls back to returning a Transport error by default.
    fn send_file_dma(
        &self,
        _peer: PeerId,
        _file: std::fs::File,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move {
            Err(RaftError::Transport(
                "DMA not supported by this transport".into(),
            ))
        }
    }
}
