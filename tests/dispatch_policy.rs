/// Dispatch policy tests.
use std::time::Duration;

use futures::executor::block_on;

use arbitro_raft::{
    DispatchAckPolicy, DispatchFailPolicy, DispatchScope, DispatchSpec, PeerId, RaftError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncParams {
    start: u64,
    end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncAck {
    saved_until: u64,
}

fn encode_sync_params(value: &SyncParams) -> Result<Vec<u8>, RaftError> {
    if value.start > value.end {
        return Err(RaftError::Dispatch("sync range is inverted".into()));
    }
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&value.start.to_le_bytes());
    out.extend_from_slice(&value.end.to_le_bytes());
    Ok(out)
}

fn decode_sync_params(bytes: &[u8]) -> Result<SyncParams, RaftError> {
    if bytes.len() != 16 {
        return Err(RaftError::Dispatch(format!(
            "sync params length must be 16 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(SyncParams {
        start: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        end: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    })
}

fn encode_sync_ack(value: &SyncAck) -> Result<Vec<u8>, RaftError> {
    if value.saved_until == 0 {
        return Err(RaftError::Dispatch("saved_until must be non-zero".into()));
    }
    let mut out = Vec::with_capacity(8);
    out.extend_from_slice(&value.saved_until.to_le_bytes());
    Ok(out)
}

fn decode_sync_ack(bytes: &[u8]) -> Result<SyncAck, RaftError> {
    if bytes.len() != 8 {
        return Err(RaftError::Dispatch(format!(
            "sync ack length must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(SyncAck {
        saved_until: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
    })
}

fn sync_spec() -> DispatchSpec<SyncParams, SyncAck> {
    DispatchSpec::new(
        0x21,
        encode_sync_params,
        decode_sync_params,
        encode_sync_ack,
        decode_sync_ack,
    )
    .with_scope(DispatchScope::Followers)
    .with_ack_policy(DispatchAckPolicy::Quorum)
    .with_fail_policy(DispatchFailPolicy::NoFailures)
    .with_timeout(Duration::from_millis(1500))
    .with_trace(true)
}

#[test]
fn dispatch_tx_respects_percent_ack_policy() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::Percent)
        .with_ack_percent(75)
        .with_fail_policy(DispatchFailPolicy::AllowFailures);
    let envelope = spec
        .dispatch(SyncParams { start: 1, end: 3 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin(
        [PeerId(2), PeerId(3), PeerId(4), PeerId(5)],
        decode_sync_ack,
    );

    tx.accept(PeerId(2), SyncAck { saved_until: 3 }).unwrap();
    tx.accept(PeerId(3), SyncAck { saved_until: 3 }).unwrap();
    assert!(!handle.is_ready());
    tx.accept(PeerId(4), SyncAck { saved_until: 3 }).unwrap();

    let result = block_on(handle.wait()).unwrap();
    assert_eq!(result.accepted_count(), 3);
}

#[test]
fn dispatch_tx_respects_max_failures_policy() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::AtLeast)
        .with_ack_count(2)
        .with_fail_policy(DispatchFailPolicy::MaxFailures)
        .with_fail_count(1);
    let envelope = spec
        .dispatch(SyncParams { start: 1, end: 2 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3), PeerId(4)], decode_sync_ack);

    tx.reject(PeerId(2), b"first".to_vec()).unwrap();
    tx.reject(PeerId(3), b"second".to_vec()).unwrap();

    let err = block_on(handle.wait()).unwrap_err();
    assert!(matches!(err, RaftError::Dispatch(_)));
}

#[test]
fn dispatch_tx_respects_max_failure_percent_policy() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::All)
        .with_fail_policy(DispatchFailPolicy::MaxFailurePercent)
        .with_fail_percent(25);
    let envelope = spec
        .dispatch(SyncParams { start: 1, end: 2 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin(
        [PeerId(2), PeerId(3), PeerId(4), PeerId(5)],
        decode_sync_ack,
    );

    tx.reject(PeerId(2), b"one".to_vec()).unwrap();
    tx.reject(PeerId(3), b"two".to_vec()).unwrap();

    let err = block_on(handle.wait()).unwrap_err();
    assert!(matches!(err, RaftError::Dispatch(_)));
}

#[test]
fn dispatch_tx_fail_fast_trips_immediately() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::All)
        .with_fail_policy(DispatchFailPolicy::FailFast);
    let envelope = spec
        .dispatch(SyncParams { start: 1, end: 2 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3)], decode_sync_ack);

    tx.fail(PeerId(2), b"boom".to_vec()).unwrap();

    let err = block_on(handle.wait()).unwrap_err();
    assert!(matches!(err, RaftError::Dispatch(_)));
}

#[test]
fn dispatch_tx_times_out_when_no_peer_finishes() {
    let spec = sync_spec()
        .with_timeout(Duration::from_millis(25))
        .with_ack_policy(DispatchAckPolicy::All);
    let envelope = spec
        .dispatch(SyncParams { start: 1, end: 2 })
        .build()
        .unwrap();
    let (handle, _tx) = envelope.begin([PeerId(2), PeerId(3)], decode_sync_ack);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let err = rt.block_on(handle.wait()).unwrap_err();
    assert!(matches!(err, RaftError::Dispatch(_)));
}
