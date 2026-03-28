use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::executor::block_on;

use arbitro_raft::{
    DispatchAckPolicy, DispatchContextView, DispatchFailPolicy, DispatchNodeRole,
    DispatchResponder, DispatchResponse, DispatchResponseKind, DispatchRoute, DispatchScope,
    DispatchSpec, DispatchTxResponder, DispatchView, PeerId, RaftCustomRegistry, RaftError,
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

fn encode_sync_params(value: &SyncParams) -> Result<Bytes, RaftError> {
    if value.start > value.end {
        return Err(RaftError::Dispatch("sync range is inverted".into()));
    }
    let mut out = BytesMut::with_capacity(16);
    out.put_u64_le(value.start);
    out.put_u64_le(value.end);
    Ok(out.freeze())
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

fn encode_sync_ack(value: &SyncAck) -> Result<Bytes, RaftError> {
    if value.saved_until == 0 {
        return Err(RaftError::Dispatch("saved_until must be non-zero".into()));
    }
    let mut out = BytesMut::with_capacity(8);
    out.put_u64_le(value.saved_until);
    Ok(out.freeze())
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

#[derive(Default)]
struct ResponseCollector {
    responses: Mutex<Vec<DispatchResponse>>,
}

impl ResponseCollector {
    fn take(&self) -> Vec<DispatchResponse> {
        std::mem::take(&mut *self.responses.lock().unwrap())
    }
}

#[async_trait]
impl DispatchResponder for ResponseCollector {
    async fn send_response(&self, response: DispatchResponse) -> Result<(), RaftError> {
        self.responses.lock().unwrap().push(response);
        Ok(())
    }
}

#[test]
fn dispatch_spec_builds_frame_and_view_reads_it_lazily() {
    let spec = sync_spec();
    let params = SyncParams { start: 40, end: 50 };

    let envelope = spec
        .dispatch(params)
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let view = envelope.view().unwrap();

    assert_eq!(view.command(), spec.command());
    assert_eq!(view.scope(), DispatchScope::Followers);
    assert_eq!(view.ack_policy(), DispatchAckPolicy::Quorum);
    assert_eq!(view.fail_policy(), DispatchFailPolicy::NoFailures);
    assert!(view.trace());
    assert_eq!(view.timeout_ms(), 3000);
    assert_eq!(view.params(&spec).unwrap(), params);
}

#[test]
fn dispatch_registry_invokes_typed_handler_and_returns_typed_response() {
    let spec = sync_spec();
    let responder = ResponseCollector::default();
    let registry = RaftCustomRegistry::new();
    let spec_for_handler = spec;

    registry
        .on_with(spec, move |params, ctx: DispatchContextView<'_>| {
            let spec_for_response = spec_for_handler;
            Box::pin(async move {
                assert_eq!(params, SyncParams { start: 40, end: 50 });
                ctx.accept_with(
                    &spec_for_response,
                    &SyncAck {
                        saved_until: params.end,
                    },
                )
                .await
            })
        })
        .unwrap();

    let envelope = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap();

    let view = block_on(registry.invoke_bytes(envelope.into_bytes(), &responder)).unwrap();
    let responses = responder.take();

    assert_eq!(view.command(), spec.command());
    assert_eq!(view.scope(), DispatchScope::Followers);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].tx_id, view.tx_id());
    assert_eq!(responses[0].command, spec.command());
    assert_eq!(responses[0].kind, DispatchResponseKind::Accepted);
    assert_eq!(
        spec.decode_response(responses[0].payload.as_ref()).unwrap(),
        SyncAck { saved_until: 50 }
    );
}

#[test]
fn dispatch_registry_reports_decode_failures_through_context() {
    let spec = sync_spec();
    let responder = ResponseCollector::default();
    let registry = RaftCustomRegistry::new();

    registry
        .on_with(spec, |_params, _ctx| Box::pin(async move { Ok(()) }))
        .unwrap();

    let mut corrupted = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap()
        .into_bytes()
        .to_vec();
    corrupted.truncate(corrupted.len() - 8);
    corrupted[24..28].copy_from_slice(&(8u32).to_le_bytes());

    let err = match block_on(registry.invoke_bytes(Bytes::from(corrupted), &responder)) {
        Ok(_) => panic!("decode failure should propagate an error"),
        Err(err) => err,
    };
    let responses = responder.take();

    assert!(matches!(err, RaftError::Dispatch(_)));
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].command, spec.command());
    assert_eq!(responses[0].kind, DispatchResponseKind::Failed);
    let message = std::str::from_utf8(responses[0].payload.as_ref()).unwrap();
    assert!(message.contains("sync params length must be 16 bytes"));
}

#[test]
fn dispatch_view_rejects_wrong_spec_for_params() {
    let spec = sync_spec();
    let envelope = spec
        .dispatch(SyncParams { start: 10, end: 20 })
        .build()
        .unwrap();
    let view: DispatchView = envelope.view().unwrap();

    let other_spec = DispatchSpec::new(
        0x22,
        encode_sync_params,
        decode_sync_params,
        encode_sync_ack,
        decode_sync_ack,
    );

    let err = view.params(&other_spec).unwrap_err();
    assert!(matches!(err, RaftError::Dispatch(_)));
}

#[test]
fn dispatch_tx_reaches_quorum_and_decodes_typed_accepts() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::Quorum)
        .with_fail_policy(DispatchFailPolicy::AllowFailures);
    let envelope = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3), PeerId(4)], decode_sync_ack);

    tx.accept(PeerId(2), SyncAck { saved_until: 50 }).unwrap();
    assert!(!handle.is_ready());

    tx.accept(PeerId(3), SyncAck { saved_until: 50 }).unwrap();
    let result = block_on(handle.wait()).unwrap();

    assert_eq!(result.accepted_count(), 2);
    assert_eq!(result.failed_count(), 0);
    assert_eq!(result.disconnected_count(), 0);
}

#[test]
fn dispatch_tx_disconnect_removes_peer_from_equation() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::All)
        .with_fail_policy(DispatchFailPolicy::AllowFailures);
    let envelope = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3)], decode_sync_ack);

    tx.disconnect(PeerId(3)).unwrap();
    tx.accept(PeerId(2), SyncAck { saved_until: 50 }).unwrap();

    let result = block_on(handle.wait()).unwrap();
    assert_eq!(result.accepted_count(), 1);
    assert_eq!(result.disconnected_count(), 1);
}

#[test]
fn dispatch_tx_respects_no_failures_policy() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::All)
        .with_fail_policy(DispatchFailPolicy::NoFailures);
    let envelope = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3)], decode_sync_ack);

    tx.accept(PeerId(2), SyncAck { saved_until: 50 }).unwrap();
    tx.reject(PeerId(3), Bytes::from_static(b"disk full"))
        .unwrap();

    let err = block_on(handle.wait()).unwrap_err();
    assert!(matches!(err, RaftError::Dispatch(_)));
}

#[test]
fn dispatch_tx_responder_bridges_remote_accepts_into_transaction() {
    let spec = sync_spec()
        .with_ack_policy(DispatchAckPolicy::Quorum)
        .with_fail_policy(DispatchFailPolicy::AllowFailures);
    let envelope = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap();
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3)], decode_sync_ack);

    let follower_2 = DispatchTxResponder::new(PeerId(2), tx.clone());
    let follower_3 = DispatchTxResponder::new(PeerId(3), tx);

    block_on(follower_2.send_response(DispatchResponse {
        tx_id: handle.tx_id(),
        command: spec.command(),
        kind: DispatchResponseKind::Accepted,
        payload: encode_sync_ack(&SyncAck { saved_until: 50 }).unwrap(),
    }))
    .unwrap();
    block_on(follower_3.send_response(DispatchResponse {
        tx_id: handle.tx_id(),
        command: spec.command(),
        kind: DispatchResponseKind::Accepted,
        payload: encode_sync_ack(&SyncAck { saved_until: 50 }).unwrap(),
    }))
    .unwrap();

    let result = block_on(handle.wait()).unwrap();
    assert_eq!(result.accepted_count(), 2);
}

#[test]
fn dispatch_scope_allows_expected_routes() {
    assert!(DispatchScope::All.allows(DispatchRoute::leader(true)));
    assert!(DispatchScope::All.allows(DispatchRoute::follower(false)));

    assert!(DispatchScope::Others.allows(DispatchRoute::leader(false)));
    assert!(DispatchScope::Others.allows(DispatchRoute::follower(false)));
    assert!(!DispatchScope::Others.allows(DispatchRoute::leader(true)));

    assert!(DispatchScope::Followers.allows(DispatchRoute::follower(false)));
    assert!(DispatchScope::Followers.allows(DispatchRoute {
        role: DispatchNodeRole::Follower,
        is_origin: true,
    }));
    assert!(!DispatchScope::Followers.allows(DispatchRoute::leader(false)));

    assert!(DispatchScope::Leader.allows(DispatchRoute::leader(false)));
    assert!(!DispatchScope::Leader.allows(DispatchRoute::follower(false)));

    assert!(DispatchScope::LocalOnly.allows(DispatchRoute::leader(true)));
    assert!(DispatchScope::LocalOnly.allows(DispatchRoute::follower(true)));
    assert!(!DispatchScope::LocalOnly.allows(DispatchRoute::leader(false)));
}

#[test]
fn dispatch_registry_enforces_scope_for_routes() {
    let responder = ResponseCollector::default();
    let registry = RaftCustomRegistry::new();
    let spec = sync_spec().with_scope(DispatchScope::Followers);

    registry
        .on_with(spec, move |_params, ctx: DispatchContextView<'_>| {
            Box::pin(async move { ctx.accept_with(&spec, &SyncAck { saved_until: 50 }).await })
        })
        .unwrap();

    let frame = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap()
        .into_bytes();

    let err = match block_on(registry.invoke_bytes_scoped(
        frame.clone(),
        &responder,
        DispatchRoute::leader(false),
    )) {
        Ok(_) => panic!("leader route should not execute follower-scoped handler"),
        Err(err) => err,
    };
    assert!(matches!(err, RaftError::Dispatch(_)));

    let view = block_on(registry.invoke_bytes_scoped(
        frame,
        &responder,
        DispatchRoute::follower(false),
    ))
    .unwrap();
    assert_eq!(view.scope(), DispatchScope::Followers);
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
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3), PeerId(4), PeerId(5)], decode_sync_ack);

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

    tx.reject(PeerId(2), Bytes::from_static(b"first")).unwrap();
    tx.reject(PeerId(3), Bytes::from_static(b"second")).unwrap();

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
    let (handle, tx) = envelope.begin([PeerId(2), PeerId(3), PeerId(4), PeerId(5)], decode_sync_ack);

    tx.reject(PeerId(2), Bytes::from_static(b"one")).unwrap();
    tx.reject(PeerId(3), Bytes::from_static(b"two")).unwrap();

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

    tx.fail(PeerId(2), Bytes::from_static(b"boom")).unwrap();

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

    let err = block_on(handle.wait()).unwrap_err();
    assert!(matches!(err, RaftError::Dispatch(_)));
}
