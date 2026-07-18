use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
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
    let spec_for_handler = spec.clone();

    registry
        .on_with(spec.clone(), move |params, ctx: DispatchContextView<'_>| {
            let spec_for_response = spec_for_handler.clone();
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

    let view = block_on(registry.invoke_bytes(envelope.bytes(), &responder)).unwrap();
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
        .on_with(spec.clone(), |_params, _ctx| Box::pin(async move { Ok(()) }))
        .unwrap();

    let mut corrupted = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap()
        .bytes()
        .to_vec();
    corrupted.truncate(corrupted.len() - 8);
    corrupted[24..28].copy_from_slice(&(8u32).to_le_bytes());

    let err = match block_on(registry.invoke_bytes(&corrupted, &responder)) {
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
    tx.reject(PeerId(3), b"disk full".to_vec()).unwrap();

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
    let spec_for_handler = spec.clone();

    registry
        .on_with(spec.clone(), move |_params, ctx: DispatchContextView<'_>| {
            let spec = spec_for_handler.clone();
            Box::pin(async move { ctx.accept_with(&spec, &SyncAck { saved_until: 50 }).await })
        })
        .unwrap();

    let envelope = spec
        .dispatch(SyncParams { start: 40, end: 50 })
        .build()
        .unwrap();

    let err = match block_on(registry.invoke_bytes_scoped(
        envelope.bytes(),
        &responder,
        DispatchRoute::leader(false),
    )) {
        Ok(_) => panic!("leader route should not execute follower-scoped handler"),
        Err(err) => err,
    };
    assert!(matches!(err, RaftError::Dispatch(_)));

    let view = block_on(registry.invoke_bytes_scoped(
        envelope.bytes(),
        &responder,
        DispatchRoute::follower(false),
    ))
    .unwrap();
    assert_eq!(view.scope(), DispatchScope::Followers);
}
