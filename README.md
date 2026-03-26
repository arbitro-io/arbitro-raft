# arbitro-raft

Transport-agnostic and storage-agnostic Raft core for Arbitro.

`arbitro-raft` contains the consensus model, protocol, node core, orchestrator, and custom
`dispatch` layer. Concrete transports, runtimes, and persistent storages belong in adapter crates.

## What it provides

- leader election
- log replication
- snapshot install/response protocol
- `ArbitroRaft<S, T>` orchestrator
- custom `dispatch` over Raft `Custom` / `CustomResponse` frames
- lazy protocol views over `Bytes`

## Main types

- `RaftNode<S, T>`
  - low-level node core
  - parameterized by `RaftStorage` and `RaftTransport`

- `ArbitroRaft<S, T>`
  - runtime/orchestrator wrapper over `RaftNode`
  - drives election timeout and heartbeat timing
  - exposes `run()`, `run_once()`, `campaign_once()`, `dispatch()`, `propose_once()`

- `DispatchSpec<P, R>`
  - typed custom command definition
  - defines command id, parameter codec, response codec, scope, ack/fail policy, and timeout

- `RaftMessage` / `RaftMessageView`
  - wire protocol message types and lazy views
  - includes `AppendEntries`, `RequestVote`, snapshots, `Custom`, and `CustomResponse`

## Features

### Consensus core

- `campaign_once()`
  - starts a leader election for the local node
- `propose_once()` / `propose_batch_once()`
  - append and replicate entries
- `send_heartbeat_once()`
  - leader heartbeat fanout
- `handle_inbound()`
  - processes a single inbound protocol frame

### Dispatch

`dispatch` is a typed command layer that rides on Raft custom frames.

- typed command registration with `DispatchSpec<P, R>`
- `scope`
  - `Leader`
  - `Followers`
  - `All`
  - `Others`
  - `LocalOnly`
- `ack_policy`
  - `All`
  - `Quorum`
  - `AtLeast`
  - `Percent`
  - `BestEffort`
- `fail_policy`
  - `AllowFailures`
  - `NoFailures`
  - `MaxFailures`
  - `MaxFailurePercent`
  - `FailFast`
- typed handler context with:
  - `accept_bytes()`
  - `accept_with()`
  - `reject()`
  - `fail()`
  - `progress_bytes()`
  - `request_bytes()`
- `request_with()`
- completion via `DispatchHandle`

#### Dispatch options

`DispatchSpec<P, R>` carries the default options for a command:

```rust
let spec = DispatchSpec::new(
    0x31,
    encode_params,
    decode_params,
    encode_response,
    decode_response,
)
.with_scope(DispatchScope::Followers)
.with_ack_policy(DispatchAckPolicy::All)
.with_fail_policy(DispatchFailPolicy::NoFailures)
.with_timeout(std::time::Duration::from_secs(2))
.with_trace(true);
```

Available default setters on the spec:

- `with_scope(...)`
- `with_ack_policy(...)`
- `with_ack_count(...)`
- `with_ack_percent(...)`
- `with_fail_policy(...)`
- `with_fail_count(...)`
- `with_fail_percent(...)`
- `with_timeout(...)`
- `with_trace(...)`

There is also a low-level builder:

```rust
let envelope = spec
    .dispatch(params)
    .scope(DispatchScope::Followers)
    .ack_policy(DispatchAckPolicy::All)
    .ack_count(3)
    .ack_percent(100)
    .fail_policy(DispatchFailPolicy::NoFailures)
    .fail_count(0)
    .fail_percent(0)
    .timeout(std::time::Duration::from_secs(2))
    .trace(true)
    .build()?;
```

Important:

- `raft.dispatch(spec, params)` currently uses the defaults from `DispatchSpec`
- the per-send override builder exists at the `DispatchSpec::dispatch(params)` layer
- the public convenience API does not yet expose a direct `dispatch(envelope)` variant

#### Leader and follower flow

The common pattern is:

1. define one or more typed specs
2. register handlers with `raft.on_with(...)`
3. from the leader, call `raft.dispatch(spec, params).await?`
4. wait on the returned `DispatchHandle`

Leader-only command:

```rust
use arbitro_raft::{
    DispatchAckPolicy, DispatchContextView, DispatchFailPolicy, DispatchScope, DispatchSpec,
    RaftError,
};
use bytes::{BufMut, Bytes, BytesMut};
use std::time::Duration;

#[derive(Clone, Copy)]
struct FetchParams {
    start: u64,
    count: u64,
}

#[derive(Clone)]
struct FetchReply {
    bytes: Bytes,
}

fn encode_fetch_params(value: &FetchParams) -> Result<Bytes, RaftError> {
    let mut out = BytesMut::with_capacity(16);
    out.put_u64_le(value.start);
    out.put_u64_le(value.count);
    Ok(out.freeze())
}

fn decode_fetch_params(bytes: &[u8]) -> Result<FetchParams, RaftError> {
    if bytes.len() != 16 {
        return Err(RaftError::Dispatch("invalid fetch params".into()));
    }
    Ok(FetchParams {
        start: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        count: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    })
}

fn encode_fetch_reply(value: &FetchReply) -> Result<Bytes, RaftError> {
    Ok(value.bytes.clone())
}

fn decode_fetch_reply(bytes: &[u8]) -> Result<FetchReply, RaftError> {
    Ok(FetchReply {
        bytes: Bytes::copy_from_slice(bytes),
    })
}

let fetch_spec = DispatchSpec::new(
    0x41,
    encode_fetch_params,
    decode_fetch_params,
    encode_fetch_reply,
    decode_fetch_reply,
)
.with_scope(DispatchScope::Leader)
.with_ack_policy(DispatchAckPolicy::All)
.with_fail_policy(DispatchFailPolicy::NoFailures)
.with_timeout(Duration::from_secs(2));

raft.on_with(fetch_spec, move |params, ctx: DispatchContextView<'_>| {
    Box::pin(async move {
        let payload = load_bytes(params.start, params.count).await?;
        ctx.accept_with(&fetch_spec, &FetchReply { bytes: payload }).await
    })
})?;
```

Follower-side command:

```rust
#[derive(Clone, Copy)]
struct SyncParams {
    start: u64,
    end: u64,
}

fn encode_sync_params(value: &SyncParams) -> Result<Bytes, RaftError> {
    let mut out = BytesMut::with_capacity(16);
    out.put_u64_le(value.start);
    out.put_u64_le(value.end);
    Ok(out.freeze())
}

fn decode_sync_params(bytes: &[u8]) -> Result<SyncParams, RaftError> {
    if bytes.len() != 16 {
        return Err(RaftError::Dispatch("invalid sync params".into()));
    }
    Ok(SyncParams {
        start: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        end: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    })
}

fn encode_empty(_: &()) -> Result<Bytes, RaftError> {
    Ok(Bytes::new())
}

fn decode_empty(bytes: &[u8]) -> Result<(), RaftError> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(RaftError::Dispatch("expected empty response".into()))
    }
}

let sync_spec = DispatchSpec::new(
    0x31,
    encode_sync_params,
    decode_sync_params,
    encode_empty,
    decode_empty,
)
.with_scope(DispatchScope::Followers)
.with_ack_policy(DispatchAckPolicy::All)
.with_fail_policy(DispatchFailPolicy::NoFailures)
.with_timeout(Duration::from_secs(2));

raft.on_with(sync_spec, move |params, ctx: DispatchContextView<'_>| {
    let fetch_spec = fetch_spec;
    Box::pin(async move {
        let fetch = FetchParams {
            start: params.start,
            count: params.end - params.start,
        };

        let reply = ctx.request_with(&fetch_spec, &fetch).await?;
        persist(reply.bytes).await?;
        ctx.accept_bytes(Bytes::new()).await
    })
})?;
```

Leader sending the command:

```rust
let handle = raft
    .dispatch(
        sync_spec,
        SyncParams {
            start: 40,
            end: 50,
        },
    )
    .await?;

let result = handle.wait().await?;

assert_eq!(result.accepted_count(), 2);
assert_eq!(result.failed_count(), 0);
assert_eq!(result.disconnected_count(), 0);
```

#### Context methods in handlers

Inside `on_with(...)`, `DispatchContextView<'_>` exposes:

- `tx_id()`
- `command()`
- `stream()`
- `request_bytes(...)`
- `request_with(...)`
- `accept_bytes(...)`
- `accept_with(...)`
- `reject(...)`
- `progress_bytes(...)`
- `fail(...)`

`ctx.request_*()` targets the origin of the current dispatch. In the common case:

- leader dispatches `Sync`
- follower handles `Sync`
- follower calls `ctx.request_with(&fetch_spec, ...)`
- that request goes back to the leader

#### What `handle.wait()` returns

`DispatchHandle<R>::wait()` resolves to `DispatchResult<R>`, which contains:

- `tx_id`
- `command`
- `options`
- `peers`

Useful summary helpers:

- `accepted_count()`
- `failed_count()`
- `disconnected_count()`

### Protocol and wire views

The crate exposes lazy views over wire bytes:

- `AppendEntriesView`
- `AppendEntriesRespView`
- `RequestVoteView`
- `RequestVoteRespView`
- `InstallSnapshotView`
- `InstallSnapshotRespView`
- `RaftCustomMessageView`
- `RaftCustomResponseView`
- `DispatchView`
- `DispatchResponseView`

These are designed so the hot path can inspect wire data without eagerly materializing owned structs.

## Design rules

- the core must not depend on TCP, UDP, disk, or a concrete runtime
- the hot path should prefer `Bytes` and lazy views
- benchmarks should use real layers, not reimplement protocol logic in the benchmark itself
- concrete implementations belong outside this crate

## What stays outside this crate

- filesystem storage
- TCP or UDP transports
- runtime-specific builders
- benchmark-only implementations

Adapter crates currently include:

- `arbitro-raft-compio`
- `arbitro-raft-fs`

## Minimal usage

```rust
use arbitro_raft::{ArbitroRaft, NodeConfig, RaftNode};

fn build<S, T>(config: NodeConfig, storage: S, transport: T) -> ArbitroRaft<S, T>
where
    S: arbitro_raft::RaftStorage,
    T: arbitro_raft::RaftTransport,
{
    let node = RaftNode::new(config, storage, transport).unwrap();
    ArbitroRaft::new(node)
}
```

## Dispatch example

```rust
use arbitro_raft::{DispatchScope, DispatchSpec};

fn spec() -> DispatchSpec<(), ()> {
    DispatchSpec::new(0x31, |_| Ok(bytes::Bytes::new()), |_| Ok(()), |_| Ok(bytes::Bytes::new()), |_| Ok(()))
        .with_scope(DispatchScope::Followers)
}
```

## Expected extension model

If you need richer behavior in Raft itself:

- extend the core API
- add protocol types or lazy views
- extend `dispatch`

If you need a concrete environment:

- implement `RaftStorage`
- implement `RaftTransport`
- keep concrete logic out of `arbitro-raft`

## Benchmarks

Last validated benchmark snapshot before this crate was split out of the main workspace:

### Consensus

- in-memory consensus
  - 3 nodes
  - 32B payload
  - 10,000 rounds
  - `~44.35K put/s`

- TCP loopback consensus
  - 3 nodes
  - 32B payload
  - 10,000 rounds
  - `~12.96K put/s` with the futures executor
  - `~12.27K put/s` with the Tokio-style executor

### Dispatch over TCP

- 1 follower
  - empty ACK
  - `~6.72K dispatch/s`
  - `~148.8 us/op`

- 4 followers
  - empty ACK
  - `~3.46K dispatch/s`
  - `~289.1 us/op`

These numbers came from the workspace benchmark harness that exercised the real core,
real Raft custom frames, and real TCP loopback transports. They are useful as a current
reference point, not as a portability guarantee.
