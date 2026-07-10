mod handle;
mod state;
mod types;

pub use handle::{DispatchHandle, DispatchTx, DispatchTxResponder};
pub use types::{DispatchFailure, DispatchPeerResult, DispatchPeerState, DispatchResult};

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::dispatch::DispatchOptions;
use crate::{PeerId, RaftError};

use state::{DispatchPeerSlot, DispatchShared, DispatchState};
use types::DispatchPeerState as PeerState;

/// Create a new `(DispatchHandle, DispatchTx)` pair for `targets`.
/// Called exclusively by `DispatchEnvelope::begin`.
pub(crate) fn begin_transaction<R>(
    tx_id: u64,
    command: u8,
    options: DispatchOptions,
    targets: impl IntoIterator<Item = PeerId>,
    decode_response: fn(&[u8]) -> Result<R, RaftError>,
) -> (DispatchHandle<R>, DispatchTx<R>)
where
    R: Clone + Send + 'static,
{
    let shared = DispatchShared {
        inner: Arc::new(Mutex::new(DispatchState {
            tx_id,
            command,
            options,
            peers: targets
                .into_iter()
                .map(|peer| DispatchPeerSlot {
                    peer,
                    state: PeerState::Pending,
                })
                .collect(),
            completion: None,
            wakers: Vec::new(),
            start_instant: Instant::now(),
            timeout: options.timeout,
        })),
    };

    let handle = DispatchHandle {
        shared: shared.clone(),
    };
    let tx = DispatchTx {
        shared,
        decode_response,
    };

    {
        let mut guard = handle.shared.inner.lock().unwrap();
        guard.evaluate();
    }

    (handle, tx)
}
