use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};

use crate::dispatch::{
    DispatchContextView, DispatchRequester, DispatchResponder, DispatchRoute, DispatchScope,
    DispatchSpec, DispatchView,
};
use crate::RaftError;

type DispatchFuture<'a> = Pin<Box<dyn Future<Output = Result<(), RaftError>> + Send + 'a>>;
type ErasedHandler = Arc<
    dyn for<'a> Fn(DispatchView<'a>, DispatchContextView<'a>) -> DispatchFuture<'a> + Send + Sync,
>;

struct DispatchRegistration {
    scope: DispatchScope,
    handler: ErasedHandler,
}

/// Slot used in the sealed (frozen) fast-path array.
type SealedSlot = Option<(DispatchScope, ErasedHandler)>;

pub struct RaftCustomRegistry {
    /// Mutable registration state — written only before `seal()`.
    handlers: RwLock<Vec<Option<DispatchRegistration>>>,
    /// Frozen read-only copy — populated by `seal()`.
    /// Once set, `invoke_bytes_for` reads from here without any lock.
    sealed: OnceLock<Box<[SealedSlot]>>,
}

impl Default for RaftCustomRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftCustomRegistry {
    pub fn new() -> Self {
        Self {
            handlers: RwLock::new(std::iter::repeat_with(|| None).take(256).collect()),
            sealed: OnceLock::new(),
        }
    }

    /// Freeze the handler table.  Call this once, before the main loop starts.
    /// After sealing, `invoke_bytes_for` reads from the lock-free array.
    pub fn seal(&self) -> Result<(), RaftError> {
        let guard = self
            .handlers
            .read()
            .map_err(|_| RaftError::Dispatch("lock poisoned".into()))?;
        let frozen: Box<[SealedSlot]> = guard
            .iter()
            .map(|entry| entry.as_ref().map(|e| (e.scope, Arc::clone(&e.handler))))
            .collect();
        // Ignore if already sealed — idempotent.
        let _ = self.sealed.set(frozen);
        Ok(())
    }

    pub fn on_with<P, R, F>(&self, spec: DispatchSpec<P, R>, handler: F) -> Result<(), RaftError>
    where
        P: Send + 'static,
        R: 'static,
        F: for<'a> Fn(P, DispatchContextView<'a>) -> DispatchFuture<'a> + Send + Sync + 'static,
    {
        if self.sealed.get().is_some() {
            return Err(RaftError::Dispatch(
                "cannot register handlers after seal()".into(),
            ));
        }
        let command = spec.command();
        let scope = spec.defaults().scope;
        let spec_for_handler = spec;
        let erased: ErasedHandler = Arc::new(move |dispatch, ctx| {
            let params = match dispatch.params(&spec_for_handler) {
                Ok(params) => params,
                Err(err) => {
                    return Box::pin(async move {
                        ctx.fail(err.to_string()).await?;
                        Err(err)
                    });
                }
            };
            handler(params, ctx)
        });

        let mut guard = self
            .handlers
            .write()
            .map_err(|_| RaftError::Dispatch("lock poisoned".into()))?;
        if guard[command as usize].is_some() {
            return Err(RaftError::Dispatch(format!(
                "dispatch command {} already registered",
                command
            )));
        }
        guard[command as usize] = Some(DispatchRegistration {
            scope,
            handler: erased,
        });
        Ok(())
    }

    pub fn contains(&self, command: u8) -> Result<bool, RaftError> {
        if let Some(sealed) = self.sealed.get() {
            return Ok(sealed[command as usize].is_some());
        }
        let guard = self
            .handlers
            .read()
            .map_err(|_| RaftError::Dispatch("lock poisoned".into()))?;
        Ok(guard[command as usize].is_some())
    }

    pub fn scope_for(&self, command: u8) -> Result<Option<DispatchScope>, RaftError> {
        if let Some(sealed) = self.sealed.get() {
            return Ok(sealed[command as usize].as_ref().map(|(scope, _)| *scope));
        }
        let guard = self
            .handlers
            .read()
            .map_err(|_| RaftError::Dispatch("lock poisoned".into()))?;
        Ok(guard[command as usize].as_ref().map(|e| e.scope))
    }

    pub async fn invoke_bytes<'a>(
        &self,
        frame: &'a [u8],
        responder: &dyn DispatchResponder,
    ) -> Result<DispatchView<'a>, RaftError> {
        self.invoke_bytes_for(frame, responder, None, None).await
    }

    pub async fn invoke_bytes_with<'a>(
        &self,
        frame: &'a [u8],
        responder: &dyn DispatchResponder,
        requester: Option<&dyn DispatchRequester>,
    ) -> Result<DispatchView<'a>, RaftError> {
        self.invoke_bytes_for(frame, responder, requester, None)
            .await
    }

    pub async fn invoke_bytes_scoped<'a>(
        &self,
        frame: &'a [u8],
        responder: &dyn DispatchResponder,
        route: DispatchRoute,
    ) -> Result<DispatchView<'a>, RaftError> {
        self.invoke_bytes_for(frame, responder, None, Some(route))
            .await
    }

    pub async fn invoke_bytes_with_scoped<'a>(
        &self,
        frame: &'a [u8],
        responder: &dyn DispatchResponder,
        requester: Option<&dyn DispatchRequester>,
        route: DispatchRoute,
    ) -> Result<DispatchView<'a>, RaftError> {
        self.invoke_bytes_for(frame, responder, requester, Some(route))
            .await
    }

    async fn invoke_bytes_for<'a>(
        &self,
        frame: &'a [u8],
        responder: &dyn DispatchResponder,
        requester: Option<&dyn DispatchRequester>,
        route: Option<DispatchRoute>,
    ) -> Result<DispatchView<'a>, RaftError> {
        let dispatch = DispatchView::parse(frame)?;
        let cmd = dispatch.command() as usize;

        let ctx = match requester {
            Some(req) => DispatchContextView::with_requester(
                dispatch.tx_id(),
                dispatch.command(),
                responder,
                req,
            ),
            None => DispatchContextView::new(dispatch.tx_id(), dispatch.command(), responder),
        };

        // Sealed fast path: borrow straight out of the frozen, lock-free
        // array. The slot is immutable once `seal()` has run, so there is
        // no need to pay for an `Arc::clone` just to escape a lock guard —
        // there is no guard to escape.
        if let Some(sealed) = self.sealed.get() {
            let entry = sealed[cmd].as_ref().ok_or_else(|| {
                RaftError::Dispatch(format!(
                    "no dispatch handler registered for command {}",
                    dispatch.command()
                ))
            })?;
            if let Some(route) = route {
                if !entry.0.allows(route) {
                    return Err(RaftError::Dispatch(format!(
                        "dispatch command {} not allowed for route {:?}",
                        dispatch.command(),
                        route
                    )));
                }
            }
            let handler = &entry.1;
            handler(dispatch, ctx).await?;
            return Ok(dispatch);
        }

        // Pre-seal path: the RwLock guard cannot be held across an `.await`,
        // so the Arc must be cloned to escape it.
        let handler: ErasedHandler = {
            let guard = self
                .handlers
                .read()
                .map_err(|_| RaftError::Dispatch("lock poisoned".into()))?;
            let entry = guard[cmd].as_ref().ok_or_else(|| {
                RaftError::Dispatch(format!(
                    "no dispatch handler registered for command {}",
                    dispatch.command()
                ))
            })?;
            if let Some(route) = route {
                if !entry.scope.allows(route) {
                    return Err(RaftError::Dispatch(format!(
                        "dispatch command {} not allowed for route {:?}",
                        dispatch.command(),
                        route
                    )));
                }
            }
            Arc::clone(&entry.handler)
        };

        handler(dispatch, ctx).await?;
        Ok(dispatch)
    }
}
