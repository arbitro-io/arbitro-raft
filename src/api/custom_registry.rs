use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::dispatch::{
    DispatchContextView, DispatchRequester, DispatchResponder, DispatchRoute, DispatchScope,
    DispatchSpec, DispatchView,
};
use crate::RaftError;

type DispatchFuture<'a> = Pin<Box<dyn Future<Output = Result<(), RaftError>> + Send + 'a>>;
type ErasedHandler =
    Arc<dyn for<'a> Fn(DispatchView, DispatchContextView<'a>) -> DispatchFuture<'a> + Send + Sync>;

struct DispatchRegistration {
    scope: DispatchScope,
    handler: ErasedHandler,
}

pub struct RaftCustomRegistry {
    handlers: RwLock<Vec<Option<DispatchRegistration>>>,
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
        }
    }

    pub fn on_with<P, R, F>(&self, spec: DispatchSpec<P, R>, handler: F) -> Result<(), RaftError>
    where
        P: Send + 'static,
        R: 'static,
        F: for<'a> Fn(P, DispatchContextView<'a>) -> DispatchFuture<'a> + Send + Sync + 'static,
    {
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
            .map_err(|_| RaftError::Dispatch("dispatch registry write lock poisoned".into()))?;
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
        let guard = self
            .handlers
            .read()
            .map_err(|_| RaftError::Dispatch("dispatch registry read lock poisoned".into()))?;
        Ok(guard[command as usize].is_some())
    }

    pub fn scope_for(&self, command: u8) -> Result<Option<DispatchScope>, RaftError> {
        let guard = self
            .handlers
            .read()
            .map_err(|_| RaftError::Dispatch("dispatch registry read lock poisoned".into()))?;
        Ok(guard[command as usize].as_ref().map(|entry| entry.scope))
    }

    pub async fn invoke_bytes(
        &self,
        frame: Bytes,
        responder: &dyn DispatchResponder,
    ) -> Result<DispatchView, RaftError> {
        self.invoke_bytes_with(frame, responder, None).await
    }

    pub async fn invoke_bytes_with(
        &self,
        frame: Bytes,
        responder: &dyn DispatchResponder,
        requester: Option<&dyn DispatchRequester>,
    ) -> Result<DispatchView, RaftError> {
        self.invoke_bytes_for(frame, responder, requester, None).await
    }

    pub async fn invoke_bytes_scoped(
        &self,
        frame: Bytes,
        responder: &dyn DispatchResponder,
        route: DispatchRoute,
    ) -> Result<DispatchView, RaftError> {
        self.invoke_bytes_for(frame, responder, None, Some(route)).await
    }

    pub async fn invoke_bytes_with_scoped(
        &self,
        frame: Bytes,
        responder: &dyn DispatchResponder,
        requester: Option<&dyn DispatchRequester>,
        route: DispatchRoute,
    ) -> Result<DispatchView, RaftError> {
        self.invoke_bytes_for(frame, responder, requester, Some(route))
            .await
    }

    async fn invoke_bytes_for(
        &self,
        frame: Bytes,
        responder: &dyn DispatchResponder,
        requester: Option<&dyn DispatchRequester>,
        route: Option<DispatchRoute>,
    ) -> Result<DispatchView, RaftError> {
        let dispatch = DispatchView::parse(frame)?;
        let handler = {
            let guard = self
                .handlers
                .read()
                .map_err(|_| RaftError::Dispatch("dispatch registry read lock poisoned".into()))?;
            let entry = guard[dispatch.command() as usize].as_ref().ok_or_else(|| {
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
            entry.handler.clone()
        };
        let ctx = match requester {
            Some(requester) => DispatchContextView::with_requester(
                dispatch.tx_id(),
                dispatch.command(),
                responder,
                requester,
            ),
            None => DispatchContextView::new(dispatch.tx_id(), dispatch.command(), responder),
        };
        handler(dispatch.clone(), ctx).await?;
        Ok(dispatch)
    }
}
