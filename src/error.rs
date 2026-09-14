//! Unified error type for the guardian's reconcile loop and health/metrics
//! server. Unlike a request-serving shim, nothing here needs to render an
//! HTTP error body for a caller -- reconcile failures are logged and fed
//! into the backoff logic in `reconcile`, not returned to anyone.

#[derive(thiserror::Error, Debug)]
pub enum GuardianError {
    #[error("kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("helm command failed: {0}")]
    Helm(String),

    #[error("pod exec failed: {0}")]
    PodExec(String),

    #[error("driver config error: {0}")]
    Config(String),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

pub type GuardianResult<T> = Result<T, GuardianError>;
