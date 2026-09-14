mod config;
mod error;
mod helm;
mod k8s;
mod metrics;
mod reconcile;
mod telemetry;
mod wheelcache;

use axum::{routing::get, Router};
use kube::Client;

use config::Config;
use k8s::K8s;
use reconcile::ReconcileState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let telemetry = telemetry::init()?;

    let config = Config::from_env()?;
    tracing::info!(
        namespace = %config.target_namespace,
        release = %config.helm_release_name,
        deployment = %config.deployment_name,
        drivers = ?config.drivers.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
        reconcile_interval_secs = config.reconcile_interval_secs,
        dry_run = config.dry_run,
        "starting neutron-ml2-guardian"
    );
    if config.dry_run {
        tracing::warn!("DRY_RUN=true: repairs will be logged but not applied -- set DRY_RUN=false to enable real repairs");
    }

    let client = Client::try_default().await?;
    let k8s = K8s::try_default(&config.target_namespace).await?;

    let listen_addr = config.listen_addr.clone();
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler));

    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    tracing::info!(addr = %listen_addr, "health/metrics listener bound");
    let server = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "health/metrics server exited");
        }
    });

    let reconcile_loop = tokio::spawn(async move {
        let mut state = ReconcileState::default();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            config.reconcile_interval_secs,
        ));
        loop {
            interval.tick().await;
            match reconcile::run_once(&config, &k8s, &client, &mut state).await {
                Ok(()) => metrics::record_reconcile_run("success"),
                Err(e) => {
                    tracing::error!(error = %e, "reconcile run failed");
                    metrics::record_reconcile_run("error");
                }
            }
        }
    });

    tokio::select! {
        _ = shutdown_signal() => {},
        _ = server => {},
        _ = reconcile_loop => {},
    }

    telemetry.shutdown();
    Ok(())
}

/// Placeholder `/metrics` handler. OTLP metric export (when
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is set) already ships metrics to a
/// collector independent of this route; a real Prometheus-scrape-format
/// endpoint (via `opentelemetry-prometheus` or a dedicated `prometheus`
/// crate registry) is a follow-up -- see the design doc's "Observability
/// rides on PCD's own stack" note, which assumes this route exists and is
/// scraped directly, not yet wired up in this first pass.
async fn metrics_handler() -> &'static str {
    "# neutron-ml2-guardian: Prometheus text-format export not yet implemented; use OTLP export via OTEL_EXPORTER_OTLP_ENDPOINT in the meantime.\n"
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received");
}
