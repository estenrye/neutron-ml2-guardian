//! Observability wiring: a stdout JSON log layer (always on) plus, when
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set, OTLP-over-gRPC export of traces,
//! logs and metrics to an OpenTelemetry Collector. Deliberately mirrors
//! `pdns4-external-dns-rest-http-cr-shim`'s `telemetry::init` almost
//! verbatim (same stack, same env-driven config) -- this guardian has no
//! meaningful HTTP request/response bodies worth logging (its only routes
//! are `/healthz` and `/metrics`), so the body-logging middleware that
//! module also carries was dropped rather than ported.

use std::env;

use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Handles to the OTel providers that must be flushed/shut down on exit. All
/// fields are `None` when OTLP export wasn't configured, in which case only
/// stdout logging runs.
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl Telemetry {
    /// Flushes and shuts down every configured OTel provider so buffered
    /// spans/logs/metrics aren't lost on exit. Best-effort: failures are
    /// printed but never fatal, since the process is already exiting.
    pub fn shutdown(&self) {
        if let Some(p) = &self.tracer_provider {
            if let Err(e) = p.shutdown() {
                eprintln!("error shutting down OTel tracer provider: {e}");
            }
        }
        if let Some(p) = &self.meter_provider {
            if let Err(e) = p.shutdown() {
                eprintln!("error shutting down OTel meter provider: {e}");
            }
        }
        if let Some(p) = &self.logger_provider {
            if let Err(e) = p.shutdown() {
                eprintln!("error shutting down OTel logger provider: {e}");
            }
        }
    }
}

/// Initializes the global `tracing` subscriber and, if configured, the OTel
/// SDK providers. `RUST_LOG` (default `info`) controls verbosity.
pub fn init() -> anyhow::Result<Telemetry> {
    let env_filter =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_filter(env_filter());

    if env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        tracing_subscriber::registry().with(fmt_layer).init();
        return Ok(Telemetry {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
        });
    }

    let resource = build_resource();

    let span_exporter = SpanExporter::builder().with_tonic().build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer("neutron-ml2-guardian");
    let otel_trace_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(env_filter());

    let metric_exporter = MetricExporter::builder().with_tonic().build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    let log_exporter = LogExporter::builder().with_tonic().build()?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource)
        .build();
    let otel_log_layer =
        OpenTelemetryTracingBridge::new(&logger_provider).with_filter(env_filter());

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .init();

    tracing::info!(
        endpoint = %env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default(),
        "OTLP export enabled for traces, logs and metrics"
    );

    Ok(Telemetry {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
        logger_provider: Some(logger_provider),
    })
}

fn build_resource() -> Resource {
    let service_name =
        env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| env!("CARGO_PKG_NAME").to_string());
    Resource::builder()
        .with_service_name(service_name)
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build()
}
