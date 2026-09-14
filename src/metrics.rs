//! Metric instruments recorded twice, deliberately: once via the OTel SDK
//! (exported over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set) and once
//! via a plain `prometheus::Registry`, gathered directly into Prometheus
//! text-exposition format for `/metrics` (see `main::metrics_handler`). Two
//! separate instrumentation calls per event, not one bridged through the
//! other: the `opentelemetry`/`prometheus` crate ecosystems move at
//! different paces, and pinning both directly (rather than a bridging crate
//! that has to track both) is the more stable choice for a controller that
//! isn't itself pinned to any particular OTel SDK version. The design doc's
//! point stands either way -- this guardian doesn't need its own
//! observability stack, it rides on whatever Prometheus already scrapes the
//! target cluster. Every metric here is per-driver (`driver` label) so a
//! cluster running several guarded drivers gets independent signal for
//! each.

use std::sync::LazyLock;

use opentelemetry::{
    global,
    metrics::{Counter, Gauge},
    KeyValue,
};
use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};

struct Instruments {
    driver_present: Gauge<u64>,
    repairs_total: Counter<u64>,
    repair_failures_total: Counter<u64>,
    reconcile_runs_total: Counter<u64>,
}

static INSTRUMENTS: LazyLock<Instruments> = LazyLock::new(|| {
    let meter = global::meter("neutron-ml2-guardian");
    Instruments {
        driver_present: meter
            .u64_gauge("ml2_driver_present")
            .with_description(
                "1 if the driver's layered present-check passes, 0 otherwise, by driver",
            )
            .build(),
        repairs_total: meter
            .u64_counter("ml2_driver_repairs_total")
            .with_description("Repair attempts applied, by driver and outcome (applied/skipped)")
            .build(),
        repair_failures_total: meter
            .u64_counter("ml2_driver_repair_failures_total")
            .with_description(
                "Repairs applied but that failed the post-repair present-check, by driver",
            )
            .build(),
        reconcile_runs_total: meter
            .u64_counter("ml2_guardian_reconcile_runs_total")
            .with_description("Reconcile loop iterations, by outcome")
            .build(),
    }
});

struct PromInstruments {
    registry: Registry,
    driver_present: IntGaugeVec,
    repairs_total: IntCounterVec,
    repair_failures_total: IntCounterVec,
    reconcile_runs_total: IntCounterVec,
}

static PROM: LazyLock<PromInstruments> = LazyLock::new(|| {
    let registry = Registry::new();

    let driver_present = IntGaugeVec::new(
        Opts::new(
            "ml2_driver_present",
            "1 if the driver's layered present-check passes, 0 otherwise, by driver",
        ),
        &["driver"],
    )
    .unwrap();
    let repairs_total = IntCounterVec::new(
        Opts::new(
            "ml2_driver_repairs_total",
            "Repair attempts applied, by driver and outcome (applied/skipped/dry_run)",
        ),
        &["driver", "outcome"],
    )
    .unwrap();
    let repair_failures_total = IntCounterVec::new(
        Opts::new(
            "ml2_driver_repair_failures_total",
            "Repairs applied but that failed the post-repair present-check, by driver",
        ),
        &["driver"],
    )
    .unwrap();
    let reconcile_runs_total = IntCounterVec::new(
        Opts::new(
            "ml2_guardian_reconcile_runs_total",
            "Reconcile loop iterations, by outcome",
        ),
        &["outcome"],
    )
    .unwrap();

    registry
        .register(Box::new(driver_present.clone()))
        .expect("registering ml2_driver_present");
    registry
        .register(Box::new(repairs_total.clone()))
        .expect("registering ml2_driver_repairs_total");
    registry
        .register(Box::new(repair_failures_total.clone()))
        .expect("registering ml2_driver_repair_failures_total");
    registry
        .register(Box::new(reconcile_runs_total.clone()))
        .expect("registering ml2_guardian_reconcile_runs_total");

    PromInstruments {
        registry,
        driver_present,
        repairs_total,
        repair_failures_total,
        reconcile_runs_total,
    }
});

/// The registry `main::metrics_handler` gathers into Prometheus text format
/// for `/metrics`.
pub fn registry() -> &'static Registry {
    &PROM.registry
}

pub fn record_present(driver: &str, present: bool) {
    INSTRUMENTS.driver_present.record(
        present as u64,
        &[KeyValue::new("driver", driver.to_string())],
    );
    PROM.driver_present
        .with_label_values(&[driver])
        .set(present as i64);
}

pub fn record_repair(driver: &str, applied: bool) {
    let outcome = if applied { "applied" } else { "skipped" };
    INSTRUMENTS.repairs_total.add(
        1,
        &[
            KeyValue::new("driver", driver.to_string()),
            KeyValue::new("outcome", outcome),
        ],
    );
    PROM.repairs_total
        .with_label_values(&[driver, outcome])
        .inc();
}

/// A repair that was computed but deliberately not applied (`DRY_RUN=true`,
/// the default -- see `Config::dry_run`). Distinct from `record_repair`'s
/// "skipped" outcome, which means "nothing needed doing" rather than
/// "something needed doing but was withheld."
pub fn record_repair_dry_run(driver: &str) {
    INSTRUMENTS.repairs_total.add(
        1,
        &[
            KeyValue::new("driver", driver.to_string()),
            KeyValue::new("outcome", "dry_run"),
        ],
    );
    PROM.repairs_total
        .with_label_values(&[driver, "dry_run"])
        .inc();
}

pub fn record_repair_failure(driver: &str) {
    INSTRUMENTS
        .repair_failures_total
        .add(1, &[KeyValue::new("driver", driver.to_string())]);
    PROM.repair_failures_total
        .with_label_values(&[driver])
        .inc();
}

pub fn record_reconcile_run(outcome: &'static str) {
    INSTRUMENTS
        .reconcile_runs_total
        .add(1, &[KeyValue::new("outcome", outcome)]);
    PROM.reconcile_runs_total
        .with_label_values(&[outcome])
        .inc();
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, TextEncoder};

    /// End-to-end smoke test for the actual `/metrics` code path: record
    /// through the public API, gather the registry, encode it, and check
    /// the result is what a Prometheus scraper actually expects -- not just
    /// that the registration calls in `PROM`'s `LazyLock` don't panic.
    #[test]
    fn recorded_metrics_gather_into_valid_prometheus_text() {
        record_present("unifi-smoke-test", true);
        record_repair("unifi-smoke-test", true);
        record_repair_dry_run("unifi-smoke-test");
        record_repair_failure("unifi-smoke-test");
        record_reconcile_run("success");

        let metric_families = registry().gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let text = String::from_utf8(buffer).unwrap();

        assert!(text.contains("ml2_driver_present{driver=\"unifi-smoke-test\"} 1"));
        assert!(text.contains(
            "ml2_driver_repairs_total{driver=\"unifi-smoke-test\",outcome=\"applied\"} 1"
        ));
        assert!(text.contains(
            "ml2_driver_repairs_total{driver=\"unifi-smoke-test\",outcome=\"dry_run\"} 1"
        ));
        assert!(text.contains("ml2_driver_repair_failures_total{driver=\"unifi-smoke-test\"} 1"));
        assert!(text.contains("ml2_guardian_reconcile_runs_total{outcome=\"success\"} 1"));
    }
}
