//! Metric instruments exported via OTLP and scraped directly off `/metrics`
//! by whatever Prometheus is already running in the target cluster (the
//! design doc's point: this guardian doesn't need its own observability
//! stack, it rides on the one already scraping the `pcd` namespace). Every
//! metric here is per-driver (`driver` label) so a cluster running several
//! guarded drivers gets independent signal for each.

use std::sync::LazyLock;

use opentelemetry::{
    global,
    metrics::{Counter, Gauge},
    KeyValue,
};

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

pub fn record_present(driver: &str, present: bool) {
    INSTRUMENTS.driver_present.record(
        present as u64,
        &[KeyValue::new("driver", driver.to_string())],
    );
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
}

pub fn record_repair_failure(driver: &str) {
    INSTRUMENTS
        .repair_failures_total
        .add(1, &[KeyValue::new("driver", driver.to_string())]);
}

pub fn record_reconcile_run(outcome: &'static str) {
    INSTRUMENTS
        .reconcile_runs_total
        .add(1, &[KeyValue::new("outcome", outcome)]);
}
