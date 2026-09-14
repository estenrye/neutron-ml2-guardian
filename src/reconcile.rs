//! The reconcile loop: for each configured driver, run the layered
//! "present" check, and repair if it fails -- with backoff so a repair that
//! doesn't actually fix things (e.g. an incompatible new base image) stops
//! being retried every cycle instead of repair-looping forever. See
//! `docs/specs/2026-09-13-neutron-ml2-guardian-design.md` for the full
//! rationale behind every decision in this module.

use std::collections::HashMap;
use std::time::Duration;

use kube::Client;

use crate::config::{Config, DriverSpec};
use crate::error::GuardianResult;
use crate::k8s::K8s;
use crate::{helm, metrics, wheelcache};

/// After this many consecutive failed repairs for a given driver, stop
/// attempting further repairs until the process restarts (a deliberate,
/// simple policy for a first pass -- see the design doc's "backoff, don't
/// repair-loop" safety rail). A restart is an intentional, visible reset
/// point (a new pod from a rollout, or an operator restarting it after
/// investigating the degraded metric/log).
const MAX_CONSECUTIVE_REPAIR_FAILURES: u32 = 3;

const MAIN_CONTAINER_NAME: &str = "neutron_server";
const ML2_CONF_PATH: &str = "/etc/neutron/plugins/ml2/ml2_conf.ini";

#[derive(Default)]
pub struct ReconcileState {
    consecutive_failures: HashMap<String, u32>,
}

pub async fn run_once(
    cfg: &Config,
    k8s: &K8s,
    client: &Client,
    state: &mut ReconcileState,
) -> GuardianResult<()> {
    let deployment = k8s.get_deployment(&cfg.deployment_name).await?;

    for driver in &cfg.drivers {
        let backing_off = state
            .consecutive_failures
            .get(&driver.name)
            .copied()
            .unwrap_or(0)
            >= MAX_CONSECUTIVE_REPAIR_FAILURES;

        if backing_off {
            tracing::warn!(driver = %driver.name, "backing off further repair attempts after repeated failures");
            metrics::record_present(&driver.name, false);
            continue;
        }

        match check_present(cfg, k8s, &deployment, driver).await {
            CheckResult::Present => {
                metrics::record_present(&driver.name, true);
                state.consecutive_failures.remove(&driver.name);
            }
            CheckResult::Degraded(reason) => {
                // Present in config but not actually working -- per the
                // design doc, this must not be treated as "needs repair"
                // the same way "absent" is, since a repair attempt almost
                // certainly won't fix a driver that's crash-looping for
                // reasons unrelated to installation.
                tracing::error!(driver = %driver.name, reason, "driver present but degraded, not attempting repair");
                metrics::record_present(&driver.name, false);
            }
            CheckResult::Absent => {
                metrics::record_present(&driver.name, false);
                tracing::info!(driver = %driver.name, "driver absent, repairing");
                match repair(cfg, k8s, client, &deployment, driver).await {
                    Ok(()) => {
                        metrics::record_repair(&driver.name, true);
                        // Re-check after repair before declaring success --
                        // a `helm upgrade`/`kubectl patch` exiting 0 doesn't
                        // mean the driver actually loaded.
                        match check_present(cfg, k8s, &deployment, driver).await {
                            CheckResult::Present => {
                                state.consecutive_failures.remove(&driver.name);
                                tracing::info!(driver = %driver.name, "repair confirmed");
                            }
                            other => {
                                bump_failure(state, &driver.name);
                                metrics::record_repair_failure(&driver.name);
                                tracing::error!(driver = %driver.name, check = other_debug(&other), "repair applied but post-repair check still failing");
                            }
                        }
                    }
                    Err(e) => {
                        metrics::record_repair(&driver.name, false);
                        bump_failure(state, &driver.name);
                        tracing::error!(driver = %driver.name, error = %e, "repair attempt failed");
                    }
                }
            }
        }
    }

    Ok(())
}

fn bump_failure(state: &mut ReconcileState, driver: &str) {
    *state
        .consecutive_failures
        .entry(driver.to_string())
        .or_insert(0) += 1;
}

enum CheckResult {
    Present,
    Absent,
    Degraded(&'static str),
}

fn other_debug(r: &CheckResult) -> &'static str {
    match r {
        CheckResult::Present => "present",
        CheckResult::Absent => "absent",
        CheckResult::Degraded(_) => "degraded",
    }
}

async fn check_present(
    _cfg: &Config,
    k8s: &K8s,
    deployment: &k8s_openapi::api::apps::v1::Deployment,
    driver: &DriverSpec,
) -> CheckResult {
    // Layer 1: injection present in the Deployment spec.
    if !K8s::has_injection(deployment) {
        return CheckResult::Absent;
    }

    // Layer 3 (checked before layer 2 deliberately -- no point exec'ing
    // into a pod that isn't Ready): pod health.
    let pod_name = match k8s.find_ready_pod(deployment).await {
        Ok(Some(name)) => name,
        Ok(None) => return CheckResult::Degraded("no Ready pod found for deployment"),
        Err(e) => {
            tracing::error!(error = %e, "error finding ready pod");
            return CheckResult::Degraded("error finding ready pod");
        }
    };

    // Layer 2: config actually lists the driver, and it actually imports.
    let ml2_conf = match k8s
        .exec_capture(&pod_name, MAIN_CONTAINER_NAME, &["cat", ML2_CONF_PATH])
        .await
    {
        Ok(out) => out,
        Err(e) => {
            tracing::error!(error = %e, "error reading ml2_conf.ini");
            return CheckResult::Degraded("error reading ml2_conf.ini");
        }
    };
    if !mechanism_drivers_line_includes(&ml2_conf, &driver.name) {
        return CheckResult::Absent;
    }

    let import_check = format!("python3 -c \"import {}\"", driver.import_module);
    match k8s
        .exec_capture(
            &pod_name,
            MAIN_CONTAINER_NAME,
            &["/bin/sh", "-c", &import_check],
        )
        .await
    {
        Ok(_) => CheckResult::Present,
        Err(_) => CheckResult::Degraded("driver listed in mechanism_drivers but import failed"),
    }
}

fn mechanism_drivers_line_includes(ml2_conf: &str, driver_name: &str) -> bool {
    ml2_conf
        .lines()
        .find(|l| l.trim_start().starts_with("mechanism_drivers"))
        .map(|l| {
            l.split('=')
                .nth(1)
                .unwrap_or("")
                .split(',')
                .any(|d| d.trim() == driver_name)
        })
        .unwrap_or(false)
}

async fn repair(
    cfg: &Config,
    k8s: &K8s,
    client: &Client,
    deployment: &k8s_openapi::api::apps::v1::Deployment,
    driver: &DriverSpec,
) -> GuardianResult<()> {
    // 1. mechanism_drivers: current list (from live values) + this driver,
    //    never a hardcoded string -- see the design doc.
    let values = helm::get_values(&cfg.target_namespace, &cfg.helm_release_name).await?;
    let current = helm::mechanism_drivers_value(&values).unwrap_or_default();
    let mut drivers: Vec<&str> = current
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if !drivers.contains(&driver.name.as_str()) {
        drivers.push(&driver.name);
    }
    let new_value = drivers.join(",");

    // NOTE: the chart reference for `helm upgrade` (pulled from the
    // release's own stored chart, per the design doc) is not yet
    // implemented here -- this is the one piece that needs verifying
    // against the real cluster (`helm get metadata`/`helm pull` semantics)
    // before this can run for real. Left as an explicit gap rather than a
    // guess.
    let chart_ref = format!("oci://unresolved/{}", cfg.helm_release_name);
    helm::upgrade_set_values_with_chart(
        &cfg.target_namespace,
        &cfg.helm_release_name,
        &chart_ref,
        &[("conf.neutron.ml2_conf.ml2.mechanism_drivers", &new_value)],
    )
    .await?;

    // 2. Driver-specific extra config, if any.
    k8s.write_driver_config_secret(driver).await?;

    // 3. Wheel cache, refreshed against whatever image is currently
    //    running, then classified so future ticks know whether a refresh
    //    is needed again on the next image change.
    let image = K8s::main_container_image(deployment, MAIN_CONTAINER_NAME)
        .ok_or_else(|| anyhow::anyhow!("deployment has no {MAIN_CONTAINER_NAME} container"))?;
    let wheel_filenames = wheelcache::refresh(
        client,
        &cfg.target_namespace,
        &cfg.wheelcache_pvc_name,
        &image,
        driver,
        Duration::from_secs(300),
    )
    .await?;
    match wheelcache::classify(&wheel_filenames) {
        Ok(portability) => {
            tracing::info!(driver = %driver.name, ?portability, "wheel cache refreshed")
        }
        Err(e) => {
            tracing::warn!(driver = %driver.name, error = %e, "could not classify wheel cache after refresh")
        }
    }

    // 4. Re-apply the initContainer/volume/PYTHONPATH injection -- not
    //    preserved by the helm upgrade above, must be reapplied every time.
    k8s.apply_injection_patch(
        &cfg.deployment_name,
        MAIN_CONTAINER_NAME,
        &cfg.wheelcache_pvc_name,
        &cfg.drivers,
    )
    .await?;

    Ok(())
}
