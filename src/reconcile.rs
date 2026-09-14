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
use crate::error::{GuardianError, GuardianResult};
use crate::k8s::{K8s, EXTRA_CONF_DIR};
use crate::{metrics, wheelcache};

/// Name of the Secret that holds every rendered Neutron config file as
/// static keys (`ml2_conf.ini`, `neutron.conf`, ...), mounted onto
/// `neutron-server` via `subPath` -- confirmed 2026-09-14 against the live
/// cluster. This is a genuine simplification over the originally-planned
/// `helm upgrade` approach: reconstructing the `neutron` chart from its own
/// Helm release Secret turns out to be a dead end (Helm's `chart.Chart` Go
/// struct keeps subchart data in an *unexported* field, invisible to the
/// release's stored JSON -- confirmed by actually decoding the release
/// Secret and finding `helm-toolkit`/`ovn` subchart templates genuinely
/// absent, and a `helm upgrade --dry-run` against the reconstructed chart
/// failing with exactly that missing-dependency error). Patching this
/// Secret's `ml2_conf.ini` key directly needs no chart at all.
const NEUTRON_ETC_SECRET_NAME: &str = "neutron-etc";
const ML2_CONF_SECRET_KEY: &str = "ml2_conf.ini";

/// The ConfigMap holding `neutron-server`'s actual startup script -- also a
/// static, once-Helm-rendered asset (confirmed 2026-09-14: `neutron-server`
/// is launched via `["/tmp/neutron-server.sh", "start"]`, itself mounted
/// from this ConfigMap, which `exec`s `neutron-server` with a fixed list of
/// `--config-file` flags and no `--config-dir`). Patched the same way as
/// `neutron-etc` above, once, to add a `--config-dir` flag -- after that,
/// wiring in a new driver's `extraConfigSecretData` never needs to touch
/// this script again, since oslo.config loads every `*.conf` file it finds
/// under a `--config-dir` automatically; only the injection patch's volume
/// mounts need to change per driver, not this script.
const NEUTRON_BIN_CONFIGMAP_NAME: &str = "neutron-bin";
const NEUTRON_SERVER_SCRIPT_KEY: &str = "neutron-server.sh";
/// Exact line confirmed live in `neutron-server.sh` -- the last
/// `--config-file` in its `neutron-server` invocation, with no trailing
/// line continuation. Anchoring on this specific text (rather than a
/// generic "last --config-file line" scan) is deliberate: if PCD ever
/// changes this script's shape, silently guessing where to insert a new
/// flag risks corrupting how Neutron starts at all -- better to fail loudly
/// (see `add_config_dir_flag`) than to guess.
const ML2_CONF_FILE_FLAG_LINE: &str = "        --config-file /etc/neutron/plugins/ml2/ml2_conf.ini";

/// After this many consecutive failed repairs for a given driver, stop
/// attempting further repairs until the process restarts (a deliberate,
/// simple policy for a first pass -- see the design doc's "backoff, don't
/// repair-loop" safety rail). A restart is an intentional, visible reset
/// point (a new pod from a rollout, or an operator restarting it after
/// investigating the degraded metric/log).
const MAX_CONSECUTIVE_REPAIR_FAILURES: u32 = 3;

// Confirmed 2026-09-14 against the live cluster (`kubectl get deployment
// neutron-server -n pcd -o jsonpath='{.spec.template.spec.containers[*].name}'`)
// -- hyphenated, not the "neutron_server" underscore form used by the
// chart's own `images.tags.neutron_server` *values* key naming convention.
// Those are two different things (a Helm values path vs. a container
// name) and it's an easy mix-up -- this was originally wrong in this file.
const MAIN_CONTAINER_NAME: &str = "neutron-server";
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
            CheckResult::Absent if cfg.dry_run => {
                metrics::record_present(&driver.name, false);
                // Intentionally does not call repair() at all -- no
                // helm upgrade, no Deployment patch, no Job creation. See
                // Config::dry_run's doc comment and the design doc's
                // "Status" section: this is the safe default until an
                // operator deliberately sets DRY_RUN=false.
                match describe_intended_repair(cfg, k8s, driver).await {
                    Ok(description) => {
                        tracing::warn!(driver = %driver.name, %description, "DRY RUN: driver absent, repair skipped (would apply the above)");
                    }
                    Err(e) => {
                        tracing::warn!(driver = %driver.name, error = %e, "DRY RUN: driver absent, and computing the intended repair also failed (this read-only failure would likely also fail a real repair)");
                    }
                }
                metrics::record_repair_dry_run(&driver.name);
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

/// Rewrites `ml2_conf`'s `mechanism_drivers` line to add `driver_name` if
/// it's missing, computed as *"whatever's currently there, plus this
/// driver"* -- never a hardcoded list -- so a future PCD default change to
/// the other drivers on that line isn't clobbered. Every other line is
/// passed through byte-for-byte unchanged. Errors if no `mechanism_drivers`
/// line exists at all (a malformed/unexpected `ml2_conf.ini` -- safer to
/// fail than guess where to insert one).
fn add_driver_to_ml2_conf(ml2_conf: &str, driver_name: &str) -> GuardianResult<String> {
    let mut found = false;
    let mut out_lines = Vec::new();

    for line in ml2_conf.lines() {
        if line.trim_start().starts_with("mechanism_drivers") {
            found = true;
            let current = line.split('=').nth(1).unwrap_or("");
            let mut drivers: Vec<&str> = current
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            if !drivers.contains(&driver_name) {
                drivers.push(driver_name);
            }
            out_lines.push(format!("mechanism_drivers = {}", drivers.join(",")));
        } else {
            out_lines.push(line.to_string());
        }
    }

    if !found {
        return Err(GuardianError::Config(
            "no mechanism_drivers line found in ml2_conf.ini -- refusing to guess where to insert one".into(),
        ));
    }

    // `ml2_conf.lines()` drops the file's trailing newline (if any); restore
    // one so this doesn't shrink the file by a byte every repair cycle.
    let mut result = out_lines.join("\n");
    if ml2_conf.ends_with('\n') {
        result.push('\n');
    }
    Ok(result)
}

/// Adds a `--config-dir EXTRA_CONF_DIR` line to `neutron-server.sh`'s
/// `neutron-server` invocation, right after the known last `--config-file`
/// line -- a one-time change; once present, per-driver extra config never
/// needs another script edit (see `EXTRA_CONF_DIR`'s doc comment).
/// Idempotent: a no-op if the flag is already there. Errors (rather than
/// guessing) if the anchor line isn't found verbatim -- see
/// `ML2_CONF_FILE_FLAG_LINE`'s doc comment for why that's the safer
/// failure mode here.
fn add_config_dir_flag(script: &str) -> GuardianResult<String> {
    if script.contains(&format!("--config-dir {EXTRA_CONF_DIR}")) {
        return Ok(script.to_string());
    }

    let mut found = false;
    let mut out_lines = Vec::new();
    for line in script.lines() {
        if line == ML2_CONF_FILE_FLAG_LINE {
            found = true;
            out_lines.push(format!("{line} \\"));
            out_lines.push(format!("        --config-dir {EXTRA_CONF_DIR}"));
        } else {
            out_lines.push(line.to_string());
        }
    }

    if !found {
        return Err(GuardianError::Config(format!(
            "neutron-server.sh didn't contain the expected anchor line ({ML2_CONF_FILE_FLAG_LINE:?}) -- refusing to guess where to insert --config-dir"
        )));
    }

    let mut result = out_lines.join("\n");
    if script.ends_with('\n') {
        result.push('\n');
    }
    Ok(result)
}

/// Computes (read-only -- one Secret `get` call, no mutation) a
/// human-readable description of what `repair` would do, for `DRY_RUN`
/// mode. Deliberately mirrors `repair`'s own logic for the mechanism_drivers
/// merge so the dry-run log line reflects the real computed value, not a
/// guess.
async fn describe_intended_repair(
    cfg: &Config,
    k8s: &K8s,
    driver: &DriverSpec,
) -> GuardianResult<String> {
    let current_ml2_conf = k8s
        .get_secret_key(NEUTRON_ETC_SECRET_NAME, ML2_CONF_SECRET_KEY)
        .await?;
    let new_ml2_conf = add_driver_to_ml2_conf(&current_ml2_conf, &driver.name)?;
    let changed = new_ml2_conf != current_ml2_conf;

    let has_extra_config = driver.has_extra_config();
    Ok(format!(
        "patch secret {NEUTRON_ETC_SECRET_NAME}/{ML2_CONF_SECRET_KEY} (mechanism_drivers change: {changed}); \
         has extra config: {has_extra_config}{}; \
         refresh wheel cache for pip package {}; \
         patch deployment {} to inject initContainer {} + PYTHONPATH + extra-config mount, forcing a rollout",
        if has_extra_config {
            format!(
                " (would also ensure --config-dir flag on {NEUTRON_BIN_CONFIGMAP_NAME}/{NEUTRON_SERVER_SCRIPT_KEY} and write its extra-config secret)"
            )
        } else {
            String::new()
        },
        driver.pip_package,
        cfg.deployment_name,
        crate::k8s::INJECTOR_CONTAINER_NAME,
    ))
}

async fn repair(
    cfg: &Config,
    k8s: &K8s,
    client: &Client,
    deployment: &k8s_openapi::api::apps::v1::Deployment,
    driver: &DriverSpec,
) -> GuardianResult<()> {
    // 1. mechanism_drivers: patch the neutron-etc Secret's ml2_conf.ini key
    //    directly (current content + this driver, never a hardcoded list --
    //    see the design doc and add_driver_to_ml2_conf). No Helm involved:
    //    reconstructing the neutron chart from its own release data to run
    //    `helm upgrade` turned out to be a dead end (subchart content isn't
    //    recoverable from the release Secret -- see the module doc comment
    //    above), and this is simpler anyway.
    let current_ml2_conf = k8s
        .get_secret_key(NEUTRON_ETC_SECRET_NAME, ML2_CONF_SECRET_KEY)
        .await?;
    let new_ml2_conf = add_driver_to_ml2_conf(&current_ml2_conf, &driver.name)?;
    if new_ml2_conf != current_ml2_conf {
        k8s.patch_secret_key(NEUTRON_ETC_SECRET_NAME, ML2_CONF_SECRET_KEY, &new_ml2_conf)
            .await?;
    }

    // 2. Driver-specific extra config, if any: write its Secret, and make
    //    sure neutron-server.sh actually loads *.conf files from
    //    EXTRA_CONF_DIR at all (a one-time change per script -- see
    //    add_config_dir_flag; harmless/idempotent to re-check every time a
    //    driver with extra config repairs).
    if driver.has_extra_config() {
        let current_script = k8s
            .get_configmap_key(NEUTRON_BIN_CONFIGMAP_NAME, NEUTRON_SERVER_SCRIPT_KEY)
            .await?;
        let new_script = add_config_dir_flag(&current_script)?;
        if new_script != current_script {
            k8s.patch_configmap_key(
                NEUTRON_BIN_CONFIGMAP_NAME,
                NEUTRON_SERVER_SCRIPT_KEY,
                &new_script,
            )
            .await?;
        }
        k8s.write_driver_config_secret(driver).await?;
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_ML2_CONF: &str = "[ml2]\n\
        extension_drivers = port_security,qos,dns_domain_keywords\n\
        mechanism_drivers = openvswitch,ovn\n\
        path_mtu = 9000\n\
        tenant_network_types = \n";

    #[test]
    fn mechanism_drivers_line_includes_true_and_false() {
        assert!(mechanism_drivers_line_includes(SAMPLE_ML2_CONF, "ovn"));
        assert!(mechanism_drivers_line_includes(
            SAMPLE_ML2_CONF,
            "openvswitch"
        ));
        assert!(!mechanism_drivers_line_includes(SAMPLE_ML2_CONF, "unifi"));
    }

    #[test]
    fn add_driver_to_ml2_conf_appends_when_missing() {
        let updated = add_driver_to_ml2_conf(SAMPLE_ML2_CONF, "unifi").unwrap();
        assert!(mechanism_drivers_line_includes(&updated, "unifi"));
        // Every other line preserved untouched.
        assert!(updated.contains("path_mtu = 9000"));
        assert!(updated.contains("extension_drivers = port_security,qos,dns_domain_keywords"));
        // Existing drivers not dropped.
        assert!(mechanism_drivers_line_includes(&updated, "ovn"));
        assert!(mechanism_drivers_line_includes(&updated, "openvswitch"));
    }

    #[test]
    fn add_driver_to_ml2_conf_is_idempotent() {
        let once = add_driver_to_ml2_conf(SAMPLE_ML2_CONF, "unifi").unwrap();
        let twice = add_driver_to_ml2_conf(&once, "unifi").unwrap();
        assert_eq!(once, twice);
        // Only one occurrence, not appended again.
        let line = twice
            .lines()
            .find(|l| l.trim_start().starts_with("mechanism_drivers"))
            .unwrap();
        assert_eq!(line.matches("unifi").count(), 1);
    }

    #[test]
    fn add_driver_to_ml2_conf_preserves_trailing_newline() {
        let with_newline = "mechanism_drivers = ovn\n";
        let without_newline = "mechanism_drivers = ovn";
        assert!(add_driver_to_ml2_conf(with_newline, "unifi")
            .unwrap()
            .ends_with('\n'));
        assert!(!add_driver_to_ml2_conf(without_newline, "unifi")
            .unwrap()
            .ends_with('\n'));
    }

    #[test]
    fn add_driver_to_ml2_conf_errors_without_mechanism_drivers_line() {
        let result = add_driver_to_ml2_conf("[ml2]\ntype_drivers = vlan\n", "unifi");
        assert!(result.is_err());
    }

    const SAMPLE_NEUTRON_SERVER_SH: &str = r#"#!/bin/bash

function start () {
  exec neutron-server \
        --config-file /etc/neutron/neutron.conf \
        --config-file /tmp/pod-shared/ovn.ini \
        --config-file /etc/neutron/plugins/ml2/ml2_conf.ini
}
"#;

    #[test]
    fn add_config_dir_flag_inserts_after_anchor_line() {
        let updated = add_config_dir_flag(SAMPLE_NEUTRON_SERVER_SH).unwrap();
        assert!(updated.contains(&format!("--config-dir {EXTRA_CONF_DIR}")));
        // The anchor line itself is preserved, just gains a continuation.
        assert!(updated.contains(&format!("{ML2_CONF_FILE_FLAG_LINE} \\")));
        // Everything else untouched.
        assert!(updated.contains("--config-file /etc/neutron/neutron.conf"));
    }

    #[test]
    fn add_config_dir_flag_is_idempotent() {
        let once = add_config_dir_flag(SAMPLE_NEUTRON_SERVER_SH).unwrap();
        let twice = add_config_dir_flag(&once).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn add_config_dir_flag_errors_without_anchor_line() {
        let result = add_config_dir_flag("#!/bin/bash\nexec neutron-server\n");
        assert!(result.is_err());
    }
}
