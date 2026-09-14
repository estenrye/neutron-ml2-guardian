//! Thin wrapper around shelling out to the `helm` CLI binary bundled in this
//! container image, matching this whole codebase's (and `pcd-ce-deploy`'s)
//! preference for shelling out to well-tested CLI tools rather than
//! reimplementing their logic against a native SDK -- see the design doc's
//! "Implementation decisions" section for why this was chosen over Go +
//! Helm's native Go SDK.

use std::process::Stdio;

use tokio::process::Command;

use crate::error::{GuardianError, GuardianResult};

/// Runs `helm get values <release> -n <namespace> -a -o json` and returns
/// the parsed computed values.
pub async fn get_values(namespace: &str, release: &str) -> GuardianResult<serde_json::Value> {
    let output = run(&[
        "get", "values", release, "-n", namespace, "-a", "-o", "json",
    ])
    .await?;
    serde_json::from_str(&output)
        .map_err(|e| GuardianError::Helm(format!("parsing `helm get values` output: {e}")))
}

/// Reads `conf.neutron.ml2_conf.ml2.mechanism_drivers` out of a release's
/// computed values. Returns the raw comma-separated string as currently
/// rendered (before the chart's own template default substitution, so a
/// `null` here does not necessarily mean no drivers are loaded -- callers
/// needing the *actual* loaded list should read it from the live pod's
/// rendered `ml2_conf.ini` instead, per the design doc's layered checks).
pub fn mechanism_drivers_value(values: &serde_json::Value) -> Option<String> {
    values
        .pointer("/conf/neutron/ml2_conf/ml2/mechanism_drivers")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Runs `helm upgrade --reuse-values --set <key>=<value> ...` against the
/// release, given an explicit chart reference (a local path to a chart
/// archive/directory -- e.g. one extracted from the release's own stored
/// data via `helm pull`/`helm show chart`, per the design doc's "pull the
/// chart directly out of the release" approach, which avoids any external
/// chart-repo dependency). Answering "where does the chart come from" is
/// left to the call site rather than hidden in this module -- see the
/// design doc's open item about verifying that extraction path against the
/// real cluster.
pub async fn upgrade_set_values_with_chart(
    namespace: &str,
    release: &str,
    chart_ref: &str,
    set_values: &[(&str, &str)],
) -> GuardianResult<()> {
    let mut args: Vec<String> = vec![
        "upgrade".into(),
        release.into(),
        chart_ref.into(),
        "--reuse-values".into(),
        "-n".into(),
        namespace.into(),
    ];
    for (k, v) in set_values {
        args.push("--set".into());
        args.push(format!("{k}={v}"));
    }
    let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    run(&args_ref).await?;
    Ok(())
}

async fn run(args: &[&str]) -> GuardianResult<String> {
    tracing::info!(args = ?args, "running helm");
    let output = Command::new("helm")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| GuardianError::Helm(format!("spawning helm: {e}")))?;

    if !output.status.success() {
        return Err(GuardianError::Helm(format!(
            "helm {:?} exited with {}: {}",
            args,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}
