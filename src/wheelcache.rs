//! Keeps `/wheelcache/<driver>` current: a `pip download` run in a
//! short-lived Job using **the same image as the currently-observed
//! `neutron-server` container** (so downloaded wheels match its Python
//! ABI), off the neutron pod's own startup critical path -- see the design
//! doc's rationale for why this can't just be a live `pip install` inside
//! the injected initContainer itself.
//!
//! The wheelcache PVC lives in the *target* namespace (alongside
//! `neutron-server`, since that's what the injected initContainer needs to
//! mount by `claimName` within its own namespace) -- not the guardian's own
//! namespace. PVCs are namespace-scoped, so the guardian (running
//! elsewhere, per the design doc's "own namespace for RBAC auditability"
//! choice) cannot mount that same PVC object directly. Instead of
//! provisioning cross-namespace storage tricks to work around that, the
//! download Job itself lists what it produced (one filename per line, on
//! its own stdout) and the guardian reads that back via the Kubernetes API
//! (the Job's pod logs) -- no shared filesystem access needed by the
//! guardian at all.

use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use kube::{
    api::{Api, DeleteParams, ListParams, LogParams, PostParams},
    Client,
};
use regex::Regex;
use serde_json::json;

use crate::config::DriverSpec;
use crate::error::{GuardianError, GuardianResult};

const WHEELCACHE_ROOT: &str = "/wheelcache";

/// Whether a driver's cached wheel set needs to be re-downloaded whenever
/// the observed `neutron-server` image's Python version changes, or is safe
/// to reuse forever. Derived empirically from wheel filename tags after a
/// download -- never declared by the operator, see the design doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Portability {
    /// Every wheel in the set is a universal `py3-none-any`-style build.
    UniversalPureAny,
    /// At least one wheel carries a specific Python/ABI/platform tag, tying
    /// the whole set to the exact image it was downloaded against.
    VersionPinned,
}

/// Runs `pip download` for one driver's package (and its transitive
/// dependency closure) in a Job using `image`, writing into
/// `/wheelcache/<driver.name>` on the shared PVC (mounted in the *target*
/// namespace, alongside `neutron-server`). Blocks (polling, not watching --
/// fine at this reconcile cadence) until the Job finishes or `timeout`
/// elapses, then returns the list of `.whl` filenames it produced (read
/// back from the Job pod's logs) before deleting the Job either way.
pub async fn refresh(
    client: &Client,
    namespace: &str,
    pvc_name: &str,
    image: &str,
    driver: &DriverSpec,
    timeout: Duration,
) -> GuardianResult<Vec<String>> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let pods: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(client.clone(), namespace);
    let job_name = format!("neutron-ml2-wheelcache-{}", driver.name);
    let dest = format!("{WHEELCACHE_ROOT}/{}", driver.name);

    // Best-effort cleanup of a stale Job from a previous, possibly-failed
    // attempt before creating a fresh one -- Job names aren't reusable
    // otherwise.
    let _ = jobs.delete(&job_name, &DeleteParams::background()).await;

    // `ls -1` after the download is what the guardian reads back from this
    // Job's pod logs to learn what got downloaded -- see module doc comment
    // for why this replaces a direct filesystem read.
    //
    // `--ignore-requires-python`: found live, 2026-09-14 -- some drivers'
    // own dependencies declare a `Requires-Python` stricter than what
    // their code actually needs (confirmed for `aiohttp-unifi>=86`, which
    // declares `>=3.12.0` but genuinely runs on 3.10 once the handful of
    // missing `typing`/`enum` symbols it uses are back-ported; see
    // `k8s::SITECUSTOMIZE_PY`). Without this flag `pip download` silently
    // discards every version past the last one that happened to declare a
    // compatible constraint, which is why this project's wheel cache kept
    // resolving a stale `unifi-ml2-driver==1.0.5` instead of a newer,
    // already-fixed release.
    // `rm -rf {dest}` before `mkdir -p`: found live, 2026-09-14 -- pip
    // download only ever adds files to `dest`, it never removes anything
    // already there, so switching a driver's pip_package (e.g. to a fixed
    // fork under a different distribution name) left the *old* package's
    // wheels sitting alongside the new ones, with no version relationship
    // pip could see between them. The next repair's install step pulled in
    // both dependency trees together and pip's resolver failed outright on
    // the resulting conflict (two different `tooz` versions "requested").
    // A clean directory every refresh is what actually matches this
    // cache's job: reflect the current resolution, not every historical
    // one.
    let script = format!(
        "set -eu; rm -rf {dest}; mkdir -p {dest}; pip download --no-cache-dir --ignore-requires-python --dest {dest} {}; ls -1 {dest}",
        driver.pip_package
    );

    let job: Job = serde_json::from_value(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": { "name": job_name, "namespace": namespace },
        "spec": {
            "backoffLimit": 1,
            "ttlSecondsAfterFinished": 300,
            "template": {
                "metadata": { "labels": { "job-name": job_name } },
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "pip-download",
                        "image": image,
                        "command": ["/bin/sh", "-c", script],
                        "volumeMounts": [{ "name": "wheelcache", "mountPath": WHEELCACHE_ROOT }],
                    }],
                    "volumes": [{
                        "name": "wheelcache",
                        "persistentVolumeClaim": { "claimName": pvc_name }
                    }]
                }
            }
        }
    }))
    .map_err(|e| GuardianError::Internal(anyhow::anyhow!("building wheelcache Job spec: {e}")))?;

    jobs.create(&PostParams::default(), &job).await?;

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            let _ = jobs.delete(&job_name, &DeleteParams::background()).await;
            return Err(GuardianError::Internal(anyhow::anyhow!(
                "wheelcache download Job {job_name} did not complete within {timeout:?}"
            )));
        }

        let current = jobs.get(&job_name).await?;
        let status = current.status.unwrap_or_default();

        if status.succeeded.unwrap_or(0) > 0 {
            let filenames = read_job_pod_wheel_list(&pods, &job_name).await?;
            let _ = jobs.delete(&job_name, &DeleteParams::background()).await;
            return Ok(filenames);
        }
        if status.failed.unwrap_or(0) > 0 {
            let _ = jobs.delete(&job_name, &DeleteParams::background()).await;
            return Err(GuardianError::Internal(anyhow::anyhow!(
                "wheelcache download Job {job_name} failed"
            )));
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn read_job_pod_wheel_list(
    pods: &Api<k8s_openapi::api::core::v1::Pod>,
    job_name: &str,
) -> GuardianResult<Vec<String>> {
    let list = pods
        .list(&ListParams::default().labels(&format!("job-name={job_name}")))
        .await?;
    let pod_name = list
        .items
        .first()
        .and_then(|p| p.metadata.name.clone())
        .ok_or_else(|| {
            GuardianError::Internal(anyhow::anyhow!("no pod found for job {job_name}"))
        })?;

    let logs = pods
        .logs(&pod_name, &LogParams::default())
        .await
        .map_err(GuardianError::Kube)?;

    Ok(logs
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| l.ends_with(".whl"))
        .collect())
}

/// Classifies a set of wheel filenames (as produced by `refresh`) by
/// inspecting their tags.
pub fn classify(filenames: &[String]) -> GuardianResult<Portability> {
    // Wheel filename format: {name}-{version}(-{build})?-{python tag}-{abi
    // tag}-{platform tag}.whl. A universal wheel has abi tag "none" and
    // platform tag "any" (commonly alongside a "py2.py3"/"py3" python tag).
    let re = Regex::new(r"^.+-.+-([^-]+)-([^-]+)-([^-]+)\.whl$").unwrap();

    if filenames.is_empty() {
        return Err(GuardianError::Internal(anyhow::anyhow!(
            "no .whl filenames produced by download"
        )));
    }

    for filename in filenames {
        match re.captures(filename) {
            Some(caps) => {
                let abi = &caps[2];
                let platform = &caps[3];
                if abi != "none" || platform != "any" {
                    return Ok(Portability::VersionPinned);
                }
            }
            // Unrecognized filename shape -- fail safe by treating it as
            // version-pinned rather than assuming portability.
            None => return Ok(Portability::VersionPinned),
        }
    }
    Ok(Portability::UniversalPureAny)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_all_universal_wheels_is_pure_any() {
        let filenames = vec![
            "unifi_ml2_driver-1.0.0-py3-none-any.whl".to_string(),
            "stevedore-5.2.0-py3-none-any.whl".to_string(),
        ];
        assert_eq!(classify(&filenames).unwrap(), Portability::UniversalPureAny);
    }

    #[test]
    fn classify_one_compiled_wheel_is_version_pinned() {
        let filenames = vec![
            "unifi_ml2_driver-1.0.0-py3-none-any.whl".to_string(),
            "aiohttp-3.9.0-cp310-cp310-manylinux_2_17_x86_64.whl".to_string(),
        ];
        assert_eq!(classify(&filenames).unwrap(), Portability::VersionPinned);
    }

    #[test]
    fn classify_unrecognized_filename_fails_safe_to_version_pinned() {
        let filenames = vec!["not-a-real-wheel-filename".to_string()];
        assert_eq!(classify(&filenames).unwrap(), Portability::VersionPinned);
    }

    #[test]
    fn classify_empty_list_errors() {
        assert!(classify(&[]).is_err());
    }
}
