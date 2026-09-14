//! Runtime configuration. Two sources, matching the split between "this
//! controller's own settings" and "the list of drivers it's guarding":
//!
//! - Scalar settings (namespace, release name, reconcile interval, ...) come
//!   from environment variables, same as `pdns4-external-dns-rest-http-cr-shim`.
//! - The `ml2Drivers` list (see `docs/specs/2026-09-13-neutron-ml2-guardian-design.md`)
//!   is a YAML file mounted from a Secret (`deploy/helm/.../templates/
//!   secret-drivers.yaml`) rather than a ConfigMap, since a driver's
//!   `extraConfigSecretData` commonly embeds real credentials.

use std::{env, fs, path::PathBuf};

use serde::Deserialize;

/// One entry in `ml2Drivers`. See the design doc for the full rationale;
/// summarized here: everything the guardian needs to know about a driver,
/// without needing to understand what the driver's own config *means*.
#[derive(Clone, Debug, Deserialize)]
pub struct DriverSpec {
    /// Short id. Used as: the `mechanism_drivers` entry Neutron loads, the
    /// wheel-cache subdirectory name, and the extra-config-file basename.
    /// Must be the driver's actual registered `neutron.ml2.mechanism_drivers`
    /// stevedore name -- the guardian does not discover this automatically.
    pub name: String,

    /// pip-installable package spec, e.g. `"unifi-ml2-driver"` or
    /// `"unifi-ml2-driver==1.2.3"`.
    pub pip_package: String,

    /// Top-level Python module to test-import as the "actually imports"
    /// health check (pip package names and import module names frequently
    /// differ -- e.g. dashes become underscores, or the name differs
    /// entirely).
    pub import_module: String,

    /// Opaque INI content this driver needs in its own Neutron config
    /// section(s) (e.g. a `[unifi]` block with a controller URL/API key, or
    /// a `[genericswitch:leaf1]` block). Never parsed or validated by the
    /// guardian -- written verbatim into a per-driver Secret and mounted as
    /// an extra `--config-file` on `neutron-server`. May be empty for a
    /// driver that needs no extra config beyond being listed in
    /// `mechanism_drivers`.
    #[serde(default)]
    pub extra_config_secret_data: String,

    /// Set for a driver that is *not* designed to coexist with other
    /// `sole_driver`-flagged entries (most physical-switch/add-on ML2
    /// drivers, including `networking-generic-switch` and
    /// `networking-unifi`, are additive and should leave this `false`).
    /// The guardian refuses to combine two `sole_driver` entries rather than
    /// silently producing a `mechanism_drivers` list neither expects.
    #[serde(default)]
    pub sole_driver: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct DriverSpecFile {
    #[serde(rename = "ml2Drivers")]
    ml2_drivers: Vec<DriverSpec>,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Address the health/metrics HTTP server binds to.
    pub listen_addr: String,

    /// Namespace the guarded `neutron-server` Deployment and its Helm
    /// release live in. PCD's own installer puts this in `pcd`, but this is
    /// not hardcoded -- see the design doc's note that the whole `pcd`
    /// namespace is just this cluster's convention, not a Neutron/PCD
    /// constant.
    pub target_namespace: String,

    /// Name of the Helm release to guard (the design doc assumes `neutron`
    /// throughout, but this is a value, never a literal, in the code).
    pub helm_release_name: String,

    /// Name of the Deployment the release manages that runs `neutron-server`
    /// (distinct from the release name -- see the chart's own resource
    /// naming, which the guardian must match against the real cluster
    /// rather than assume).
    pub deployment_name: String,

    /// Name of the ReadWriteMany PVC used as the shared wheel cache.
    pub wheelcache_pvc_name: String,

    /// How often the reconcile loop runs.
    pub reconcile_interval_secs: u64,

    /// When true (the default), a detected-absent driver is logged with
    /// exactly what repair *would* do (the computed `mechanism_drivers`
    /// value, the injection patch, the wheel-cache refresh) without
    /// actually calling `helm upgrade`, patching the Deployment, or
    /// creating the download Job. Given the guardian's RBAC grant is
    /// effectively "can run `helm upgrade neutron` and rewrite its
    /// Deployment's pod spec," defaulting to dry-run means a fresh
    /// deployment of this chart is safe to install and observe before
    /// anyone deliberately flips it to `false`. See the design doc's
    /// "Status" section.
    pub dry_run: bool,

    /// The drivers this guardian is responsible for keeping present.
    pub drivers: Vec<DriverSpec>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
        let target_namespace = env::var("TARGET_NAMESPACE").unwrap_or_else(|_| "pcd".to_string());
        let helm_release_name =
            env::var("HELM_RELEASE_NAME").unwrap_or_else(|_| "neutron".to_string());
        let deployment_name =
            env::var("DEPLOYMENT_NAME").unwrap_or_else(|_| "neutron-server".to_string());
        let wheelcache_pvc_name = env::var("WHEELCACHE_PVC_NAME")
            .unwrap_or_else(|_| "neutron-ml2-wheelcache".to_string());
        let reconcile_interval_secs = env::var("RECONCILE_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        // Defaults to safe (true): an operator must deliberately opt in to
        // letting this controller actually mutate the target release.
        let dry_run = env::var("DRY_RUN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(true);

        let drivers_file = env::var("DRIVERS_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/neutron-ml2-guardian/drivers.yaml".to_string());
        let drivers = load_drivers(&drivers_file)?;

        if drivers.is_empty() {
            tracing::warn!(
                path = %drivers_file,
                "no ml2Drivers configured -- guardian will run but never find anything to repair"
            );
        }

        let sole_drivers: Vec<&str> = drivers
            .iter()
            .filter(|d| d.sole_driver)
            .map(|d| d.name.as_str())
            .collect();
        if sole_drivers.len() > 1 {
            anyhow::bail!(
                "more than one driver is flagged sole_driver ({:?}) -- these can't be combined, fix the drivers config",
                sole_drivers
            );
        }

        Ok(Config {
            listen_addr,
            target_namespace,
            helm_release_name,
            deployment_name,
            wheelcache_pvc_name,
            reconcile_interval_secs,
            dry_run,
            drivers,
        })
    }
}

fn load_drivers(path: &str) -> anyhow::Result<Vec<DriverSpec>> {
    let path = PathBuf::from(path);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("reading drivers config {}: {e}", path.display()))?;
    let parsed: DriverSpecFile = serde_yaml::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("parsing drivers config {}: {e}", path.display()))?;
    Ok(parsed.ml2_drivers)
}
