//! Kubernetes API access: reading the guarded Deployment/Pods, exec'ing into
//! a live pod for the config/import checks, reading/patching the
//! `neutron-etc` Secret directly (see `reconcile.rs`'s module doc comment
//! for why this replaced a `helm upgrade`-based approach), and patching in
//! the initContainer/volume/env injection. Everything here is scoped to
//! `Config::target_namespace` -- this module never assumes `pcd` as a
//! literal, per the design doc's note that the namespace is this cluster's
//! convention, not a constant.
//!
//! The main container name (`neutron-server`, hyphenated) and `PYTHONPATH`
//! propagation through its real entrypoint were both confirmed against the
//! live cluster on 2026-09-14 -- see
//! `docs/specs/2026-09-13-neutron-ml2-guardian-design.md`'s "Status"
//! section.

use std::collections::BTreeMap;

use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Pod, Secret},
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, AttachParams, Patch, PatchParams};
use kube::Client;
use serde_json::json;
use tokio::io::AsyncBufReadExt;

use crate::config::DriverSpec;
use crate::error::{GuardianError, GuardianResult};

/// Name of the initContainer the guardian injects. Its presence (by name)
/// is the cheap first-line "injection present" check.
pub const INJECTOR_CONTAINER_NAME: &str = "ml2-driver-injector";
const PLUGIN_VOLUME_NAME: &str = "ml2-driver-plugins";
const PLUGIN_MOUNT_PATH: &str = "/opt/ml2-plugins";
const WHEELCACHE_VOLUME_NAME: &str = "ml2-wheelcache";
const WHEELCACHE_MOUNT_PATH: &str = "/wheelcache";

/// Directory oslo.config scans for extra `*.conf` files, once `--config-dir`
/// is added to `neutron-server.sh` (see `reconcile::add_config_dir_flag`).
/// Each driver with non-empty `extraConfigSecretData` gets its own file
/// mounted here by `apply_injection_patch`.
pub const EXTRA_CONF_DIR: &str = "/etc/neutron-ml2-guardian/extra-conf.d";

/// Name of the per-driver extra-config Secret `write_driver_config_secret`
/// creates and `apply_injection_patch` mounts -- factored out so the two
/// can't drift out of sync with each other.
fn driver_config_secret_name(driver_name: &str) -> String {
    format!("neutron-ml2-{driver_name}-config")
}

pub struct K8s {
    client: Client,
    namespace: String,
}

impl K8s {
    pub async fn try_default(namespace: &str) -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
        })
    }

    fn deployments(&self) -> Api<Deployment> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    fn secrets(&self) -> Api<Secret> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    fn configmaps(&self) -> Api<ConfigMap> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    pub async fn get_deployment(&self, name: &str) -> GuardianResult<Deployment> {
        self.deployments()
            .get(name)
            .await
            .map_err(GuardianError::Kube)
    }

    /// The image reference of the Deployment's *main* `neutron-server`
    /// container -- deliberately read live off the Deployment rather than
    /// configured anywhere, so the injector always matches whatever image
    /// PCD currently ships (the whole point of not pinning a custom image).
    pub fn main_container_image(
        deployment: &Deployment,
        main_container_name: &str,
    ) -> Option<String> {
        deployment
            .spec
            .as_ref()?
            .template
            .spec
            .as_ref()?
            .containers
            .iter()
            .find(|c| c.name == main_container_name)
            .and_then(|c| c.image.clone())
    }

    /// True if the injector initContainer is already present, by name.
    /// Layer 1 of the "present" check -- see the design doc.
    pub fn has_injection(deployment: &Deployment) -> bool {
        deployment
            .spec
            .as_ref()
            .and_then(|s| s.template.spec.as_ref())
            .and_then(|s| s.init_containers.as_ref())
            .map(|cs| cs.iter().any(|c| c.name == INJECTOR_CONTAINER_NAME))
            .unwrap_or(false)
    }

    /// Finds one `Ready` pod backing the given Deployment (by matching the
    /// Deployment's own selector labels), for exec-based checks. Returns
    /// `None` if nothing is Ready yet -- callers should treat that as
    /// "degraded", not "absent" (see the design doc's layered-check note).
    pub async fn find_ready_pod(&self, deployment: &Deployment) -> GuardianResult<Option<String>> {
        let selector = deployment
            .spec
            .as_ref()
            .and_then(|s| s.selector.match_labels.clone())
            .unwrap_or_default();
        let label_selector = selector
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");

        let list = self
            .pods()
            .list(&kube::api::ListParams::default().labels(&label_selector))
            .await?;

        for pod in list.items {
            let ready = pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conds| {
                    conds
                        .iter()
                        .any(|c| c.type_ == "Ready" && c.status == "True")
                })
                .unwrap_or(false);
            if ready {
                if let Some(name) = pod.metadata.name {
                    return Ok(Some(name));
                }
            }
        }
        Ok(None)
    }

    /// Execs `cmd` (via `/bin/sh -c`) inside `container` of `pod_name` and
    /// returns captured stdout. Used for the config/import checks -- these
    /// deliberately run *inside* the real pod rather than trying to
    /// reconstruct its filesystem/environment locally.
    pub async fn exec_capture(
        &self,
        pod_name: &str,
        container: &str,
        cmd: &[&str],
    ) -> GuardianResult<String> {
        let ap = AttachParams::default()
            .container(container)
            .stdout(true)
            .stderr(false);
        let owned_cmd: Vec<String> = cmd.iter().map(|s| s.to_string()).collect();
        let mut attached = self
            .pods()
            .exec(pod_name, owned_cmd, &ap)
            .await
            .map_err(GuardianError::Kube)?;

        let stdout = attached
            .stdout()
            .ok_or_else(|| GuardianError::PodExec("no stdout stream".into()))?;
        let mut lines = String::new();
        let mut reader = tokio::io::BufReader::new(stdout).lines();
        while let Some(line) = reader
            .next_line()
            .await
            .map_err(|e| GuardianError::PodExec(e.to_string()))?
        {
            lines.push_str(&line);
            lines.push('\n');
        }
        // Drain the join handle so the exec session closes cleanly.
        if let Some(status) = attached.take_status() {
            let _ = status.await;
        }
        Ok(lines)
    }

    /// Applies (or re-applies) the strategic-merge patch that injects the
    /// initContainer + shared volumes + `PYTHONPATH` addition into the
    /// guarded Deployment. Idempotent: re-running with the same driver list
    /// produces the same patch.
    ///
    /// NOTE: as documented in the design doc, this patch is *not* preserved
    /// by a `helm upgrade` of the same release (Helm computes its own patch
    /// from release history, which never included this) -- callers must
    /// reapply it on every repair cycle, not just once.
    pub async fn apply_injection_patch(
        &self,
        deployment_name: &str,
        main_container_name: &str,
        wheelcache_pvc: &str,
        drivers: &[DriverSpec],
    ) -> GuardianResult<()> {
        let deployment = self.get_deployment(deployment_name).await?;
        let image =
            Self::main_container_image(&deployment, main_container_name).ok_or_else(|| {
                GuardianError::Config(format!(
                    "deployment {deployment_name} has no container named {main_container_name}"
                ))
            })?;

        let install_targets: Vec<String> = drivers.iter().map(|d| d.pip_package.clone()).collect();
        let find_links: Vec<String> = drivers
            .iter()
            .map(|d| format!("{WHEELCACHE_MOUNT_PATH}/{}", d.name))
            .collect();

        let install_cmd = format!(
            "pip install --no-index {} --target={PLUGIN_MOUNT_PATH} {}",
            find_links
                .iter()
                .map(|p| format!("--find-links={p}"))
                .collect::<Vec<_>>()
                .join(" "),
            install_targets.join(" "),
        );

        // Unconditionally bumped on every call: Kubernetes only starts a new
        // rollout (picking up a freshly-patched neutron-etc Secret key,
        // which subPath mounts never hot-reload) when spec.template
        // actually changes. If this call's initContainer/volume/env content
        // happens to be byte-identical to what's already there (e.g. a
        // repair that's only fixing the ml2_conf.ini Secret, not the
        // injection itself), this annotation is what still forces a fresh
        // rollout so the Secret change actually takes effect.
        let restarted_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string();

        // One volume + volumeMount per driver that actually has extra
        // config -- mounted into EXTRA_CONF_DIR, which neutron-server.sh's
        // --config-dir flag (added once by reconcile::add_config_dir_flag)
        // scans for *.conf files. subPath must match the Secret's own key
        // (write_driver_config_secret uses "<name>.ini"); mountPath is free
        // to end in .conf instead, since oslo.config only globs by the
        // *mounted* filename, not the Secret's internal key name.
        let mut extra_config_volumes = Vec::new();
        let mut extra_config_mounts = Vec::new();
        for driver in drivers
            .iter()
            .filter(|d| !d.extra_config_secret_data.is_empty())
        {
            let volume_name = format!("ml2-extra-config-{}", driver.name);
            extra_config_volumes.push(json!({
                "name": volume_name,
                "secret": { "secretName": driver_config_secret_name(&driver.name) },
            }));
            extra_config_mounts.push(json!({
                "name": volume_name,
                "mountPath": format!("{EXTRA_CONF_DIR}/{}.conf", driver.name),
                "subPath": format!("{}.ini", driver.name),
                "readOnly": true,
            }));
        }

        let mut volumes = vec![
            json!({ "name": PLUGIN_VOLUME_NAME, "emptyDir": {} }),
            json!({ "name": WHEELCACHE_VOLUME_NAME, "persistentVolumeClaim": { "claimName": wheelcache_pvc, "readOnly": true } }),
        ];
        volumes.extend(extra_config_volumes);

        let mut main_container_mounts = vec![json!({
            "name": PLUGIN_VOLUME_NAME,
            "mountPath": PLUGIN_MOUNT_PATH,
        })];
        main_container_mounts.extend(extra_config_mounts);

        let patch = json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "neutron-ml2-guardian/restarted-at": restarted_at
                        }
                    },
                    "spec": {
                        "volumes": volumes,
                        "initContainers": [
                            {
                                "name": INJECTOR_CONTAINER_NAME,
                                "image": image,
                                "command": ["/bin/sh", "-c", install_cmd],
                                "volumeMounts": [
                                    { "name": PLUGIN_VOLUME_NAME, "mountPath": PLUGIN_MOUNT_PATH },
                                    { "name": WHEELCACHE_VOLUME_NAME, "mountPath": WHEELCACHE_MOUNT_PATH, "readOnly": true },
                                ],
                            }
                        ],
                        "containers": [
                            {
                                "name": main_container_name,
                                "env": [
                                    { "name": "PYTHONPATH", "value": PLUGIN_MOUNT_PATH }
                                ],
                                "volumeMounts": main_container_mounts,
                            }
                        ]
                    }
                }
            }
        });

        self.deployments()
            .patch(
                deployment_name,
                &PatchParams::apply("neutron-ml2-guardian"),
                &Patch::Apply(patch),
            )
            .await?;
        Ok(())
    }

    /// Reads one key out of a Secret's `data` map as a UTF-8 string. Used to
    /// read the *authoritative* `ml2_conf.ini` content directly from the
    /// `neutron-etc` Secret (mounted via `subPath` on the real
    /// `neutron-server` container -- confirmed 2026-09-14 against the live
    /// cluster, see the design doc: this Secret key already holds the fully
    /// Helm-templated final file content, statically, not something
    /// re-rendered at pod startup), rather than the possibly-stale content
    /// of a currently-running pod.
    pub async fn get_secret_key(&self, name: &str, key: &str) -> GuardianResult<String> {
        let secret = self.secrets().get(name).await?;
        let bytes = secret
            .data
            .as_ref()
            .and_then(|d| d.get(key))
            .ok_or_else(|| GuardianError::Config(format!("secret {name} has no key {key}")))?;
        String::from_utf8(bytes.0.clone()).map_err(|e| {
            GuardianError::Internal(anyhow::anyhow!(
                "secret {name}/{key} is not valid UTF-8: {e}"
            ))
        })
    }

    /// Patches a single key of an existing Secret's `stringData`, leaving
    /// every other key untouched. Used to update just `ml2_conf.ini` inside
    /// `neutron-etc` -- a Secret owned by the `neutron` Helm release, not by
    /// this controller. Per the design doc: a subsequent real `helm
    /// upgrade` of that release can and will overwrite this again (expected
    /// -- that's the drift this whole controller exists to detect and
    /// re-heal, not a bug in this patch).
    pub async fn patch_secret_key(&self, name: &str, key: &str, value: &str) -> GuardianResult<()> {
        let mut data = BTreeMap::new();
        data.insert(key.to_string(), value.to_string());
        let patch = Secret {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            string_data: Some(data),
            ..Default::default()
        };
        self.secrets()
            .patch(
                name,
                &PatchParams::apply("neutron-ml2-guardian"),
                &Patch::Apply(patch),
            )
            .await?;
        Ok(())
    }

    /// ConfigMap analog of `get_secret_key` -- used to read
    /// `neutron-server.sh` out of the `neutron-bin` ConfigMap.
    pub async fn get_configmap_key(&self, name: &str, key: &str) -> GuardianResult<String> {
        let cm = self.configmaps().get(name).await?;
        cm.data
            .as_ref()
            .and_then(|d| d.get(key))
            .cloned()
            .ok_or_else(|| GuardianError::Config(format!("configmap {name} has no key {key}")))
    }

    /// ConfigMap analog of `patch_secret_key` -- used to add the
    /// `--config-dir` flag to `neutron-server.sh` inside `neutron-bin`.
    /// Same caveat as `patch_secret_key`: owned by the `neutron` Helm
    /// release, and a subsequent real `helm upgrade` can and will overwrite
    /// this again -- expected, not a bug.
    pub async fn patch_configmap_key(
        &self,
        name: &str,
        key: &str,
        value: &str,
    ) -> GuardianResult<()> {
        let mut data = BTreeMap::new();
        data.insert(key.to_string(), value.to_string());
        let patch = ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        };
        self.configmaps()
            .patch(
                name,
                &PatchParams::apply("neutron-ml2-guardian"),
                &Patch::Apply(patch),
            )
            .await?;
        Ok(())
    }

    /// Writes (or updates) the per-driver extra-config Secret. Content is
    /// opaque to the guardian -- see `DriverSpec::extra_config_secret_data`.
    pub async fn write_driver_config_secret(&self, driver: &DriverSpec) -> GuardianResult<()> {
        if driver.extra_config_secret_data.is_empty() {
            return Ok(());
        }
        let name = driver_config_secret_name(&driver.name);
        let mut data = BTreeMap::new();
        data.insert(
            format!("{}.ini", driver.name),
            driver.extra_config_secret_data.clone(),
        );

        let secret = Secret {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            string_data: Some(data),
            ..Default::default()
        };

        self.secrets()
            .patch(
                &name,
                &PatchParams::apply("neutron-ml2-guardian"),
                &Patch::Apply(secret),
            )
            .await?;
        Ok(())
    }
}
