//! Kubernetes API access: reading the guarded Deployment/Pods, exec'ing into
//! a live pod for the config/import checks, and patching in the
//! initContainer/volume/env injection. Everything here is scoped to
//! `Config::target_namespace` -- this module never assumes `pcd` as a
//! literal, per the design doc's note that the namespace is this cluster's
//! convention, not a constant.
//!
//! NOTE: the exact container name inside the `neutron-server` pod
//! (`neutron-server` is assumed below, matching the Helm chart's own
//! container naming convention seen in `helm get manifest`) and the
//! resource names the `neutron` chart's `mechanism_drivers` `helm upgrade`
//! touches have not yet been re-verified against a live cluster from this
//! codebase -- confirm against the real cluster before trusting this in
//! production. See `docs/specs/2026-09-13-neutron-ml2-guardian-design.md`.

use std::collections::BTreeMap;

use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{Pod, Secret},
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

    pub async fn get_deployment(&self, name: &str) -> GuardianResult<Deployment> {
        self.deployments()
            .get(name)
            .await
            .map_err(GuardianError::Kube)
    }

    /// The image reference of the Deployment's *main* `neutron_server`
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

        let patch = json!({
            "spec": {
                "template": {
                    "spec": {
                        "volumes": [
                            { "name": PLUGIN_VOLUME_NAME, "emptyDir": {} },
                            { "name": WHEELCACHE_VOLUME_NAME, "persistentVolumeClaim": { "claimName": wheelcache_pvc, "readOnly": true } },
                        ],
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
                                "volumeMounts": [
                                    { "name": PLUGIN_VOLUME_NAME, "mountPath": PLUGIN_MOUNT_PATH },
                                ],
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

    /// Writes (or updates) the per-driver extra-config Secret. Content is
    /// opaque to the guardian -- see `DriverSpec::extra_config_secret_data`.
    pub async fn write_driver_config_secret(&self, driver: &DriverSpec) -> GuardianResult<()> {
        if driver.extra_config_secret_data.is_empty() {
            return Ok(());
        }
        let name = format!("neutron-ml2-{}-config", driver.name);
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
