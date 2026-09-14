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
//!
//! Every `Patch::Apply` call here uses `PatchParams::apply(...).force()`.
//! Discovered the hard way against the real cluster: every field on
//! `neutron-etc`/`neutron-bin`/the guarded Deployment is already owned by
//! field manager "helm" from the original `helm install`. Without
//! `.force()`, server-side apply refuses the patch with a 409 conflict
//! ("conflict with \"helm\" using v1: .data.neutron-server.sh") the moment
//! the field path collides with one Helm already owns -- which is every
//! field this controller needs to touch, definitionally. Forcing ownership
//! away from Helm is not a workaround, it's the actual point of this
//! controller: see `reconcile.rs`'s module doc comment on why Helm
//! re-asserting its own ownership on a future real `helm upgrade` is
//! expected drift this guardian exists to detect and re-heal, not a
//! conflict to avoid.

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
/// Each driver with `has_extra_config()` true gets its own file mounted
/// here by `apply_injection_patch`, sourced from
/// `DriverSpec::extra_config_secret_name()` -- either the guardian-managed
/// Secret or a user-provided `extraConfigSecretRef`.
pub const EXTRA_CONF_DIR: &str = "/etc/neutron-ml2-guardian/extra-conf.d";

/// Package distribution names (wheel-filename form: non-alphanumeric runs
/// become `_`, matched case-insensitively since wheel filenames preserve
/// original casing like `SQLAlchemy`/`WebOb`) assumed already present in
/// any real `neutron-server` image, and therefore excluded from
/// installation into the isolated plugin directory even when a driver's
/// resolved dependency closure includes them. See `apply_injection_patch`'s
/// doc comment for the full rationale and how this list was derived --
/// this is the actual set observed from a real `pip download
/// unifi-ml2-driver` run, not a guess, and it's ecosystem-level (standard
/// OpenStack/Neutron tooling), not specific to that one driver: any
/// Neutron ML2 driver plugin is likely to declare a similar set, since
/// they're all guaranteed to already be present by definition of running
/// inside `neutron-server`'s own process.
const ASSUMED_PRESENT_PACKAGES: &[&str] = &[
    // Neutron itself and its immediate OVN/OVS/networking-service stack.
    "neutron",
    "neutron_lib",
    "os_vif",
    "os_resource_classes",
    "os_service_types",
    "os_traits",
    "os_ken",
    "ovs",
    "ovsdbapp",
    // OpenStack client/auth libraries any Neutron deployment integrating
    // with Keystone/Nova/Designate (as this one does) already has.
    "keystoneauth1",
    "keystonemiddleware",
    "python_keystoneclient",
    "python_novaclient",
    "python_designateclient",
    "python_neutronclient",
    "openstacksdk",
    "osc_lib",
    "cliff",
    // Every oslo.* subpackage -- a regex fragment, not an exhaustive
    // enumeration, since new ones shouldn't need a code change here.
    // `[a-z0-9_]+`, not just letters: `oslo.i18n` -> `oslo_i18n` has a
    // digit, and slipped through an earlier letters-only version of this
    // pattern during validation against a real dependency download.
    "oslo_[a-z0-9_]+",
    // Common OpenStack-ecosystem support libraries.
    "osprofiler",
    "pycadf",
    "debtcollector",
    "futurist",
    "fasteners",
    "cotyledon",
    "dogpile_cache",
    "stevedore",
    "eventlet",
    "greenlet",
    "alembic",
    "sqlalchemy",
    "webob",
    "paste",
    "pastedeploy",
    "pbr",
    "mako",
    "prettytable",
    "prometheus_client",
    "psutil",
    // WSGI framework used by parts of the OpenStack API ecosystem --
    // showed up in the real dependency resolve validated below via
    // neutron's own transitive requirements, not anything UniFi-specific.
    "pecan",
    "pyjwt",
    "pynacl",
    "pyopenssl",
    "python_dateutil",
    "pyyaml",
    "requests",
    "urllib3",
    "certifi",
    "rfc3986",
    "jsonschema",
    "jsonschema_specifications",
    "prompt_toolkit",
    "cmd2",
    "autopage",
    "six",
    "decorator",
    "iso8601",
    "netaddr",
    "jmespath",
    "jsonpatch",
    "jsonpointer",
    "msgpack",
    "amqp",
    "kombu",
    "vine",
    "cachetools",
    // NOT "orjson": listed here in a first pass on the mistaken assumption
    // that anything appearing in the dependency resolve must already be
    // present -- it's an optional accelerator library some packages use
    // if available, and the real target image genuinely doesn't have it.
    // Confirmed the hard way: the first `DRY_RUN=false` attempt after this
    // exclusion list existed still crashed, this time on `neutron_server`
    // loading the `unifi` entry point itself with `ModuleNotFoundError: No
    // module named 'orjson'` -- see the design doc's incident log. Kept as
    // an explicit negative entry (rather than just omitted) so a future
    // pass doesn't re-add it on the same flawed reasoning.
    "lxml",
    "httplib2",
    "pyparsing",
    "jinja2",
    "markupsafe",
    "packaging",
    "platformdirs",
    "cffi",
    "cryptography",
    "bcrypt",
    "idna",
    "wrapt",
    "sortedcontainers",
    "tenacity",
    "tzdata",
    "typing_extensions",
    "wcwidth",
    "voluptuous",
    "setuptools",
    "dnspython",
];

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

        // NOT a plain `pip install --find-links=... <package>` -- that lets
        // pip resolve and install the driver's *entire* dependency closure,
        // which for any real Neutron ML2 driver includes `neutron`/
        // `neutron-lib` themselves (they're declared dependencies, since
        // that's how Python packaging expresses "this needs Neutron's
        // API," even though the package is guaranteed to be running inside
        // neutron-server's own process already). With PYTHONPATH
        // prepending this directory ahead of the image's real
        // site-packages, that silently shadows the image's actual,
        // Platform9-patched, OVN-customized `neutron` package with a
        // generic upstream copy -- confirmed live: this is exactly what
        // crashed `neutron-server` on 2026-09-14 with `ModuleNotFoundError:
        // No module named 'neutron.cmd.eventlet'` (see the design doc's
        // incident writeup).
        //
        // Fix: walk every driver's cached wheels, exclude ones matching
        // `ASSUMED_PRESENT_PACKAGES` below, and install only what's left,
        // each with `--no-deps` so pip doesn't try to re-resolve (and
        // re-pull-in) an excluded one transitively. A shell filter rather
        // than something computed in Rust and threaded through as an
        // explicit file list, so it stays driver-agnostic -- no
        // per-driver knowledge needed here beyond where its wheels are
        // cached.
        //
        // The exclusion list itself needed to be much broader than a first
        // guess: a real `pip download unifi-ml2-driver` (run locally,
        // 2026-09-14, to check this fix before another live attempt)
        // resolved ~150 packages, the large majority of them standard
        // OpenStack/Neutron-ecosystem tooling (keystoneauth1,
        // python-novaclient, SQLAlchemy, every oslo.* subpackage, etc.) --
        // exactly the class of thing any real Neutron+OVN+Designate
        // deployment already has, not just `neutron`/`neutron-lib`
        // narrowly. This list is that real, observed set, not a guess --
        // still an assumption that PCD's specific image has all of them
        // (reasonable for a working OpenStack Neutron service, but not
        // independently verified item-by-item), so treat a repair that
        // still crashes on a *different* shadowed package as "the list
        // needs one more entry," not "the approach is wrong."
        // Second real crash, 2026-09-14 (after the shadowing fix above):
        // `unifi-ml2-driver`'s own dependency `aiohttp-unifi` (the `aiounifi`
        // package) has three modules (`firewall_policy.py`,
        // `firewall_zone.py`, `message.py`) that do a bare `from typing
        // import Self` with no `typing_extensions` fallback -- unlike every
        // other module in the same package, which does guard it. `Self` was
        // added to `typing` in Python 3.11 (PEP 673); this target image runs
        // 3.10, so those three imports raise `ImportError` and take down
        // ML2's mechanism-driver loading with them. This is a real upstream
        // bug in that dependency, not something fixable via the exclusion
        // list above (the package itself must be installed -- it's the
        // driver's own transitive dependency, not something already present
        // in the base image). Rather than patch that one package's files (a
        // per-driver, per-version-fragile fix), write a `sitecustomize.py`
        // into the installed-plugins directory: Python's `site` module
        // auto-imports `sitecustomize` if it's importable on `sys.path` at
        // interpreter startup, regardless of how that path entry got there
        // (PYTHONPATH included) -- so this runs before neutron-server loads
        // any mechanism driver, and back-ports `Self` onto the real `typing`
        // module from the already-present `typing_extensions` when running
        // under <3.11. General on purpose: any future driver hitting the
        // same Python-version gap in one of its own dependencies is covered
        // for free, with no per-driver knowledge needed here.
        let install_cmd = format!(
            "mkdir -p {PLUGIN_MOUNT_PATH} && \
             find {WHEELCACHE_MOUNT_PATH} -mindepth 2 -maxdepth 2 -name '*.whl' \
             | grep -Eiv '/({})-[0-9]' \
             | xargs -r pip install --no-index --no-deps --target={PLUGIN_MOUNT_PATH} && \
             cat > {PLUGIN_MOUNT_PATH}/sitecustomize.py <<'PYEOF'\n\
import sys\n\
if sys.version_info < (3, 11):\n\
    import typing\n\
    if not hasattr(typing, \"Self\"):\n\
        import typing_extensions\n\
        typing.Self = typing_extensions.Self\n\
PYEOF",
            ASSUMED_PRESENT_PACKAGES.join("|"),
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
        for driver in drivers.iter().filter(|d| d.has_extra_config()) {
            let volume_name = format!("ml2-extra-config-{}", driver.name);
            extra_config_volumes.push(json!({
                "name": volume_name,
                "secret": { "secretName": driver.extra_config_secret_name() },
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

        // apiVersion/kind/metadata.name are required here, not decorative:
        // unlike the typed Secret/ConfigMap structs the other patch methods
        // in this file use (which get these fields for free from their own
        // Serialize impls), this is a raw serde_json::Value, and Kubernetes
        // server-side apply rejects a Patch::Apply body that doesn't
        // self-identify its type -- confirmed live: omitting them produces
        // "invalid object type: /, Kind=" (400 BadRequest), not a clearer
        // "apiVersion required" message, which is why this is called out
        // explicitly rather than left to look like normal patch scaffolding.
        let patch = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": deployment_name,
                "namespace": self.namespace,
            },
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
                &PatchParams::apply("neutron-ml2-guardian").force(),
                &Patch::Apply(patch),
            )
            .await?;
        Ok(())
    }

    /// Reverts everything `apply_injection_patch` may have added, for the
    /// given drivers. Used when `Config::dry_run` finds a guardian
    /// footprint already present -- see `reconcile::revert_or_preview`.
    ///
    /// Deliberately uses an explicit `Patch::Strategic` with `$patch:
    /// delete` entries, **not** a `Patch::Apply` that simply omits the
    /// fields we no longer want. SSA's "omitting a field you own removes
    /// it" behavior is plausible but was never actually verified against
    /// this cluster, and this project has already been burned once by an
    /// unverified assumption about Kubernetes patch behavior (see the
    /// design doc's incident writeup). `$patch: delete` is the exact,
    /// explicit mechanism already confirmed working, by hand, during that
    /// incident's real recovery -- reusing proven behavior here rather than
    /// a new untested code path. `PatchParams::default()` (no field
    /// manager/force) is correct for strategic merge patches -- those are
    /// only meaningful for `Patch::Apply`.
    ///
    /// NOTE: `volumeMounts`' strategic-merge-patch key is `mountPath`, not
    /// `name` -- confirmed the hard way during that same incident, unlike
    /// `volumes`/`containers`/`initContainers`, which all use `name`.
    /// Idempotent: deleting an already-absent list item is a no-op.
    pub async fn remove_injection_patch(
        &self,
        deployment_name: &str,
        main_container_name: &str,
        drivers: &[DriverSpec],
    ) -> GuardianResult<()> {
        let mut volume_deletes = vec![
            json!({ "name": PLUGIN_VOLUME_NAME, "$patch": "delete" }),
            json!({ "name": WHEELCACHE_VOLUME_NAME, "$patch": "delete" }),
        ];
        let mut mount_deletes = vec![json!({ "mountPath": PLUGIN_MOUNT_PATH, "$patch": "delete" })];
        for driver in drivers {
            volume_deletes.push(json!({
                "name": format!("ml2-extra-config-{}", driver.name),
                "$patch": "delete",
            }));
            mount_deletes.push(json!({
                "mountPath": format!("{EXTRA_CONF_DIR}/{}.conf", driver.name),
                "$patch": "delete",
            }));
        }

        let patch = json!({
            "spec": {
                "template": {
                    "spec": {
                        "initContainers": [
                            { "name": INJECTOR_CONTAINER_NAME, "$patch": "delete" }
                        ],
                        "volumes": volume_deletes,
                        "containers": [
                            {
                                "name": main_container_name,
                                "env": [
                                    { "name": "PYTHONPATH", "$patch": "delete" }
                                ],
                                "volumeMounts": mount_deletes,
                            }
                        ]
                    }
                }
            }
        });

        self.deployments()
            .patch(
                deployment_name,
                &PatchParams::default(),
                &Patch::Strategic(patch),
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
                &PatchParams::apply("neutron-ml2-guardian").force(),
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
                &PatchParams::apply("neutron-ml2-guardian").force(),
                &Patch::Apply(patch),
            )
            .await?;
        Ok(())
    }

    /// Writes (or updates) the per-driver extra-config Secret from
    /// `DriverSpec::extra_config_secret_data`. A no-op if that's empty, or
    /// if `extra_config_secret_ref` is set instead -- that Secret is
    /// externally managed, and this method must never write to it (see
    /// that field's doc comment).
    pub async fn write_driver_config_secret(&self, driver: &DriverSpec) -> GuardianResult<()> {
        if driver.extra_config_secret_data.is_empty() || driver.extra_config_secret_ref.is_some() {
            return Ok(());
        }
        let name = driver.managed_secret_name();
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
                &PatchParams::apply("neutron-ml2-guardian").force(),
                &Patch::Apply(secret),
            )
            .await?;
        Ok(())
    }
}
