# Spec: `neutron-ml2-guardian` design

This repo grew out of an investigation carried out in
[`pcd-ce-deploy`](https://github.com/estenrye/pcd-ce-deploy)'s
`docs/neutron-vlan-udm-se-integration-spec.md`, which explores how to get
Private Cloud Director's Neutron to create/configure VLANs on a Ubiquiti
UDM-SE. That doc covers the full investigation (why a raw ML2 mechanism
driver install looked risky at first, what was found by inspecting the live
cluster, and why the notification-bus shim alternative was set aside) --
this doc is scoped to just this controller's own design, assuming the ML2
route (a purpose-built driver like
[`ubiquiti-community/networking-unifi`](https://github.com/ubiquiti-community/networking-unifi))
is the chosen approach.

## Goal

Keep one or more third-party Neutron ML2 mechanism drivers installed and
configured on a PCD `neutron` Helm release, **without a custom container
image**, and **self-heal after every PCD upgrade silently reverts it** --
consistent with how PCD upgrades were found to work: the whole `pcd`
namespace is installed by a one-shot imperative `helm install` per
subchart, not continuously reconciled, so a future upgrade almost
certainly re-runs the `neutron` subchart with its own baked-in default
values (not `--reuse-values`), silently dropping any prior customization.

## Why not a custom image

A pinned custom image (`FROM quay.io/platform9/pf9-neutron:X` + `pip
install <driver>`) is the obvious first approach, but it goes stale the
moment PCD ships a new base image on any upgrade -- a version-skew problem
on top of the config-drift problem this whole controller already exists to
solve. The alternative implemented here: install the driver into a shared
volume **at pod-start time**, against whatever image PCD currently ships,
so there's nothing to re-pin, ever.

This is possible because of a specific Neutron/OVN architecture detail: ML2
mechanism drivers are loaded *in-process* by `neutron-server` via Python
stevedore entry points and called as direct method calls from the ML2
plugin's driver manager -- there's no out-of-process/RPC protocol for them
(unlike, say, Designate's HTTP-based backend targets). So the driver
package has to be importable on `neutron-server`'s own `sys.path` when it
starts, but that doesn't require it to be baked into the image at build
time.

## What "present" means (layered check, not a single signal)

Each layer catches a failure mode the others miss:

1. **Injection check** (cheap, first line): the `neutron-server`
   Deployment's pod template has the guardian's injected initContainer (by
   name, `ml2-driver-injector`) and its associated volume/env changes
   present. A single `kubectl get deployment -o json` field read.
2. **Config check** (authoritative): exec into a live, `Ready`
   `neutron-server` pod and confirm `mechanism_drivers` in the rendered
   `/etc/neutron/plugins/ml2/ml2_conf.ini` actually includes the driver's
   stevedore name, *and* that the package actually imports (`python3 -c
   "import <import_module>"`). Catches drift the injection check alone
   would miss (e.g. the patch is present but its pip-install step silently
   failed).
3. **Health check** (safety, easy to overlook): the pod must actually be
   `Ready` and not crash-looping. A driver that's textually "present" but
   crashing on load must be treated as **degraded**, not **absent** --
   otherwise the controller would interpret a broken repair as "still needs
   repairing" and retry it forever against something a repair can't fix.

Implemented in `src/reconcile.rs::check_present`.

## Runtime driver injection via initContainer

- An initContainer (`ml2-driver-injector`) is patched onto the
  `neutron-server` Deployment using **the same image reference as the
  Deployment's own main container**, read dynamically from the live pod
  spec, never hardcoded (`src/k8s/mod.rs::main_container_image`). It runs
  `pip install --no-index --find-links=/wheelcache/<driver> ...
  --target=/opt/ml2-plugins <pip packages>` into a shared `emptyDir`.
- The main `neutron-server` container gets that `emptyDir` mounted and a
  `PYTHONPATH` addition pointing at it. **Confirmed 2026-09-14 against the
  live cluster**: `neutron-server`'s entrypoint
  (`/var/lib/openstack/bin/neutron-server`) is exactly the plain setuptools
  `console_scripts` shebang script this design assumed
  (`#!/var/lib/openstack/bin/python3`, no `-S`/isolated-mode flags), and an
  `env PYTHONPATH=... python3 -c "import sys; print(sys.path)"` exec test
  against the real running pod confirmed the path shows up in `sys.path` as
  expected. The `.pth`-file fallback described below is therefore not
  needed for this image and is kept only as a documented contingency in
  case a future PCD base image changes this.
- `--no-index --find-links`, never a live `pip install`, is the important
  detail: it keeps the neutron-server pod's *startup* path fully offline. A
  pod restart is not rare (node reboot, OOM, rescheduling, an unrelated
  `helm upgrade`) -- making every one of them depend on PyPI reachability
  would turn a build-time convenience into a runtime availability risk for
  a core control-plane service.

Implemented in `src/k8s/mod.rs::apply_injection_patch`.

## Wheel cache: a separate concern from pod startup

The **guardian**, not the initContainer, keeps `/wheelcache/<driver>`
current -- on its own schedule, tolerant of retry/backoff, off the neutron
pod's critical path. Whenever it detects the target image (and therefore
its Python version) has changed, it refreshes the cache by running `pip
download` for the driver's package and its dependency closure, in a Job
using **that same current image** (so downloaded wheels match its Python
ABI).

**Design correction made during implementation:** the wheel cache PVC has
to live in the *target* namespace (alongside `neutron-server`), not the
guardian's own namespace -- PVCs are namespace-scoped, and both the
injected initContainer and the download Job need to mount it by
`claimName` within their own namespace. That means the guardian (running in
its own namespace for RBAC-auditability reasons, see below) can't mount
that same PVC object to read back what the download Job produced. Rather
than provision cross-namespace storage tricks to work around that, the
download Job's own command lists the wheel filenames it produced
(`ls -1`) and the guardian reads that back via the Kubernetes API (the
Job's pod logs) -- no shared filesystem access needed by the guardian at
all. See `src/wheelcache.rs`'s module doc comment.

**Pure-Python vs. version-pinned detection is derived automatically**, not
declared per driver: after a download, `src/wheelcache.rs::classify`
inspects the wheel filenames' ABI/platform tags. A `*-none-any.whl`
universal wheel needs no refresh trigger ever; anything with a specific
tag ties the whole set to the exact image it was downloaded against,
requiring a refresh whenever the observed image's Python version changes.

## Repair logic

**Revised 2026-09-14 -- no Helm involved at all, for either read or write.**
The original plan was to read the release's current `mechanism_drivers`
value via `helm get values` and apply the change via `helm upgrade
--reuse-values --set ...`, using a chart reference pulled directly out of
the release's own stored data (to avoid needing an external chart repo).
That chart-extraction idea was tested directly against the live cluster and
is a dead end: Helm's `chart.Chart` Go struct keeps subchart data in an
*unexported* `dependencies` field, which `encoding/json` silently omits
from everything Helm stores about a release. Decoding the real
`sh.helm.release.v1.neutron.v1` Secret confirmed this empirically -- the
reconstructed chart's `templates` list contained only the parent chart's
own files, missing the `helm-toolkit` and `ovn` subcharts entirely, and a
`helm upgrade --dry-run` against that reconstruction failed immediately:
`found in Chart.yaml, but missing in charts/ directory: helm-toolkit, ovn`.
This isn't specific to a missing implementation detail -- there is no
`helm` CLI incantation that recovers this data, because it was never
serialized in the first place.

The actual fix is simpler than the original plan anyway: `ml2_conf.ini`
isn't re-templated at pod startup at all. It's a **static key in the
`neutron-etc` Secret**, mounted onto `neutron-server` via `subPath` at
`/etc/neutron/plugins/ml2/ml2_conf.ini` (confirmed live) -- already the
exact final rendered text, produced once by Helm's own templating at
install/upgrade time. Repair now:

1. Reads `neutron-etc`'s `ml2_conf.ini` key directly (`k8s::get_secret_key`)
   and computes *"current mechanism_drivers list + this driver's name if
   missing"* (`reconcile::add_driver_to_ml2_conf` -- never a hardcoded
   string, preserving every other line byte-for-byte), then patches just
   that one key back (`k8s::patch_secret_key`) if it changed. No `helm`
   binary needed in the image at all anymore.
2. Write the driver's `extraConfigSecretData` (if any) into its own Secret.
3. Refresh the wheel cache (see above) and classify what it produced.
4. **Re-apply the Deployment injection patch**, which now also
   unconditionally bumps a `neutron-ml2-guardian/restarted-at` pod-template
   annotation (`k8s::apply_injection_patch`). This is the piece that forces
   a fresh rollout: `subPath` mounts are never hot-reloaded by kubelet, so
   without a pod template change, running pods would never pick up the
   just-patched `ml2_conf.ini` even though the Secret itself is already
   correct. The injection patch itself is still *not* preserved by any
   future `helm upgrade` of this release (Helm computes its patch from its
   own release history, which never included this out-of-band addition),
   so it's reapplied every repair cycle regardless of whether its own
   content changed.
5. Re-run the full "present" check to confirm the repair actually took
   effect before declaring success -- exiting 0 on the patch calls doesn't
   mean the driver actually loaded.

Implemented in `src/reconcile.rs::repair`. A real end-to-end validation of
step 1's *mechanism* (reconstruct-and-dry-run, not the final Secret-patch
design) was performed against the live cluster on 2026-09-14 via manual
`kubectl`/`helm` commands, confirming both that the chart-reconstruction
path fails as described above and that `neutron-etc`'s `ml2_conf.ini`
content matches what `kubectl exec ... cat` returns from the running pod.
The Rust implementation of the *replacement* approach has not yet been
exercised against the cluster with `DRY_RUN=false`.

## Safety rails

- **Backoff, don't repair-loop.** After
  `MAX_CONSECUTIVE_REPAIR_FAILURES` (3, in `src/reconcile.rs`) failed
  repairs for a driver, the guardian stops attempting further repairs for
  it until the process restarts, and surfaces a sustained "degraded" signal
  instead of repeatedly restarting a broken deployment.
- **Least-privilege, cross-namespace RBAC.** A `Role` (not `ClusterRole`)
  scoped to the target namespace only (`deploy/helm/.../templates/role.yaml`),
  bound to the guardian's ServiceAccount from its own namespace, and named
  to specific resources (`resourceNames` on the Deployment) rather than the
  chart's full surface -- narrower than an earlier draft of this Role,
  since dropping the `helm upgrade` approach (see "Repair logic" above)
  removed the need for broad access across whatever resource kinds the
  `neutron` chart happens to render. Still worth reviewing deliberately: it
  can rewrite `neutron-server`'s rendered config and pod spec directly.
- **Scoped to specific named resources.** The guardian never touches any
  Deployment/Secret other than the ones it's explicitly configured for
  (`deploymentName`, `neutron-etc`, and its own per-driver config Secrets).
- **Idempotent.** No Secret/Deployment patch call at all on a reconcile
  tick where the present-check already passes.
- **Observability rides on the target cluster's own stack.** `/metrics`
  is annotated for Prometheus auto-discovery
  (`prometheus.io/scrape`) rather than needing a separate observability
  stack. (First-pass gap: the `/metrics` route currently returns a
  placeholder string, not real Prometheus text-format output -- OTLP
  export via `OTEL_EXPORTER_OTLP_ENDPOINT` is fully wired and is the
  reliable path for now; see `src/main.rs::metrics_handler`.)

## Generalizing: a driver-agnostic ML2 injector

Almost nothing above actually needs to know about any specific driver --
each `ml2Drivers` entry just supplies a pip package name, the
stevedore/`mechanism_drivers` name it registers, and the top-level module
to test-import (pip package names and import module names frequently
differ). This makes the tool a general-purpose "PCD ML2 driver injector,"
reusable for any pip-installable, stevedore-registered Neutron ML2 driver
-- `networking-generic-switch`, `networking-unifi`, or anything else --
with UniFi simply as the first configured instance, not something built
into the tool's logic. See `src/config.rs::DriverSpec` and
`deploy/helm/neutron-ml2-guardian/values.yaml`'s `ml2Drivers` schema.

`extraConfigSecretData` is the piece that makes this genuinely
driver-agnostic: opaque content the guardian never parses, written
verbatim into a per-driver Secret and mounted as an additional
`--config-file` on `neutron-server` -- Neutron already natively supports
stacking multiple `--config-file` arguments (the same mechanism
`ml2_conf.ini` itself relies on), so this composes without the guardian
needing any driver-specific logic.

**`extraConfigSecretRef`, added 2026-09-14, is the credential-safe
alternative to the above.** `extraConfigSecretData` puts real credentials
(a controller API key, switch passwords) inline in this chart's own Helm
values -- fine for config with nothing sensitive in it, but every real
deployment so far has needed actual credentials. `extraConfigSecretRef`
takes the name of an **existing** Secret instead (created out-of-band --
`kubectl create secret`, an external-secrets operator, whatever the
deployer already uses) that the guardian only ever references by name in
its mount (`k8s::apply_injection_patch`) -- it never reads or writes that
Secret's contents at all, so nothing sensitive passes through this chart's
values or release storage. The two are mutually exclusive per driver
(`config::validate_extra_config_exclusive` refuses to start otherwise);
the referenced Secret must contain a key named `<driver name>.ini`, the
same convention the guardian's own managed Secrets use
(`DriverSpec::extra_config_secret_name`/`managed_secret_name`), so
`apply_injection_patch`'s mount logic doesn't need to care which path
produced the Secret it's mounting.

`soleDriver` (optional, default `false`) exists for the uncommon case of a
driver that isn't designed to coexist additively alongside
`openvswitch`/`ovn` and other add-on drivers -- the guardian refuses to
combine two `soleDriver`-flagged entries (`src/config.rs::Config::from_env`)
rather than silently producing a broken `mechanism_drivers` list.

## Implementation decisions

- **Language/tooling: Rust + `kube-rs`**, consistent with
  `estenrye/pdns4-shim`'s existing stack. Originally paired with shelling
  out to a bundled `helm` CLI binary for the `mechanism_drivers` change,
  but that approach (and the `helm` binary along with it) was dropped once
  the Secret-patching alternative in "Repair logic" above turned out to be
  both simpler and not dependent on a chart reference that can't actually
  be resolved. `kube-rs` alone now covers everything this controller does.
- **Repair autonomy: fully automatic.** On detecting drift, the guardian
  immediately repairs without waiting for a human trigger -- reasonable for
  a home-lab context, with the safety rails above standing in for a
  manual-confirm gate.
- **Reconcile interval: 5 minutes** by default (`reconcileIntervalSecs`
  chart value / `RECONCILE_INTERVAL_SECS` env var), tunable without a code
  change.

## Status

This is a first-pass scaffold: it builds cleanly (`cargo check`/`cargo
clippy -- -D warnings` both pass) and the Helm chart lints/renders, but it
has **not been run against a real cluster yet** (i.e. it has never actually
performed a repair).

**Verified 2026-09-14, against the live cluster or `networking-unifi`'s own
source:**
- `PYTHONPATH` propagation through `neutron-server`'s real entrypoint --
  confirmed working, no `.pth`-file fallback needed (see "Runtime driver
  injection" above).
- The real main container name is `neutron-server` (hyphenated) -- an
  actual bug in the first-pass scaffold (`MAIN_CONTAINER_NAME` was
  `"neutron_server"`, confusing the Helm chart's own
  `images.tags.neutron_server` *values key* naming convention with the
  container name in the rendered pod spec), now fixed.
- `networking-unifi`'s registered entry point: `unifi =
  "unifi_ml2_driver.unifi_mech:UnifiMechDriver"` under
  `neutron.ml2.mechanism_drivers` -- confirms `name: unifi` /
  `importModule: unifi_ml2_driver` in the values example were already
  correct.
- `networking-unifi`'s real `[unifi]` config schema (from
  `unifi_ml2_driver/config.py`): `host`, `port` (default 8443), `apikey`
  (preferred -- if set, `username`/`password` are ignored), `username`,
  `password`, `site` (default `"default"`), `verify_ssl`, `cafile`, plus
  driver-behavior options (`use_all_networks_for_trunk`,
  `enable_port_security`, `enable_qos`, DNS integration, etc.). It talks to
  the UniFi controller's own API (there's a `unifi_api.py` module), not raw
  per-switch SSH/Netmiko -- the `devstack/plugin.sh` NGS/Netmiko-flavored
  variables in that repo appear to be inherited scaffolding from its
  `networking-generic-switch` ancestry rather than the real runtime path.
  `values.yaml`'s example has been corrected to this real schema.

**Resolved 2026-09-14:** the `helm upgrade` chart-reference gap and the
dry-run mode, both described in "Repair logic" and the "Implementation
decisions" update above -- the chart-reference approach was replaced
entirely (patch `neutron-etc` directly, no Helm involved), and `DRY_RUN`
now defaults to `true`.

**Wheel cache storage class, decided 2026-09-14:** `pcd.rye.ninja` has
exactly one StorageClass, `pcd-sc` (Platform9 PCD's own default),
provisioner `kubevirt.io.hostpath-provisioner` -- confirmed node-local,
`WaitForFirstConsumer` binding, not a genuinely RWX-capable class (a
hostpath volume is one node's local disk, not real shared storage). Set as
`values.yaml`'s default anyway, deliberately: PCD Community Edition is
typically single-node, where this works in practice (multiple pods on the
*only* node sharing one local directory), and on-prem installs are likely
similarly configured. This will not work on an actual multi-node cluster,
where `wheelcache.storageClassName` must be overridden to a real RWX class
(NFS-backed, etc.) -- still a plain overridable values field, just with a
different default than "empty."

**Resolved 2026-09-14 (continued):**
- **`extraConfigSecretData` is now wired in.** `neutron-server` is actually
  launched by a static script (`neutron-server.sh`, itself a key in the
  `neutron-bin` ConfigMap -- confirmed live: the container's `command` is
  `["/tmp/neutron-server.sh", "start"]`) that `exec`s `neutron-server` with
  a fixed list of `--config-file` flags and no `--config-dir`.
  `reconcile::add_config_dir_flag` patches that script once (same
  patch-a-static-asset pattern as `neutron-etc`, applied to a ConfigMap
  instead of a Secret) to add `--config-dir EXTRA_CONF_DIR`; from then on,
  `apply_injection_patch` mounts each driver's extra-config Secret as
  `EXTRA_CONF_DIR/<driver>.conf` (subPath matching the Secret's own
  `<driver>.ini` key, mountPath renamed to end in `.conf` since oslo.config
  globs `--config-dir` by that extension) -- no further script edits needed
  as drivers are added, since oslo.config loads every `*.conf` file it
  finds there automatically.
- **A real Prometheus text-format `/metrics` handler.** Metrics are now
  recorded twice on every event -- once via the existing OTel instruments
  (OTLP export, unchanged) and once via a plain `prometheus::Registry`,
  gathered into real Prometheus text format for `/metrics`
  (`main::metrics_handler`). Deliberately two separate instrumentation
  calls rather than one bridged through the other, to avoid coupling this
  controller to a specific `opentelemetry`-ecosystem crate version for
  Prometheus export specifically.
- **A test suite now exists** (17 tests): `add_driver_to_ml2_conf`,
  `add_config_dir_flag`, `wheelcache::classify`, driver-config YAML
  parsing, `validate_sole_drivers`, and an end-to-end smoke test that
  actually gathers+encodes the Prometheus registry and checks the real
  output text, not just that instrument registration doesn't panic.

**Resolved 2026-09-14 (continued): UDM-SE reachability and auth.** Verified
directly (not assumed) that the UDM-SE is reachable at `10.45.0.1` and its
Network Integration API authenticates, from **both** the PCD host and from
inside a pod's own network namespace in this cluster (`kubectl exec` into
the `neutron-server` pod, no `curl` available so done via Python's
`urllib`): `GET https://10.45.0.1/proxy/network/integration/v1/sites` with
header `X-API-KEY: <key>` returned HTTP 200 with one site,
`internalReference: "default"` -- matching `unifi-ml2-driver`'s own `site`
config default, so no non-default site name is needed. This also resolves
the still-open item from `pcd-ce-deploy`'s original investigation spec
about confirming the Integration API's exact endpoint/schema. `host =
10.45.0.1` / `site = default` in `values.yaml`'s example are now real,
verified values -- only `apikey` remains a placeholder (kept out of the
repo; the real key lives in 1Password at
`op://controlplane/unifi-os-xnetworksegment/credential`).

**First live `DRY_RUN=false` attempt, 2026-09-14: partial failure, safely
contained, root-caused and fixed.** Deployed for real against
`pcd.rye.ninja` (release `neutron-ml2-guardian`, driver `unifi` via
`extraConfigSecretRef`). The dry-run pass first (installed with
`dryRun: true`, confirmed the logged intended action matched expectations
exactly) caught nothing wrong -- the actual failure only showed up once
patches started hitting the real API server:

- `k8s::patch_secret_key` (against `neutron-etc`) succeeded on the first
  real attempt -- `mechanism_drivers = openvswitch,ovn,unifi` was
  confirmed live in the Secret.
- `k8s::patch_configmap_key` (against `neutron-bin`) failed immediately
  with a 409: `Apply failed with 1 conflict: conflict with "helm" using
  v1: .data.neutron-server.sh`. Root cause: every field on every asset
  this controller touches is already owned by field manager `"helm"` from
  the original `helm install`, and `PatchParams::apply(...)` without
  `.force()` refuses to take a field away from another manager. The Secret
  patch happened to succeed anyway only because it goes through `stringData`
  (a field Helm's own Secret creation never populated, so no ownership
  collision existed there) while the ConfigMap patch goes through `data`
  directly (the same field path Helm used) -- an accidental asymmetry, not
  a deliberate design difference. Fixed by adding `.force()` to every
  `Patch::Apply` call in `k8s.rs` -- see that module's doc comment for the
  full rationale (this is the actual point of the controller, not a
  workaround).
- **Real-world validation of the "safe partial failure" design property**:
  the repair aborted after step 1 with step 4 (the Deployment injection
  patch, which is what actually triggers a rollout) never reached. The
  live `neutron-server` pod was completely unaffected -- confirmed still
  `Running`, unchanged `AGE`, throughout. This is exactly the behavior
  "Repair logic" above was designed around: a mid-repair failure leaves
  static assets partially updated but triggers no rollout, so nothing
  user-facing breaks until the *last* step (the one that forces a
  rollout) actually succeeds.
- Also discovered along the way: `helm upgrade` from a stale local chart
  checkout on the deploying host silently used an outdated template
  (missing the not-yet-`git pull`ed `extraConfigSecretRef` support) --
  not a guardian bug, but a real deployment-process gotcha worth
  remembering: always re-pull a git-cloned chart checkout immediately
  before `helm upgrade`, not just once at the start of a session.
- Also discovered: a plain `helm upgrade` that only changes a Secret's
  *content* (not the Deployment's `spec.template`) does not restart pods
  that already have that Secret mounted as a plain volume (non-`subPath`)
  -- kubelet live-syncs the file on disk, but a process that already read
  it into memory at startup (like this guardian reading `drivers.yaml`)
  never sees the update without an explicit `kubectl rollout restart`.
  Only relevant to *this* guardian's own drivers-config Secret, not to its
  patches against `neutron-etc`/`neutron-bin` (those are read fresh every
  reconcile tick).

**Second live attempt, same day, after the `.force()` fix: progress, then a
second real bug, also safely contained.** Re-deployed (pinning the
Deployment to the exact new image digest to sidestep a `main`-tag caching
question, not a guardian bug -- worth remembering as its own deployment
gotcha: a moving tag plus `IfNotPresent` can silently keep running a stale
image after a fix ships). This run got further:
- `mechanism_drivers` patch and the `neutron-bin` `--config-dir` patch both
  succeeded this time -- `.force()` fixed exactly what it was meant to.
- **The wheel-cache download Job succeeded against the real `pf9-neutron`
  image** -- confirms `pip` is available and has outbound PyPI access
  there, closing that open question. Classified as `VersionPinned`
  (`unifi-ml2-driver`'s dependency closure has at least one
  platform/ABI-specific wheel), confirming the per-image-version refresh
  design (not the simpler "safe forever" pure-Python case) is the one that
  actually matters here.
- **New failure**: `apply_injection_patch`'s `Deployment` patch -- a 400
  `"invalid object type: /, Kind="`. Root cause: that patch is built as a
  raw `serde_json::Value` (`json!({"spec": {...}})`), with no
  `apiVersion`/`kind`/`metadata.name`. The other three patch methods in
  this file (`patch_secret_key`, `patch_configmap_key`,
  `write_driver_config_secret`) never hit this because they serialize
  actual typed `Secret`/`ConfigMap` structs, which carry these fields for
  free -- server-side apply requires a `Patch::Apply` body to self-identify
  its type, and a bare partial `Value` doesn't. Fixed by adding
  `apiVersion`/`kind`/`metadata` explicitly to that one `json!` call.
- **Safety property held again**: this failure was caught *after* the
  wheel cache was already refreshed but *before* the Deployment patch (the
  step that actually triggers a rollout) took effect. `neutron-server` was
  confirmed unaffected throughout both failed attempts -- same pod, same
  age, `3/3 Running`, the entire time.

Next: re-deploy with this fix and confirm a full real repair succeeds
end-to-end, including the post-repair present-check.

**Still open:**
- Confirm the post-repair present-check (`mechanism_drivers` line +
  `python3 -c "import unifi_ml2_driver"`) actually passes once the
  initContainer has had a chance to run.
- Confirm `neutron-server` itself comes back `Ready` and stable after the
  injection patch finally triggers its first real rollout.
