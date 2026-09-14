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

**The guardian mechanism itself is live-verified as of 2026-09-14; the
`unifi-ml2-driver` it was tested against is not yet actually working.**
A real repair loaded the driver successfully, `neutron-server` held
`3/3 Running` under real GET-heavy API traffic with zero restarts, and
the guardian's own present-check confirmed it via `/metrics`
(`ml2_driver_present{driver="unifi"} 1`) -- all real, all still true. But
the very first real `openstack network create` against that driver
failed outright, with a genuine bug in the upstream package (see the
"Fifth live attempt" writeup's correction, below) that none of this
project's checks had exercised. The guardian's own job -- detect drift,
inject a driver, keep it installed, revert cleanly on command -- is
proven. Whether `unifi-ml2-driver` is a working driver once injected is
a separate, still-open question. It builds cleanly (`cargo check`/`cargo
clippy -- -D warnings` both pass, 27 unit tests) and the Helm chart
lints/renders.

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

**Third live attempt, same day: real repair completed, real outage,
safely reverted -- and a serious design flaw in the core "no custom image"
premise, not a small bug.** With the SSA/apiVersion fixes both in, the full
repair actually completed: `mechanism_drivers` patched, wheel cache
refreshed, and the Deployment injection patch succeeded and triggered a
real rollout. The post-repair present-check correctly reported "absent" --
but for the wrong reason at first glance, which led to finding a *second*
real bug before finding the serious one:

- **Bug: the post-repair re-check reused a stale, pre-repair `Deployment`
  object.** `run_once` fetches `deployment` once at the top, before
  `repair()` runs, and passes that same (now-stale) value into the
  post-repair `check_present` call -- so Layer 1 (`has_injection`) always
  sees the pre-repair state regardless of what `repair()` actually did.
  Not yet fixed as of this writing (moot for this incident, since the real
  problem below made it need reverting anyway, but still a real bug to fix
  before the next attempt: the post-repair check must re-fetch the
  Deployment, not reuse the pre-repair copy).
- **The actual serious problem**: the new `neutron-server` pod went
  `CrashLoopBackOff` (2/3 Ready) with `ModuleNotFoundError: No module
  named 'neutron.cmd.eventlet'`. Root cause: `pip install
  --target=/opt/ml2-plugins unifi-ml2-driver` doesn't install just that
  one package -- it resolves and installs its **entire declared
  dependency closure**, which includes `neutron (>=13.0.0.0b1)` and
  `neutron-lib (>=1.18.0)` themselves (see `unifi-ml2-driver`'s own
  `pyproject.toml`). `PYTHONPATH` prepends `/opt/ml2-plugins` ahead of the
  image's real site-packages in `sys.path`, so Python resolved `import
  neutron` to the **freshly pip-installed, generic upstream package**
  instead of `/var/lib/openstack/lib/python3.10/site-packages/neutron` --
  the image's actual, Platform9-patched, OVN-customized one. The generic
  one doesn't have (or lays out differently) `neutron.cmd.eventlet`,
  hence the crash. This is a fundamental problem with "just `pip install
  --target` a plugin's full dependency closure and prepend it via
  `PYTHONPATH`" as a strategy for extending an existing, already-populated
  Python environment -- it silently shadows any dependency the new
  package happens to share with the base image, not just the ones that
  are actually missing. Every design note in this doc calling the
  runtime-injection approach a clean way to "avoid a custom image" was
  correct about avoiding *image staleness*, but missed this *dependency
  shadowing* risk entirely -- discovered only by actually running it.
- **Recovery, done immediately, by hand** (the guardian was paused --
  scaled to 0 -- for the duration, so it wouldn't reapply while this was
  in progress): reverted `neutron-server`'s Deployment (strategic merge
  patch, `$patch: delete` on the injected initContainer, both/all added
  volumes, the `PYTHONPATH` env entry, and the added volumeMounts --
  note `volumeMounts` for `Container` uses `mountPath`, not `name`, as
  its strategic-merge-patch key, unlike `volumes`/`containers`/
  `initContainers`), reverted `neutron-etc`'s `mechanism_drivers` back to
  `openvswitch,ovn`, and reverted `neutron-bin`'s `neutron-server.sh`
  back to its original `--config-file`-only form (the `--config-dir` flag
  alone, pointed at a directory whose volume mount had just been removed,
  produced a *second*, different crash --
  `oslo_config.cfg.ConfigDirNotFoundError` -- on the first revert attempt,
  before this third piece was also reverted). `neutron-server` came back
  `3/3 Running` and confirmed processing real OVN/Nova port events
  normally afterward. Total live-impact window: on the order of minutes,
  fully self-contained to changes made in this same session, no user data
  or unrelated state affected.
- **What the "safe partial failure" design property actually delivered,
  precisely**: it prevented the *first two* failed attempts (SSA conflict,
  then the apiVersion/kind bug) from having any live impact at all, exactly
  as designed -- those aborted before the rollout-triggering step. It does
  **not** and structurally cannot protect against the injection succeeding
  and being *wrong in its own content*, which is what actually happened
  here. That's a different, harder problem this design doesn't yet solve.

**Fixed 2026-09-14, validated offline against the real dependency tree
before trusting it again.** Went with the exclusion-list approach (the
first candidate fix above), not the checked-last-import-order approach --
simpler, and directly testable without another live cluster round-trip.

The initContainer's install step no longer runs a plain `pip install
--find-links=... <package>` (which lets pip resolve and install the
driver's entire dependency closure unconditionally). It now walks every
driver's cached wheels, filters out filenames matching
`k8s::ASSUMED_PRESENT_PACKAGES` (a maintained list, not a one-off guess),
and installs only what's left, each with `--no-deps` so pip can't
transitively re-pull an excluded package back in.

That exclusion list needed to be **far broader than the first guess**
(`neutron`, `neutron-lib`, `oslo-*`, `stevedore`, `eventlet`). Validated by
actually running `pip download unifi-ml2-driver` locally (not against the
cluster) on 2026-09-14: it resolves **144 packages**, the large majority
of them standard OpenStack/Neutron-ecosystem tooling --
`keystoneauth1`, `python-novaclient`, `python-designateclient`,
`openstacksdk`, `SQLAlchemy`, `alembic`, `WebOb`, every `oslo.*`
subpackage, etc. -- exactly the class of thing a real Neutron+OVN+Designate
deployment already has, not just the two packages named in the original
crash. The final list (~85 entries, see `k8s.rs`) was built from this real
output, not a guess, and validated by applying the exact generated
`grep -Eiv` pattern against the real 144 filenames: it correctly excludes
all the OpenStack-ecosystem ones and leaves 41 genuinely new packages
(`unifi-ml2-driver` itself, its `aiohttp`/`aiohttp-unifi` stack,
coordination libraries `etcd3gw`/`tooz`, network-automation tooling
`netmiko`/`ncclient`/`paramiko`/`scp`/`textfsm`, and a tail of small
CLI/testing/utility libraries) -- none of them core OpenStack packages
that could plausibly already exist in the base image and get shadowed.

Two real mistakes this validation pass caught before they could cause a
*third* incident:
- The `oslo_[a-z_]+` regex fragment doesn't match `oslo_i18n` --
  `oslo.i18n`'s normalized wheel-filename form has a digit in it (`i18n`),
  which `[a-z_]+` (letters and underscores only) doesn't cover. Fixed to
  `oslo_[a-z0-9_]+`.
- `pecan` (a WSGI framework pulled in transitively by the same resolve,
  OpenStack-ecosystem-adjacent rather than UniFi-specific) wasn't on the
  list at all. Added.

**Fourth live attempt, 2026-09-14: real progress, one narrow miss, fixed
and re-validated against the *real* target platform.** With both fixes
(the exclusion list and automated revert) deployed, this attempt got
significantly further than any before it: `neutron` loaded correctly (no
more shadowing), ML2 loaded `['openvswitch', 'ovn', 'unifi']` correctly,
and it reached the point of actually loading the `unifi` entry point
itself -- which then failed with `ModuleNotFoundError: No module named
'orjson'`.

Root cause, and an important methodological lesson: the offline
validation that produced the original ~85-entry exclusion list was run
with `pip download` **on a local macOS/Python 3.14 machine**, not the
target's Linux/Python 3.10. Platform- and interpreter-conditional
dependency markers mean these two resolutions genuinely differ -- proven
by fetching the *actual* wheel-cache contents from the live PVC (via a
throwaway debug pod mounting it, `kubectl exec ... find /wheelcache
-mindepth 2 -maxdepth 2`) and comparing against local validation: 147 real
packages vs. 144 estimated locally, with real, meaningful differences
(`python-neutronclient`, `pyroute2`, `tomli`, `backports.strenum`,
`async-timeout` appear on the real platform and hadn't been seen locally
at all). Two concrete bugs came from this, both found and fixed by
re-validating against the *real* list before trusting a fix again, not by
another live-fire guess:
- **`orjson` was wrongly on the exclusion list in the first place.** It
  had shown up in the original dependency dump and was assumed (wrongly)
  to already be present -- it's an optional accelerator library some
  packages use if available, and the real image genuinely doesn't have
  it. The crash was direct proof. Removed, with an explicit comment
  explaining why it's *not* there, so a future pass doesn't re-add it on
  the same flawed reasoning.
- **`python-neutronclient` was missing from the list entirely**, despite
  its siblings (`python-keystoneclient`, `python-novaclient`,
  `python-designateclient`) already being present -- a plain oversight,
  caught once the real wheel list made it visible. Added.

Also caught, while re-validating: the quick Python script used to extract
`ASSUMED_PRESENT_PACKAGES` out of `k8s.rs` for testing wasn't
comment-aware, and briefly "found" `orjson` again because the word
appeared inside this very doc-comment's explanation of why it was
removed. Not a code bug -- the actual `.rs` array was correct -- but a
reminder that any tooling built to double-check this list needs to
exclude comments the same way the compiler does.

Re-validated the corrected list against the **real, authoritative** wheel
list from the live PVC (not another local approximation): 45 packages
survive the filter now (up from 44 -- `orjson` correctly included, and
`python-neutronclient` correctly newly excluded), and every one of them is
`unifi-ml2-driver` itself, its `aiohttp`/`aiohttp-unifi` stack,
coordination libraries, network-automation tooling, or small
CLI/testing/utility libraries -- nothing that looks like a core OpenStack
package.

**The updated, still-honest caveat**: this list is now validated against
the actual target platform's actual resolved dependency set, not a
cross-platform guess -- meaningfully stronger evidence than the previous
pass had. It's still not verified item-by-item against `pf9-neutron`'s
real site-packages (i.e., "resolved as a dependency" and "already
installed in the base image" are still two different questions this
approach conflates), so a future repair crashing on a *new* shadowed
package remains possible and should still be read as "add one more
entry," not "the approach is wrong." The debug pod and local files used
for this validation were cleaned up afterward; this isn't a standing
diagnostic tool, just how this specific investigation was done.

Still true from the second attempt: confirm the post-repair present-check
and `neutron-server`'s stability once a real repair can be attempted
safely again -- now the actual next step, with meaningfully higher
confidence than before.

**Fifth live attempt, 2026-09-14: complete success, after three more
narrow misses in the same dependency-shadowing family and one RBAC
bug.** The `orjson`/`python-neutronclient` fix above got past the
`neutron`/`neutron-lib` shadowing entirely, but the *driver's own*
dependency `aiohttp-unifi` (the `aiounifi` package) turned out to have a
different problem: several of its modules assume a newer Python than the
target image's 3.10. Each crash was fixed and re-attempted in turn:
- `cannot import name 'Self' from 'typing'` (PEP 673, Python 3.11+).
- Then `cannot import name 'NotRequired' from 'typing'` (PEP 655,
  3.11+) -- the *same* unguarded import statement in `firewall_policy.py`/
  `firewall_zone.py` names both symbols together, so fixing only `Self`
  wasn't enough; a single import fails whole if any named attribute is
  missing.
- Then `cannot import name 'StrEnum' from 'enum'` (3.11+), from
  `traffic_route.py`.

Rather than keep discovering these one crash at a time, the third one
prompted a full static scan of every `.py` file in every cached wheel
(147 wheels) for unconditional imports of any Python 3.11+-only
typing/enum name -- confirming `aiohttp-unifi` has exactly four unguarded
call sites total (all now covered) and that every other hit the scan
found (in `aiohttp`, `multidict`, `yarl`, `setuptools`, `packaging`,
`cmd2`, `oslo_db`, `dogpile_cache`, `fixtures`, `pyjwt`, `aiosignal`, even
`typing_extensions` itself) was a false positive from a regex too naive
to recognize `if sys.version_info` guards and `TYPE_CHECKING` blocks --
confirmed by hand for a sample (the `typing_extensions` "hit" was
literally inside its own docstring).

The fix: rather than patch `aiounifi`'s own files (fragile across
version bumps, and this is a genuine upstream bug in a third-party
dependency, not something this project should be forking), the injector
now also writes a `sitecustomize.py` into the installed-plugins
directory. Python's `site` module auto-imports it at interpreter startup
for anything importable on `sys.path` -- PYTHONPATH included -- so it
runs before `neutron-server` loads any mechanism driver, and back-ports
`typing.Self`/`NotRequired`/`Required` and `enum.StrEnum` onto the real
stdlib modules from the already-cached `typing_extensions` and
`backports.strenum` packages, guarded by `sys.version_info`. Deliberately
general rather than `aiounifi`-specific: any future driver whose own
dependencies assume a newer Python than the target image provides is
covered for free, with no per-driver knowledge needed.

One more real bug surfaced along the way, self-inflicted this time: the
first version of that shim string was built with Rust's
backslash-newline line-continuation spread across source lines for
readability, which (correctly, if surprisingly) strips *all* leading
whitespace from the continued line -- silently eating every line's
Python indentation and producing `IndentationError: expected an
indented block after 'if' statement on line 2`. Caught live (one more
crash-and-revert cycle), fixed by moving the script into its own
`SITECUSTOMIZE_PY` constant as a single-line string with explicit `\n`
escapes, and verified from then on by actually rendering the exact
`install_cmd` output and running it through Python's `compile()` before
touching the cluster again -- a cheap check that should have been done
from the start.

With all four `aiounifi` import sites fixed, the repair finally
succeeded end to end: `unifi` loaded, initialized, and registered as a
mechanism driver with no errors, `neutron-server` reached and held
`3/3 Running` with zero restarts, and its logs showed real API traffic
(`GET /v2.0/security-groups`, `/v2.0/routers`, `/v2.0/subnets`, etc.)
all returning `200`.

The one remaining gap after that was in the guardian's *own*
verification, not the repair itself: the post-repair present-check kept
reporting a false "degraded" via a `403 Forbidden` on every exec
attempt, including on pods with no rollout in progress at all -- so not
just the rollout-timing race it first looked like (`find_ready_pod` was
also fixed to exclude pods with a `deletionTimestamp` set, and the
post-repair check now retries for up to ~30s, both genuine improvements,
but neither was the actual cause here). Root-caused by minting a real
short-lived token for the guardian's own ServiceAccount and curling the
exec endpoint directly rather than trusting `kubectl auth can-i` or a
`kubectl exec --as` impersonation test (both said "yes"/appeared to
work, but neither actually authenticates as the ServiceAccount's own
bearer token the way the real in-cluster client does): the API server's
actual error was `cannot get resource "pods/exec"` -- note *get*, not
*create*. The RBAC convention that `pods/exec` needs the `create` verb
assumes `kubectl`'s traditional SPDY exec transport, which upgrades via
an HTTP POST. This controller's `kube-rs` client is built with the `ws`
cargo feature (the newer WebSocket-based exec transport), which upgrades
via an HTTP GET instead -- confirmed via `kube-rs`'s own trace-level
logging -- and Kubernetes maps HTTP GET to the RBAC verb `get`. The
chart's Role granted only `create`, so it denied every single exec
request from this specific client, consistently, not intermittently.
Fixed by granting both verbs. After that fix, `/metrics` showed
`ml2_driver_present{driver="unifi"} 1` with no further errors, and
`neutron-server` remained untouched and stable throughout (a genuine
present-check running against an already-healthy driver never
triggers a repair).

**Correction, later the same day: "fully successful" above was premature.**
Everything in it is true as far as it goes -- the driver loads, `neutron-
server` stays healthy, the guardian's own present-check agrees -- but
none of that actually exercises the mechanism driver's real job. `unifi`
only *does* anything on `create_network_postcommit`/`update_.../delete_...`
-- i.e. an actual `openstack network create`. Every check this project
had run up to this point (health, `/metrics`, GET-only API traffic) never
triggered that code path even once. The very next real network create
(a `tofu apply` for an unrelated VLAN network, from a different, ongoing
piece of work against this same cluster) did, and it failed immediately:

```
oslo_config.cfg.NoSuchOptError: no such option controller in group [unifi]
```

Root cause, found by pulling `unifi_ml2_driver`'s actual wheel content out
of the cache and reading it directly: `unifi_mech.py`'s `_get_controller()`
checks `if CONF.unifi.controller not in self._controllers` before every
single controller-API call, but `config.py` never registers a `controller`
option in the `[unifi]` group at all -- only `host`, `port`, `apikey`,
`username`, `password`, `site`, `verify_ssl`, `cafile`, and assorted
feature flags. This isn't a config mistake on this project's side; it's a
genuine bug in the published package -- confirmed still present in the
latest release on PyPI (1.0.9) by downloading and reading it directly,
not just the 1.0.5 this cluster's wheel cache happened to resolve. Every
call to `_get_controller()` -- which is to say, every real network
create/update/delete -- raises unconditionally. `create_network_postcommit`
isn't wrapped in the same broad try/except that `initialize()` uses (which
is why *that* call succeeds silently at startup and never surfaces this);
Neutron's own ML2 manager catches the resulting `MechanismDriverError` and
rolls back by deleting the network it just created -- which is exactly
what happened to the unrelated `tofu apply`'s `vlan1000-net`, and why it
came back as a hard failure rather than the driver just quietly not
syncing anything.

A second, independent gap surfaced by the same investigation: 1.0.5 (what
was actually running) doesn't support API-key auth at all --
`get_unifi_api()` only passes `username`/`password` to `aiounifi`'s
`Configuration`, and `config.py` has no `apikey` option, despite this
project's whole config schema (`extraConfigSecretData`/`extraConfigSecretRef`,
`host`/`apikey`/`site`) being built around the newer key-based API this
repo's original investigation found documented. 1.0.9 does add a real
`apikey` StrOpt and does pass it through to `Configuration` -- so this
part is fixed upstream, just not in the version the wheel cache had
resolved. `_sync_networks()` remains a documented no-op stub in both
versions ("Implementation would require access to the Neutron DB...
Skipped for this example") -- not exercised by this bug, but a sign this
package may be closer to a reference implementation than a
production-hardened driver.

**Immediate response**: reverted via the same `DRY_RUN=true` mechanism as
every other incident this project has hit -- `neutron-server` was back to
serving normal network CRUD within seconds, unblocking the VLAN work this
bug had broken. **Not yet done**: a real fix (most likely, following this
project's own established pattern, a `sitecustomize.py` addition that
registers the missing `controller` oslo_config option with some harmless
default -- the value doesn't matter functionally, since `self._controllers`
is actually keyed by `CONF.unifi.host`, not `CONF.unifi.controller`, so
this really is just a dead/leftover conditional) combined with pinning the
wheel cache to `unifi-ml2-driver>=1.0.6` for real `apikey` support, then --
critically -- actually exercising a real `openstack network create`
end-to-end before calling this driver working again. Every previous
"success" in this doc was real for what it tested, but what it tested
turned out not to include the one thing that matters.

## Automated revert: `DRY_RUN=true` now means "zero footprint," not just "don't touch anything"

Added 2026-09-14, directly in response to the incident above: switching
`DRY_RUN` back to `true` after a bad `DRY_RUN=false` attempt used to do
nothing but stop future repairs -- whatever damage was already done stayed
in place, requiring exactly the by-hand `kubectl patch` recovery this
project's own incident needed. Now `revert_or_preview` (in `reconcile.rs`)
handles the entire `DRY_RUN=true` path, and covers two distinct cases:

- **No guardian footprint found** (nothing in `mechanism_drivers` matches a
  configured driver, and the injection isn't present): unchanged from the
  original design -- log what a real repair *would* do
  (`describe_intended_repair`), mutate nothing. This preserves the
  original safety value of previewing a driver's first-ever activation
  before ever setting `DRY_RUN=false`.
- **A footprint is found**: actively revert it. Removes each configured
  driver from `mechanism_drivers`, removes the `--config-dir` flag from
  `neutron-server.sh`, and removes the entire injected
  initContainer/volumes/env/mounts from the Deployment
  (`k8s::remove_injection_patch`) -- unconditionally for every configured
  driver, regardless of whether `check_present` would call its individual
  state `Present` or `Degraded`. That distinction deliberately isn't
  reused here: it answers "is the driver working," a different question
  from "did the guardian leave something behind," and the actual incident
  state (crash-looping, no Ready pod) reads as `Degraded`, not `Present` --
  a naive "only revert what's `Present`" design would have missed exactly
  the case that motivated this feature.

`remove_injection_patch` deliberately uses an explicit `Patch::Strategic`
with `$patch: delete` entries -- not a `Patch::Apply` that simply omits
fields it no longer wants (which SSA's "omission removes what you own"
semantics would plausibly also achieve, but that was never actually
verified against this cluster, and this project already got burned once by
an unverified assumption about Kubernetes patch behavior in the same
incident). `$patch: delete` is the exact mechanism already confirmed
working, by hand, during that incident's real recovery -- this reuses
proven behavior rather than a new untested code path. One real detail this
surfaced: `volumeMounts`' strategic-merge-patch key is `mountPath`, not
`name`, unlike `volumes`/`containers`/`initContainers` (all `name`) --
discovered the hard way during the incident's manual recovery, now baked
into the automated version too.

New pure functions (`remove_driver_from_ml2_conf`,
`remove_config_dir_flag`), both infallible and idempotent, with
round-trip tests (`remove_X(add_X(input)) == input`) against the
corresponding `add_X` functions.

**Live-tested twice on 2026-09-14, both times successfully, one
genuinely on a real incident:**
1. A self-inflicted deployment-sequencing mistake (scaling the guardian
   back up *before* applying the values/image update that set
   `DRY_RUN=true` and the fixed image) briefly let the *old*, pre-fix pod
   run with `DRY_RUN=false` against an already-clean cluster, re-adding
   `unifi` to `mechanism_drivers` and re-adding the `--config-dir` flag
   before being replaced -- never reaching the Deployment injection step,
   so `neutron-server` itself was never touched. The next (fixed,
   `DRY_RUN=true`) pod correctly detected this partial footprint and
   reverted it (`mechanism_drivers` and the `--config-dir` flag both), on
   its very first reconcile tick, no human intervention. Confirmed a real
   operational lesson too: always update values/image *before* scaling a
   paused guardian back up, never after -- a stale-config replica can
   start in the gap otherwise.
2. **The real test**: after the fourth live repair attempt above crashed
   `neutron-server` on the `orjson` gap, `DRY_RUN` was flipped back to
   `true` (deliberately, as the actual recovery mechanism this time,
   instead of the by-hand `kubectl patch` sequence the earlier incident
   needed). The guardian detected the full footprint -- `mechanism_drivers`,
   the `--config-dir` flag, *and* the injected initContainer/volumes/env --
   and reverted all three, unprompted beyond the `DRY_RUN=true` flip.
   `neutron-server` came back `3/3 Running` within roughly a minute, zero
   restarts afterward, confirmed processing real API traffic (`GET
   /v2.0/ports` returning `200`). This is the actual scenario the feature
   was built for, working as designed, under genuine pressure -- not a
   synthetic test.
