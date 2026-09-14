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

1. `mechanism_drivers`: read the release's current computed value, compute
   *"current list + this driver's name if missing"* (never a hardcoded
   string, so a future PCD default change isn't clobbered), and apply via
   `helm upgrade --reuse-values --set conf.neutron.ml2_conf.ml2.mechanism_drivers=...`.
2. Write the driver's `extraConfigSecretData` (if any) into its own Secret.
3. Refresh the wheel cache (see above) and classify what it produced.
4. **Re-apply the Deployment injection patch.** This is *not* preserved by
   the `helm upgrade` in step 1 -- Helm computes its upgrade patch from its
   own release history, which never included this out-of-band addition, so
   any `helm upgrade` of this release (including the guardian's own
   `mechanism_drivers` fix) can silently drop it. It must be reapplied
   every repair cycle, not treated as one-time setup.
5. Re-run the full "present" check to confirm the repair actually took
   effect before declaring success -- a `helm upgrade`/`kubectl patch`
   exiting 0 doesn't mean the driver actually loaded.

Implemented in `src/reconcile.rs::repair`.

**Known implementation gap:** `helm upgrade` needs a chart reference, and
per the design intent that should be pulled directly out of the release's
own stored data (avoiding any external chart-repo dependency) via
something like `helm get metadata`/`helm pull` against the running
release. This repo's first pass (`src/reconcile.rs::repair`) has a
placeholder chart reference and explicitly does not resolve this yet --
confirming the exact `helm` subcommand/flow for this against a real PCD
cluster is the next concrete step before this can run for real.

## Safety rails

- **Backoff, don't repair-loop.** After
  `MAX_CONSECUTIVE_REPAIR_FAILURES` (3, in `src/reconcile.rs`) failed
  repairs for a driver, the guardian stops attempting further repairs for
  it until the process restarts, and surfaces a sustained "degraded" signal
  instead of repeatedly restarting a broken deployment.
- **Least-privilege, cross-namespace RBAC.** A `Role` (not `ClusterRole`)
  scoped to the target namespace only (`deploy/helm/.../templates/role.yaml`),
  bound to the guardian's ServiceAccount from its own namespace. This is
  worth calling out plainly: this grant is effectively "can run `helm
  upgrade neutron` and rewrite its Deployment's pod spec" -- review and
  approve it deliberately, don't treat it as routine.
- **Scoped to one release, by name.** The guardian never touches any Helm
  release other than the one configured (`helmReleaseName`, default
  `neutron`).
- **Idempotent.** No `helm upgrade`/`kubectl patch` call at all on a
  reconcile tick where the present-check already passes.
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

`soleDriver` (optional, default `false`) exists for the uncommon case of a
driver that isn't designed to coexist additively alongside
`openvswitch`/`ovn` and other add-on drivers -- the guardian refuses to
combine two `soleDriver`-flagged entries (`src/config.rs::Config::from_env`)
rather than silently producing a broken `mechanism_drivers` list.

## Implementation decisions

- **Language/tooling: Rust + `kube-rs`, shelling out to the bundled `helm`
  CLI binary** (see `src/helm.rs`), rather than Go + Helm's native SDK.
  Consistent with `estenrye/pdns4-shim`'s existing stack and this
  ecosystem's general preference for shelling out to well-tested CLI tools
  over reimplementing their logic.
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

**Still open before this is safe to actually run:**
- The `helm upgrade` chart-reference resolution (see "Repair logic" above)
  -- still a placeholder.
- A real Prometheus text-format `/metrics` handler (OTLP export works
  today; the annotated-scrape path doesn't yet).
- No test suite yet -- `wheelcache::classify` and the driver-config
  YAML/`mechanism_drivers`-string parsing are pure functions and should be
  the first ones covered, since they need no live cluster.
- **A dry-run mode.** Given the guardian's RBAC grant is real ("can run
  `helm upgrade neutron` and rewrite its Deployment's pod spec"), the first
  run against a real cluster should log its intended `helm upgrade`
  args/patch instead of executing them, so one cycle's output can be
  reviewed before enabling fully-automatic repair. Not yet implemented.
