# neutron-ml2-guardian

A self-healing Kubernetes controller that keeps one or more third-party
Neutron ML2 mechanism drivers installed and configured on a [Platform9
Private Cloud Director](https://platform9.com/private-cloud-director/)
`neutron` Helm release -- **without a custom container image**, and
surviving PCD upgrades that would otherwise silently revert the
customization.

```
neutron-ml2-guardian --reconcile every 5m-->
  1. checks whether each configured driver is actually loaded in the live
     neutron-server pod (not just "configured" -- actually importable and
     not crash-looping)
  2. if not: helm-upgrades the mechanism_drivers list, patches in a
     pip-install initContainer (using whatever image neutron-server
     currently runs), and refreshes a shared wheel cache -- then re-checks
     before declaring success
```

See
[`docs/specs/2026-09-13-neutron-ml2-guardian-design.md`](docs/specs/2026-09-13-neutron-ml2-guardian-design.md)
for the full design rationale, and
[`estenrye/pcd-ce-deploy`](https://github.com/estenrye/pcd-ce-deploy)'s
`docs/neutron-vlan-udm-se-integration-spec.md` for the broader investigation
this grew out of (getting PCD's Neutron to drive VLAN config on a Ubiquiti
UDM-SE).

## Why this exists

Platform9 PCD Community Edition has no supported, documented way to add a
third-party Neutron ML2 mechanism driver (see the design doc's
"Investigated" sections for what was tried). `neutron-server` turns out to
run as an ordinary Kubernetes Deployment inside PCD's own management-plane
cluster, installed by a one-shot `helm install` with no ongoing
reconciliation -- so a driver *can* be added via a normal `helm upgrade`,
but nothing stops a future PCD version upgrade from silently reverting it
back to defaults. This controller is that missing piece: it detects drift
and repairs it, indefinitely, without requiring a rebuilt/re-pinned
container image on every PCD release.

## Configuring drivers

Nothing about this tool is UniFi-specific -- it's driven entirely by a
list of driver specs. Example `values.yaml` override:

```yaml
targetNamespace: pcd          # namespace neutron-server lives in
helmReleaseName: neutron      # Helm release to guard
deploymentName: neutron-server

ml2Drivers:
  - name: unifi                              # the mechanism_drivers/stevedore name
    pipPackage: unifi-ml2-driver-estenrye    # pip-installable package -- github.com/estenrye/networking-unifi,
                                              # a fork fixing three real bugs and a requires-python constraint
                                              # tighter than the code actually needs (see the design doc's
                                              # "Sixth live attempt"); same import name/entry point as upstream
    importModule: unifi_ml2_driver           # top-level module to test-import
    extraConfigSecretData: |           # opaque -- never parsed by the guardian
      [unifi]
      host = 10.45.0.1                 # real, verified reachable UDM-SE address for this cluster
      port = 443                       # required for a UDM-SE -- its default (8443) is the classic/
                                        # self-hosted-controller port, not a UniFi OS console's
      apikey = REPLACE_ME
      site = default
      verify_ssl = false               # required -- UDM-SEs use a self-signed certificate
```

Add more entries to guard multiple drivers with the same controller. See
`deploy/helm/neutron-ml2-guardian/values.yaml` for the full schema
(`soleDriver`, wheel cache sizing, reconcile interval, etc.).

Keep any values file containing real credentials (like the `apikey` above)
out of plaintext version control -- `.gitignore` already excludes
`values.secrets.yaml` as a convention for this.

## Deploying

```sh
helm install neutron-ml2-guardian deploy/helm/neutron-ml2-guardian \
  --namespace neutron-ml2-guardian --create-namespace \
  -f my-drivers.values.yaml
```

This installs:
- The guardian itself (its own namespace).
- A `Role`/`RoleBinding` in `targetNamespace` granting the guardian access
  there (cross-namespace, on purpose -- see the design doc's "Safety
  rails": this can rewrite `neutron-server`'s rendered config and pod spec
  directly, a meaningfully powerful grant, and should be reviewed
  deliberately -- though narrower than an earlier draft, since it no longer
  needs `helm upgrade`, see the design doc's "Repair logic").
- A shared `ReadWriteMany` PVC for the wheel cache, in `targetNamespace`.
  Defaults to `pcd-sc`, Platform9 PCD's own default StorageClass -- checked
  against a live PCD CE cluster and confirmed to be
  `kubevirt.io.hostpath-provisioner`-backed, which is node-local rather
  than genuinely RWX-capable. That's fine for PCD CE (typically
  single-node) and likely fine for on-prem installs too, but it will not
  work on an actual multi-node cluster -- override
  `wheelcache.storageClassName` to a real RWX class (NFS-backed, etc.) if
  yours is one.

`dryRun: true` (the default) means **the guardian's real-world footprint
should be zero** -- which is two different behaviors depending on current
state, not just "do nothing":
- If nothing's been installed yet, it only logs what a repair would do
  (the computed `mechanism_drivers` value, the intended Deployment patch,
  the wheel-cache refresh) without mutating anything. Watch a few
  reconcile cycles' logs, confirm they look right, then deliberately set
  `dryRun: false` to let it actually repair.
- If something *was* installed (e.g. you'd previously set `dryRun: false`
  and want to undo it), it actively reverts -- removing the driver from
  `mechanism_drivers`, removing the injected initContainer/volumes/env
  from `neutron-server`, and removing the `--config-dir` flag it added.
  This is a real, deliberate safety feature (added after an actual
  incident during development -- see the design doc), not a side effect:
  flipping back to `dryRun: true` is meant to be usable as an undo button
  if a repair goes wrong, without needing to hand-craft `kubectl` patches
  under pressure.

This default exists specifically because the RBAC grant above is real
power and shouldn't run unattended before you've seen what it intends to
do -- in either direction.

## Status

**Fully working end to end, confirmed against real production
hardware, 2026-09-14.** A real `openstack network create` against a
guardian-installed `unifi` driver succeeded, and the resulting VLAN
genuinely exists on the real UDM-SE -- confirmed directly via its own
API, both for create and delete. `neutron-server` stays healthy
throughout and the guardian's own post-repair check agrees
(`ml2_driver_present{driver="unifi"} 1`). Getting here took ten real
bugs found live, one crash/config-gap at a time -- a
dependency-shadowing crash, four separate Python-3.11-only gaps an
upstream driver dependency assumed were available, an RBAC verb
mismatch between `kubectl`'s exec transport and this project's
WebSocket-based one, a Terraform config bug in this project's own
`extraConfigSecretRef` wiring, and three real bugs in the driver itself
(a bad config-option reference, a backwards SSL purpose, and a missing
`vlan_enabled` flag) that needed a fork
([estenrye/networking-unifi](https://github.com/estenrye/networking-unifi),
published as `unifi-ml2-driver-estenrye`) to fix now rather than wait on
upstream -- every one of those was also submitted there as its own PR.
Each bug was caught live and, where it mattered, recovered via the
automated `DRY_RUN=true` revert described below, exercised repeatedly as
a genuine incident-recovery mechanism, not just tested synthetically.
See the design doc's "Status" section (the "Sixth live attempt"
writeup) for the full blow-by-blow.

It builds clean (`cargo check`, `cargo clippy -- -D warnings`), has a
27-test suite covering every pure-function piece (including a smoke test
that gathers+encodes real Prometheus text, not just that instrument
registration doesn't panic), and the Helm chart lints/renders.
`extraConfigSecretData`/`extraConfigSecretRef` are both fully wired
(mounted via a `--config-dir` oslo.config scans automatically) and
`/metrics` returns real Prometheus text format.

## Building and running

```sh
cargo build --release
```

Runs against your current kubeconfig context locally, or in-cluster
ServiceAccount config when run as a Pod (standard `kube-rs` behavior).
Requires a `helm` binary on `PATH` (the container image bundles one; see
the Dockerfile).

Configuration is environment variables plus a mounted driver-list file --
see `src/config.rs` for the full list (`TARGET_NAMESPACE`,
`HELM_RELEASE_NAME`, `DEPLOYMENT_NAME`, `WHEELCACHE_PVC_NAME`,
`RECONCILE_INTERVAL_SECS`, `DRIVERS_CONFIG_PATH`).

## Logging and observability

Structured JSON logs on stdout always (`RUST_LOG`, default `info`). Set
`OTEL_EXPORTER_OTLP_ENDPOINT` to additionally export traces/logs/metrics
via OTLP/gRPC. A `/metrics` HTTP route exists for Prometheus
annotation-based scraping, but currently returns a placeholder rather than
real Prometheus text format -- OTLP export is the reliable path today (see
the design doc's "Status").

## Container image

Multi-arch (`linux/amd64`, `linux/arm64`), built on
`gcr.io/distroless/cc-debian12:nonroot`, bundling a pinned `helm` binary
alongside the guardian:

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t neutron-ml2-guardian:local .
```

Published images: `docker.io/estenrye/neutron-ml2-guardian` (see
`.github/workflows/ci-cd.yml` for the tagging scheme).
