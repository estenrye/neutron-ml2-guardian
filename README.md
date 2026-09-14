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
  - name: unifi                        # the mechanism_drivers/stevedore name
    pipPackage: unifi-ml2-driver       # pip-installable package
    importModule: unifi_ml2_driver     # top-level module to test-import
    extraConfigSecretData: |           # opaque -- never parsed by the guardian
      [unifi]
      host = 192.168.1.1
      apikey = REPLACE_ME
      site = default
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

By default (`dryRun: true`) it will not actually touch the target release --
it logs exactly what a repair would do (the computed `mechanism_drivers`
value, the intended Deployment patch, the wheel-cache refresh) without
patching anything or creating a Job. Watch a few reconcile cycles' logs,
confirm they look right, then deliberately set `dryRun: false` (`--set
dryRun=false` or in your values file) to let it actually repair. This
default exists specifically because the RBAC grant above is real power and
shouldn't run unattended before you've seen what it intends to do.

## Status

**First-pass implementation, not yet run against a real cluster with
`DRY_RUN=false`.** It builds clean (`cargo check`, `cargo clippy -- -D
warnings`) and the Helm chart lints/renders. Verified against the live
cluster: `PYTHONPATH` propagation, the real container name, and (via a
manual, `--dry-run`-only reproduction) that patching `neutron-etc`'s
`ml2_conf.ini` key directly is the right approach -- the originally-planned
`helm upgrade` route turned out to be a dead end (Helm can't recover
subchart data from its own release storage) and was replaced. Remaining
gaps: no real Prometheus `/metrics` handler yet, no test suite, and a
driver needing its own `extraConfigSecretData` isn't fully wired in yet
(the Secret gets created but nothing mounts it into `neutron-server` yet) --
see the design doc's "Status" section for the full list.

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
