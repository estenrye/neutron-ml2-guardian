# syntax=docker/dockerfile:1

# Multi-arch (linux/amd64, linux/arm64) image, built via `docker buildx
# build --platform linux/amd64,linux/arm64`. Structure mirrors
# estenrye/pdns4-shim's Dockerfile (cross-compile rather than emulate,
# distroless runtime, no C dependencies of our own since kube-rs and the
# OTLP exporter both use rustls) -- see that file for the full rationale.
# The one addition here: this image also bundles a pinned `helm` CLI
# binary, since the guardian shells out to it rather than using a native
# Helm SDK (see docs/specs/2026-09-13-neutron-ml2-guardian-design.md's
# "Implementation decisions").

ARG RUST_VERSION=1
ARG HELM_VERSION=3.16.4

FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-bookworm AS builder
ARG TARGETARCH
WORKDIR /build
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# hadolint ignore=DL3008
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) rust_target=x86_64-unknown-linux-gnu; cross_pkgs=(gcc-x86-64-linux-gnu libc6-dev-amd64-cross); cross_linker=x86_64-linux-gnu-gcc ;; \
      arm64) rust_target=aarch64-unknown-linux-gnu; cross_pkgs=(gcc-aarch64-linux-gnu libc6-dev-arm64-cross); cross_linker=aarch64-linux-gnu-gcc ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    echo "$rust_target" > /rust_target.txt; \
    rustup target add "$rust_target"; \
    host_target="$(rustc -vV | sed -n 's/^host: //p')"; \
    if [ "$rust_target" != "$host_target" ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends "${cross_pkgs[@]}"; \
      rm -rf /var/lib/apt/lists/*; \
      mkdir -p .cargo; \
      printf '[target.%s]\nlinker = "%s"\n' "$rust_target" "$cross_linker" >> .cargo/config.toml; \
    fi

# Cached dependency layer -- see pdns4-shim's Dockerfile for why this shape.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked --target "$(cat /rust_target.txt)" \
    && rm -rf src "target/$(cat /rust_target.txt)/release/neutron-ml2-guardian" \
         target/"$(cat /rust_target.txt)"/release/deps/neutron_ml2_guardian-*

COPY src ./src
RUN cargo build --release --frozen --target "$(cat /rust_target.txt)" \
    && cp target/"$(cat /rust_target.txt)"/release/neutron-ml2-guardian /build/neutron-ml2-guardian

# Fetches the pinned, statically-linked `helm` release binary for
# $TARGETARCH. No cross-compilation needed here, unlike the Rust build
# above -- just picking the right prebuilt tarball.
FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS helm
ARG TARGETARCH
ARG HELM_VERSION
WORKDIR /helm
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://get.helm.sh/helm-v${HELM_VERSION}-linux-${TARGETARCH}.tar.gz" -o helm.tar.gz \
    && tar -xzf helm.tar.gz "linux-${TARGETARCH}/helm" --strip-components=1 \
    && chmod +x helm

# hadolint ignore=DL3065
FROM --platform=$TARGETPLATFORM gcr.io/distroless/cc-debian12:nonroot AS runtime
WORKDIR /

# rustls-native-certs (used by both the Kubernetes client and the OTLP
# exporter) reads the system trust store.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /build/neutron-ml2-guardian /usr/local/bin/neutron-ml2-guardian
COPY --from=helm /helm/helm /usr/local/bin/helm

USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/neutron-ml2-guardian"]
