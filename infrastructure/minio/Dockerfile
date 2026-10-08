# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

# MinIO Community Edition stopped publishing durable binary container images.
# Build the last project-validated server and client releases from immutable
# source commits instead. Source archives are checksum-verified before use;
# Go's committed go.sum files verify the module graph.
ARG GO_BUILDER_IMAGE="golang:1.24.6-alpine3.22@sha256:c8c5f95d64aa79b6547f3b626eb84b16a7ce18a139e3e9ca19a8c078b85ba80d"
ARG RUNTIME_IMAGE="alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1"

FROM ${GO_BUILDER_IMAGE} AS minio-builder

ARG MINIO_VERSION="2025-09-07T16:13:09Z"
ARG MINIO_RELEASE_TAG="RELEASE.2025-09-07T16-13-09Z"
ARG MINIO_COMMIT="07c3a429bfed433e49018cb0f78a52145d4bedeb"
ARG MINIO_SOURCE_URL="https://codeload.github.com/minio/minio/tar.gz/07c3a429bfed433e49018cb0f78a52145d4bedeb"
ARG MINIO_SOURCE_SHA256="8819e3e7817e46b7b3798f8f200ead208562e571563c2e040352378031abe9f2"

ENV GOTOOLCHAIN=local
WORKDIR /src/minio

RUN wget -q -O /tmp/minio-source.tar.gz "${MINIO_SOURCE_URL}" \
    && echo "${MINIO_SOURCE_SHA256}  /tmp/minio-source.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/minio-source.tar.gz --strip-components=1 \
    && rm /tmp/minio-source.tar.gz \
    && test "$(grep '^toolchain ' go.mod)" = "toolchain go1.24.2"

RUN --mount=type=cache,id=omni-minio-go-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=omni-minio-go-build,target=/root/.cache/go-build,sharing=locked \
    CGO_ENABLED=0 go build \
      -tags kqueue \
      -trimpath \
      -ldflags "-s -w \
        -X github.com/minio/minio/cmd.Version=${MINIO_VERSION} \
        -X github.com/minio/minio/cmd.CopyrightYear=2025 \
        -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_RELEASE_TAG} \
        -X github.com/minio/minio/cmd.CommitID=${MINIO_COMMIT} \
        -X github.com/minio/minio/cmd.ShortCommitID=07c3a429bfed \
        -X github.com/minio/minio/cmd.GOPATH=/go \
        -X github.com/minio/minio/cmd.GOROOT=/usr/local/go" \
      -o /out/minio .

FROM ${GO_BUILDER_IMAGE} AS mc-builder

ARG MC_VERSION="2025-08-13T08:35:41Z"
ARG MC_RELEASE_TAG="RELEASE.2025-08-13T08-35-41Z"
ARG MC_COMMIT="7394ce0dd2a80935aded936b09fa12cbb3cb8096"
ARG MC_SOURCE_URL="https://codeload.github.com/minio/mc/tar.gz/7394ce0dd2a80935aded936b09fa12cbb3cb8096"
ARG MC_SOURCE_SHA256="95cd293c7119f16921a6dc515a1fb74a2227f19fd994b9c8b770a154e802ac44"

ENV GOTOOLCHAIN=local
WORKDIR /src/mc

RUN wget -q -O /tmp/mc-source.tar.gz "${MC_SOURCE_URL}" \
    && echo "${MC_SOURCE_SHA256}  /tmp/mc-source.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/mc-source.tar.gz --strip-components=1 \
    && rm /tmp/mc-source.tar.gz \
    && test "$(grep '^toolchain ' go.mod)" = "toolchain go1.23.10"

RUN --mount=type=cache,id=omni-mc-go-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=omni-mc-go-build,target=/root/.cache/go-build,sharing=locked \
    CGO_ENABLED=0 go build \
      -tags kqueue \
      -trimpath \
      -ldflags "-s -w \
        -X github.com/minio/mc/cmd.Version=${MC_VERSION} \
        -X github.com/minio/mc/cmd.CopyrightYear=2025 \
        -X github.com/minio/mc/cmd.ReleaseTag=${MC_RELEASE_TAG} \
        -X github.com/minio/mc/cmd.CommitID=${MC_COMMIT} \
        -X github.com/minio/mc/cmd.ShortCommitID=7394ce0dd2a8" \
      -o /out/mc .

FROM ${RUNTIME_IMAGE} AS minio

ARG MINIO_RELEASE_TAG="RELEASE.2025-09-07T16-13-09Z"
ARG MINIO_COMMIT="07c3a429bfed433e49018cb0f78a52145d4bedeb"
ARG MC_RELEASE_TAG="RELEASE.2025-08-13T08-35-41Z"
ARG MC_COMMIT="7394ce0dd2a80935aded936b09fa12cbb3cb8096"

LABEL org.opencontainers.image.title="OmniRetail MinIO" \
      org.opencontainers.image.description="Repository-owned pinned MinIO Community source build" \
      org.opencontainers.image.version="${MINIO_RELEASE_TAG}" \
      org.opencontainers.image.revision="${MINIO_COMMIT}" \
      io.omniretail.mc.version="${MC_RELEASE_TAG}" \
      io.omniretail.mc.revision="${MC_COMMIT}"

COPY --from=minio-builder /out/minio /usr/bin/minio
COPY --from=minio-builder /src/minio/LICENSE /licenses/minio/LICENSE
COPY --from=minio-builder /src/minio/CREDITS /licenses/minio/CREDITS
COPY --from=mc-builder /out/mc /usr/bin/mc
COPY --from=mc-builder /src/mc/LICENSE /licenses/minio-mc/LICENSE
COPY --from=mc-builder /src/mc/CREDITS /licenses/minio-mc/CREDITS
COPY entrypoint.sh /usr/local/bin/minio-entrypoint.sh

RUN chmod 0755 /usr/bin/minio /usr/bin/mc /usr/local/bin/minio-entrypoint.sh \
    && mkdir -p /data

EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/minio-entrypoint.sh"]
CMD ["--help"]

FROM ${RUNTIME_IMAGE} AS minio-mc

ARG MC_RELEASE_TAG="RELEASE.2025-08-13T08-35-41Z"
ARG MC_COMMIT="7394ce0dd2a80935aded936b09fa12cbb3cb8096"

LABEL org.opencontainers.image.title="OmniRetail MinIO Client" \
      org.opencontainers.image.description="Repository-owned pinned MinIO Client source build" \
      org.opencontainers.image.version="${MC_RELEASE_TAG}" \
      org.opencontainers.image.revision="${MC_COMMIT}"

COPY --from=mc-builder /out/mc /usr/bin/mc
COPY --from=mc-builder /src/mc/LICENSE /licenses/minio-mc/LICENSE
COPY --from=mc-builder /src/mc/CREDITS /licenses/minio-mc/CREDITS

RUN chmod 0755 /usr/bin/mc

ENTRYPOINT ["mc"]
