# Repository-owned MinIO images

MinIO Community Edition no longer provides a durable prebuilt-image supply for
the releases used by OmniRetail. The `minio` and `minio-init` services therefore
build their runtime images from immutable upstream source commits.

This is a packaging repair for TD-001. It does not change MinIO's architecture,
storage responsibilities, credentials, buckets, healthcheck, or persistent
volume.

## Frozen inputs

| Component | Release | Commit | Source archive SHA-256 |
| --- | --- | --- | --- |
| MinIO server | `RELEASE.2025-09-07T16-13-09Z` | `07c3a429bfed433e49018cb0f78a52145d4bedeb` | `8819e3e7817e46b7b3798f8f200ead208562e571563c2e040352378031abe9f2` |
| MinIO Client (`mc`) | `RELEASE.2025-08-13T08-35-41Z` | `7394ce0dd2a80935aded936b09fa12cbb3cb8096` | `95cd293c7119f16921a6dc515a1fb74a2227f19fd994b9c8b770a154e802ac44` |

The Dockerfile also pins the Go builder and Alpine runtime images by immutable
multi-platform digest. `GOTOOLCHAIN=local` prevents Go from downloading a newer
toolchain, and each upstream `go.sum` verifies its module graph.

## Build and startup

The normal entry point builds both images before starting the core profile:

```bash
make up
```

To build or diagnose the images without starting services:

```bash
make minio-build
docker run --rm omni-retail/minio:RELEASE.2025-09-07T16-13-09Z --version
docker run --rm omni-retail/minio-mc:RELEASE.2025-08-13T08-35-41Z --version
```

A cold build needs network access to the pinned GitHub source archives, the
pinned Docker base images, and the Go modules referenced by the upstream lock
files. Repeated builds reuse BuildKit caches. Compose uses `pull_policy: never`
for the two final local images, so startup does not substitute a registry image
or discover a newer MinIO release. `MINIO_UPDATE=off` also disables MinIO's
in-place update endpoint.

## Update policy

These versions are intentionally frozen for the completed, maintenance-only
capstone. There is no automatic update check or moving source reference. An
update should be considered only for a critical vulnerability, compatibility
failure, or breakage of the delivered data path, and must repeat source checksum,
version-output, core smoke, and persistent-volume compatibility validation.

This frozen dependency is suitable for the local educational scope described in
`README.md`; it is not a production support or security-update strategy.
