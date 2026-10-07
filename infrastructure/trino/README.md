# Trino 483 REST OAuth2 backport

The local `omni-retail/trino:483-pr30816` image keeps the released Trino 483
runtime and backports the upstream fix from
[`trinodb/trino#30816`](https://github.com/trinodb/trino/pull/30816), merged as
commit `16b2e88ca76930d410bd6d95d0af387a34c2da59`.

Trino 483 creates a random Iceberg REST-catalog session ID for every operation
when `iceberg.rest-catalog.session=NONE`. The bundled Iceberg OAuth manager
therefore misses its session cache and requests a new Polaris token for every
catalog operation. Write-heavy dbt builds can eventually receive an empty-body
HTTP 401 while committing a table.

The two Java files under `patches/30816/` are the exact Apache-2.0-licensed
upstream changes. The Dockerfile compiles them against the connector libraries
inside the pinned Trino 483 base image and replaces/adds only those classes in
the Iceberg plugin JAR. It does not change catalog session mode, authorization
roles, SQL behavior, or platform services.

Keep this backport until a compatibility task upgrades the pinned image to a
released Trino version containing PR 30816. At that point remove the local
Dockerfile, patch sources, and Compose build stanza together, then rerun the
consecutive-build soak test before accepting the upgrade.
