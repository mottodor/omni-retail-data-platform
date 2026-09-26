"""Idempotent initializer for the Polaris ``lakehouse`` catalog backed by MinIO.

Runs as a one-shot container (``polaris-init``) after Polaris becomes healthy:

1. obtains an OAuth2 token with the bootstrap client credentials;
2. checks whether the catalog already exists (idempotent restarts);
3. creates the catalog with a static-key S3 storage config pointing at MinIO.

Only the standard library is used so the script can run in a plain
``python:3.12-alpine`` image without installing dependencies.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

REQUEST_TIMEOUT_SECONDS = 15

#: Role auto-created by Polaris for every catalog; the grant target.
CATALOG_ADMIN_ROLE = "catalog_admin"

#: Content privileges the Trino principal needs for the full table/view
#: lifecycle (create, drop, purge-drop). The auto-created catalog_admin only
#: carries CATALOG_MANAGE_ACCESS/METADATA, which does not cover Trino's
#: dropTable(purge=true) — root cause of the slice-1 teardown workaround.
REQUIRED_CATALOG_PRIVILEGES = ("CATALOG_MANAGE_CONTENT",)


def build_catalog_payload(
    *,
    catalog_name: str,
    base_location: str,
    storage: dict[str, str],
) -> dict[str, Any]:
    """Build the Polaris management-API payload for an internal S3 catalog.

    S3 credentials are deliberately NOT embedded: Polaris resolves them from
    its own ``AWS_ACCESS_KEY_ID``/``AWS_SECRET_ACCESS_KEY`` environment.
    """
    return {
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {
                "default-base-location": base_location,
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [base_location],
                "endpoint": storage["endpoint"],
                "region": storage["region"],
                "pathStyleAccess": True,
            },
        }
    }


def decide_action(*, catalog_exists: bool) -> str:
    """Return the action to perform: create or skip (idempotency policy)."""
    return "skip" if catalog_exists else "create"


def obtain_token(
    *, base_url: str, client_id: str, client_secret: str, timeout: int = REQUEST_TIMEOUT_SECONDS
) -> str:
    """Obtain an OAuth2 access token via the client-credentials grant."""
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = b"grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL"
    request = urllib.request.Request(
        url=f"{base_url}/api/catalog/v1/oauth/tokens",
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    token = payload.get("access_token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("oauth token response did not contain access_token")
    return token


def _management_request(
    *,
    method: str,
    url: str,
    token: str,
    realm: str,
    body: dict[str, Any] | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> tuple[int, dict[str, Any] | None]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Polaris-Realm": realm,
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        return exc.code, json.loads(raw) if raw else None


def catalog_exists(*, base_url: str, token: str, realm: str, catalog_name: str) -> bool:
    """Check catalog existence; any unexpected HTTP status is an error."""
    status, payload = _management_request(
        method="GET",
        url=f"{base_url}/api/management/v1/catalogs/{catalog_name}",
        token=token,
        realm=realm,
    )
    if status == 200:
        return True
    if status == 404:
        return False
    raise RuntimeError(f"unexpected status {status} while checking catalog: {payload}")


def create_catalog(
    *, base_url: str, token: str, realm: str, payload: dict[str, Any], catalog_name: str
) -> None:
    """Create the catalog; a 409 conflict after a concurrent create is tolerated."""
    status, response = _management_request(
        method="POST",
        url=f"{base_url}/api/management/v1/catalogs",
        token=token,
        realm=realm,
        body=payload,
    )
    if status not in (200, 201):
        raise RuntimeError(
            f"failed to create catalog {catalog_name}: status={status} body={response}"
        )


def list_catalog_role_grants(
    *,
    base_url: str,
    token: str,
    realm: str,
    catalog_name: str,
    catalog_role_name: str = CATALOG_ADMIN_ROLE,
) -> list[dict[str, Any]]:
    """List grants held by a catalog role (management API)."""
    status, payload = _management_request(
        method="GET",
        url=(
            f"{base_url}/api/management/v1/catalogs/{catalog_name}"
            f"/catalog-roles/{catalog_role_name}/grants"
        ),
        token=token,
        realm=realm,
    )
    if status != 200 or payload is None:
        raise RuntimeError(
            f"failed to list grants of {catalog_role_name!r} in {catalog_name}: "
            f"status={status} body={payload}"
        )
    grants = payload.get("grants")
    if not isinstance(grants, list):
        raise RuntimeError(f"unexpected grants payload for {catalog_role_name!r}: {payload}")
    return grants


def decide_grant_action(grants: list[dict[str, Any]]) -> str:
    """Return the grant step action: grant missing privileges or skip."""
    present = {
        (grant.get("type"), grant.get("privilege")) for grant in grants if isinstance(grant, dict)
    }
    missing = [
        privilege
        for privilege in REQUIRED_CATALOG_PRIVILEGES
        if ("catalog", privilege) not in present
    ]
    return "skip" if not missing else "grant"


def grant_catalog_privilege(
    *,
    base_url: str,
    token: str,
    realm: str,
    catalog_name: str,
    privilege: str,
    catalog_role_name: str = CATALOG_ADMIN_ROLE,
) -> None:
    """Add one catalog-level privilege to a catalog role (PUT adds, POST revokes)."""
    status, response = _management_request(
        method="PUT",
        url=(
            f"{base_url}/api/management/v1/catalogs/{catalog_name}"
            f"/catalog-roles/{catalog_role_name}/grants"
        ),
        token=token,
        realm=realm,
        body={"type": "catalog", "privilege": privilege},
    )
    if status not in (200, 201, 409):
        raise RuntimeError(
            f"failed to grant {privilege} to {catalog_role_name!r} in {catalog_name}: "
            f"status={status} body={response}"
        )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable {name} is not set")
    return value


def main() -> int:
    base_url = _required_env("POLARIS_BASE_URL")
    realm = _required_env("POLARIS_REALM")
    client_id = _required_env("POLARIS_CLIENT_ID")
    client_secret = _required_env("POLARIS_CLIENT_SECRET")
    catalog_name = _required_env("POLARIS_CATALOG_NAME")
    endpoint = _required_env("MINIO_ENDPOINT_URL")
    region = _required_env("S3_REGION")
    bucket = _required_env("S3_BUCKET")

    base_location = f"s3://{bucket}/"
    token = obtain_token(base_url=base_url, client_id=client_id, client_secret=client_secret)
    exists = catalog_exists(base_url=base_url, token=token, realm=realm, catalog_name=catalog_name)
    action = decide_action(catalog_exists=exists)

    if action == "create":
        payload = build_catalog_payload(
            catalog_name=catalog_name,
            base_location=base_location,
            storage={
                "endpoint": endpoint,
                "region": region,
            },
        )
        create_catalog(
            base_url=base_url, token=token, realm=realm, payload=payload, catalog_name=catalog_name
        )

    grants = list_catalog_role_grants(
        base_url=base_url, token=token, realm=realm, catalog_name=catalog_name
    )
    grant_action = decide_grant_action(grants)
    if grant_action == "grant":
        for privilege in REQUIRED_CATALOG_PRIVILEGES:
            grant_catalog_privilege(
                base_url=base_url,
                token=token,
                realm=realm,
                catalog_name=catalog_name,
                privilege=privilege,
            )

    print(
        f"polaris_init: catalog={catalog_name} base_location={base_location} "
        f"storage_endpoint={endpoint} action={action} content_grant={grant_action}"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 - one-shot job: fail with full context
        print(f"polaris_init: FAILED error={type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
