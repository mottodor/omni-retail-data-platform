"""Diagnostic dbt invocation helpers for live integration tests.

Each invocation owns separate target and log directories. Failed builds remain
failed; this module records enough structured evidence to identify the first
dbt/Trino boundary error and deliberately contains no retry policy.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_TIMEOUT_SECONDS = 900
_FAILURE_STATUSES = frozenset({"error", "fail"})
_QUERY_ID_PATTERN = re.compile(r"\bquery_id=([a-zA-Z0-9_]+)")
_TRINO_ERROR_CLASS_PATTERN = re.compile(r"\b(Trino[A-Za-z]+Error)\(")
_TRINO_ERROR_TYPE_PATTERN = re.compile(r"\btype=([^,)]+)")
_TRINO_ERROR_NAME_PATTERN = re.compile(r"\bname=([^,)]+)")
_TRINO_ERROR_MESSAGE_PATTERN = re.compile(
    r'\bmessage=(?:"(?P<double>[^"]*)"|\'(?P<single>[^\']*)\'|(?P<plain>.*?))'
    r",\s*query_id="
)


class DbtArtifactError(RuntimeError):
    """A dbt result artifact is absent or cannot be classified safely."""


class DbtBuildFailure(AssertionError):
    """A dbt integration build failed without being retried."""


@dataclass(frozen=True)
class DbtNodeFailure:
    """One terminal node result extracted from ``run_results.json``."""

    unique_id: str
    status: str
    message: str
    query_ids: tuple[str, ...]
    adapter_error_class: str | None
    adapter_error_type: str | None
    adapter_error_name: str | None
    adapter_error_message: str | None


@dataclass(frozen=True)
class DbtRunResults:
    """Validated diagnostic subset of a dbt run-results artifact."""

    invocation_id: str
    result_count: int
    status_counts: tuple[tuple[str, int], ...]
    failures: tuple[DbtNodeFailure, ...]
    skipped_count: int


@dataclass(frozen=True)
class DbtInvocation:
    """Paths and process output retained for one dbt invocation."""

    label: str
    command: tuple[str, ...]
    returncode: int
    target_path: Path
    log_path: Path
    stdout_path: Path
    stderr_path: Path
    results: DbtRunResults | None
    artifact_error: str | None


def _required_string(mapping: Mapping[str, object], key: str, *, context: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise DbtArtifactError(f"{context}.{key} must be a non-empty string")
    return value


def _optional_match(pattern: re.Pattern[str], message: str) -> str | None:
    match = pattern.search(message)
    return match.group(1).strip() if match is not None else None


def _adapter_message(message: str) -> str | None:
    match = _TRINO_ERROR_MESSAGE_PATTERN.search(message)
    if match is None:
        return None
    return next((value for value in match.groupdict().values() if value is not None), "").strip()


def parse_dbt_run_results(path: Path) -> DbtRunResults:
    """Parse dbt results strictly enough that failures cannot be misclassified."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise DbtArtifactError(f"dbt run-results artifact is missing: {path}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise DbtArtifactError(f"cannot read dbt run-results artifact {path}: {error}") from error

    if not isinstance(payload, dict):
        raise DbtArtifactError("dbt run-results root must be an object")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise DbtArtifactError("dbt run-results metadata must be an object")
    invocation_id = _required_string(metadata, "invocation_id", context="metadata")
    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        raise DbtArtifactError("dbt run-results results must be an array")

    statuses: Counter[str] = Counter()
    failures: list[DbtNodeFailure] = []
    for index, raw_result in enumerate(raw_results):
        context = f"results[{index}]"
        if not isinstance(raw_result, dict):
            raise DbtArtifactError(f"{context} must be an object")
        status = _required_string(raw_result, "status", context=context)
        unique_id = _required_string(raw_result, "unique_id", context=context)
        raw_message = raw_result.get("message")
        if raw_message is None:
            message = ""
        elif isinstance(raw_message, str):
            message = raw_message
        else:
            raise DbtArtifactError(f"{context}.message must be a string or null")
        statuses[status] += 1
        if status not in _FAILURE_STATUSES:
            continue

        query_ids = tuple(dict.fromkeys(_QUERY_ID_PATTERN.findall(message)))
        failures.append(
            DbtNodeFailure(
                unique_id=unique_id,
                status=status,
                message=message,
                query_ids=query_ids,
                adapter_error_class=_optional_match(_TRINO_ERROR_CLASS_PATTERN, message),
                adapter_error_type=_optional_match(_TRINO_ERROR_TYPE_PATTERN, message),
                adapter_error_name=_optional_match(_TRINO_ERROR_NAME_PATTERN, message),
                adapter_error_message=_adapter_message(message),
            )
        )

    return DbtRunResults(
        invocation_id=invocation_id,
        result_count=len(raw_results),
        status_counts=tuple(sorted(statuses.items())),
        failures=tuple(failures),
        skipped_count=statuses["skipped"],
    )


def _tail(path: Path, *, lines: int = 30) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        return f"<cannot read {path}: {error}>"
    return "\n".join(content.splitlines()[-lines:])


def _format_failure(
    invocation: DbtInvocation,
    *,
    schema_names: Sequence[str],
) -> str:
    details = [
        f"dbt build failed: label={invocation.label} returncode={invocation.returncode}",
        f"schemas={','.join(schema_names)}",
        f"command={shlex.join(invocation.command)}",
        f"target_path={invocation.target_path}",
        f"log_path={invocation.log_path}",
    ]
    if invocation.results is None:
        details.append(f"artifact_error={invocation.artifact_error}")
    else:
        results = invocation.results
        details.extend(
            (
                f"invocation_id={results.invocation_id}",
                f"status_counts={dict(results.status_counts)}",
                f"skipped_count={results.skipped_count}",
            )
        )
        if not results.failures:
            details.append("primary_failures=<none in artifact>")
        for failure in results.failures:
            details.append(
                "primary_failure="
                f"node={failure.unique_id} status={failure.status} "
                f"adapter_class={failure.adapter_error_class} "
                f"adapter_type={failure.adapter_error_type} "
                f"adapter_name={failure.adapter_error_name} "
                f"adapter_message={failure.adapter_error_message!r} "
                f"query_ids={failure.query_ids} message={failure.message!r}"
            )
    details.extend(
        (
            f"stdout_path={invocation.stdout_path}",
            f"stderr_path={invocation.stderr_path}",
            "stdout_tail:\n" + _tail(invocation.stdout_path),
            "stderr_tail:\n" + _tail(invocation.stderr_path),
        )
    )
    return "\n".join(details)


def run_dbt_build(
    *,
    schema_env: Mapping[str, str],
    schema_names: Sequence[str],
    run_dir: Path,
    label: str,
) -> DbtInvocation:
    """Run one full dbt build with isolated artifacts and fail without retry."""
    target_path = run_dir / "target"
    log_path = run_dir / "logs"
    stdout_path = run_dir / "stdout.log"
    stderr_path = run_dir / "stderr.log"
    run_dir.mkdir(parents=True, exist_ok=True)
    command = (
        "uv",
        "run",
        "python",
        "-m",
        "omni_retail.lakehouse.dbt_cli",
        "build",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        # Serialize DDL in this two-build determinism scenario. A catalog
        # failure must remain visible rather than being confused with worker
        # concurrency.
        "--threads",
        "1",
    )
    env = {
        **os.environ,
        "TRINO_HOST": os.environ.get("TRINO_HOST", "127.0.0.1"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        **schema_env,
    }
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=DBT_TIMEOUT_SECONDS,
        env=env,
    )
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")

    artifact_error: str | None = None
    results: DbtRunResults | None = None
    try:
        results = parse_dbt_run_results(target_path / "run_results.json")
    except DbtArtifactError as error:
        artifact_error = str(error)

    invocation = DbtInvocation(
        label=label,
        command=command,
        returncode=completed.returncode,
        target_path=target_path,
        log_path=log_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        results=results,
        artifact_error=artifact_error,
    )
    has_terminal_results = results is not None and bool(results.failures)
    if completed.returncode != 0 or results is None or has_terminal_results:
        raise DbtBuildFailure(_format_failure(invocation, schema_names=schema_names))
    return invocation
