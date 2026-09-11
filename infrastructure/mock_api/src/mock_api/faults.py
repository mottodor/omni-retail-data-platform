"""Deterministic fault injection for the mock API (pure stdlib).

A request "faults" when a hash of (seed, path, query-without-fault-params)
maps below ``fault_rate``. Each fingerprint faults **at most once per process
lifetime**: the first attempt fails deterministically (same seed, same
request, same outcome on a fresh server) while an identical retry succeeds,
which makes client retry scenarios (429 -> retry -> success) reproducible
(Phase 3 design spec §7, §8).
"""

import hashlib
from collections.abc import Mapping
from typing import Any

FAULT_MODES = frozenset({"429", "500", "timeout"})

_FAULT_PARAM_NAMES = {"fault", "fault_rate"}


def fingerprint(seed: int, path: str, params: Mapping[str, Any]) -> str:
    """Stable identity of a request, ignoring the fault parameters themselves."""
    relevant = sorted(
        (key, str(value)) for key, value in params.items() if key not in _FAULT_PARAM_NAMES
    )
    material = f"{seed}|{path}|{relevant}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def fault_probability(seed: int, path: str, params: Mapping[str, Any]) -> float:
    """Deterministic probability draw in [0, 1) for the request fingerprint."""
    digest = fingerprint(seed, path, params)
    return int(digest[:16], 16) / float(16**16)


def should_fault(
    seed: int,
    path: str,
    params: Mapping[str, Any],
    fault: str | None,
    fault_rate: float,
    already_faulted: set[str],
) -> bool:
    """Decide whether this request must inject the requested fault.

    Returns False when fault injection is disabled (no mode or zero rate), when
    the fingerprint draw is above the rate, or when this fingerprint already
    faulted once during the server lifetime.
    """
    if fault is None or fault_rate <= 0:
        return False
    request_fingerprint = fingerprint(seed, path, params)
    if fault_probability(seed, path, params) >= fault_rate:
        return False
    if request_fingerprint in already_faulted:
        return False
    already_faulted.add(request_fingerprint)
    return True
