"""Invoke dbt with a process-local monotonic wall clock.

Dbt Core 1.10 uses ``time.time()`` timestamps to decide which parsed manifest
nodes need dependency processing. A backward host-clock correction during a
full parse can make every new node appear older than the invocation, leaving
``refs`` populated but ``depends_on.nodes`` empty. Runtime compilation then
fails with misleading dependency-inference errors.

This entry point preserves dbt's normal CLI and exit behavior while deriving
``time.time()`` from ``time.monotonic()`` for the lifetime of the dbt process.
No command is retried and genuine parse, compilation, SQL, test, or
authorization failures remain terminal.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from unittest.mock import patch


def _monotonic_wall_time(
    *,
    wall_time: Callable[[], float] = time.time,
    monotonic_time: Callable[[], float] = time.monotonic,
) -> Callable[[], float]:
    """Return a wall-time-shaped clock immune to later realtime corrections."""
    wall_started = wall_time()
    monotonic_started = monotonic_time()

    def stable_time() -> float:
        return wall_started + (monotonic_time() - monotonic_started)

    return stable_time


def main() -> None:
    """Run dbt's Click CLI under the stable process-local clock."""
    stable_time = _monotonic_wall_time()
    with patch.object(time, "time", stable_time):
        # Import inside the clock boundary so dbt modules cannot capture the
        # host's adjustable realtime function before command initialization.
        from dbt.cli.main import cli

        cli(prog_name="dbt")


if __name__ == "__main__":
    main()
