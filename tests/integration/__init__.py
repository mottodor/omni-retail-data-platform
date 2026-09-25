"""Integration test package: helpers are imported as ``integration.*``.

``tests`` is on the pytest/pythonpath and mypy search paths (same mechanism
as the ``fakes`` package), so live-test modules can share seeding helpers
without copying them around.
"""
