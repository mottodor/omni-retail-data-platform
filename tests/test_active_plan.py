"""Contract guard: the active task plan pointer stays honest (§41.3)."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PROGRESS = REPO_ROOT / "PROGRESS.md"
ACTIVE_PLAN = REPO_ROOT / "docs" / "plans" / "active.md"


def test_progress_references_active_plan_only_when_it_exists() -> None:
    progress = PROGRESS.read_text(encoding="utf-8")
    references = bool(re.search(r"`docs/plans/active\.md`", progress))
    assert references == ACTIVE_PLAN.is_file(), (
        "PROGRESS.md current focus and docs/plans/active.md must agree: "
        "reference the file only while it exists (see engineering-practices §41.3)"
    )


def test_active_plan_has_required_sections() -> None:
    if not ACTIVE_PLAN.is_file():
        return  # no active task — nothing to check
    content = ACTIVE_PLAN.read_text(encoding="utf-8")
    for section in ("## Goal", "## Context to read first", "## Steps", "## Validation"):
        assert section in content, f"active plan is missing section {section!r}"
