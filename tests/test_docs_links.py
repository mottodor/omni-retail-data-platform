"""Contract guard for repository-local targets in maintained Markdown links."""

import re
from pathlib import Path
from urllib.parse import unquote

REPO_ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
DOC_TREES = (
    REPO_ROOT / ".github",
    REPO_ROOT / "docs",
    REPO_ROOT / "infrastructure",
    REPO_ROOT / "postgres",
    REPO_ROOT / "superset",
    REPO_ROOT / "trino",
)
EXTERNAL_PREFIXES = ("http://", "https://", "mailto:")


def _markdown_files() -> list[Path]:
    files = list(REPO_ROOT.glob("*.md"))
    for tree in DOC_TREES:
        files.extend(tree.rglob("*.md"))
    return sorted(set(files))


def _local_target(raw_target: str) -> str | None:
    target = raw_target.strip().split(maxsplit=1)[0].strip("<>")
    if not target or target.startswith("#") or target.startswith(EXTERNAL_PREFIXES):
        return None
    return unquote(target.split("#", 1)[0].split("?", 1)[0])


def test_markdown_local_link_targets_exist() -> None:
    missing: list[str] = []

    for document in _markdown_files():
        for line_number, line in enumerate(
            document.read_text(encoding="utf-8").splitlines(), start=1
        ):
            for raw_target in MARKDOWN_LINK.findall(line):
                local_target = _local_target(raw_target)
                if local_target is None:
                    continue
                if not (document.parent / local_target).exists():
                    path = document.relative_to(REPO_ROOT)
                    missing.append(f"{path}:{line_number}: {raw_target}")

    assert not missing, "Markdown links have missing local targets:\n" + "\n".join(missing)
