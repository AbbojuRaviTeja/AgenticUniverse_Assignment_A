from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "assignment-a-data" / "schemas").is_dir():
            return parent
    raise FileNotFoundError("assignment-a-data/schemas was not found above the package")


def schemas_dir() -> Path:
    return repo_root() / "assignment-a-data" / "schemas"
