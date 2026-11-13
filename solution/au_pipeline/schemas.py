from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from au_pipeline.paths import schemas_dir


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool
    pii: bool
    aliases: tuple[str, ...]
    values: frozenset[str]
    default: str | None
    reference: bool


@dataclass(frozen=True)
class WorkflowSchema:
    workflow_id: str
    fields: tuple[Field, ...]

    @property
    def reference_field(self) -> str:
        for field in self.fields:
            if field.reference:
                return field.name
        raise ValueError(f"{self.workflow_id} has no reference field")


def load_schemas(directory: Path | None = None) -> dict[str, WorkflowSchema]:
    root = directory or schemas_dir()
    schemas: dict[str, WorkflowSchema] = {}
    for path in sorted(root.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        fields = tuple(
            Field(
                name=item["name"],
                type=item["type"],
                required=bool(item.get("required", False)),
                pii=bool(item.get("pii", False)),
                aliases=tuple(item.get("aliases", ())),
                values=frozenset(value.lower() for value in item.get("values", ())),
                default=item.get("default"),
                reference=bool(item.get("reference", False)),
            )
            for item in raw["fields"]
        )
        schemas[raw["workflow_id"]] = WorkflowSchema(raw["workflow_id"], fields)
    return schemas


def load_disposition_rows(directory: Path | None = None) -> list[tuple[str, str, str, bool, bool]]:
    path = (directory or schemas_dir()) / "dispositions.csv"
    rows: list[tuple[str, str, str, bool, bool]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for record in csv.DictReader(handle):
            rows.append(
                (
                    record["workflow_id"].strip(),
                    record["code"].strip(),
                    record["label"].strip(),
                    record["is_terminal"].strip().lower() == "true",
                    record["is_success"].strip().lower() == "true",
                )
            )
    return rows
