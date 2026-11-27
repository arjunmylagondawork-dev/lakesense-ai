"""Source schema contracts and drift detection.

Each raw file's columns are checked against the contract in config/pipeline.yaml
*before* it is ingested:

  * missing required column -> file REJECTED (not ingested), recorded in state
  * unknown new column      -> file ACCEPTED, column kept in bronze (schema
                               evolution) and reported as drift so a human can
                               decide whether to map it in silver
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class DriftResult:
    source: str
    file: str
    columns: list[str]
    new_columns: list[str] = field(default_factory=list)
    missing_required: list[str] = field(default_factory=list)
    missing_optional: list[str] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return not self.missing_required

    @property
    def has_drift(self) -> bool:
        return bool(self.new_columns or self.missing_required or self.missing_optional)

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "accepted": self.accepted}


def read_columns(path: Path, fmt: str) -> list[str]:
    """Column names of a raw file without loading it into Spark."""
    if fmt == "csv":
        with open(path, newline="", encoding="utf-8") as fh:
            return [c.strip() for c in next(csv.reader(fh), [])]
    cols: dict[str, None] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                cols.update(dict.fromkeys(json.loads(line)))
    return list(cols)


def check_file(source: str, path: Path, fmt: str, contract: dict[str, list[str]]) -> DriftResult:
    cols = read_columns(path, fmt)
    required = contract.get("required", [])
    optional = contract.get("optional", [])
    known = set(required) | set(optional)
    return DriftResult(
        source=source, file=path.name, columns=cols,
        new_columns=[c for c in cols if c not in known],
        missing_required=[c for c in required if c not in cols],
        missing_optional=[c for c in optional if c not in cols],
    )
