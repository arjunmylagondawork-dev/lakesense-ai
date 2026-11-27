"""Pipeline state: which raw files were ingested, and which batches each layer processed.

Stored as a small JSON file next to the lake (`<lake>/_state/state.json`).
This is what makes loads incremental and re-runs idempotent:
  * bronze ingests only files it has not seen (same name, size and mtime = seen)
  * silver processes only bronze batches it has not processed yet
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakesense.config import Settings


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class PipelineState:
    path: Path
    files: dict[str, dict[str, Any]] = field(default_factory=dict)     # "orders/x.csv" -> info
    batches: list[dict[str, Any]] = field(default_factory=list)

    # -- persistence --------------------------------------------------------------
    @classmethod
    def load(cls, settings: Settings) -> PipelineState:
        path = settings.paths.lake / "_state" / "state.json"
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            return cls(path, data.get("files", {}), data.get("batches", []))
        return cls(path)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"files": self.files, "batches": self.batches}, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)   # atomic

    @classmethod
    def reset(cls, settings: Settings) -> PipelineState:
        state = cls(settings.paths.lake / "_state" / "state.json")
        state.save()
        return state

    # -- files --------------------------------------------------------------------
    @staticmethod
    def fingerprint(p: Path) -> dict[str, Any]:
        st = p.stat()
        return {"size": st.st_size, "mtime": int(st.st_mtime)}

    def is_new(self, key: str, p: Path) -> bool:
        seen = self.files.get(key)
        return seen is None or {k: seen.get(k) for k in ("size", "mtime")} != self.fingerprint(p)

    def record_file(self, key: str, p: Path, batch_id: str, status: str, **extra: Any) -> None:
        self.files[key] = {**self.fingerprint(p), "batch_id": batch_id, "status": status,
                           "recorded_at": _now(), **extra}

    # -- batches ------------------------------------------------------------------
    def add_batch(self, batch_id: str, files: int) -> None:
        self.batches.append({"batch_id": batch_id, "created_at": _now(), "files": files, "silver_done": False})

    def pending_batches(self) -> list[str]:
        return [b["batch_id"] for b in self.batches if not b["silver_done"]]

    def mark_silver_done(self, batch_ids: list[str]) -> None:
        for b in self.batches:
            if b["batch_id"] in batch_ids:
                b["silver_done"] = True
                b["silver_done_at"] = _now()
