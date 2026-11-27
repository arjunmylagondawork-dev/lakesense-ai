"""Configuration loading. One YAML file drives the whole pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


def _find_project_root() -> Path:
    """Project root = LAKESENSE_HOME, else the source checkout (editable install),
    else the current working directory (regular `pip install .`, Docker, Airflow)."""
    env = os.getenv("LAKESENSE_HOME")
    if env:
        return Path(env).resolve()
    checkout = Path(__file__).resolve().parents[2]
    if (checkout / "config" / "pipeline.yaml").exists():
        return checkout
    return Path.cwd()


PROJECT_ROOT = _find_project_root()
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class Paths:
    raw: Path
    lake: Path
    warehouse: Path
    reports: Path

    def layer(self, name: str) -> Path:
        """Path of a lake layer: bronze, silver, gold or quarantine."""
        return self.lake / name


@dataclass(frozen=True)
class Settings:
    raw: dict[str, Any]
    paths: Paths

    def section(self, name: str) -> dict[str, Any]:
        return self.raw.get(name, {})


def _resolve(p: str, root: Path) -> Path:
    path = Path(p)
    return path if path.is_absolute() else root / path


def load_settings(config_path: str | Path | None = None, root: Path | None = None) -> Settings:
    config_path = Path(config_path or os.getenv("LAKESENSE_CONFIG", DEFAULT_CONFIG))
    root = Path(root or PROJECT_ROOT)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found at {config_path}. Run from the project folder, "
            "or set LAKESENSE_HOME / LAKESENSE_CONFIG."
        )
    with open(config_path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    p = raw["paths"]
    paths = Paths(
        raw=_resolve(p["raw"], root),
        lake=_resolve(p["lake"], root),
        warehouse=_resolve(p["warehouse"], root),
        reports=_resolve(p["reports"], root),
    )
    return Settings(raw=raw, paths=paths)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()
