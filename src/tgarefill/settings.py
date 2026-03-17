from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def find_project_root(start: Path | None = None) -> Path:
    current = (start or Path(__file__)).resolve()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return current.parent


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in YAML file: {path}")
    return data


@dataclass
class ProjectPaths:
    root: Path
    data: Path = field(init=False)
    raw: Path = field(init=False)
    staging: Path = field(init=False)
    processed: Path = field(init=False)
    outputs: Path = field(init=False)
    output_tables: Path = field(init=False)
    output_figures: Path = field(init=False)
    configs: Path = field(init=False)
    docs: Path = field(init=False)

    def __post_init__(self) -> None:
        self.data = self.root / "data"
        self.raw = self.data / "raw"
        self.staging = self.data / "staging"
        self.processed = self.data / "processed"
        self.outputs = self.root / "outputs"
        self.output_tables = self.outputs / "tables"
        self.output_figures = self.outputs / "figures"
        self.configs = self.root / "configs"
        self.docs = self.root / "docs"

    def ensure(self) -> None:
        for path in [
            self.data,
            self.raw,
            self.staging,
            self.processed,
            self.outputs,
            self.output_tables,
            self.output_figures,
            self.configs,
            self.docs,
        ]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class ProjectSettings:
    root: Path = field(default_factory=find_project_root)
    paths: ProjectPaths = field(init=False)
    data_sources: dict[str, Any] = field(init=False)
    fred_series: dict[str, Any] = field(init=False)
    stfm_queries: dict[str, Any] = field(init=False)
    episode_rules: dict[str, Any] = field(init=False)
    analysis: dict[str, Any] = field(init=False)

    def __post_init__(self) -> None:
        self.paths = ProjectPaths(self.root)
        self.paths.ensure()
        self.data_sources = load_yaml(self.paths.configs / "data_sources.yaml")
        self.fred_series = load_yaml(self.paths.configs / "fred_series.yaml")
        self.stfm_queries = load_yaml(self.paths.configs / "stfm_queries.yaml")
        self.episode_rules = load_yaml(self.paths.configs / "episode_rules.yaml")
        self.analysis = load_yaml(self.paths.configs / "analysis.yaml")


def get_settings() -> ProjectSettings:
    return ProjectSettings()
