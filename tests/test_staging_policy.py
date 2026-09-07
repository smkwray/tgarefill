from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.build_staging import stage_fiscaldata, stage_fred
from tgarefill.data.raw_store import RawPayloadStore
from tgarefill.utils.files import write_json


def _settings(tmp_path: Path) -> SimpleNamespace:
    raw = tmp_path / "raw"
    staging = tmp_path / "staging"
    raw.mkdir(parents=True, exist_ok=True)
    staging.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(
        paths=SimpleNamespace(raw=raw, staging=staging),
        fred_series={
            "series": {
                "required_series": {
                    "id": "REQ",
                    "frequency": "weekly",
                    "required_for_mvp": True,
                },
                "optional_series": {
                    "id": "OPT",
                    "frequency": "weekly",
                    "required_for_mvp": False,
                },
            }
        },
        data_sources={
            "fiscaldata": {
                "endpoints": {
                    "required_endpoint": {"required_for_mvp": True},
                    "optional_endpoint": {"required_for_mvp": False},
                }
            }
        },
    )


def test_stage_fred_fails_on_missing_required_input(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    fred_raw = settings.paths.raw / "fred"
    fred_raw.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"DATE": ["2024-01-03"], "OPT": [1.0]}).to_csv(
        fred_raw / "optional_series__OPT.csv",
        index=False,
    )

    with pytest.raises(RuntimeError, match="required_series"):
        stage_fred(settings)


def test_stage_fiscaldata_allows_missing_optional_input(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    fiscal_raw = settings.paths.raw / "fiscaldata"
    fiscal_raw.mkdir(parents=True, exist_ok=True)
    write_json(
        {"records": [{"record_date": "2024-01-03", "value": 1}]},
        fiscal_raw / "required_endpoint.json",
    )

    manifest = stage_fiscaldata(settings)

    assert len(manifest) == 1
    assert manifest[0]["source_file"] == "required_endpoint.json"


def test_stage_fiscaldata_reads_compact_payload_store(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    fiscal_raw = settings.paths.raw / "fiscaldata"
    store = RawPayloadStore(fiscal_raw / "fiscaldata.sqlite")
    store.put_json(
        "required_endpoint",
        {"records": [{"record_date": "2024-01-03", "value": 1}]},
        source="/endpoint",
    )

    manifest = stage_fiscaldata(settings)

    assert len(manifest) == 1
    assert manifest[0]["source_file"] == "fiscaldata.sqlite#required_endpoint"
    assert (settings.paths.staging / "fiscaldata" / "required_endpoint.parquet").exists()
