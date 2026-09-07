from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.build_staging import stage_fiscaldata, stage_fred
from tgarefill.data.raw_store import RawPayloadStore


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


def _snapshot(settings, monkeypatch, optional_fails=True):
    import scripts.download_fiscaldata as downloader
    settings.data_sources["fiscaldata"]["base_url"] = "https://example.test"
    for key, meta in settings.data_sources["fiscaldata"]["endpoints"].items():
        meta["path"] = key
    monkeypatch.setattr(downloader, "get_settings", lambda: settings)
    monkeypatch.setattr(downloader, "build_session", lambda: None)
    def fetch(**kwargs):
        if optional_fails and kwargs["endpoint_path"] == "optional_endpoint":
            raise RuntimeError("optional unavailable")
        return {"records": [{"record_date": "2024-01-03", "value": 1}], "record_count": 1}
    monkeypatch.setattr(downloader, "fetch_paginated_endpoint", fetch)
    downloader.main()


def test_stage_fiscaldata_allows_missing_optional_input(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    _snapshot(settings, monkeypatch)
    manifest = stage_fiscaldata(settings)
    assert len(manifest) == 1
    assert manifest[0]["source_file"].endswith("fiscaldata.sqlite#required_endpoint")


def test_stage_fiscaldata_reads_compact_payload_store(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    _snapshot(settings, monkeypatch)
    stage_fiscaldata(settings)
    assert (settings.paths.staging / "fiscaldata/required_endpoint.parquet").exists()


def test_failed_optional_cannot_reuse_previous_snapshot_key(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    _snapshot(settings, monkeypatch, optional_fails=False)
    stage_fiscaldata(settings)
    old_optional = settings.paths.staging / "fiscaldata/optional_endpoint.parquet"
    assert old_optional.exists()
    _snapshot(settings, monkeypatch, optional_fails=True)
    manifest = stage_fiscaldata(settings)
    assert len(manifest) == 1
    assert not old_optional.exists()
    from tgarefill.data.raw_store import open_fiscaldata_snapshot
    store, entries, _ = open_fiscaldata_snapshot(settings.paths.raw / "fiscaldata")
    assert "optional_endpoint" not in store
    assert entries["optional_endpoint"]["status"].startswith("failed")


def test_required_failure_preserves_previous_snapshot(tmp_path, monkeypatch):
    import scripts.download_fiscaldata as downloader
    settings = _settings(tmp_path)
    _snapshot(settings, monkeypatch)
    pointer = settings.paths.raw / "fiscaldata/current.json"
    previous = pointer.read_bytes()
    def fail(**kwargs):
        raise RuntimeError("failed")
    monkeypatch.setattr(downloader, "fetch_paginated_endpoint", fail)
    with pytest.raises(RuntimeError, match="Required FiscalData downloads failed"):
        downloader.main()
    assert pointer.read_bytes() == previous


def test_unbound_legacy_store_is_rejected(tmp_path):
    settings = _settings(tmp_path)
    store = RawPayloadStore(settings.paths.raw / "fiscaldata/fiscaldata.sqlite")
    store.put_json("required_endpoint", {"records": [{"value": 1}]})
    with pytest.raises(RuntimeError, match="manifest-bound"):
        stage_fiscaldata(settings)
