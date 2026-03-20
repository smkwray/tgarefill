import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_readme_documents_canonical_mvp_outputs() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "make mvp" in readme
    assert "data/processed/master_weekly_panel.parquet" in readme
    assert "data/processed/event_candidates.parquet" in readme
    assert "outputs/tables/attribution_baseline.csv" in readme
    assert "outputs/site/" not in readme


def test_committed_site_events_have_non_null_sizes() -> None:
    payload = json.loads((ROOT / "site" / "data" / "events.json").read_text(encoding="utf-8"))
    assert payload
    assert all(row["delta_tga_bn"] is not None for row in payload)
