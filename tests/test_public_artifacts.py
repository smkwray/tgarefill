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


def test_public_claims_remain_aggregate_and_descriptive():
    text = "\n".join((ROOT / p).read_text() for p in ["README.md", "site/index.html", "src/tgarefill/mmfalloc.py"])
    forbidden = ["who actually provides the funding", "Banks bought Treasuries", "MMFs bought Treasuries", "Strips ~94% of predictable auction supply", "Identifies <em>issuance-driven", "bill-size surprise instrument", "Fund-Level MMF Validation", "supports the focused", "validation layer underneath", "$229B", "$141B", "Fund-level validation boundary", "evidence supports MMF allocation rotation"]
    assert not any(phrase in text for phrase in forbidden)
    assert not (ROOT / "site/img/mmfalloc_eventstudy.svg").exists()
    assert not (ROOT / "site/img/irf_binary_vs_bill_surprise.png").exists()


def test_site_extended_effects_match_retained_summary():
    import math
    expected = {"mmf_treasury_holdings": {"shock_sd_bn": 19.2845, "h4_effect_bn": 40.6306, "h4_t_stat_nw": 3.4443}, "on_rrp_daily_total": {"shock_sd_bn": 19.2845, "h4_effect_bn": -30.5821, "h4_t_stat_nw": -4.9778}}
    site = json.loads((ROOT / "site/data/summary.json").read_text())
    for variable in ["mmf_treasury_holdings", "on_rrp_daily_total"]:
        row = expected[variable]
        assert math.isclose(site["bill_shock_sd_bn"], round(float(row["shock_sd_bn"]), 2), abs_tol=0.005)
        assert math.isclose(site["headline_effects"][variable]["beta_bn"], round(float(row["h4_effect_bn"]), 2), abs_tol=0.005)
        assert math.isclose(site["headline_effects"][variable]["t_stat_nw"], round(float(row["h4_t_stat_nw"]), 2), abs_tol=0.005)


def test_site_figure_copy_excludes_unadmitted_or_unit_invalid_figures(tmp_path):
    from scripts.build_site_data import copy_figures
    source = tmp_path / "source"
    source.mkdir()
    for name in ["irf_binary_vs_bill_surprise.png", "mmfalloc_eventstudy.svg", "allowed.svg"]:
        (source / name).write_bytes(b"figure")
    site = tmp_path / "site"
    copy_figures(source, site)
    assert [p.name for p in (site / "img").iterdir()] == ["allowed.svg"]
