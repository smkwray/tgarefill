import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.build_site_data import build_events, build_summary, build_lp_comparison, _extended_results


def test_build_events_uses_delta_tga_event(tmp_path: Path) -> None:
    events_csv = tmp_path / "event_candidates.csv"
    pd.DataFrame(
        [
            {
                "event_id": "event_001",
                "baseline_date": "2024-01-03",
                "start_date": "2024-01-10",
                "end_date": "2024-01-17",
                "duration_weeks": 2,
                "delta_tga_event": 1250.0,
                "issuance_mix": "bill-heavy",
                "manual_tags": "tagged",
            }
        ]
    ).to_csv(events_csv, index=False)

    build_events(events_csv, tmp_path)

    payload = json.loads((tmp_path / "data" / "events.json").read_text())
    assert payload[0]["delta_tga_bn"] == 1.2
    assert payload[0]["baseline_date"] == "2024-01-03"


def test_build_events_requires_schema_columns(tmp_path: Path) -> None:
    events_csv = tmp_path / "event_candidates.csv"
    pd.DataFrame([{"event_id": "event_001"}]).to_csv(events_csv, index=False)

    with pytest.raises(ValueError, match="missing required columns"):
        build_events(events_csv, tmp_path)


def test_build_summary_generates_headline_metrics(tmp_path: Path) -> None:
    panel = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-03", periods=3, freq="W-WED"),
            "tga_weekly_wednesday": [100.0, 110.0, 120.0],
            "on_rrp_daily_total": [200.0, 190.0, 180.0],
        }
    )
    events_csv = tmp_path / "event_candidates.csv"
    pooled_lp_csv = tmp_path / "local_projections.csv"
    auction_lp_csv = tmp_path / "auction_shock_lp.csv"

    pd.DataFrame([{"event_id": "event_001"}]).to_csv(events_csv, index=False)
    pd.DataFrame(
        [
            {
                "shock_spec": "binary",
                "regime": None,
                "response_var": "mmf_treasury_holdings",
                "horizon": -1,
                "significant_5pct": True,
                "beta": 0.0,
                "t_stat_nw": 0.0,
            },
            {
                "shock_spec": "binary",
                "regime": None,
                "response_var": "on_rrp_daily_total",
                "horizon": -1,
                "significant_5pct": False,
                "beta": 0.0,
                "t_stat_nw": 0.0,
            },
        ]
    ).to_csv(pooled_lp_csv, index=False)
    pd.DataFrame(
        [
            {
                "shock_spec": "bill_surprise_extended_latest",
                "regime": None,
                "response_var": "mmf_treasury_holdings",
                "horizon": -1,
                "significant_5pct": False,
                "beta": 0.0,
                "t_stat_nw": 0.0,
            },
            {
                "shock_spec": "bill_surprise_extended_latest",
                "regime": None,
                "response_var": "on_rrp_daily_total",
                "horizon": -1,
                "significant_5pct": True,
                "beta": 0.0,
                "t_stat_nw": 0.0,
            },
            {
                "shock_spec": "bill_surprise_extended_latest",
                "regime": None,
                "response_var": "mmf_treasury_holdings",
                "horizon": 4,
                "significant_5pct": True,
                "beta": 2.1,
                "t_stat_nw": 3.4,
            },
            {
                "shock_spec": "bill_surprise_extended_latest",
                "regime": None,
                "response_var": "on_rrp_daily_total",
                "horizon": 4,
                "significant_5pct": True,
                "beta": -1.6,
                "t_stat_nw": -5.0,
            },
        ]
    ).to_csv(auction_lp_csv, index=False)

    auction = pd.read_csv(auction_lp_csv)
    for column, value in {"sample": "extended_through_latest_with_post_rmp_labeled", "timing": "issue_week", "canonical_sample_end": "2025-11-30", "rmp_regime_start": "2025-12-01"}.items():
        auction[column] = value
    for column in ["ci_lower", "ci_upper", "se", "se_nw", "t_stat", "n_obs", "r_squared"]:
        auction[column] = 1.0
    auction.to_csv(auction_lp_csv, index=False)
    binary = pd.read_csv(pooled_lp_csv)
    for column in ["ci_lower", "ci_upper", "se", "se_nw", "t_stat", "n_obs", "r_squared"]:
        binary[column] = 1.0
    binary.to_csv(pooled_lp_csv, index=False)
    robustness_csv = tmp_path / "promotion_robustness_summary.csv"
    pd.DataFrame([
        {"spec": "extended_latest", "response_var": "mmf_treasury_holdings", "shock_sd_bn": 19.2845, "h4_effect_bn": 40.49745},
        {"spec": "extended_latest", "response_var": "on_rrp_daily_total", "shock_sd_bn": 19.2845, "h4_effect_bn": -30.8552},
    ]).to_csv(robustness_csv, index=False)
    build_summary(
        panel=panel,
        events_csv=events_csv,
        pooled_lp_csv=pooled_lp_csv,
        auction_shock_lp_csv=auction_lp_csv,
        robustness_csv=robustness_csv,
        site_dir=tmp_path,
    )

    summary = json.loads((tmp_path / "data" / "summary.json").read_text())
    assert summary["event_count"] == 1
    assert summary["bill_shock_sd_bn"] == 19.28
    assert summary["headline_effects"]["mmf_treasury_holdings"]["beta_bn"] == 40.5
    assert summary["headline_effects"]["on_rrp_daily_total"]["beta_bn"] == -30.86
    assert summary["pretrends"]["binary"]["significant_placebo_count"] == 1
    assert summary["pretrends"]["bill_surprise"]["channels_with_hits"] == 1

    assert summary["selected_spec"] == "extended_latest"
    assert summary["timing"] == "issue_week"
    assert summary["canonical_sample_end"] == "2025-11-30"
    build_lp_comparison(pooled_lp_csv, auction_lp_csv, robustness_csv, tmp_path)
    comparison = json.loads((tmp_path / "data/lp_comparison.json").read_text())
    continuous = [r for r in comparison if r["shock_spec"] == "bill_surprise_extended_latest"]
    assert continuous
    for row in continuous:
        assert row["beta_bn"] == round(row["beta"] * 19.2845, 2)
        assert row["ci_lower_bn"] == round(row["ci_lower"] * 19.2845, 2)
        assert row["ci_upper_bn"] == round(row["ci_upper"] * 19.2845, 2)
        assert row["sample"] == summary["sample"]
    wrong = pd.read_csv(robustness_csv)
    wrong["shock_sd_bn"] = 18.7735
    wrong.to_csv(robustness_csv, index=False)
    with pytest.raises(ValueError, match="coefficient/SD mismatch"):
        _extended_results(auction_lp_csv, robustness_csv)
