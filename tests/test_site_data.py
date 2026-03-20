import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.build_site_data import build_events, build_summary


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
                "shock_spec": "bill_surprise",
                "regime": None,
                "response_var": "mmf_treasury_holdings",
                "horizon": -1,
                "significant_5pct": False,
                "beta": 0.0,
                "t_stat_nw": 0.0,
            },
            {
                "shock_spec": "bill_surprise",
                "regime": None,
                "response_var": "on_rrp_daily_total",
                "horizon": -1,
                "significant_5pct": True,
                "beta": 0.0,
                "t_stat_nw": 0.0,
            },
            {
                "shock_spec": "bill_surprise",
                "regime": None,
                "response_var": "mmf_treasury_holdings",
                "horizon": 4,
                "significant_5pct": True,
                "beta": 2.1,
                "t_stat_nw": 3.4,
            },
            {
                "shock_spec": "bill_surprise",
                "regime": None,
                "response_var": "on_rrp_daily_total",
                "horizon": 4,
                "significant_5pct": True,
                "beta": -1.6,
                "t_stat_nw": -5.0,
            },
        ]
    ).to_csv(auction_lp_csv, index=False)

    build_summary(
        panel=panel,
        events_csv=events_csv,
        pooled_lp_csv=pooled_lp_csv,
        auction_shock_lp_csv=auction_lp_csv,
        bill_shock_sd=19_284.5,
        site_dir=tmp_path,
    )

    summary = json.loads((tmp_path / "data" / "summary.json").read_text())
    assert summary["event_count"] == 1
    assert summary["bill_shock_sd_bn"] == 19.28
    assert summary["headline_effects"]["mmf_treasury_holdings"]["beta_bn"] == 40.5
    assert summary["headline_effects"]["on_rrp_daily_total"]["beta_bn"] == -30.86
    assert summary["pretrends"]["binary"]["significant_placebo_count"] == 1
    assert summary["pretrends"]["bill_surprise"]["channels_with_hits"] == 1
