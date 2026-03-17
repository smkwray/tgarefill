import pandas as pd

from tgarefill.analytics.attribution import build_baseline_attribution


def test_build_baseline_attribution() -> None:
    events = pd.DataFrame(
        {
            "event_id": ["event_001"],
            "baseline_date": [pd.Timestamp("2024-01-03")],
            "start_date": [pd.Timestamp("2024-01-10")],
            "end_date": [pd.Timestamp("2024-01-24")],
            "issuance_mix": ["bill-heavy"],
        }
    )

    weekly_panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03", "2024-01-10", "2024-01-17", "2024-01-24"]),
            "tga_weekly_wednesday": [100, 140, 180, 220],
            "reserve_balances_weekly_wednesday": [3000, 2950, 2920, 2890],
            "commercial_bank_deposits_weekly_nsa": [18000, 17950, 17900, 17850],
            "on_rrp_daily_total": [500, 460, 430, 400],
            "bank_treasury_and_agency_securities_weekly_nsa": [2000, 2010, 2020, 2030],
        }
    )

    out = build_baseline_attribution(events, weekly_panel)
    assert not out.empty
    assert out.loc[0, "delta_tga"] == 120
    assert out.loc[0, "positive_proxy_total"] > 0


def test_build_baseline_attribution_applies_staleness_guard() -> None:
    events = pd.DataFrame(
        {
            "event_id": ["event_001"],
            "baseline_date": [pd.Timestamp("2024-01-03")],
            "start_date": [pd.Timestamp("2024-01-10")],
            "end_date": [pd.Timestamp("2024-01-31")],
            "issuance_mix": ["mixed"],
        }
    )

    weekly_panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03", "2024-01-10", "2024-01-17", "2024-01-24", "2024-01-31"]),
            "tga_weekly_wednesday": [100, 120, 140, 160, 180],
            "reserve_balances_weekly_wednesday": [3000, 2950, None, None, None],
        }
    )

    out = build_baseline_attribution(events, weekly_panel, max_lookback_days=14)
    assert pd.isna(out.loc[0, "reserve_drain_proxy_delta"])
    assert pd.isna(out.loc[0, "reserve_drain_proxy_positive_proxy_amount"])
