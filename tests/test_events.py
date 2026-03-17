import pandas as pd

from tgarefill.analytics.events import classify_issuance_mix, compute_event_flags, detect_rebuild_events


def test_classify_issuance_mix() -> None:
    assert classify_issuance_mix(0.8) == "bill-heavy"
    assert classify_issuance_mix(0.2) == "coupon-heavy"
    assert classify_issuance_mix(0.5) == "mixed"


def test_detect_rebuild_events_basic() -> None:
    dates = pd.date_range("2024-01-03", periods=12, freq="W-WED")
    tga = [100, 102, 103, 105, 180, 260, 320, 325, 330, 332, 333, 335]
    bill_share = [0.2, 0.3, 0.2, 0.3, 0.8, 0.85, 0.9, 0.8, 0.75, 0.7, 0.6, 0.5]
    weekly = pd.DataFrame({"date": dates, "tga_weekly_wednesday": tga, "bill_share": bill_share})

    events = detect_rebuild_events(
        weekly_df=weekly,
        tga_col="tga_weekly_wednesday",
        bill_share_col="bill_share",
        rules={
            "delta_tga_quantile": 0.8,
            "rolling_4w_quantile": 0.8,
            "rolling_8w_quantile": 0.8,
            "bill_heavy_threshold": 0.7,
            "coupon_heavy_threshold": 0.3,
        },
    )

    assert not events.empty
    assert "issuance_mix" in events.columns


def test_compute_event_flags_uses_positive_only_rolling_thresholds() -> None:
    weekly = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-03", periods=9, freq="W-WED"),
            "tga_weekly_wednesday": [100, 500, 80, 50, 150, 160, 60, 70, 160],
        }
    )

    flagged = compute_event_flags(
        weekly_df=weekly,
        tga_col="tga_weekly_wednesday",
        delta_tga_quantile=1.0,
        rolling_4w_quantile=0.25,
        rolling_8w_quantile=1.0,
    )

    assert flagged["rolling_4w_tga_change_threshold"].iloc[0] == 15.0
