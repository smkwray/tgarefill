import pandas as pd

from tgarefill.analytics.local_projections import classify_regime, estimate_local_projections


def test_estimate_local_projections_drops_pre_tga_rows() -> None:
    panel = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=30, freq="W-WED"),
            "shock": [None] * 5 + [0.0] * 24 + [1.0],
            "response": [i * i for i in range(30)],
        }
    )

    results = estimate_local_projections(
        panel=panel,
        shock_col="shock",
        response_cols=["response"],
        max_horizon=1,
        lags=0,
    )

    assert results
    assert results[0].n_obs == 24


def test_classify_regime_leaves_pre_facility_weeks_missing() -> None:
    panel = pd.DataFrame({"on_rrp_daily_total": [None, 50_000, 150_000]})

    regime = classify_regime(panel)

    assert pd.isna(regime.iloc[0])
    assert regime.iloc[1] == "on_rrp_scarce"
    assert regime.iloc[2] == "on_rrp_abundant"
