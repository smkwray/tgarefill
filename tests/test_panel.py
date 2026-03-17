from pathlib import Path

import pandas as pd

from tgarefill.analytics.panel import build_dts_wednesday_tga


def test_build_dts_wednesday_tga_uses_prior_business_day_for_holiday_week(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    fiscaldata_dir = staging_dir / "fiscaldata"
    fiscaldata_dir.mkdir(parents=True)

    df = pd.DataFrame(
        {
            "record_date": [
                "2024-06-12",
                "2024-06-18",
                "2024-06-20",
                "2024-06-26",
            ],
            "account_type": [
                "Treasury General Account",
                "Treasury General Account",
                "Treasury General Account",
                "Treasury General Account",
            ],
            "close_today_bal": ["800", "900", "950", "1000"],
        }
    )
    df.to_parquet(fiscaldata_dir / "dts_operating_cash_balance.parquet", index=False)

    out = build_dts_wednesday_tga(staging_dir)

    holiday_week = out.loc[out["date"] == pd.Timestamp("2024-06-19")].iloc[0]
    assert holiday_week["tga_dts_wednesday"] == 900
    assert holiday_week["tga_dts_source_date"] == pd.Timestamp("2024-06-18")
