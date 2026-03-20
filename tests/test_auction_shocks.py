import pandas as pd

from tgarefill.analytics.auction_shocks import build_bill_size_surprise


def test_build_bill_size_surprise_supports_grouping_modes() -> None:
    auctions = pd.DataFrame(
        {
            "issue_date": pd.date_range("2024-01-03", periods=5, freq="W-WED"),
            "offering_amt": [10_000_000, 20_000_000, 30_000_000, 40_000_000, 50_000_000],
            "security_type": ["Bill"] * 5,
            "cash_management_bill_cmb": ["No"] * 5,
            "security_term": ["4-Week"] * 5,
            "reopening": ["No", "Yes", "No", "Yes", "No"],
        }
    )

    term_reopening = build_bill_size_surprise(
        auctions,
        lookback=8,
        min_history=1,
        grouping="term_reopening",
    )
    term_only = build_bill_size_surprise(
        auctions,
        lookback=8,
        min_history=1,
        grouping="term_only",
    )

    merged = term_reopening.merge(
        term_only,
        on="date",
        suffixes=("_term_reopening", "_term_only"),
    )
    target = merged.loc[merged["date"] == pd.Timestamp("2024-01-17")].iloc[0]

    assert target["bill_size_surprise_term_reopening"] == 20.0
    assert target["bill_size_surprise_term_only"] == 15.0
    assert not merged["bill_size_surprise_term_reopening"].equals(
        merged["bill_size_surprise_term_only"]
    )
