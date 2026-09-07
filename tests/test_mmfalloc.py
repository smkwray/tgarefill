from __future__ import annotations

import pandas as pd

from tgarefill.mmfalloc import (
    build_baseline_table,
    build_downstream_summary,
    build_event_panel,
    build_validation_gates,
    classify_positions,
)


def test_classify_positions_core_categories() -> None:
    df = pd.DataFrame(
        {
            "investmentcategory": [
                "U.S. Treasury Debt",
                "U.S. Treasury Debt",
                "U.S. Treasury Repurchase Agreement, if collateralized only by U.S. Treasuries (including Strips) and cash",
                "U.S. Treasury Repurchase Agreement, if collateralized only by U.S. Treasuries (including Strips) and cash",
                "U.S. Government Agency Repurchase Agreement, collateralized only by U.S. Government Agency securities, U.S. Treasuries, and cash",
                "Financial Company Commercial Paper",
            ],
            "nameofissuer": [
                "United States Treasury",
                "United States Treasury",
                "Federal Reserve Bank of New York",
                "Royal Bank of Canada",
                "Dealer",
                "Issuer",
            ],
            "titleofissuer": [
                "United States Treasury Bill",
                "United States Treasury Note",
                "Tri-party Federal Reserve Bank of New York",
                "Tri-party repo",
                "Agency repo",
                "Commercial paper",
            ],
        }
    )

    assert classify_positions(df).tolist() == [
        "treasury_bills",
        "treasury_coupons",
        "fed_onrrp",
        "private_treasury_repo",
        "agency_repo",
        "private_instruments",
    ]


def test_validation_gates_require_contiguous_months_and_mapping() -> None:
    months = pd.date_range("2023-01-31", periods=36, freq="ME")
    coverage = pd.DataFrame(
        {
            "report_month": months,
            "total_assets": 100.0,
            "treasury_bills": 25.0,
            "treasury_coupons": 10.0,
            "fed_onrrp": 5.0,
            "private_treasury_repo": 10.0,
            "agency_repo": 5.0,
            "unmapped": 1.0,
            "mapped_share": 0.99,
        }
    )
    reconciliation = pd.DataFrame({"pct_diff": [0.1, -0.2, 0.3]})

    gates = build_validation_gates(coverage, reconciliation)

    assert gates.set_index("gate").loc["coverage", "passed"]
    assert gates.set_index("gate").loc["mapping", "passed"]
    assert gates.set_index("gate").loc["reconciliation", "passed"]


def test_event_panel_and_baseline_use_existing_bill_surprise_months() -> None:
    fund_month = pd.DataFrame(
        {
            "report_month": pd.to_datetime(["2023-04-30", "2023-05-31", "2023-06-30"]),
            "fund_id": ["F1", "F1", "F1"],
            "fund_name": ["Fund", "Fund", "Fund"],
            "adviser_or_family": ["Adviser", "Adviser", "Adviser"],
            "fund_type": ["Government", "Government", "Government"],
            "total_assets": [100.0, 120.0, 130.0],
            "treasury_bills": [20.0, 40.0, 45.0],
            "treasury_coupons": [10.0, 8.0, 8.0],
            "fed_onrrp": [50.0, 35.0, 30.0],
            "private_treasury_repo": [5.0, 7.0, 6.0],
            "agency_repo": [3.0, 2.0, 2.0],
            "agency_debt": [1.0, 1.0, 1.0],
            "private_instruments": [1.0, 1.0, 1.0],
            "cash_other": [10.0, 26.0, 37.0],
            "unmapped": [0.0, 0.0, 0.0],
            "wam": [30.0, 31.0, 32.0],
            "wal": [40.0, 41.0, 42.0],
        }
    )
    shocks = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-05-10"]),
            "bill_size_surprise": [10.0],
        }
    )

    event_panel = build_event_panel(fund_month, shocks, top_n_events=1)
    baseline = build_baseline_table(event_panel)

    tbill = baseline.set_index("category").loc["treasury_bills"]
    onrrp = baseline.set_index("category").loc["fed_onrrp"]
    assert tbill["delta_millions"] > 0
    assert onrrp["delta_millions"] < 0


def test_downstream_summary_keeps_claim_boundary() -> None:
    gates = pd.DataFrame(
        {
            "gate": ["coverage", "mapping", "reconciliation"],
            "passed": [True, True, True],
            "value": [36, 1.0, 0.04],
            "threshold": [36, 0.95, "<=0.75"],
            "note": ["", "", ""],
        }
    )
    coverage = pd.DataFrame(
        {
            "report_month": pd.date_range("2023-01-31", periods=2, freq="ME"),
            "mapped_share": [1.0, 0.99],
        }
    )
    baseline = pd.DataFrame(
        {
            "event_month": pd.to_datetime(["2023-02-28", "2023-02-28"]),
            "category": ["treasury_bills", "fed_onrrp"],
            "delta_millions": [100.0, -80.0],
        }
    )

    summary = build_downstream_summary(gates, coverage, baseline)

    values = summary.set_index("metric")["value"]
    assert values["source_gates_passed"] == "True"
    assert values["claim_boundary"] == "focused_tga_refill_mmf_allocation_support_only"
