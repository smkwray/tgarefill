"""Build auction-schedule surprise shocks and run LPs to test pre-trends."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from tgarefill.analytics.auction_shocks import (
    build_bill_size_surprise,
    build_short_notice_cmb,
    build_tax_receipt_surprise,
)
from tgarefill.analytics.events import compute_event_flags
from tgarefill.analytics.local_projections import (
    classify_regime,
    estimate_local_projections,
    results_to_dataframe,
)
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import write_dataframe

logger = logging.getLogger(__name__)

RESPONSE_VARS = [
    ("reserve_balances_weekly_wednesday", "Reserves"),
    ("commercial_bank_deposits_weekly_nsa", "Bank Deposits"),
    ("on_rrp_daily_total", "ON RRP"),
    ("bank_treasury_and_agency_securities_weekly_nsa", "Bank T&A Holdings"),
    ("mmf_treasury_holdings", "MMF Treasury Holdings"),
    ("dealer_treasury_repo", "Dealer Treasury Repo"),
]


def main() -> None:
    configure_logging()
    settings = get_settings()

    # Load panel and auction data
    panel = pd.read_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    auctions = pd.read_parquet(settings.paths.staging / "fiscaldata" / "auctions_query.parquet")
    dts_dw = pd.read_parquet(settings.paths.staging / "fiscaldata" / "dts_deposits_withdrawals_operating_cash.parquet")

    # Build shocks
    bill_surprise = build_bill_size_surprise(auctions)
    logger.info("Bill-size surprise: %s weeks with nonzero values", (bill_surprise["bill_size_surprise"] != 0).sum())

    cmb_shock = build_short_notice_cmb(auctions)
    logger.info("Short-notice CMB: %s weeks with positive values", (cmb_shock["short_notice_issuance"] > 0).sum())

    tax_surprise = build_tax_receipt_surprise(dts_dw)
    logger.info("Tax-receipt surprise: %s weeks", len(tax_surprise))

    # Merge into panel
    panel = panel.merge(bill_surprise, on="date", how="left")
    panel = panel.merge(cmb_shock, on="date", how="left")
    panel = panel.merge(tax_surprise, on="date", how="left")

    # Fill missing shocks with 0 (no auction that week = no surprise)
    for col in ["bill_size_surprise", "short_notice_issuance", "tax_receipt_surprise"]:
        panel[col] = panel[col].fillna(0)

    panel["regime"] = classify_regime(panel)
    bill_shock_sd = float(panel["bill_size_surprise"].std())

    response_cols = [col for col, _ in RESPONSE_VARS]
    lp_cfg = settings.analysis.get("local_projections", {})
    max_horizon = int(lp_cfg.get("max_horizon", 12))
    placebo = int(lp_cfg.get("placebo_horizons", 4))
    shock_lags = int(lp_cfg.get("shock_lags", 2))
    response_lags = int(lp_cfg.get("response_lags", 1))
    add_month = bool(lp_cfg.get("add_month_dummies", True))
    rules = settings.episode_rules.get("weekly", {})

    flagged = compute_event_flags(panel, tga_col="tga_weekly_wednesday", **{
        k: rules[k] for k in (
            "delta_tga_quantile",
            "rolling_4w_quantile",
            "rolling_8w_quantile",
            "rolling_z_window_weeks",
            "delta_tga_zscore_threshold",
            "rolling_4w_zscore_threshold",
            "rolling_8w_zscore_threshold",
            "min_auction_total",
        )
        if k in rules
    })
    panel["rapid_rebuild_flag"] = flagged["rapid_rebuild_flag"].astype(float)

    binary_lp = results_to_dataframe(estimate_local_projections(
        panel=panel,
        shock_col="rapid_rebuild_flag",
        response_cols=response_cols,
        min_horizon=-placebo,
        max_horizon=max_horizon,
        lags=shock_lags,
        response_lags=response_lags,
        add_month_dummies=add_month,
        control_cols=["tax_receipt_surprise"],
    ))
    binary_lp["shock_spec"] = "binary_with_tax_control"

    # === Bill-size surprise LP ===
    logger.info("Running bill-size surprise LP...")
    bill_lp = results_to_dataframe(estimate_local_projections(
        panel=panel,
        shock_col="bill_size_surprise",
        response_cols=response_cols,
        min_horizon=-placebo,
        max_horizon=max_horizon,
        lags=shock_lags,
        response_lags=response_lags,
        add_month_dummies=add_month,
        control_cols=["tax_receipt_surprise"],
    ))
    bill_lp["shock_spec"] = "bill_surprise"
    logger.info("Bill-surprise LP: %s results, %s significant",
                len(bill_lp), bill_lp["significant_5pct"].sum())

    # === Bill-size surprise LP with regime split ===
    logger.info("Running bill-size surprise LP (regime split)...")
    bill_regime_lp = results_to_dataframe(estimate_local_projections(
        panel=panel,
        shock_col="bill_size_surprise",
        response_cols=response_cols,
        min_horizon=-placebo,
        max_horizon=max_horizon,
        lags=shock_lags,
        response_lags=response_lags,
        add_month_dummies=add_month,
        control_cols=["tax_receipt_surprise"],
        regime_col="regime",
    ))
    bill_regime_lp["shock_spec"] = "bill_surprise"

    # === Short-notice CMB LP (robustness) ===
    logger.info("Running short-notice CMB LP...")
    cmb_lp = results_to_dataframe(estimate_local_projections(
        panel=panel,
        shock_col="short_notice_issuance",
        response_cols=response_cols,
        min_horizon=-placebo,
        max_horizon=max_horizon,
        lags=shock_lags,
        response_lags=response_lags,
        add_month_dummies=add_month,
        control_cols=["tax_receipt_surprise"],
    ))
    cmb_lp["shock_spec"] = "short_notice_cmb"
    logger.info("CMB LP: %s results, %s significant",
                len(cmb_lp), cmb_lp["significant_5pct"].sum())

    # Save results
    combined = pd.concat([bill_lp, bill_regime_lp, cmb_lp], ignore_index=True)
    write_dataframe(combined, settings.paths.processed / "auction_shock_lp.parquet")
    write_dataframe(combined, settings.paths.output_tables / "auction_shock_lp.csv")

    # === Compare pre-trends: binary vs bill-surprise ===
    labels = {col: label for col, label in RESPONSE_VARS}

    print("\n" + "=" * 90)
    print("PRE-TREND COMPARISON: Binary Shock vs Bill-Size Surprise")
    print("=" * 90)
    for var, label in labels.items():
        orig_pre = binary_lp[(binary_lp["response_var"] == var) & (binary_lp["horizon"] < 0)]
        bill_pre = bill_lp[(bill_lp["response_var"] == var) & (bill_lp["horizon"] < 0)]
        orig_sig = len(orig_pre[orig_pre["significant_5pct"]])
        bill_sig = len(bill_pre[bill_pre["significant_5pct"]])
        print(f"  {label:25s}: Binary pre-trend sigs={orig_sig}/4  |  Bill-surprise pre-trend sigs={bill_sig}/4")

    print("\n" + "=" * 90)
    print("POST-SHOCK BILL-SURPRISE LP (h=4, NW)")
    print("=" * 90)
    h4 = bill_lp[(bill_lp["horizon"] == 4) & (bill_lp["regime"].isna())]
    for _, row in h4.iterrows():
        label = labels.get(row["response_var"], row["response_var"])
        sig = "*" if row["significant_5pct"] else " "
        scaled_bn = row["beta"] * bill_shock_sd / 1000
        print(f"  {label:25s}: 1sd={scaled_bn:>8.1f}bn  t_NW={row['t_stat_nw']:>5.1f} {sig}")

    # === Generate comparison figure ===
    fig_dir = settings.paths.output_figures
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    axes = axes.flatten()
    for i, (col, label) in enumerate(RESPONSE_VARS):
        if i >= len(axes):
            break
        ax = axes[i]

        for spec_name, spec_df, color, ls in [
            ("Binary", binary_lp, "#1f77b4", "-"),
            ("Bill surprise", bill_lp[bill_lp["regime"].isna()], "#d62728", "--"),
        ]:
            sub = spec_df[spec_df["response_var"] == col].sort_values("horizon")
            if sub.empty:
                continue
            ax.plot(sub["horizon"], sub["beta"], color=color, linewidth=2, linestyle=ls, label=spec_name)
            ax.fill_between(sub["horizon"], sub["ci_lower"], sub["ci_upper"], alpha=0.1, color=color)

        ax.axhline(0, color="black", linewidth=0.5, linestyle="--")
        ax.axvline(0, color="gray", linewidth=0.5, linestyle=":")
        ax.set_title(label, fontsize=11)
        ax.set_xlabel("Weeks" if i >= 3 else "")
        ax.set_ylabel("Δ (millions)" if i % 3 == 0 else "")
        ax.grid(alpha=0.3)
        if i == 0:
            ax.legend(fontsize=8)

    fig.suptitle(
        "IRF Comparison: Binary Shock vs Bill-Size Surprise\n"
        "(solid blue = binary, dashed red = bill surprise, vertical line = shock)",
        fontsize=13,
    )
    fig.tight_layout()
    fig.savefig(fig_dir / "irf_binary_vs_bill_surprise.png", dpi=150)
    plt.close(fig)
    logger.info("Saved irf_binary_vs_bill_surprise.png")


if __name__ == "__main__":
    main()
