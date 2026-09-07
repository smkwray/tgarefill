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

CANONICAL_SAMPLE_END = pd.Timestamp("2025-11-30")
RMP_REGIME_START = pd.Timestamp("2025-12-01")


def _run_bill_surprise_lp(
    panel: pd.DataFrame,
    response_cols: list[str],
    shock_col: str,
    shock_lags: int,
    response_lags: int,
    add_month: bool,
    placebo: int,
    max_horizon: int,
    regime_col: str | None = None,
) -> pd.DataFrame:
    return results_to_dataframe(
        estimate_local_projections(
            panel=panel,
            shock_col=shock_col,
            response_cols=response_cols,
            min_horizon=-placebo,
            max_horizon=max_horizon,
            lags=shock_lags,
            response_lags=response_lags,
            add_month_dummies=add_month,
            control_cols=["tax_receipt_surprise"],
            regime_col=regime_col,
        )
    )


def _build_grouping_comparison(
    grouped_results: dict[str, tuple[pd.DataFrame, float]]
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for grouping, (lp_df, bill_shock_sd) in grouped_results.items():
        placebo = lp_df[lp_df["horizon"] < 0]
        placebo_hits = int(placebo["significant_5pct"].sum())
        placebo_channels = int(
            placebo.groupby("response_var")["significant_5pct"].any().sum()
        )
        h4 = lp_df[(lp_df["horizon"] == 4) & (lp_df["regime"].isna())]
        for response_var, label in RESPONSE_VARS:
            row = {
                "grouping": grouping,
                "response_var": response_var,
                "response_label": label,
                "bill_shock_sd_bn": round(bill_shock_sd / 1000, 4),
                "placebo_significant_count": placebo_hits,
                "placebo_channels_with_hits": placebo_channels,
                "h4_effect_bn": None,
                "h4_t_stat_nw": None,
                "h4_significant_5pct": None,
            }
            sub = h4[h4["response_var"] == response_var]
            if not sub.empty:
                result = sub.iloc[0]
                row["h4_effect_bn"] = round(float(result["beta"]) * bill_shock_sd / 1000, 4)
                row["h4_t_stat_nw"] = round(float(result["t_stat_nw"]), 4)
                row["h4_significant_5pct"] = bool(result["significant_5pct"])
            rows.append(row)
    return pd.DataFrame(rows)


def _canonical_sample(panel: pd.DataFrame) -> pd.DataFrame:
    """Main promoted sample: pre-RMP bill-plumbing regime through 2025-11."""
    return panel[panel["date"] <= CANONICAL_SAMPLE_END].copy()


def _add_sample_metadata(df: pd.DataFrame, sample: str, timing: str) -> pd.DataFrame:
    out = df.copy()
    out["sample"] = sample
    out["timing"] = timing
    out["canonical_sample_end"] = CANONICAL_SAMPLE_END.date().isoformat()
    out["rmp_regime_start"] = RMP_REGIME_START.date().isoformat()
    return out


def _headline_summary(
    lp_frames: dict[str, tuple[pd.DataFrame, float, str]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for label, (lp_df, shock_sd, note) in lp_frames.items():
        pooled = lp_df[lp_df["regime"].isna()].copy()
        placebo = pooled[pooled["horizon"] < 0]
        h4 = pooled[pooled["horizon"] == 4]
        for response_var, response_label in RESPONSE_VARS:
            row = {
                "spec": label,
                "response_var": response_var,
                "response_label": response_label,
                "shock_sd_bn": round(shock_sd / 1000, 4),
                "placebo_significant_count": int(
                    placebo.loc[placebo["response_var"] == response_var, "significant_5pct"].sum()
                ),
                "all_channel_placebo_significant_count": int(placebo["significant_5pct"].sum()),
                "h4_effect_bn": None,
                "h4_t_stat_nw": None,
                "h4_significant_5pct": None,
                "note": note,
            }
            sub = h4[h4["response_var"] == response_var]
            if not sub.empty:
                result = sub.iloc[0]
                row["h4_effect_bn"] = round(float(result["beta"]) * shock_sd / 1000, 4)
                row["h4_t_stat_nw"] = round(float(result["t_stat_nw"]), 4)
                row["h4_significant_5pct"] = bool(result["significant_5pct"])
            rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    configure_logging()
    settings = get_settings()

    # Load panel and auction data
    panel = pd.read_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    auctions = pd.read_parquet(settings.paths.staging / "fiscaldata" / "auctions_query.parquet")
    dts_dw = pd.read_parquet(settings.paths.staging / "fiscaldata" / "dts_deposits_withdrawals_operating_cash.parquet")

    # Build shocks
    bill_surprise = build_bill_size_surprise(
        auctions,
        grouping="term_reopening",
        timing="issue_week",
    )
    logger.info("Bill-size surprise: %s weeks with nonzero values", (bill_surprise["bill_size_surprise"] != 0).sum())
    bill_surprise_term_only = build_bill_size_surprise(auctions, grouping="term_only")
    logger.info(
        "Bill-size surprise (term only): %s weeks with nonzero values",
        (bill_surprise_term_only["bill_size_surprise"] != 0).sum(),
    )
    bill_surprise_announcement_week = build_bill_size_surprise(
        auctions,
        grouping="term_reopening",
        timing="announcement_week",
    )
    logger.info(
        "Bill-size surprise (announcement-week timing): %s weeks with nonzero values",
        (bill_surprise_announcement_week["bill_size_surprise"] != 0).sum(),
    )

    cmb_shock = build_short_notice_cmb(auctions)
    logger.info("Short-notice CMB: %s weeks with positive values", (cmb_shock["short_notice_issuance"] > 0).sum())

    tax_surprise = build_tax_receipt_surprise(dts_dw)
    logger.info("Tax-receipt surprise: %s weeks", len(tax_surprise))

    # Merge into panel
    panel = panel.merge(bill_surprise, on="date", how="left")
    panel = panel.merge(
        bill_surprise_term_only.rename(
            columns={"bill_size_surprise": "bill_size_surprise_term_only"}
        ),
        on="date",
        how="left",
    )
    panel = panel.merge(
        bill_surprise_announcement_week.rename(
            columns={"bill_size_surprise": "bill_size_surprise_announcement_week"}
        ),
        on="date",
        how="left",
    )
    panel = panel.merge(cmb_shock, on="date", how="left")
    panel = panel.merge(tax_surprise, on="date", how="left")

    # Fill missing shocks with 0 (no auction that week = no surprise)
    for col in [
        "bill_size_surprise",
        "bill_size_surprise_term_only",
        "bill_size_surprise_announcement_week",
        "short_notice_issuance",
        "tax_receipt_surprise",
    ]:
        panel[col] = panel[col].fillna(0)

    panel["regime"] = classify_regime(panel)
    panel["rmp_regime"] = "pre_rmp"
    panel.loc[panel["date"] >= RMP_REGIME_START, "rmp_regime"] = "post_rmp"
    panel["post_rmp_regime"] = (panel["date"] >= RMP_REGIME_START).astype(float)

    canonical_panel = _canonical_sample(panel)
    bill_shock_sd = float(canonical_panel["bill_size_surprise"].std())
    bill_shock_sd_term_only = float(canonical_panel["bill_size_surprise_term_only"].std())
    bill_shock_sd_announcement_week = float(
        canonical_panel["bill_size_surprise_announcement_week"].std()
    )
    bill_shock_sd_extended = float(panel["bill_size_surprise"].std())

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
    canonical_panel = _canonical_sample(panel)
    canonical_shocks = canonical_panel[
        [
            "date",
            "bill_size_surprise",
            "bill_size_surprise_announcement_week",
            "tax_receipt_surprise",
            "rapid_rebuild_flag",
            "regime",
            "rmp_regime",
        ]
    ].copy()
    canonical_shocks["sample"] = "canonical_pre_rmp_through_2025_11"
    canonical_shocks["canonical_sample_end"] = CANONICAL_SAMPLE_END.date().isoformat()
    write_dataframe(
        canonical_shocks,
        settings.paths.processed / "canonical_bill_surprise_shocks.parquet",
    )
    write_dataframe(
        canonical_shocks,
        settings.paths.output_tables / "canonical_bill_surprise_shocks.csv",
    )

    binary_lp = results_to_dataframe(estimate_local_projections(
        panel=canonical_panel,
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
    binary_lp = _add_sample_metadata(
        binary_lp,
        sample="canonical_pre_rmp_through_2025_11",
        timing="issue_week",
    )

    # === Bill-size surprise LP ===
    logger.info("Running bill-size surprise LP...")
    bill_lp = _run_bill_surprise_lp(
        panel=canonical_panel,
        shock_col="bill_size_surprise",
        response_cols=response_cols,
        shock_lags=shock_lags,
        response_lags=response_lags,
        add_month=add_month,
        placebo=placebo,
        max_horizon=max_horizon,
    )
    bill_lp["shock_spec"] = "bill_surprise"
    bill_lp = _add_sample_metadata(
        bill_lp,
        sample="canonical_pre_rmp_through_2025_11",
        timing="issue_week",
    )
    logger.info("Bill-surprise LP: %s results, %s significant",
                len(bill_lp), bill_lp["significant_5pct"].sum())

    # === Bill-size surprise LP with regime split ===
    logger.info("Running bill-size surprise LP (regime split)...")
    bill_regime_lp = _run_bill_surprise_lp(
        panel=canonical_panel,
        shock_col="bill_size_surprise",
        response_cols=response_cols,
        shock_lags=shock_lags,
        response_lags=response_lags,
        add_month=add_month,
        placebo=placebo,
        max_horizon=max_horizon,
        regime_col="regime",
    )
    bill_regime_lp["shock_spec"] = "bill_surprise"
    bill_regime_lp = _add_sample_metadata(
        bill_regime_lp,
        sample="canonical_pre_rmp_through_2025_11",
        timing="issue_week",
    )

    logger.info("Running bill-size surprise LP (term-only robustness)...")
    bill_lp_term_only = _run_bill_surprise_lp(
        panel=canonical_panel,
        shock_col="bill_size_surprise_term_only",
        response_cols=response_cols,
        shock_lags=shock_lags,
        response_lags=response_lags,
        add_month=add_month,
        placebo=placebo,
        max_horizon=max_horizon,
    )
    bill_lp_term_only["shock_spec"] = "bill_surprise_term_only"
    bill_lp_term_only = _add_sample_metadata(
        bill_lp_term_only,
        sample="canonical_pre_rmp_through_2025_11",
        timing="issue_week",
    )

    logger.info("Running bill-size surprise LP (announcement-week timing robustness)...")
    bill_lp_announcement_week = _run_bill_surprise_lp(
        panel=canonical_panel,
        shock_col="bill_size_surprise_announcement_week",
        response_cols=response_cols,
        shock_lags=shock_lags,
        response_lags=response_lags,
        add_month=add_month,
        placebo=placebo,
        max_horizon=max_horizon,
    )
    bill_lp_announcement_week["shock_spec"] = "bill_surprise_announcement_week"
    bill_lp_announcement_week = _add_sample_metadata(
        bill_lp_announcement_week,
        sample="same_week_timing_canonical_pre_rmp",
        timing="announcement_week",
    )

    logger.info("Running bill-size surprise LP (extended-through-latest robustness)...")
    bill_lp_extended = _run_bill_surprise_lp(
        panel=panel,
        shock_col="bill_size_surprise",
        response_cols=response_cols,
        shock_lags=shock_lags,
        response_lags=response_lags,
        add_month=add_month,
        placebo=placebo,
        max_horizon=max_horizon,
    )
    bill_lp_extended["shock_spec"] = "bill_surprise_extended_latest"
    bill_lp_extended = _add_sample_metadata(
        bill_lp_extended,
        sample="extended_through_latest_with_post_rmp_labeled",
        timing="issue_week",
    )

    # === Short-notice CMB LP (robustness) ===
    logger.info("Running short-notice CMB LP...")
    cmb_lp = results_to_dataframe(estimate_local_projections(
        panel=canonical_panel,
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
    cmb_lp = _add_sample_metadata(
        cmb_lp,
        sample="canonical_pre_rmp_through_2025_11",
        timing="issue_week",
    )
    logger.info("CMB LP: %s results, %s significant",
                len(cmb_lp), cmb_lp["significant_5pct"].sum())

    # Save results
    combined = pd.concat(
        [
            binary_lp,
            bill_lp,
            bill_regime_lp,
            bill_lp_announcement_week,
            bill_lp_extended,
            cmb_lp,
        ],
        ignore_index=True,
    )
    write_dataframe(combined, settings.paths.processed / "auction_shock_lp.parquet")
    write_dataframe(combined, settings.paths.output_tables / "auction_shock_lp.csv")
    grouping_comparison = _build_grouping_comparison(
        {
            "term_reopening": (bill_lp, bill_shock_sd),
            "term_only": (bill_lp_term_only, bill_shock_sd_term_only),
        }
    )
    write_dataframe(
        grouping_comparison,
        settings.paths.processed / "auction_shock_grouping_comparison.parquet",
    )
    write_dataframe(
        grouping_comparison,
        settings.paths.output_tables / "auction_shock_grouping_comparison.csv",
    )
    promotion_summary = _headline_summary(
        {
            "canonical_issue_week": (
                bill_lp,
                bill_shock_sd,
                "Main promoted sample: pre-RMP through 2025-11.",
            ),
            "same_week_announcement_timing": (
                bill_lp_announcement_week,
                bill_shock_sd_announcement_week,
                "Timing robustness: shock aligned to announcement week.",
            ),
            "extended_latest": (
                bill_lp_extended,
                bill_shock_sd_extended,
                "Through-latest robustness only; post-2025-11 RMP regime included.",
            ),
        }
    )
    write_dataframe(
        promotion_summary,
        settings.paths.output_tables / "promotion_robustness_summary.csv",
    )

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
