"""Run Jordà local projections and generate IRF figures."""
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

def plot_irfs(lp_df: pd.DataFrame, fig_dir: Path, response_vars: list[tuple[str, str]], filename: str) -> None:
    """Plot impulse response functions from LP results."""
    response_order = lp_df["response_var"].unique()
    has_regime = lp_df["regime"].notna().any()

    if not has_regime:
        # Single-regime plot
        n_vars = len(response_order)
        fig, axes = plt.subplots(2, 3, figsize=(15, 8), sharey=False)
        axes = axes.flatten()

        var_labels = {col: label for col, label in response_vars}

        for i, var in enumerate(response_order):
            if i >= len(axes):
                break
            ax = axes[i]
            sub = lp_df[(lp_df["response_var"] == var) & (lp_df["regime"].isna())]
            if sub.empty:
                sub = lp_df[lp_df["response_var"] == var]
            sub = sub.sort_values("horizon")

            ax.plot(sub["horizon"], sub["beta"], color="#1f77b4", linewidth=2)
            ax.fill_between(
                sub["horizon"], sub["ci_lower"], sub["ci_upper"],
                alpha=0.2, color="#1f77b4",
            )
            ax.axhline(0, color="black", linewidth=0.5, linestyle="--")

            # Mark significant horizons
            sig = sub[sub["significant_5pct"]]
            ax.scatter(sig["horizon"], sig["beta"], color="red", s=20, zorder=5)

            label = var_labels.get(var, var)
            ax.set_title(label, fontsize=11)
            ax.set_xlabel("Weeks" if i >= 3 else "")
            ax.set_ylabel("Δ (millions)" if i % 3 == 0 else "")
            ax.grid(alpha=0.3)

        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        fig.suptitle(
            "Impulse Response: TGA Rebuild Shock → Funding Channels\n"
            "(Jordà LP, 95% CI, red = significant at 5%)",
            fontsize=13,
        )
        fig.tight_layout()
        fig.savefig(fig_dir / filename, dpi=150)
        plt.close(fig)
        logger.info("Saved %s", filename)

    # Regime-split plot
    if has_regime:
        regimes = sorted(lp_df["regime"].dropna().unique())
        var_labels = {col: label for col, label in response_vars}
        colors = {"on_rrp_abundant": "#2ca02c", "on_rrp_scarce": "#d62728"}

        fig, axes = plt.subplots(2, 3, figsize=(15, 8))
        axes = axes.flatten()

        for i, (col, label) in enumerate(response_vars):
            if i >= len(axes):
                break
            ax = axes[i]

            for regime in regimes:
                sub = lp_df[(lp_df["response_var"] == col) & (lp_df["regime"] == regime)]
                sub = sub.sort_values("horizon")
                if sub.empty:
                    continue

                c = colors.get(regime, "gray")
                ax.plot(sub["horizon"], sub["beta"], color=c, linewidth=2, label=regime)
                ax.fill_between(sub["horizon"], sub["ci_lower"], sub["ci_upper"], alpha=0.15, color=c)

            ax.axhline(0, color="black", linewidth=0.5, linestyle="--")
            ax.set_title(label, fontsize=11)
            ax.set_xlabel("Weeks" if i >= 3 else "")
            ax.set_ylabel("Δ (millions)" if i % 3 == 0 else "")
            ax.grid(alpha=0.3)
            if i == 0:
                ax.legend(fontsize=8)

        for j in range(len(response_vars), len(axes)):
            axes[j].set_visible(False)

        fig.suptitle(
            "Impulse Response by ON RRP Regime\n"
            "(green = ON RRP abundant ≥$100B, red = scarce <$100B)",
            fontsize=13,
        )
        fig.tight_layout()
        fig.savefig(fig_dir / filename, dpi=150)
        plt.close(fig)
        logger.info("Saved %s", filename)


def main() -> None:
    configure_logging()
    settings = get_settings()
    lp_cfg = settings.analysis.get("local_projections", {})

    panel = pd.read_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    rules = settings.episode_rules.get("weekly", {})
    response_var_map = lp_cfg.get("responses", {})
    response_pairs = list(response_var_map.items())
    if not response_pairs:
        raise ValueError("analysis.local_projections.responses must not be empty")
    response_cols = [col for col, _ in response_pairs]
    max_horizon = int(lp_cfg.get("max_horizon", 12))
    placebo_horizons = int(lp_cfg.get("placebo_horizons", 4))
    shock_lags = int(lp_cfg.get("shock_lags", 2))
    response_lags = int(lp_cfg.get("response_lags", 1))
    add_month_dummies = bool(lp_cfg.get("add_month_dummies", True))

    # Add rebuild shock flag to the panel
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
    panel["delta_tga"] = flagged["delta_tga"]

    # Classify ON RRP regime
    panel["regime"] = classify_regime(panel)

    # === Pooled LP (no regime split) ===
    logger.info("Running pooled local projections...")
    pooled_results = estimate_local_projections(
        panel=panel,
        shock_col="rapid_rebuild_flag",
        response_cols=response_cols,
        min_horizon=-placebo_horizons,
        max_horizon=max_horizon,
        lags=shock_lags,
        response_lags=response_lags,
        add_month_dummies=add_month_dummies,
    )
    pooled_df = results_to_dataframe(pooled_results)
    pooled_df["shock_spec"] = "binary"
    logger.info("Pooled LP: %s results across %s variables", len(pooled_df), pooled_df["response_var"].nunique())

    # === Regime-split LP ===
    logger.info("Running regime-split local projections...")
    regime_results = estimate_local_projections(
        panel=panel,
        shock_col="rapid_rebuild_flag",
        response_cols=response_cols,
        min_horizon=-placebo_horizons,
        max_horizon=max_horizon,
        lags=shock_lags,
        response_lags=response_lags,
        add_month_dummies=add_month_dummies,
        regime_col="regime",
    )
    regime_df = results_to_dataframe(regime_results)
    regime_df["shock_spec"] = "binary"
    logger.info("Regime LP: %s results", len(regime_df))

    logger.info("Running continuous-shock local projections...")
    continuous_pooled = results_to_dataframe(
        estimate_local_projections(
            panel=panel,
            shock_col="delta_tga",
            response_cols=response_cols,
            min_horizon=-placebo_horizons,
            max_horizon=max_horizon,
            lags=shock_lags,
            response_lags=response_lags,
            add_month_dummies=add_month_dummies,
        )
    )
    continuous_pooled["shock_spec"] = "continuous"
    continuous_regime = results_to_dataframe(
        estimate_local_projections(
            panel=panel,
            shock_col="delta_tga",
            response_cols=response_cols,
            min_horizon=-placebo_horizons,
            max_horizon=max_horizon,
            lags=shock_lags,
            response_lags=response_lags,
            add_month_dummies=add_month_dummies,
            regime_col="regime",
        )
    )
    continuous_regime["shock_spec"] = "continuous"

    combined = pd.concat([pooled_df, regime_df], ignore_index=True).sort_values(
        ["shock_spec", "response_var", "regime", "horizon"]
    )
    write_dataframe(combined, settings.paths.processed / "local_projections.parquet")
    write_dataframe(combined, settings.paths.output_tables / "local_projections.csv")
    continuous = pd.concat([continuous_pooled, continuous_regime], ignore_index=True).sort_values(
        ["shock_spec", "response_var", "regime", "horizon"]
    )
    write_dataframe(continuous, settings.paths.processed / "local_projections_continuous.parquet")
    write_dataframe(continuous, settings.paths.output_tables / "local_projections_continuous.csv")

    # Generate figures
    fig_dir = settings.paths.output_figures
    fig_dir.mkdir(parents=True, exist_ok=True)
    plot_irfs(pooled_df, fig_dir, response_pairs, "irf_pooled.png")
    plot_irfs(regime_df, fig_dir, response_pairs, "irf_by_regime.png")
    plot_irfs(continuous_regime, fig_dir, response_pairs, "irf_continuous_regime.png")

    # Summary
    sig = combined[combined["significant_5pct"]]
    logger.info(
        "Significant responses (5%%): %s out of %s (%s%%)",
        len(sig), len(combined), round(100 * len(sig) / max(len(combined), 1)),
    )


if __name__ == "__main__":
    main()
