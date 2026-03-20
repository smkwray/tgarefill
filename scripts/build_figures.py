"""Generate research figures for TGA refill attribution study."""
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
import matplotlib.dates as mdates
import pandas as pd

from tgarefill.analytics.eras import ERA_ORDER, assign_era
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings

logger = logging.getLogger(__name__)

PROXY_COLS = [
    ("reserve_drain_proxy_positive_proxy_amount", "Reserve drain"),
    ("deposit_drawdown_proxy_positive_proxy_amount", "Deposit drawdown"),
    ("on_rrp_runoff_proxy_positive_proxy_amount", "ON RRP runoff"),
    ("bank_treasury_absorption_proxy_positive_proxy_amount", "Bank Treasury absorption"),
    ("mmf_treasury_absorption_proxy_positive_proxy_amount", "MMF Treasury absorption"),
    ("dealer_repo_proxy_positive_proxy_amount", "Dealer repo"),
]


def fig_tga_timeline_with_events(panel: pd.DataFrame, events: pd.DataFrame, out: Path) -> None:
    """TGA level over time with rebuild events shaded."""
    fig, ax = plt.subplots(figsize=(14, 5))
    tga = panel.dropna(subset=["tga_weekly_wednesday"]).copy()
    ax.plot(tga["date"], tga["tga_weekly_wednesday"] / 1000, color="#1f77b4", linewidth=0.8)
    for _, ev in events.iterrows():
        ax.axvspan(ev["start_date"], ev["end_date"], alpha=0.15, color="red")
    ax.set_ylabel("TGA Balance ($B)")
    ax.set_title("Treasury General Account with Rebuild Episodes Highlighted")
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_xlim(tga["date"].min(), tga["date"].max())
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out.name)


def fig_attribution_stacked(attr: pd.DataFrame, out: Path) -> None:
    """Stacked bar chart of proxy attribution for top 20 events."""
    top = attr.nlargest(20, "delta_tga").copy()
    top = top.sort_values("baseline_date")

    cols = [c for c, _ in PROXY_COLS]
    labels = [l for _, l in PROXY_COLS]

    plot_data = top[cols].fillna(0).clip(lower=0)
    # Convert to $B
    plot_data = plot_data / 1000
    delta_tga = top["delta_tga"].values / 1000

    fig, ax = plt.subplots(figsize=(14, 6))
    x = range(len(top))
    bottom = pd.Series(0.0, index=plot_data.index)
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]

    for (col, label), color in zip(PROXY_COLS, colors):
        vals = plot_data[col].values
        ax.bar(x, vals, bottom=bottom.values, label=label, color=color, width=0.7)
        bottom += plot_data[col]

    ax.scatter(x, delta_tga, color="black", zorder=5, s=30, label="ΔTGA")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [f"{d.strftime('%b %y')}" for d in top["baseline_date"]],
        rotation=45, ha="right", fontsize=8,
    )
    ax.set_ylabel("$B (millions / 1000)")
    ax.set_title("Attribution Decomposition — Top 20 Largest Rebuild Events")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out.name)


def fig_era_dominant_source(attr: pd.DataFrame, out: Path) -> None:
    """Grouped bar chart of dominant funding source by era."""
    proxy_amount_cols = [c for c, _ in PROXY_COLS]
    proxy_labels = {c: l for c, l in PROXY_COLS}

    dominant = []
    for _, row in attr.iterrows():
        best_col, best_val = None, 0
        for col in proxy_amount_cols:
            val = row[col]
            if pd.notna(val) and val > best_val:
                best_val = val
                best_col = col
        dominant.append(proxy_labels.get(best_col, "None"))
    attr = attr.copy()
    attr["dominant"] = dominant

    year = pd.to_datetime(attr["baseline_date"]).dt.year.astype(int)
    attr["era"] = year.map(assign_era)

    ct = pd.crosstab(attr["era"], attr["dominant"])
    ct = ct.reindex(ERA_ORDER, fill_value=0)
    # Reorder columns by total
    ct = ct[ct.sum().sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(10, 5))
    ct.plot(kind="bar", stacked=True, ax=ax, colormap="tab10", width=0.7)
    ax.set_ylabel("Number of Events")
    ax.set_title("Dominant Funding Source by Era")
    ax.set_xlabel("")
    ax.legend(title="", loc="upper right", fontsize=8)
    plt.xticks(rotation=0)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out.name)


def fig_onrrp_era(panel: pd.DataFrame, events: pd.DataFrame, out: Path) -> None:
    """ON RRP balance over time with rebuild events — zoomed to 2021-2026."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    mask = panel["date"] >= "2021-01-01"
    sub = panel[mask].copy()

    # TGA
    ax1.plot(sub["date"], sub["tga_weekly_wednesday"] / 1000, color="#1f77b4", linewidth=1)
    ax1.set_ylabel("TGA ($B)")
    ax1.set_title("TGA and ON RRP During the ON RRP Facility Era (2021-2026)")
    ax1.grid(axis="y", alpha=0.3)

    # ON RRP
    ax2.plot(sub["date"], sub["on_rrp_daily_total"] / 1000, color="#2ca02c", linewidth=1)
    ax2.set_ylabel("ON RRP ($B)")
    ax2.grid(axis="y", alpha=0.3)

    for _, ev in events.iterrows():
        if ev["start_date"] >= pd.Timestamp("2021-01-01"):
            for ax in (ax1, ax2):
                ax.axvspan(ev["start_date"], ev["end_date"], alpha=0.15, color="red")

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out.name)


def fig_event_size_over_time(events: pd.DataFrame, out: Path) -> None:
    """Scatter of event size over time, colored by issuance mix."""
    fig, ax = plt.subplots(figsize=(12, 5))
    colors = {"bill-heavy": "#1f77b4", "mixed": "#ff7f0e", "coupon-heavy": "#2ca02c"}
    for mix, grp in events.groupby("issuance_mix"):
        ax.scatter(
            grp["start_date"], grp["delta_tga_event"] / 1000,
            c=colors.get(mix, "gray"), label=mix, s=grp["duration_weeks"] * 15 + 10,
            alpha=0.7, edgecolors="black", linewidths=0.3,
        )
    ax.set_ylabel("ΔTGA ($B)")
    ax.set_title("Rebuild Event Size Over Time (bubble size = duration)")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out.name)


def main() -> None:
    configure_logging()
    settings = get_settings()
    fig_dir = settings.paths.output_figures
    fig_dir.mkdir(parents=True, exist_ok=True)

    panel = pd.read_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    events = pd.read_parquet(settings.paths.processed / "event_candidates.parquet")
    attr = pd.read_parquet(settings.paths.processed / "attribution_baseline.parquet")

    fig_tga_timeline_with_events(panel, events, fig_dir / "tga_timeline_events.png")
    fig_attribution_stacked(attr, fig_dir / "attribution_stacked_top20.png")
    fig_era_dominant_source(attr, fig_dir / "era_dominant_source.png")
    fig_onrrp_era(panel, events, fig_dir / "onrrp_era.png")
    fig_event_size_over_time(events, fig_dir / "event_size_over_time.png")

    logger.info("All figures saved to %s", fig_dir)


if __name__ == "__main__":
    main()
