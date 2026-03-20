"""Export pipeline outputs to site/data/ JSON for the GitHub Pages site."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import json
import logging
import shutil

import numpy as np
import pandas as pd

from tgarefill.analytics.auction_shocks import build_bill_size_surprise
from tgarefill.analytics.eras import ERA_ORDER, assign_era
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings

logger = logging.getLogger(__name__)

RESPONSE_LABELS = {
    "reserve_balances_weekly_wednesday": "Reserves",
    "commercial_bank_deposits_weekly_nsa": "Bank Deposits",
    "on_rrp_daily_total": "ON RRP",
    "bank_treasury_and_agency_securities_weekly_nsa": "Bank T&A",
    "mmf_treasury_holdings": "MMF Treasury",
    "dealer_treasury_repo": "Dealer Repo",
}

PROXY_COLS = {
    "reserve_drain_proxy_positive_proxy_amount": "reserve_drain_bn",
    "deposit_drawdown_proxy_positive_proxy_amount": "deposit_drawdown_bn",
    "on_rrp_runoff_proxy_positive_proxy_amount": "on_rrp_runoff_bn",
    "bank_treasury_absorption_proxy_positive_proxy_amount": "bank_t&a_bn",
    "mmf_treasury_absorption_proxy_positive_proxy_amount": "mmf_treasury_bn",
    "dealer_repo_proxy_positive_proxy_amount": "dealer_repo_bn",
}

SOURCE_MAP = {
    "reserve_drain_proxy_positive_proxy_amount": "Reserve drain",
    "deposit_drawdown_proxy_positive_proxy_amount": "Deposit drawdown",
    "on_rrp_runoff_proxy_positive_proxy_amount": "ON RRP runoff",
    "bank_treasury_absorption_proxy_positive_proxy_amount": "Bank T&A",
    "mmf_treasury_absorption_proxy_positive_proxy_amount": "MMF Treasury",
    "dealer_repo_proxy_positive_proxy_amount": "Dealer repo",
}


def _require_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")


def _pick_first_column(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    for column in candidates:
        if column in df.columns:
            return column
    raise ValueError(f"{label} is missing all candidate columns: {', '.join(candidates)}")


def _write_json(data: list | dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    try:
        rel_path = path.relative_to(ROOT)
    except ValueError:
        rel_path = path
    logger.info("Wrote %s", rel_path)


def build_timeline(panel: pd.DataFrame, site_dir: Path) -> None:
    tl = panel[["date", "tga_weekly_wednesday", "on_rrp_daily_total"]].dropna(
        subset=["tga_weekly_wednesday"]
    ).copy()
    tl["date"] = tl["date"].dt.strftime("%Y-%m-%d")
    tl["tga_bn"] = (tl["tga_weekly_wednesday"] / 1000).round(1)
    tl["onrrp_bn"] = (tl["on_rrp_daily_total"] / 1000).round(1)
    records = tl[["date", "tga_bn", "onrrp_bn"]].replace({np.nan: None}).to_dict("records")
    _write_json(records, site_dir / "data" / "timeline.json")


def build_events(events_csv: Path, site_dir: Path) -> None:
    events = pd.read_csv(events_csv)
    _require_columns(
        events,
        ["event_id", "baseline_date", "start_date", "end_date", "duration_weeks", "issuance_mix"],
        "event_candidates.csv",
    )
    delta_col = _pick_first_column(
        events,
        ["delta_tga_event", "delta_tga"],
        "event_candidates.csv",
    )
    out = []
    for _, e in events.iterrows():
        out.append({
            "event_id": e["event_id"],
            "baseline_date": str(e["baseline_date"])[:10],
            "start_date": str(e["start_date"])[:10],
            "end_date": str(e["end_date"])[:10],
            "delta_tga_bn": round(float(e.get(delta_col, 0)) / 1000, 1) if pd.notna(e.get(delta_col)) else None,
            "duration_weeks": int(e.get("duration_weeks", 1)) if pd.notna(e.get("duration_weeks")) else 1,
            "issuance_mix": str(e.get("issuance_mix", "")) if pd.notna(e.get("issuance_mix")) else "",
            "manual_tags": str(e.get("manual_tags", "")) if pd.notna(e.get("manual_tags")) else "",
        })
    _write_json(out, site_dir / "data" / "events.json")


def build_attribution_enriched(attr_csv: Path, site_dir: Path) -> None:
    attr = pd.read_csv(attr_csv)
    out = []
    for _, row in attr.iterrows():
        d = {
            "event_id": row["event_id"],
            "baseline_date": str(row["baseline_date"])[:10],
            "end_date": str(row["end_date"])[:10],
            "delta_tga_bn": round(float(row["delta_tga"]) / 1000, 1) if pd.notna(row.get("delta_tga")) else None,
        }
        for src_col, out_col in PROXY_COLS.items():
            val = row.get(src_col)
            d[out_col] = round(float(val) / 1000, 1) if pd.notna(val) else 0
        out.append(d)
    _write_json(out, site_dir / "data" / "attribution_enriched.json")


def build_lp_comparison(
    lp_csv: Path, aslp_csv: Path, bill_shock_sd: float, site_dir: Path
) -> None:
    lp = pd.read_csv(lp_csv)
    _require_columns(lp, ["shock_spec", "regime", "response_var", "horizon", "beta", "ci_lower", "ci_upper"], "local_projections.csv")
    binary_pooled = lp[(lp["shock_spec"] == "binary") & (lp["regime"].isna())].copy()
    binary_pooled["beta_bn"] = (binary_pooled["beta"] / 1000).round(2)
    binary_pooled["ci_lower_bn"] = (binary_pooled["ci_lower"] / 1000).round(2)
    binary_pooled["ci_upper_bn"] = (binary_pooled["ci_upper"] / 1000).round(2)

    aslp = pd.read_csv(aslp_csv)
    _require_columns(aslp, ["shock_spec", "regime", "response_var", "horizon", "beta", "ci_lower", "ci_upper"], "auction_shock_lp.csv")
    bill_pooled = aslp[(aslp["shock_spec"] == "bill_surprise") & (aslp["regime"].isna())].copy()
    bill_pooled["beta_bn"] = (bill_pooled["beta"] * bill_shock_sd / 1000).round(2)
    bill_pooled["ci_lower_bn"] = (bill_pooled["ci_lower"] * bill_shock_sd / 1000).round(2)
    bill_pooled["ci_upper_bn"] = (bill_pooled["ci_upper"] * bill_shock_sd / 1000).round(2)

    keep = [
        "response_var", "horizon", "beta", "se", "se_nw", "t_stat", "t_stat_nw",
        "ci_lower", "ci_upper", "n_obs", "r_squared", "regime", "significant_5pct",
        "shock_spec", "beta_bn", "ci_lower_bn", "ci_upper_bn",
    ]
    combined = pd.concat([binary_pooled[keep], bill_pooled[keep]], ignore_index=True)
    combined = combined.replace({np.nan: None})
    _write_json(combined.to_dict("records"), site_dir / "data" / "lp_comparison.json")


def build_era_summary(attr_csv: Path, site_dir: Path) -> None:
    attr = pd.read_csv(attr_csv)
    _require_columns(attr, ["baseline_date"], "attribution_baseline.csv")
    attr["baseline_date"] = pd.to_datetime(attr["baseline_date"])
    attr["year"] = attr["baseline_date"].dt.year
    attr["era"] = attr["year"].apply(assign_era)
    out = []
    for era in ERA_ORDER:
        era_df = attr[attr["era"] == era]
        counts: dict[str, int] = {v: 0 for v in SOURCE_MAP.values()}
        for _, row in era_df.iterrows():
            best_src, best_val = None, -1.0
            for col, label in SOURCE_MAP.items():
                val = row.get(col, 0)
                if pd.notna(val) and float(val) > best_val:
                    best_val = float(val)
                    best_src = label
            if best_src:
                counts[best_src] += 1
        for source, count in counts.items():
            out.append({"era": era, "source": source, "count": count})
    _write_json(out, site_dir / "data" / "era_summary.json")


def build_local_projections_json(lp_csv: Path, site_dir: Path) -> None:
    lp = pd.read_csv(lp_csv)
    lp = lp.replace({np.nan: None})
    _write_json(lp.to_dict("records"), site_dir / "data" / "local_projections.json")


def build_summary(
    panel: pd.DataFrame,
    events_csv: Path,
    pooled_lp_csv: Path,
    auction_shock_lp_csv: Path,
    bill_shock_sd: float,
    site_dir: Path,
) -> None:
    events = pd.read_csv(events_csv)
    _require_columns(events, ["event_id"], "event_candidates.csv")
    pooled_lp = pd.read_csv(pooled_lp_csv)
    auction_lp = pd.read_csv(auction_shock_lp_csv)

    binary_pooled = pooled_lp[(pooled_lp["shock_spec"] == "binary") & (pooled_lp["regime"].isna())].copy()
    bill_pooled = auction_lp[(auction_lp["shock_spec"] == "bill_surprise") & (auction_lp["regime"].isna())].copy()

    def _pretrend_counts(df: pd.DataFrame) -> dict[str, int]:
        placebo = df[df["horizon"] < 0]
        return {
            "significant_placebo_count": int(placebo["significant_5pct"].sum()),
            "channels_with_hits": int(
                placebo.groupby("response_var")["significant_5pct"].any().sum()
            ),
        }

    def _headline_effect(df: pd.DataFrame, response_var: str, scale: float) -> dict[str, float | bool]:
        row = df[(df["horizon"] == 4) & (df["response_var"] == response_var)]
        if row.empty:
            raise ValueError(f"Missing h=4 pooled result for {response_var}")
        result = row.iloc[0]
        return {
            "beta_bn": round(float(result["beta"]) * scale / 1000, 2),
            "t_stat_nw": round(float(result["t_stat_nw"]), 2),
            "significant_5pct": bool(result["significant_5pct"]),
        }

    summary = {
        "event_count": int(events["event_id"].nunique()),
        "bill_shock_sd_bn": round(bill_shock_sd / 1000, 2),
        "panel": {
            "weekly_observations": int(len(panel)),
            "variable_count": int(len(panel.columns)),
            "start_date": panel["date"].min().strftime("%Y-%m-%d"),
            "end_date": panel["date"].max().strftime("%Y-%m-%d"),
        },
        "headline_effects": {
            "mmf_treasury_holdings": _headline_effect(
                bill_pooled,
                "mmf_treasury_holdings",
                bill_shock_sd,
            ),
            "on_rrp_daily_total": _headline_effect(
                bill_pooled,
                "on_rrp_daily_total",
                bill_shock_sd,
            ),
        },
        "pretrends": {
            "binary": _pretrend_counts(binary_pooled),
            "bill_surprise": _pretrend_counts(bill_pooled),
        },
    }
    _write_json(summary, site_dir / "data" / "summary.json")


def copy_figures(figures_dir: Path, site_dir: Path) -> None:
    img_dir = site_dir / "img"
    img_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for src in figures_dir.glob("*.png"):
        shutil.copy2(src, img_dir / src.name)
        count += 1
    logger.info("Copied %s figures to site/img/", count)


def main() -> None:
    configure_logging()
    settings = get_settings()
    site_dir = settings.paths.root / "site"

    panel = pd.read_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    auctions = pd.read_parquet(settings.paths.staging / "fiscaldata" / "auctions_query.parquet")
    bill_surprise = build_bill_size_surprise(auctions, grouping="term_reopening")
    panel_merged = panel.merge(bill_surprise, on="date", how="left")
    panel_merged["bill_size_surprise"] = panel_merged["bill_size_surprise"].fillna(0)
    bill_shock_sd = float(panel_merged["bill_size_surprise"].std())

    build_timeline(panel, site_dir)
    build_events(settings.paths.output_tables / "event_candidates.csv", site_dir)
    build_attribution_enriched(settings.paths.output_tables / "attribution_baseline.csv", site_dir)
    build_lp_comparison(
        settings.paths.output_tables / "local_projections.csv",
        settings.paths.output_tables / "auction_shock_lp.csv",
        bill_shock_sd,
        site_dir,
    )
    build_era_summary(settings.paths.output_tables / "attribution_baseline.csv", site_dir)
    build_local_projections_json(settings.paths.output_tables / "local_projections.csv", site_dir)
    build_summary(
        panel=panel,
        events_csv=settings.paths.output_tables / "event_candidates.csv",
        pooled_lp_csv=settings.paths.output_tables / "local_projections.csv",
        auction_shock_lp_csv=settings.paths.output_tables / "auction_shock_lp.csv",
        bill_shock_sd=bill_shock_sd,
        site_dir=site_dir,
    )
    copy_figures(settings.paths.output_figures, site_dir)

    logger.info("Site data export complete")


if __name__ == "__main__":
    main()
