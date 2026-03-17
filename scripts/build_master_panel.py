from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging

from tgarefill.analytics.panel import (
    build_dts_wednesday_tga,
    build_monthly_panel_from_weekly,
    build_ofr_monthly_columns,
    build_ofr_weekly_columns,
    build_weekly_auction_mix,
    build_weekly_panel_from_fred,
    load_staged_fiscal_table,
    load_staged_fred_long,
    merge_weekly_panel,
)
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import write_dataframe


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()

    fred_long = load_staged_fred_long(settings.paths.staging)
    weekly_fred = build_weekly_panel_from_fred(fred_long, settings.fred_series)

    # Replace WTREGEN (week-average) with DTS Wednesday-close TGA
    dts_tga = build_dts_wednesday_tga(settings.paths.staging)
    if not dts_tga.empty:
        # Keep WTREGEN as tga_weekly_average for reference
        if "tga_weekly_wednesday" in weekly_fred.columns:
            weekly_fred = weekly_fred.rename(columns={"tga_weekly_wednesday": "tga_weekly_average"})
        # Merge DTS Wednesday TGA
        weekly_fred = weekly_fred.merge(dts_tga, on="date", how="left")
        # Use DTS as the canonical TGA column
        weekly_fred["tga_weekly_wednesday"] = weekly_fred["tga_dts_wednesday"]
        logger.info("Replaced WTREGEN (week-average) with DTS Wednesday-close TGA (%s obs)", len(dts_tga))
    else:
        logger.warning("DTS Wednesday TGA not available; falling back to WTREGEN (week-average)")

    # Restrict panel to TGA era (2005+) to avoid pre-TGA junk rows
    tga_start = weekly_fred.loc[weekly_fred["tga_weekly_wednesday"].notna(), "date"].min()
    if tga_start is not None:
        weekly_fred = weekly_fred[weekly_fred["date"] >= tga_start].reset_index(drop=True)
        logger.info("Panel restricted to TGA era starting %s", tga_start.date())

    auctions = load_staged_fiscal_table(settings.paths.staging, "auctions_query")
    weekly_auction_mix = build_weekly_auction_mix(auctions) if auctions is not None else None

    weekly_ofr = build_ofr_weekly_columns(settings.paths.staging)
    if weekly_ofr is not None:
        logger.info("Added OFR weekly columns: %s", [c for c in weekly_ofr.columns if c != "date"])

    monthly_ofr = build_ofr_monthly_columns(settings.paths.staging)
    if monthly_ofr is not None:
        logger.info("Added OFR monthly columns: %s", [c for c in monthly_ofr.columns if c != "date"])

    weekly_panel = merge_weekly_panel(weekly_fred, weekly_auction_mix, weekly_ofr, monthly_ofr)
    write_dataframe(weekly_panel, settings.paths.processed / "master_weekly_panel.parquet")
    write_dataframe(weekly_panel, settings.paths.output_tables / "master_weekly_panel.csv")

    # Monthly panel: don't re-merge monthly_ofr (already in weekly via merge_asof)
    monthly_panel = build_monthly_panel_from_weekly(weekly_panel)
    write_dataframe(monthly_panel, settings.paths.processed / "master_monthly_panel.parquet")
    write_dataframe(monthly_panel, settings.paths.output_tables / "master_monthly_panel.csv")

    logger.info("Built weekly panel with %s rows, %s cols", len(weekly_panel), len(weekly_panel.columns))
    logger.info("Built monthly panel with %s rows, %s cols", len(monthly_panel), len(monthly_panel.columns))


if __name__ == "__main__":
    main()
