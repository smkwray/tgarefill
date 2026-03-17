from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging

import pandas as pd

from tgarefill.analytics.attribution import build_baseline_attribution
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import write_dataframe


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()

    weekly_panel = pd.read_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    events = pd.read_parquet(settings.paths.processed / "event_candidates.parquet")
    attribution_cfg = settings.analysis.get("attribution", {})

    attribution = build_baseline_attribution(
        events_df=events,
        weekly_panel=weekly_panel,
        tga_col="tga_weekly_wednesday",
        proxy_map=attribution_cfg.get("proxies"),
        max_lookback_days=attribution_cfg.get("max_lookback_days", 14),
    )

    write_dataframe(attribution, settings.paths.processed / "attribution_baseline.parquet")
    write_dataframe(attribution, settings.paths.output_tables / "attribution_baseline.csv")
    logger.info("Wrote %s attribution rows", len(attribution))


if __name__ == "__main__":
    main()
