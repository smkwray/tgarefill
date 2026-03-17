from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging

import pandas as pd

from tgarefill.analytics.events import attach_manual_labels, detect_rebuild_events
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import write_dataframe


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()

    weekly_panel = pd.read_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    events = detect_rebuild_events(
        weekly_df=weekly_panel,
        tga_col="tga_weekly_wednesday",
        bill_share_col="bill_share",
        rules=settings.episode_rules.get("weekly", {}),
    )

    manual_path = settings.paths.configs / "manual_event_labels_template.csv"
    manual_labels = pd.read_csv(manual_path) if manual_path.exists() else None
    events = attach_manual_labels(events, manual_labels)

    write_dataframe(events, settings.paths.processed / "event_candidates.parquet")
    write_dataframe(events, settings.paths.output_tables / "event_candidates.csv")
    logger.info("Detected %s candidate events", len(events))


if __name__ == "__main__":
    main()
