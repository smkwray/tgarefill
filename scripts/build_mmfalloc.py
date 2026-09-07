from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from tgarefill.logging_utils import configure_logging
from tgarefill.mmfalloc import (
    DEFAULT_END_MONTH,
    DEFAULT_START_MONTH,
    discover_sec_nmfp_links,
    download_sec_zips,
    month_range,
    parse_sec_nmfp_zips,
    select_monthly_sec_links,
    write_all_outputs,
)
from tgarefill.settings import get_settings
from tgarefill.utils.http import build_session


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build MMF fund-level allocation validation outputs.")
    parser.add_argument("--start-month", default=os.getenv("MMFALLOC_START_MONTH", DEFAULT_START_MONTH))
    parser.add_argument("--end-month", default=os.getenv("MMFALLOC_END_MONTH", DEFAULT_END_MONTH))
    parser.add_argument("--overwrite-raw", action="store_true", help="Re-download SEC zip files even if present.")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Use already-downloaded SEC zips in data/raw/sec_nmfp.",
    )
    return parser.parse_args()


def _load_optional_parquet(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        logger.warning("Optional input missing: %s", path)
        return None
    return pd.read_parquet(path)


def main() -> None:
    configure_logging()
    args = parse_args()
    settings = get_settings()
    session = build_session(
        os.getenv("TGAREFILL_SEC_USER_AGENT", "Shane Wray shanewray@example.com tgarefill/0.1")
    )

    raw_dir = settings.paths.raw / "sec_nmfp"
    months = month_range(args.start_month, args.end_month)
    if not args.skip_download:
        links = discover_sec_nmfp_links(session)
        selected = select_monthly_sec_links(links, months)
        manifest = download_sec_zips(session, selected, raw_dir, overwrite=args.overwrite_raw)
        logger.info("SEC N-MFP raw manifest: %s monthly zips", len(manifest))

    zip_paths = sorted(raw_dir.glob("*_nmfp.zip"))
    wanted_names = set()
    if not args.skip_download:
        wanted_names = {Path(url).name for url in selected["url"]}
        zip_paths = [path for path in zip_paths if path.name in wanted_names]
    if not zip_paths:
        raise RuntimeError(f"No SEC N-MFP zips found in {raw_dir}")

    logger.info("Parsing %s SEC N-MFP monthly zips", len(zip_paths))
    positions, fund_month = parse_sec_nmfp_zips(zip_paths)
    logger.info(
        "Parsed %s positions and %s fund-month rows",
        len(positions),
        len(fund_month),
    )

    weekly_panel = _load_optional_parquet(settings.paths.processed / "master_weekly_panel.parquet")
    shocks = _load_optional_parquet(settings.paths.processed / "canonical_bill_surprise_shocks.parquet")
    if shocks is None:
        shocks = pd.DataFrame(columns=["date", "bill_size_surprise"])

    results = write_all_outputs(
        positions=positions,
        fund_month=fund_month,
        processed_dir=settings.paths.processed / "mmfalloc",
        output_tables_dir=settings.paths.output_tables,
        output_figures_dir=settings.paths.output_figures,
        output_reports_dir=settings.paths.outputs / "reports",
        weekly_panel=weekly_panel,
        shocks=shocks,
        source_months=(args.start_month, args.end_month),
    )

    gates = results["gates"]
    failed = gates[~gates["passed"]]
    if not failed.empty:
        logger.warning("MMF allocation source gates failed:\n%s", failed.to_string(index=False))
    else:
        logger.info("MMF allocation source gates passed")


if __name__ == "__main__":
    main()
