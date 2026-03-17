from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging

from tgarefill.data.treasury_home import extract_links, safe_download_links
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import ensure_dir, write_json
from tgarefill.utils.http import build_session


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    session = build_session()

    source_cfg = settings.data_sources["treasury_home"]
    raw_dir = ensure_dir(settings.paths.raw / "treasury_home")

    manifest: list[dict[str, object]] = []

    investor_links = extract_links(
        session=session,
        page_url=source_cfg["investor_class_page"],
        suffixes=[".xls", ".xlsx", ".csv", ".txt"],
        include_terms=["bill", "coupon", "historical", "allotment"],
    )
    investor_results = safe_download_links(session, investor_links, raw_dir / "investor_class")
    manifest.append({"source": "investor_class_page", "downloaded": investor_results})

    tic_links = extract_links(
        session=session,
        page_url=source_cfg["tic_page"],
        suffixes=[".csv", ".txt", ".xls", ".xlsx"],
        include_terms=["csv", "txt", "foreign", "holders", "history", "holdings", "country"],
    )
    tic_results = safe_download_links(session, tic_links, raw_dir / "tic")
    manifest.append({"source": "tic_page", "downloaded": tic_results})

    refunding_links = extract_links(
        session=session,
        page_url=source_cfg["refunding_page"],
        suffixes=[".xml", ".pdf", ".xls", ".xlsx"],
        include_terms=["auction", "schedule", "refunding"],
    )
    refunding_results = safe_download_links(session, refunding_links, raw_dir / "refunding")
    manifest.append({"source": "refunding_page", "downloaded": refunding_results})

    write_json(manifest, raw_dir / "manifest.json")
    logger.info("Saved Treasury home/TIC files")


if __name__ == "__main__":
    main()
