from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import json
import logging

from tgarefill.data.fred import download_series_csv
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import ensure_dir, write_json
from tgarefill.utils.http import build_session


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    session = build_session()

    raw_dir = ensure_dir(settings.paths.raw / "fred")
    manifest: list[dict[str, str | bool]] = []
    required_failures: list[str] = []

    for series_key, meta in settings.fred_series.get("series", {}).items():
        series_id = meta["id"]
        filename = f"{series_key}__{series_id}.csv"
        destination = raw_dir / filename
        required = bool(meta.get("required_for_mvp", False))
        logger.info("Downloading FRED series %s (%s)", series_key, series_id)
        try:
            download_series_csv(session, series_id, destination)
            manifest.append(
                {
                    "series_key": series_key,
                    "series_id": series_id,
                    "frequency": str(meta.get("frequency", "")),
                    "description": str(meta.get("description", "")),
                    "required_for_mvp": required,
                    "saved_to": str(destination),
                    "status": "downloaded",
                }
            )
        except Exception as exc:
            manifest.append(
                {
                    "series_key": series_key,
                    "series_id": series_id,
                    "frequency": str(meta.get("frequency", "")),
                    "description": str(meta.get("description", "")),
                    "required_for_mvp": required,
                    "saved_to": "",
                    "status": f"failed: {exc}",
                }
            )
            if required:
                required_failures.append(series_key)
                logger.error("Required FRED series failed: %s", series_key)
            else:
                logger.warning("Optional FRED series failed: %s (%s)", series_key, exc)

    write_json(manifest, raw_dir / "manifest.json")
    logger.info("Processed %s FRED series", len(manifest))
    if required_failures:
        raise RuntimeError(f"Required FRED downloads failed: {', '.join(required_failures)}")


if __name__ == "__main__":
    main()
