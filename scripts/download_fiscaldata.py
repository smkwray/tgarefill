from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging

from tgarefill.data.fiscaldata import fetch_paginated_endpoint
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import ensure_dir, write_json
from tgarefill.utils.http import build_session


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    session = build_session()

    source_cfg = settings.data_sources["fiscaldata"]
    base_url = source_cfg["base_url"]
    raw_dir = ensure_dir(settings.paths.raw / "fiscaldata")
    manifest: list[dict[str, object]] = []

    for key, meta in source_cfg.get("endpoints", {}).items():
        logger.info("Downloading FiscalData endpoint %s", key)
        payload = fetch_paginated_endpoint(
            session=session,
            base_url=base_url,
            endpoint_path=meta["path"],
        )
        destination = raw_dir / f"{key}.json"
        write_json(payload, destination)
        manifest.append(
            {
                "key": key,
                "path": meta["path"],
                "frequency": meta.get("frequency"),
                "description": meta.get("description"),
                "record_count": payload.get("record_count", 0),
                "saved_to": str(destination),
            }
        )

    write_json(manifest, raw_dir / "manifest.json")
    logger.info("Saved %s FiscalData payloads", len(manifest))


if __name__ == "__main__":
    main()
