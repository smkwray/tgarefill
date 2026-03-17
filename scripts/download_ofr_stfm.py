from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging

from tgarefill.data.ofr import fetch_dataset_payload, search_metadata
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import ensure_dir, write_json
from tgarefill.utils.http import build_session


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    session = build_session()

    source_cfg = settings.data_sources["ofr"]
    base_url = source_cfg["base_url"]

    raw_dir = ensure_dir(settings.paths.raw / "ofr")
    dataset_dir = ensure_dir(raw_dir / "datasets")
    metadata_dir = ensure_dir(raw_dir / "metadata")

    manifest: list[dict[str, object]] = []

    for dataset in source_cfg.get("datasets", []):
        logger.info("Downloading OFR dataset %s", dataset)
        payload = fetch_dataset_payload(
            session=session,
            base_url=base_url,
            dataset=dataset,
            start_date="2000-01-01",
        )
        destination = dataset_dir / f"{dataset}.json"
        write_json(payload, destination)
        manifest.append(
            {
                "kind": "dataset",
                "dataset": dataset,
                "saved_to": str(destination),
            }
        )

    for dataset, meta in settings.stfm_queries.get("datasets", {}).items():
        for query in meta.get("search_terms", []):
            logger.info("Searching OFR metadata for dataset=%s query=%s", dataset, query)
            payload = search_metadata(session=session, base_url=base_url, query=query)
            safe_query = "".join(ch if ch.isalnum() else "_" for ch in query.lower()).strip("_")
            destination = metadata_dir / f"{dataset}__{safe_query}.json"
            write_json(payload, destination)
            manifest.append(
                {
                    "kind": "metadata_search",
                    "dataset": dataset,
                    "query": query,
                    "saved_to": str(destination),
                }
            )

    write_json(manifest, raw_dir / "manifest.json")
    logger.info("Saved %s OFR payloads", len(manifest))


if __name__ == "__main__":
    main()
