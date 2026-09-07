from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging
import hashlib
import os
import shutil
import uuid

from tgarefill.data.fiscaldata import fetch_paginated_endpoint
from tgarefill.data.raw_store import RawPayloadStore
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
    run_id = uuid.uuid4().hex
    snapshots = ensure_dir(raw_dir / "snapshots")
    temporary = ensure_dir(snapshots / f".{run_id}")
    store = RawPayloadStore(temporary / "fiscaldata.sqlite")
    manifest: list[dict[str, object]] = []
    required_failures: list[str] = []

    for key, meta in source_cfg.get("endpoints", {}).items():
        logger.info("Downloading FiscalData endpoint %s", key)
        required = bool(meta.get("required_for_mvp", False))
        try:
            payload = fetch_paginated_endpoint(
                session=session,
                base_url=base_url,
                endpoint_path=meta["path"],
            )
            store.put_json(key, payload, source=meta["path"])
            manifest.append(
                {
                    "key": key,
                    "path": meta["path"],
                    "frequency": meta.get("frequency"),
                    "description": meta.get("description"),
                    "required_for_mvp": required,
                    "record_count": payload.get("record_count", 0),
                    "saved_to": f"{store.path.name}#{key}",
                    "status": "downloaded",
                    "sha256": hashlib.sha256(store.get_bytes(key)).hexdigest(),
                }
            )
        except Exception as exc:
            manifest.append(
                {
                    "key": key,
                    "path": meta["path"],
                    "frequency": meta.get("frequency"),
                    "description": meta.get("description"),
                    "required_for_mvp": required,
                    "record_count": 0,
                    "saved_to": "",
                    "status": f"failed: {exc}",
                }
            )
            if required:
                required_failures.append(key)
                logger.error("Required FiscalData endpoint failed: %s", key)
            else:
                logger.warning("Optional FiscalData endpoint failed: %s (%s)", key, exc)

    store.compact()
    logger.info("Saved %s FiscalData payloads", len(manifest))
    if required_failures:
        shutil.rmtree(temporary)
        raise RuntimeError(
            f"Required FiscalData downloads failed: {', '.join(required_failures)}"
        )

    write_json({"run_id": run_id, "payload_kind": "normalized_paginated_payload",
                "store_sha256": hashlib.sha256(store.path.read_bytes()).hexdigest(),
                "endpoints": manifest}, temporary / "manifest.json")
    manifest_hash = hashlib.sha256((temporary / "manifest.json").read_bytes()).hexdigest()
    os.replace(temporary, snapshots / run_id)
    pointer = raw_dir / f".current-{run_id}.json"
    write_json({"run_id": run_id, "manifest_sha256": manifest_hash}, pointer)
    os.replace(pointer, raw_dir / "current.json")


if __name__ == "__main__":
    main()
