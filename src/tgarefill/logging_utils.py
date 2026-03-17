from __future__ import annotations

import logging
import os


def configure_logging(level: str | None = None) -> None:
    resolved = level or os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, resolved.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
