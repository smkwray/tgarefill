from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import logging
import subprocess
import sys

from tgarefill.logging_utils import configure_logging


logger = logging.getLogger(__name__)


def run(script_name: str) -> None:
    logger.info("Running %s", script_name)
    subprocess.run([sys.executable, str(ROOT / "scripts" / script_name)], check=True)


def main() -> None:
    configure_logging()
    jobs = [
        ("download_fred.py", True),
        ("download_fiscaldata.py", True),
        ("download_ofr_stfm.py", False),
        ("download_treasury_home.py", False),
    ]
    required_failures: list[str] = []
    for script, required in jobs:
        try:
            run(script)
        except subprocess.CalledProcessError as exc:
            if required:
                required_failures.append(script)
                logger.error("Required downloader failed: %s", script)
            else:
                logger.warning("Optional downloader failed: %s (%s)", script, exc)
    if required_failures:
        raise RuntimeError(
            f"Required download steps failed: {', '.join(required_failures)}"
        )


if __name__ == "__main__":
    main()
