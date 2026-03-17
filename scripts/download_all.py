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
    for script in [
        "download_fred.py",
        "download_fiscaldata.py",
        "download_ofr_stfm.py",
        "download_treasury_home.py",
    ]:
        run(script)


if __name__ == "__main__":
    main()
