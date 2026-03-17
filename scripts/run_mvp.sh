#!/usr/bin/env bash
set -euo pipefail

python scripts/download_all.py
python scripts/build_staging.py
python scripts/build_master_panel.py
python scripts/build_event_candidates.py
python scripts/build_attribution_baseline.py
