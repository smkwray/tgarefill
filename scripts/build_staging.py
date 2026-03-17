from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import json
import logging
from pathlib import Path

import pandas as pd

from tgarefill.data.fiscaldata import payload_to_frame
from tgarefill.data.fred import load_series_csv
from tgarefill.data.ofr import dataset_payload_to_long_frame
from tgarefill.logging_utils import configure_logging
from tgarefill.settings import get_settings
from tgarefill.utils.files import (
    coerce_payload_to_frames,
    ensure_dir,
    normalize_columns,
    read_excel_best_effort,
    read_text_table,
    read_tic_table,
    snake_case,
    write_dataframe,
    write_json,
)


logger = logging.getLogger(__name__)


def stage_fred(settings) -> list[dict[str, str]]:
    raw_dir = settings.paths.raw / "fred"
    staging_dir = ensure_dir(settings.paths.staging / "fred")
    manifest: list[dict[str, str]] = []
    frames: list[pd.DataFrame] = []

    for series_key, meta in settings.fred_series.get("series", {}).items():
        series_id = meta["id"]
        raw_path = raw_dir / f"{series_key}__{series_id}.csv"
        if not raw_path.exists():
            logger.warning("Missing FRED raw file: %s", raw_path)
            continue
        df = load_series_csv(raw_path, series_key=series_key, series_id=series_id)
        frames.append(df)
        manifest.append({"series_key": series_key, "series_id": series_id, "rows": str(len(df))})

    fred_long = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["date", "value", "series_key", "series_id"]
    )
    write_dataframe(fred_long, staging_dir / "fred_long.parquet")
    if not fred_long.empty:
        fred_wide = fred_long.pivot_table(
            index="date", columns="series_key", values="value", aggfunc="last"
        ).reset_index()
    else:
        fred_wide = pd.DataFrame(columns=["date"])
    write_dataframe(fred_wide, staging_dir / "fred_wide.parquet")
    write_json(manifest, staging_dir / "manifest.json")
    return manifest


def stage_fiscaldata(settings) -> list[dict[str, str]]:
    raw_dir = settings.paths.raw / "fiscaldata"
    staging_dir = ensure_dir(settings.paths.staging / "fiscaldata")
    manifest: list[dict[str, str]] = []

    for path in sorted(raw_dir.glob("*.json")):
        if path.name == "manifest.json":
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        df = payload_to_frame(payload)
        if df.empty:
            logger.warning("No records parsed for FiscalData payload %s", path.name)
            continue
        write_dataframe(df, staging_dir / f"{path.stem}.parquet")
        manifest.append({"source_file": path.name, "rows": str(len(df))})

    write_json(manifest, staging_dir / "manifest.json")
    return manifest


def stage_ofr(settings) -> list[dict[str, str]]:
    raw_root = settings.paths.raw / "ofr"
    staging_dir = ensure_dir(settings.paths.staging / "ofr")
    manifest: list[dict[str, str]] = []

    # Stage dataset files (timeseries format)
    dataset_dir = raw_root / "datasets"
    if dataset_dir.exists():
        for path in sorted(dataset_dir.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            df = dataset_payload_to_long_frame(payload)
            if df.empty:
                logger.warning("No timeseries extracted from OFR dataset: %s", path.name)
                continue
            out_name = f"{path.stem}_long.parquet"
            write_dataframe(df, staging_dir / out_name)
            n_series = df["series_key"].nunique() if "series_key" in df.columns else 0
            manifest.append({
                "source_file": path.name,
                "kind": "dataset",
                "rows": str(len(df)),
                "series_count": str(n_series),
            })
            logger.info("Staged OFR dataset %s: %s rows, %s series", path.stem, len(df), n_series)

    # Stage metadata search results (list-of-dicts format)
    metadata_dir = raw_root / "metadata"
    if metadata_dir.exists():
        for path in sorted(metadata_dir.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            frames = coerce_payload_to_frames(payload)
            if not frames:
                continue
            for frame_name, df in frames.items():
                out_name = f"{path.stem}__{frame_name}.parquet"
                write_dataframe(df, staging_dir / out_name)
                manifest.append({"source_file": path.name, "kind": "metadata", "frame": frame_name, "rows": str(len(df))})

    write_json(manifest, staging_dir / "manifest.json")
    return manifest


def stage_treasury_home(settings) -> list[dict[str, str]]:
    raw_root = settings.paths.raw / "treasury_home"
    staging_root = ensure_dir(settings.paths.staging / "treasury_home")
    manifest: list[dict[str, str]] = []

    # investor-class excel files
    investor_dir = raw_root / "investor_class"
    if investor_dir.exists():
        out_dir = ensure_dir(staging_root / "investor_class")
        for path in sorted(investor_dir.glob("*")):
            if path.suffix.lower() not in {".xls", ".xlsx"}:
                continue
            try:
                sheets = read_excel_best_effort(path)
            except Exception as exc:
                logger.warning("Failed to parse investor-class workbook %s: %s", path.name, exc)
                continue
            for sheet_name, df in sheets.items():
                # Coerce object columns to string to prevent mixed-type parquet errors
                for col in df.select_dtypes(include=["object"]).columns:
                    df[col] = df[col].astype(str).replace("nan", pd.NA).replace("None", pd.NA).replace("NaT", pd.NA)
                out_name = f"{snake_case(path.stem)}__{sheet_name}.parquet"
                try:
                    write_dataframe(df, out_dir / out_name)
                except Exception as exc:
                    logger.warning("Failed to write parquet for %s/%s: %s", path.name, sheet_name, exc)
                    continue
                manifest.append({"source_file": path.name, "sheet": sheet_name, "rows": str(len(df))})

    # tic flat files
    tic_dir = raw_root / "tic"
    if tic_dir.exists():
        out_dir = ensure_dir(staging_root / "tic")
        for path in sorted(tic_dir.glob("*")):
            if path.suffix.lower() not in {".csv", ".txt"}:
                continue
            df = None
            try:
                df = read_text_table(path)
            except Exception:
                pass
            if df is None or df.empty:
                df = read_tic_table(path)
            if df is None or df.empty:
                logger.warning("Could not parse TIC file %s with any method", path.name)
                continue
            # Coerce object columns to string for safe parquet write
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).replace({"nan": pd.NA, "None": pd.NA, "NaT": pd.NA})
            out_name = f"{snake_case(path.stem)}.parquet"
            try:
                write_dataframe(df, out_dir / out_name)
            except Exception as exc:
                logger.warning("Failed to write TIC parquet %s: %s", path.name, exc)
                continue
            manifest.append({"source_file": path.name, "rows": str(len(df))})

    write_json(manifest, staging_root / "manifest.json")
    return manifest


def main() -> None:
    configure_logging()
    settings = get_settings()
    stage_fred(settings)
    stage_fiscaldata(settings)
    stage_ofr(settings)
    stage_treasury_home(settings)
    logger.info("Staging complete")


if __name__ == "__main__":
    main()
