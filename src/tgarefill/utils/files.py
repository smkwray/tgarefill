from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


def ensure_dir(path: Path | str) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def snake_case(value: str) -> str:
    value = re.sub(r"[^0-9a-zA-Z]+", "_", value.strip())
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value.lower()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [snake_case(str(col)) for col in out.columns]
    return out


def write_json(payload: Any, path: Path | str) -> Path:
    path = Path(path)
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def write_dataframe(df: pd.DataFrame, path: Path | str, index: bool = False) -> Path:
    path = Path(path)
    ensure_dir(path.parent)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=index)
    elif path.suffix.lower() == ".csv":
        df.to_csv(path, index=index)
    else:
        raise ValueError(f"Unsupported dataframe output suffix: {path.suffix}")
    return path


def guess_delimiter(path: Path) -> str | None:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(4096)
        return csv.Sniffer().sniff(sample).delimiter
    except Exception:
        return None


def read_text_table(path: Path) -> pd.DataFrame:
    delimiter = guess_delimiter(path)
    if delimiter:
        return normalize_columns(pd.read_csv(path, sep=delimiter))
    return normalize_columns(pd.read_csv(path, sep=None, engine="python"))


def read_excel_best_effort(path: Path) -> dict[str, pd.DataFrame]:
    sheets = pd.read_excel(path, sheet_name=None)
    return {snake_case(name): normalize_columns(df) for name, df in sheets.items()}


def coerce_payload_to_frames(payload: Any) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    if isinstance(payload, list):
        frames["data"] = normalize_columns(pd.json_normalize(payload))
        return frames

    if isinstance(payload, dict):
        for preferred in ("records", "data", "results", "series", "items", "values"):
            value = payload.get(preferred)
            if isinstance(value, list):
                frames[snake_case(preferred)] = normalize_columns(pd.json_normalize(value))
        if frames:
            return frames

        for key, value in payload.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                frames[snake_case(key)] = normalize_columns(pd.json_normalize(value))
    return frames


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


def read_tic_table(path: Path) -> pd.DataFrame | None:
    """Parse a TIC CSV/TXT file with multi-row headers and title junk.

    Strategy: scan lines for the first row that has mostly non-empty fields
    (likely the column header), then read from there.
    """
    try:
        raw_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return None

    if not raw_lines:
        return None

    delimiter = guess_delimiter(path) or ","

    # Find the first plausible header row by looking for text-heavy labels followed
    # by a nearby numeric row, then fall back to the first numeric-looking row.
    data_start = None
    for i, line in enumerate(raw_lines):
        fields = line.replace('"', '').split(delimiter)
        non_empty = [f.strip() for f in fields if f.strip()]
        if len(non_empty) < 3:
            continue
        text_like = sum(1 for f in non_empty if any(ch.isalpha() for ch in f))
        if text_like >= 2:
            for next_line in raw_lines[i + 1:i + 4]:
                next_fields = [f.strip() for f in next_line.replace('"', '').split(delimiter) if f.strip()]
                numeric_count = sum(1 for f in next_fields if _looks_numeric(f))
                if len(next_fields) >= 3 and numeric_count >= 2:
                    data_start = i
                    break
        if data_start is not None:
            break

        numeric_count = sum(1 for f in non_empty if _looks_numeric(f))
        if numeric_count >= 2:
            data_start = max(i - 1, 0)
            break

    if data_start is None:
        return None

    try:
        df = pd.read_csv(
            path,
            skiprows=data_start,
            sep=delimiter,
            encoding="utf-8",
            encoding_errors="ignore",
            on_bad_lines="skip",
            dtype=str,
        )
        df = normalize_columns(df)
        # Drop fully empty rows/columns
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty:
            return None
        return df
    except Exception:
        return None


def _looks_numeric(s: str) -> bool:
    """Check if a string looks like a number (with optional commas/decimals)."""
    cleaned = s.strip().replace(",", "").replace("$", "").replace("%", "")
    if not cleaned:
        return False
    try:
        float(cleaned)
        return True
    except ValueError:
        return False
