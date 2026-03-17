from __future__ import annotations

from pathlib import Path

import pandas as pd
from requests import Session

from tgarefill.utils.files import ensure_dir, normalize_columns
from tgarefill.utils.http import default_timeout


def fred_csv_url(series_id: str) -> str:
    return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"


def download_series_csv(session: Session, series_id: str, destination: Path) -> Path:
    """Download a single FRED CSV (small file, no streaming needed)."""
    ensure_dir(destination.parent)
    response = session.get(fred_csv_url(series_id), timeout=default_timeout())
    response.raise_for_status()
    destination.write_text(response.text, encoding="utf-8")
    return destination


def load_series_csv(path: Path, series_key: str, series_id: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = normalize_columns(df)

    date_col = "date" if "date" in df.columns else df.columns[0]
    value_col = series_id.lower() if series_id.lower() in df.columns else df.columns[1]

    out = df.rename(columns={date_col: "date", value_col: "value"}).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out["series_key"] = series_key
    out["series_id"] = series_id
    return out[["date", "value", "series_key", "series_id"]].dropna(subset=["date"])
