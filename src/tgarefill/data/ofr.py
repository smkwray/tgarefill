from __future__ import annotations

from typing import Any

import pandas as pd
from requests import Session

from tgarefill.utils.http import get_json


def fetch_dataset_payload(
    session: Session,
    base_url: str,
    dataset: str,
    start_date: str | None = None,
) -> Any:
    params: dict[str, str] = {"dataset": dataset}
    if start_date:
        params["start_date"] = start_date
    return get_json(session, f"{base_url.rstrip('/')}/series/dataset", params=params)


def search_metadata(session: Session, base_url: str, query: str) -> Any:
    return get_json(session, f"{base_url.rstrip('/')}/metadata/search", params={"query": query})


def _extract_observations(inner_ts: dict[str, Any]) -> list[Any]:
    for key in ("aggregation", "observations", "data", "values"):
        observations = inner_ts.get(key)
        if isinstance(observations, list):
            return observations
    for value in inner_ts.values():
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for nested in value.values():
                if isinstance(nested, list):
                    return nested
    return []


def dataset_payload_to_long_frame(payload: dict[str, Any]) -> pd.DataFrame:
    """Convert an OFR dataset payload into a long-format DataFrame.

    OFR datasets have structure:
        {timeseries: {SERIES_KEY: {timeseries: {aggregation: [[date, value], ...]}, metadata: {...}}}}
    """
    timeseries = payload.get("timeseries")
    if not isinstance(timeseries, dict):
        return pd.DataFrame()

    dataset_name = payload.get("short_name", "")
    rows: list[dict[str, Any]] = []

    for series_key, series_data in timeseries.items():
        if not isinstance(series_data, dict):
            continue
        inner_ts = series_data.get("timeseries", {})
        observations = _extract_observations(inner_ts)
        if not isinstance(observations, list):
            continue

        meta = series_data.get("metadata", {})
        desc = meta.get("description", {})
        unit = meta.get("unit", {})
        schedule = meta.get("schedule", {})

        for obs in observations:
            if not isinstance(obs, list) or len(obs) < 2:
                continue
            rows.append({
                "date": obs[0],
                "value": obs[1],
                "series_key": series_key,
                "dataset": dataset_name,
                "name": desc.get("name", ""),
                "frequency": schedule.get("observation_frequency", ""),
                "unit": unit.get("name", ""),
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df
