from __future__ import annotations

import math
from typing import Any
from urllib.parse import urljoin

import pandas as pd
from requests import Session

from tgarefill.utils.files import normalize_columns
from tgarefill.utils.http import default_timeout


def _infer_total_pages(payload: dict[str, Any]) -> int | None:
    meta = payload.get("meta") or {}
    candidates = [
        meta.get("total-pages"),
        meta.get("total_pages"),
        meta.get("pages"),
        meta.get("page-count"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def fetch_paginated_endpoint(
    session: Session,
    base_url: str,
    endpoint_path: str,
    params: dict[str, Any] | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    url = urljoin(base_url.rstrip("/") + "/", endpoint_path.lstrip("/"))
    params = dict(params or {})
    params.setdefault("format", "json")
    params.setdefault("page[size]", 10000)
    page_size = int(params["page[size]"])

    records: list[dict[str, Any]] = []
    page = 1
    first_payload: dict[str, Any] | None = None

    while True:
        page_params = {**params, "page[number]": page}
        response = session.get(url, params=page_params, timeout=default_timeout())
        response.raise_for_status()
        payload = response.json()

        if first_payload is None:
            first_payload = {
                "meta": payload.get("meta"),
                "links": payload.get("links"),
            }

        batch = payload.get("data") or []
        if isinstance(batch, list):
            records.extend(batch)

        total_pages = _infer_total_pages(payload)
        links = payload.get("links") or {}
        has_next = bool(links.get("next"))

        if max_pages is not None and page >= max_pages:
            break

        if not batch:
            break

        if has_next:
            page += 1
            continue

        if total_pages is not None and page < total_pages:
            page += 1
            continue

        if total_pages is None and len(batch) >= page_size:
            page += 1
            continue

        break

    return {
        "endpoint": endpoint_path,
        "params": params,
        "records": records,
        "sample": first_payload or {},
        "record_count": len(records),
    }


def payload_to_frame(payload: dict[str, Any]) -> pd.DataFrame:
    records = payload.get("records") or payload.get("data") or []
    if not isinstance(records, list):
        return pd.DataFrame()
    return normalize_columns(pd.json_normalize(records))
