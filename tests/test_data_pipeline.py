from __future__ import annotations

from dataclasses import dataclass

from tgarefill.data.fiscaldata import fetch_paginated_endpoint
from tgarefill.data.ofr import dataset_payload_to_long_frame


@dataclass
class _FakeResponse:
    payload: dict

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class _FakeSession:
    def __init__(self, payloads: list[dict]) -> None:
        self.payloads = payloads
        self.calls = 0

    def get(self, url: str, params: dict | None = None, timeout: int | None = None) -> _FakeResponse:
        payload = self.payloads[self.calls]
        self.calls += 1
        return _FakeResponse(payload)


def test_fetch_paginated_endpoint_falls_back_without_total_pages() -> None:
    session = _FakeSession(
        [
            {"data": [{"value": 1}, {"value": 2}]},
            {"data": [{"value": 3}]},
        ]
    )

    payload = fetch_paginated_endpoint(session, "https://example.com", "/endpoint", params={"page[size]": 2})

    assert payload["record_count"] == 3
    assert session.calls == 2


def test_dataset_payload_to_long_frame_accepts_non_aggregation_keys() -> None:
    payload = {
        "short_name": "repo",
        "timeseries": {
            "SERIES_A": {
                "timeseries": {"observations": [["2024-01-01", "100"]]},
                "metadata": {"description": {"name": "Series A"}},
            }
        },
    }

    out = dataset_payload_to_long_frame(payload)

    assert len(out) == 1
    assert out.loc[0, "series_key"] == "SERIES_A"
