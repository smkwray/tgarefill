from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session(user_agent: str | None = None) -> Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.75,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    ua = user_agent or os.getenv(
        "TGAREFILL_USER_AGENT",
        f"python-requests/{requests.__version__} tgarefill/0.1",
    )
    session.headers.update({"User-Agent": ua, "Accept": "*/*"})
    return session


def default_timeout() -> int:
    return int(os.getenv("TGAREFILL_TIMEOUT", "120"))


def raise_for_status(response: Response) -> None:
    response.raise_for_status()


def get_json(session: Session, url: str, params: dict[str, Any] | None = None) -> Any:
    response = session.get(url, params=params, timeout=default_timeout())
    raise_for_status(response)
    return response.json()


def get_text(session: Session, url: str, params: dict[str, Any] | None = None) -> str:
    response = session.get(url, params=params, timeout=default_timeout())
    raise_for_status(response)
    return response.text


def download_file(session: Session, url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, timeout=default_timeout(), stream=True) as response:
        raise_for_status(response)
        with destination.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return destination
