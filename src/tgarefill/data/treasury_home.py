from __future__ import annotations

from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from requests import Session

from tgarefill.utils.files import snake_case
from tgarefill.utils.http import download_file, get_text


def extract_links(
    session: Session,
    page_url: str,
    suffixes: Iterable[str],
    include_terms: Iterable[str] | None = None,
) -> list[dict[str, str]]:
    suffixes = tuple(s.lower() for s in suffixes)
    include_terms_lower = [term.lower() for term in (include_terms or [])]

    html = get_text(session, page_url)
    soup = BeautifulSoup(html, "lxml")

    found: list[dict[str, str]] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(page_url, href)
        text = " ".join(a.get_text(" ", strip=True).split())
        combined = f"{text} {full_url}".lower()

        if not full_url.lower().endswith(suffixes):
            continue
        if include_terms_lower and not any(term in combined for term in include_terms_lower):
            continue
        if full_url in seen:
            continue
        seen.add(full_url)
        found.append(
            {
                "text": text,
                "url": full_url,
                "filename": Path(urlparse(full_url).path).name,
            }
        )
    return found


def safe_download_links(
    session: Session,
    links: list[dict[str, str]],
    destination_dir: Path,
) -> list[dict[str, str]]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, str]] = []
    for item in links:
        filename = item["filename"] or f"{snake_case(item['text'])}.bin"
        destination = destination_dir / filename
        download_file(session, item["url"], destination)
        results.append({**item, "saved_to": str(destination)})
    return results
