"""Harvest FACEM pages into data/raw/html/ (the only step that uses the network).

Run it with its own verb, it is never part of the default pipeline:

    python main.py harvest

Every page is stored exactly as the server sent it. Parsing happens offline in
step_parse.py, so a parser fix never needs a new crawl.
"""

# =============================================================================
#  CONFIG - everything you may want to change is in this block
# =============================================================================

# Fabric pages to fetch. Start with the African Red Slip Ware fabrics of
# release 10 (November 2026); extend the list or use SEARCH_URL for more.
FABRIC_IDS = [
    "bd-ars-1", "bd-ars-2", "bd-ars-3",      # Bordj el Djerbi
    "hb-ars-1", "hb-ars-2", "hb-ars-3",      # Henchir el Biar
    "ou-ars-1", "ou-ars-2",                  # Oudna
    "ph-ars-1", "ph-ars-2", "ph-ars-3", "ph-ars-4",   # Pheradi Maius
]

# Optional: paste page 1 of a FACEM search-results URL and every fabric found
# by the crawl is added to FABRIC_IDS. Leave as None to use FABRIC_IDS only.
SEARCH_URL = None
# SEARCH_URL = "https://facem.at/search/results.php?c=29&page=1"

# Also fetch the other samples of each fabric (the "Other samples assigned to ..."
# list on a fabric page, e.g. m-201-2). Each sample has its own page with its own
# object data. Decided 2026-11-10.
FOLLOW_SAMPLE_PAGES = True

# True = fetch again even if the file is already on disk.
FORCE = False

# Seconds between requests. Be polite to the server.
DELAY = 1.5

# =============================================================================

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests
from bs4 import BeautifulSoup

from facem_utils import (
    BASE_URL, RAW_HTML, HARVEST_MANIFEST,
    ensure_dirs, write_json, read_json, sha256_bytes,
)

HEADERS = {
    "User-Agent": "facem2lod-harvest/0.3 (research; "
                  "https://github.com/Research-Squirrel-Engineers/facem2lod)"
}

# Slugs look like ou-ars-1 or bnap-a-11; sample pages like m-160-1 also match.
RECORD_URL_RE = re.compile(r"^https://facem\.at/[a-z0-9]{1,8}-[a-z0-9]{1,6}-\d+/?$")


def fetch(session: requests.Session, url: str):
    """Return (status_code, bytes) or (None, error message)."""
    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException as exc:
        return None, str(exc)
    return resp.status_code, resp.content


def crawl_search_results(session: requests.Session, search_url: str) -> list[str]:
    """Collect fabric ids from a search-results URL, following the pagination."""
    ids: list[str] = []
    page = 1
    while True:
        base = re.sub(r"[&?]page=\d+", "", search_url).rstrip("?&")
        paged = f"{base}{'&' if '?' in base else '?'}page={page}"
        status, body = fetch(session, paged)
        if status != 200:
            print(f"WARN search page {page}: {status} {body if status is None else ''}")
            break
        html = body.decode("utf-8", errors="replace")
        found = []
        for href in re.findall(r"""href\s*=\s*["']([^"']+)["']""", html):
            full = urljoin(paged, href)
            if RECORD_URL_RE.match(full):
                slug = urlparse(full).path.strip("/").lower()
                if slug not in ids and slug not in found:
                    found.append(slug)
        print(f"  search page {page}: {len(found)} new ids")
        if not found:
            break
        ids.extend(found)
        page += 1
        time.sleep(DELAY)
    return ids


def sample_ids(html: str, base_url: str) -> list[str]:
    """Slugs of the further samples listed on a fabric page."""
    soup = BeautifulSoup(html, "html.parser")
    found = []
    for a in soup.find_all("a", class_="sample", href=True):
        slug = urlparse(urljoin(base_url, a["href"])).path.strip("/").lower()
        if re.fullmatch(r"[a-z0-9]{1,8}-[a-z0-9]{1,6}-\d+", slug) and slug not in found:
            found.append(slug)
    return found


def get_page(session, slug: str, manifest: dict, counts: dict):
    """Store one page unchanged. Returns its bytes, or None if it failed."""
    target = RAW_HTML / f"{slug}.html"
    url = f"{BASE_URL}/{slug}"
    if target.exists() and not FORCE:
        counts["kept"] += 1
        print(f"  kept     {slug}")
        return target.read_bytes()
    status, body = fetch(session, url)
    time.sleep(DELAY)
    if status != 200:
        counts["failed"] += 1
        print(f"WARN {slug}: {status if status else 'no response'} "
              f"{body if status is None else ''}")
        manifest[f"html/{slug}.html"] = {"url": url, "status": status}
        return None
    target.write_bytes(body)
    manifest[f"html/{slug}.html"] = {
        "url": url, "status": status,
        "sha256": sha256_bytes(body), "bytes": len(body),
    }
    counts["fetched"] += 1
    print(f"  fetched  {slug}  ({len(body)} bytes)")
    return body


def run() -> int:
    ensure_dirs()
    manifest = read_json(HARVEST_MANIFEST) if HARVEST_MANIFEST.exists() else {}
    counts = {"fetched": 0, "kept": 0, "failed": 0}

    with requests.Session() as session:
        session.headers.update(HEADERS)

        ids = [i.lower() for i in FABRIC_IDS]
        if SEARCH_URL:
            print(f"Crawling search results: {SEARCH_URL}")
            for i in crawl_search_results(session, SEARCH_URL):
                if i not in ids:
                    ids.append(i)
        print(f"{len(ids)} fabric pages to harvest")

        samples: list[str] = []
        for fid in ids:
            body = get_page(session, fid, manifest, counts)
            if body is not None and FOLLOW_SAMPLE_PAGES:
                for sid in sample_ids(body.decode("utf-8", errors="replace"),
                                      f"{BASE_URL}/{fid}"):
                    if sid not in ids and sid not in samples:
                        samples.append(sid)

        if FOLLOW_SAMPLE_PAGES:
            print(f"{len(samples)} sample pages to harvest")
            for sid in samples:
                get_page(session, sid, manifest, counts)

    write_json(HARVEST_MANIFEST, manifest)
    print(f"harvest: {counts['fetched']} fetched, {counts['kept']} kept, "
          f"{counts['failed']} failed")
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(run())
