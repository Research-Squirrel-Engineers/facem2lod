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

# Also fetch the map pages (production sites, discovery sites) that the fabric
# pages link to. They are the source for place names and coordinates.
FOLLOW_SITE_PAGES = True

# True = fetch again even if the file is already on disk.
FORCE = False

# Seconds between requests. Be polite to the server.
DELAY = 1.5

# =============================================================================

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests

from facem_utils import (
    BASE_URL, RAW_HTML, RAW_SITES, HARVEST_MANIFEST,
    ensure_dirs, write_json, read_json, sha256_bytes,
)

HEADERS = {
    "User-Agent": "facem2lod-harvest/0.3 (research; "
                  "https://github.com/Research-Squirrel-Engineers/facem2lod)"
}

# Slugs look like ou-ars-1 or bnap-a-11; sample pages like m-160-1 also match.
RECORD_URL_RE = re.compile(r"^https://facem\.at/[a-z0-9]{1,8}-[a-z0-9]{1,6}-\d+/?$")
SITE_HREF_RE = re.compile(
    r"""href\s*=\s*["']([^"']*map/[A-Za-z_]+\.php\?[^"']*id=\d+[^"']*)["']""",
    re.IGNORECASE,
)


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


def site_file_name(url: str) -> str | None:
    """map/production_site.php?id=12 -> production_site_12.html"""
    parts = urlparse(url)
    m = re.search(r"/map/([A-Za-z_]+)\.php", parts.path)
    ident = parse_qs(parts.query).get("id", [None])[0]
    if not m or not ident or not ident.isdigit():
        return None
    return f"{m.group(1)}_{ident}.html"


def run() -> int:
    ensure_dirs()
    manifest = read_json(HARVEST_MANIFEST) if HARVEST_MANIFEST.exists() else {}
    failures = 0
    fetched = kept = 0

    with requests.Session() as session:
        session.headers.update(HEADERS)

        ids = [i.lower() for i in FABRIC_IDS]
        if SEARCH_URL:
            print(f"Crawling search results: {SEARCH_URL}")
            for i in crawl_search_results(session, SEARCH_URL):
                if i not in ids:
                    ids.append(i)
        print(f"{len(ids)} fabric pages to harvest")

        site_urls: dict[str, str] = {}   # file name -> url

        for fid in ids:
            target = RAW_HTML / f"{fid}.html"
            url = f"{BASE_URL}/{fid}"
            body = None
            if target.exists() and not FORCE:
                kept += 1
                body = target.read_bytes()
                print(f"  kept     {fid}")
            else:
                status, body = fetch(session, url)
                time.sleep(DELAY)
                if status != 200:
                    failures += 1
                    print(f"WARN {fid}: {status if status else 'no response'} "
                          f"{body if status is None else ''}")
                    manifest[f"html/{fid}.html"] = {"url": url, "status": status}
                    continue
                target.write_bytes(body)
                manifest[f"html/{fid}.html"] = {
                    "url": url, "status": status,
                    "sha256": sha256_bytes(body), "bytes": len(body),
                }
                fetched += 1
                print(f"  fetched  {fid}  ({len(body)} bytes)")

            if FOLLOW_SITE_PAGES:
                text = body.decode("utf-8", errors="replace")
                for href in SITE_HREF_RE.findall(text):
                    full = urljoin(url + "/", href.replace("&amp;", "&"))
                    name = site_file_name(full)
                    if name:
                        site_urls[name] = full

        for name, url in sorted(site_urls.items()):
            target = RAW_SITES / name
            if target.exists() and not FORCE:
                kept += 1
                print(f"  kept     sites/{name}")
                continue
            status, body = fetch(session, url)
            time.sleep(DELAY)
            if status != 200:
                failures += 1
                print(f"WARN sites/{name}: {status if status else 'no response'} "
                      f"{body if status is None else ''}")
                manifest[f"html/sites/{name}"] = {"url": url, "status": status}
                continue
            target.write_bytes(body)
            manifest[f"html/sites/{name}"] = {
                "url": url, "status": status,
                "sha256": sha256_bytes(body), "bytes": len(body),
            }
            fetched += 1
            print(f"  fetched  sites/{name}  ({len(body)} bytes)")

    write_json(HARVEST_MANIFEST, manifest)
    print(f"harvest: {fetched} fetched, {kept} kept, {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run())
