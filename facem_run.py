"""
facem_run.py
------------
Main entry point for the FACEM RDF pipeline.
Edit the CONFIG block below, then run:

    python facem_run.py

This script:
  1. Generates the FACEM ontology  →  output/facem_ontology.ttl
  2. Scrapes fabric pages          →  output/facem_records.json
                                   →  output/facem.ttl  (RDF instances)

All three scripts must sit in the same folder:
    facem_ontology.py   ←  namespace constants + ontology builder
    facem_pipeline.py   ←  scraper + RDF instance builder
    facem_run.py        ←  YOU ARE HERE (edit CONFIG below)
"""

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG  —  everything you need to change is in this block
# ══════════════════════════════════════════════════════════════════════════════

# ── Input mode ────────────────────────────────────────────────────────────────
# "url"    → scrape a single FACEM fabric page
# "search" → crawl all pages of a FACEM search-results URL (pagination handled)
MODE = "url"

# ── URLs ──────────────────────────────────────────────────────────────────────
# Used when MODE = "url"
SINGLE_URL = "https://facem.at/bnap-a-11"

# Used when MODE = "search"
# Tip: copy the URL directly from your FACEM search results page.
# The &page=N parameter is managed automatically — just paste page 1.
SEARCH_URL = (
    "https://facem.at/search/results.php?c=29&c=9&c=8&c=11&c=32&c=31&c=7&page=1"
)

# ── Output ────────────────────────────────────────────────────────────────────
# Directory for all output files (relative to this script, or absolute path).
# Windows example:  r"C:\Users\florian\facem-output"
OUTPUT_DIR = "output"

# True  → one combined facem_records.json + facem.ttl
# False → one <FABRIC-ID>.json + <FABRIC-ID>.ttl per fabric
COMBINED_OUTPUT = True

# True → skip RDF; produce JSON only
JSON_ONLY = False

# ── Behaviour ─────────────────────────────────────────────────────────────────
# Seconds to wait between HTTP requests (be polite to the server)
REQUEST_DELAY = 1.5

# Regenerate the ontology file on every run (recommended: True)
REGENERATE_ONTOLOGY = True

# ══════════════════════════════════════════════════════════════════════════════

import time
from pathlib import Path

from facem_ontology import build_and_save
from facem_pipeline import crawl_search_results, run


def main():
    out_dir = Path(__file__).parent / OUTPUT_DIR

    print("=" * 60)
    print("  FACEM RDF Pipeline")
    print("=" * 60)

    # ── Step 1: ontology ──────────────────────────────────────────────────────
    if REGENERATE_ONTOLOGY:
        onto_path = build_and_save(out_dir)
        print(f"[1/2] Ontology   → {onto_path}")
    else:
        print(f"[1/2] Ontology   skipped (REGENERATE_ONTOLOGY = False)")

    # ── Step 2: collect URLs ──────────────────────────────────────────────────
    print(f"\n[2/2] Scraping   (mode={MODE!r})")

    if MODE == "url":
        urls = [SINGLE_URL]
        print(f"  Single URL: {SINGLE_URL}")
    elif MODE == "search":
        print(f"  Crawling: {SEARCH_URL}")
        urls = crawl_search_results(SEARCH_URL, delay=REQUEST_DELAY)
        print(f"  Found {len(urls)} fabric URLs")
    else:
        raise ValueError(f"Unknown MODE={MODE!r} — use 'url' or 'search'")

    # ── Step 3: scrape + build RDF ────────────────────────────────────────────
    print()
    run(
        urls=urls,
        out_dir=out_dir,
        combined=COMBINED_OUTPUT,
        json_only=JSON_ONLY,
        delay=REQUEST_DELAY,
    )

    print("\n" + "=" * 60)
    print("  Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()
