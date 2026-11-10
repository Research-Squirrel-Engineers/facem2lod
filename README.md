# facem2lod

FACEM (Fabrics of Ancient Ceramics in the Mediterranean, <https://facem.at>) as Linked Open Data, with a bridge to
[GeoScience-FAIRification-LOD](https://github.com/Research-Squirrel-Engineers/GeoScience-FAIRification-LOD).

The plan, the decisions taken so far and the step overview are in `PRIMER.md` (German, internal working document).

## Status

Work in progress. Harvest, parse, vocabularies and the RDF build exist; the Wikidata links, the bridge to
geo-lod and the published IRIs are still to do. The first prototype (`facem_*.py`, `output/`) was removed
when the rebuild replaced it; it stays in the Git history.

## Run

Everything is driven by one command. Offline steps are the default; fetching from facem.at has its own verb.

    python -m pip install -r requirements.txt
    python main.py harvest
    python main.py

`python main.py --list` shows the steps, `--only`, `--from`, `--skip`, `--dry-run` and `--strict` select and check them.

| Step | What it does | Writes |
|---|---|---|
| `harvest` | fetches the fabric pages configured in `py/step_harvest.py` and the sample pages they list (network) | `data/raw/html/`, `data/raw/harvest_manifest.json` |
| `parse` | turns each page into JSON (tabs and sections, map points, further samples, images) and builds a catalogue of all fields seen | `data/derived/json/`, `data/derived/field_catalogue.json`, `dist/pipeline_report.txt` |
| `vocab` | builds SKOS vocabularies (typologies, fabrics, periods, descriptors) and splits each object type into structured assignments | `data/derived/vocab/`, `dist/facem-vocabularies.ttl` |
| `rdf` | builds the ontology, the instance graph and the DCAT graph from the JSON and the vocabularies, then checks them with SHACL (needs `pyshacl`) | `dist/facem-ontology.ttl`, `dist/facem.ttl`, `dist/facem-dcat.ttl` |
| `wikidata` | proposes Wikidata items for the places and periods found in the data, with GeoNames and OSM IDs where Wikidata has them; you confirm the rows by hand (network) | `data/curated/wikidata_candidates.csv` |

Raw pages are stored exactly as the server sent them, so the parser can be improved without crawling again.
No generated file contains a timestamp; running the pipeline twice leaves `git status` clean.

## Tests

    python -m unittest discover -s tests -v

The parser tests use a synthetic page in `tests/fixtures/`. It is hand-written and is not a copy of FACEM markup.

## Data and licence

The code is MIT licensed (see `LICENSE`). The FACEM content belongs to its authors and editors; no data licence is stated
on the site, so the harvested pages are working material and not a redistribution. Cite FACEM as requested on each page
(for example "FACEM - http://facem.at/ou-ars-1").
