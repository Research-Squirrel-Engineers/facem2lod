"""Build the SKOS vocabularies and the structured object types (offline, no clock).

Reads   data/derived/json/*.json   (from step_parse.py)
        data/curated/wikidata_candidates.csv   (your decisions from step_wikidata.py, for the periods)
Writes  data/derived/vocab/vocabularies.json   every scheme and concept with counts and sources
        data/derived/vocab/object_types.json   `object type` of every page, split into assignments
        dist/facem-vocabularies.ttl            the same schemes as SKOS

Decisions of 2026-11-10 (PRIMER A4): one scheme per typology (Hayes, Atlante, El Mahrine, Fulford,
PM), hierarchy derived from the notation, uncertainty and other qualifiers kept on the assignment,
a slash gives several assignments, literature stays separate; one scheme for every other controlled
value. Labels are exactly the FACEM wording, nothing is corrected here.
"""

import csv
import re
import sys
from collections import Counter, OrderedDict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from facem_utils import (DATA_DERIVED, DERIVED_JSON, DIST, NS_BASE, ROOT, read_json,
                         write_json)

VOCAB_DIR = DATA_DERIVED / "vocab"
VOCAB_JSON = VOCAB_DIR / "vocabularies.json"
TYPES_JSON = VOCAB_DIR / "object_types.json"
VOCAB_TTL = DIST / "facem-vocabularies.ttl"
WIKIDATA_CSV = ROOT / "data" / "curated" / "wikidata_candidates.csv"
VOCAB_NS = NS_BASE + "vocab/"
WD = "http://www.wikidata.org/entity/"

# typology name in the data -> (scheme id, title, literature the typology comes from)
TYPOLOGIES = OrderedDict([
    ("Hayes", ("hayes", "Hayes typology", "Hayes 1972")),
    ("Atlante", ("atlante", "Atlante typology", "Atlante I")),
    ("El Mahrine", ("el-mahrine", "El Mahrine typology", "Mackensen 1993")),
    ("Fulford", ("fulford", "Fulford typology", "Fulford 1984")),
    ("PM", ("pm", "PM typology", "Ben Moussa 2007")),
])
ABBREVIATIONS = {"EM": "El Mahrine"}
TYPE_RE = re.compile(r"^(" + "|".join(sorted(list(TYPOLOGIES) + list(ABBREVIATIONS), key=len, reverse=True))
                     + r")\s+(.+)$")
CODE_RE = re.compile(r"^(?P<main>[IVXLC]+|\d+(?:\.\d+)?)(?P<sep>\s?)(?P<var>.*)$")
QUALIFIER_RE = re.compile(r"^(var\.|late\b|no\.)", re.I)

# simple schemes: id -> (title, section label, field key)
INCLUSION_SECTION = "fabric description > Inclusions (all lengths in mm)"
SIMPLE = OrderedDict([
    ("ware", ("Ware", "object data > Object", "ware")),
    ("object-form", ("Object form", "object data > Object", "object form")),
    ("fragmentation", ("Fragmentation", "object data > Object", "fragmentation")),
    ("surface", ("Surface description", "object data > Object", "surface description")),
    ("fracture-texture", ("Texture of fresh break", "fabric description > Visual Examination of Fresh Break", "texture of fresh break")),
    ("hardness", ("Hardness", "fabric description > Visual Examination of Fresh Break", "hardness")),
    ("sorting", ("Sorting", "fabric description > Observation Under the Binocular Stereoscope (all lengths in mm)", "sorting")),
    ("method", ("Method of sample analysis", "archaeometry", "applied method for sample analysis")),
])
INCLUSION_PARTS = ["frequency", "roundness", "shape", "colour"]     # then the size range
INCLUSION_SCHEMES = {
    "inclusion-frequency": "Frequency of inclusions",
    "inclusion-roundness": "Roundness of inclusions",
    "inclusion-shape": "Shape of inclusions",
    "inclusion-colour": "Colour of inclusions",
}


# ---------------------------------------------------------------- helpers

def slug(text: str) -> str:
    s = re.sub(r"[^a-z0-9.]+", "-", text.lower())
    return s.strip("-")


def natural_key(text: str):
    return [int(p) if p.isdigit() else p for p in re.split(r"(\d+)", text)]


def split_code(code: str):
    m = CODE_RE.match(code)
    return (m.group("main"), m.group("sep"), m.group("var")) if m else (code, "", "")


def parent_code(code: str):
    """Broader code derived from the notation: 61B3 -> 61B -> 61, XLIX, 10 -> XLIX, 18.2 -> 18."""
    main, sep, var = split_code(code)
    if var:
        m = re.fullmatch(r"([A-Za-z])\d+", var)
        return main + sep + m.group(1) if m else main
    if "." in main:
        return main.split(".")[0]
    return None


# ---------------------------------------------------------------- object type parser

def parse_object_type(raw: str) -> dict:
    """'Hayes 61B3, late ( Hayes 1972 ; Bonifay 2004 )' -> assignments, literature, note.

    One assignment per type; `relation` is 'alternative' for a slash inside one typology,
    'concordance' for a slash between two typologies. `certainty` is certain, uncertain (`?`),
    possible (`possibly`) or similar (`sim.`). `qualifiers` keeps late, no. 1, a bare var.;
    `variant` holds the name of a 'var. X' that becomes a sub-concept.
    """
    text = " ".join(raw.split())
    certainty = "certain"
    for prefix, value in (("possibly ", "possible"), ("sim. ", "similar")):
        if text.lower().startswith(prefix):
            certainty, text = value, text[len(prefix):]
    literature, note = [], ""
    m = re.match(r"^(?P<head>.*?)\s*\((?P<lit>[^()]*)\)(?P<tail>.*)$", text)
    if m:
        text = m.group("head")
        literature = [x.strip() for x in m.group("lit").split(";") if x.strip()]
        note = re.sub(r"[()]", "", m.group("tail")).strip(" ,")
        note = " ".join(note.split())
    if text.endswith("?"):
        certainty, text = "uncertain", text[:-1].rstrip()

    tokens = [t.strip() for t in text.split(",")]
    qualifiers, variant = [], ""
    first, extra = tokens[0], []
    for tok in tokens[1:]:
        if QUALIFIER_RE.match(tok):
            if tok.lower().startswith("var."):
                name = tok[4:].strip()
                if name:
                    variant = name
                else:
                    qualifiers.append("var.")
            else:
                qualifiers.append(tok)
        else:
            extra.append(tok)

    parts = [p.strip() for p in first.split("/")]
    assignments, prev_typology, prev_code = [], None, None
    for i, part in enumerate(parts):
        tm = TYPE_RE.match(part)
        if tm:
            typology = ABBREVIATIONS.get(tm.group(1), tm.group(1))
            code = tm.group(2).strip()
        else:
            typology = prev_typology
            code = part
            if typology is None:
                continue
            if re.match(r"^[A-Z]\d*$", code) and prev_code:       # 61A/B4, VIII A/C
                main, sep, _ = split_code(prev_code)
                code = main + sep + code
        if i == len(parts) - 1 and extra:
            code = code + ", " + ", ".join(extra)
        relation = ""
        if i > 0:
            relation = "alternative" if typology == prev_typology else "concordance"
        assignments.append({"typology": typology, "code": code, "relation": relation})
        prev_typology, prev_code = typology, code

    for a in assignments:
        a["certainty"] = certainty
        a["qualifiers"] = []
        a["variant"] = ""
    if assignments:
        assignments[-1]["qualifiers"] = qualifiers
        assignments[-1]["variant"] = variant
    return {"raw": raw, "assignments": assignments, "literature": literature, "note": note}


# ---------------------------------------------------------------- collecting

def load_pages() -> list:
    return [read_json(p) for p in sorted(DERIVED_JSON.glob("*.json")) if p.name != "index.json"
            and re.fullmatch(r"[a-z]{1,2}-(?:ars-)?\d+(?:-\d+)?", p.stem)]


def field_values(page: dict, section: str, key: str) -> list:
    out = []
    for s in page["sections"]:
        label = " > ".join(x for x in (s.get("tab"), s["name"]) if x)
        if label != section:
            continue
        for f in s["fields"]:
            if f["key"] == key:
                v = f.get("value")
                out.append(v if v is not None else " | ".join(f.get("values", [])))
    return out


class Scheme:
    def __init__(self, sid, title, source=""):
        self.id, self.title, self.source = sid, title, source
        self.concepts = {}                  # notation -> dict

    def add(self, notation, label=None, count=1, broader=None, **extra):
        c = self.concepts.setdefault(notation, {"notation": notation, "label": label or notation,
                                                "count": 0, "broader": None, "match": [], "origin": []})
        c["count"] += count
        if broader and not c["broader"]:
            c["broader"] = broader
        for k, v in extra.items():
            if k == "origin":
                if v not in c["origin"]:
                    c["origin"].append(v)
            elif k == "match":
                for item in v:
                    if item not in c["match"]:
                        c["match"].append(item)
                c["match"].sort(key=lambda m: (m["property"], m["target"]))
        return c

    def to_json(self):
        concepts = []
        for n in sorted(self.concepts, key=natural_key):
            c = dict(self.concepts[n])
            c["iri"] = f"{VOCAB_NS}{self.id}/{slug(n)}"
            c["origin"] = sorted(c["origin"])
            concepts.append(c)
        return {"id": self.id, "iri": f"{VOCAB_NS}{self.id}", "title": self.title,
                "source": self.source, "concepts": concepts}


def add_type_concept(scheme: Scheme, typology: str, code: str, count: int, origin: str) -> None:
    """Adds the concept and every ancestor derived from the notation."""
    scheme.add(code, label=f"{typology} {code}", count=count, broader=parent_code(code), origin=origin)
    parent = parent_code(code)
    while parent:
        scheme.add(parent, label=f"{typology} {parent}", count=0, broader=parent_code(parent),
                   origin="implied")
        parent = parent_code(parent)


def build(pages: list) -> tuple:
    schemes = OrderedDict()
    types = OrderedDict()
    for name, (sid, title, source) in TYPOLOGIES.items():
        schemes[sid] = Scheme(sid, title, source)
    # object types
    for page in pages:
        for raw in field_values(page, "object data > Object", "object type"):
            parsed = parse_object_type(raw)
            types[page["id"]] = parsed
            for a in parsed["assignments"]:
                sid = TYPOLOGIES[a["typology"]][0]
                sch = schemes[sid]
                if a["variant"]:
                    base = a["code"]
                    vcode = f"{base} var. {a['variant']}"
                    add_type_concept(sch, a["typology"], base, 0, "implied")
                    c = sch.add(vcode, label=f"{a['typology']} {vcode}", count=1, broader=base,
                                origin=page["id"])
                    vm = TYPE_RE.match(a["variant"])
                    if vm:                                    # 'var. PM 34.11' -> PM concept
                        other = ABBREVIATIONS.get(vm.group(1), vm.group(1))
                        osch = schemes[TYPOLOGIES[other][0]]
                        add_type_concept(osch, other, vm.group(2).strip(), 0, "implied")
                        c["match"].append({"property": "closeMatch",
                                           "target": f"{VOCAB_NS}{osch.id}/{slug(vm.group(2).strip())}"})
                        c["match"].sort(key=lambda m: (m["property"], m["target"]))
                    a["concept"] = f"{sid}/{slug(vcode)}"
                else:
                    add_type_concept(sch, a["typology"], a["code"], 1, page["id"])
                    a["concept"] = f"{sid}/{slug(a['code'])}"
    # fabrics
    fab = Scheme("fabric", "FACEM fabric")
    schemes["fabric"] = fab
    for page in pages:
        for v in field_values(page, "object data", "fabric type"):
            fab.add(v, count=1, origin=page["id"])
    # simple schemes
    for sid, (title, section, key) in SIMPLE.items():
        sch = schemes.setdefault(sid, Scheme(sid, title))
        for page in pages:
            for v in field_values(page, section, key):
                for term in ([t.strip() for t in re.split(r"\s+and\s+", v)] if sid == "fragmentation" else [v]):
                    if term:
                        sch.add(term, count=1, origin=page["id"])
    # inclusions
    kinds = Scheme("inclusion-kind", "Kind of inclusion")
    schemes["inclusion-kind"] = kinds
    parts = {sid: Scheme(sid, t) for sid, t in INCLUSION_SCHEMES.items()}
    schemes.update(parts)
    voids = Scheme("void-form", "Form of voids")
    schemes["void-form"] = voids
    for page in pages:
        for s in page["sections"]:
            label = " > ".join(x for x in (s.get("tab"), s["name"]) if x)
            if label == INCLUSION_SECTION:
                for f in s["fields"]:
                    kinds.add(f["key"], count=1, origin=page["id"])
                    cells = [c.strip() for c in (f.get("value") or "").split(",")]
                    for name, cell in zip(INCLUSION_PARTS, cells):
                        for term in [t.strip() for t in cell.split(";") if t.strip()]:
                            parts["inclusion-" + name].add(term, count=1, origin=page["id"])
        for v in field_values(page, "fabric description > Observation Under the Binocular Stereoscope (all lengths in mm)", "porosity"):
            m = re.search(r"void form:\s*([^;]+(?:;[^;]*?)*?);\s*void length", v)
            if m:
                for term in [t.strip() for t in m.group(1).split(";") if t.strip()]:
                    voids.add(term, count=1, origin=page["id"])
    # periods, from the decisions in the Wikidata CSV
    periods = Scheme("period", "FACEM period")
    schemes["period"] = periods
    decided = {}
    if WIKIDATA_CSV.exists():
        with open(WIKIDATA_CSV, encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh):
                if r["kind"] == "period" and r["confirmed"] == "ja" and r["match"]:
                    decided.setdefault(r["source_label"], []).append((r["match"], r["qid"]))
    for page in pages:
        for v in field_values(page, "object data > Object", "chronology of object"):
            m = re.match(r"^(Roman-[A-Z][A-Za-z]*(?: [A-Z][A-Za-z]*)*)(?:\s|$)", v)
            if m:
                c = periods.add(m.group(1), count=1, origin=page["id"])
    for label, items in decided.items():
        if label in periods.concepts:
            for match, qid in sorted(items):
                periods.add(label, count=0, match=[{"property": match, "target": WD + qid}])
    return schemes, types


# ---------------------------------------------------------------- Turtle

def lit(text: str) -> str:
    s = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "")
    return f'"{s}"'


def to_turtle(schemes: dict) -> str:
    out = ["@prefix dct: <http://purl.org/dc/terms/> .",
           "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .", ""]
    for sid in sorted(schemes):
        sj = schemes[sid].to_json()
        by = {c["notation"]: c for c in sj["concepts"]}
        tops = [c for c in sj["concepts"] if not c["broader"]]
        out.append(f"<{sj['iri']}> a skos:ConceptScheme ;")
        out.append(f"    dct:title {lit(sj['title'])}@en" + (" ;" if sj["source"] or tops else " ."))
        if sj["source"]:
            out.append(f"    dct:source {lit(sj['source'])}" + (" ;" if tops else " ."))
        for i, c in enumerate(tops):
            out.append(f"    skos:hasTopConcept <{c['iri']}>" + (" ;" if i < len(tops) - 1 else " ."))
        out.append("")
        for c in sj["concepts"]:
            lines = [f"<{c['iri']}> a skos:Concept",
                     f"skos:inScheme <{sj['iri']}>",
                     f"skos:prefLabel {lit(c['label'])}@en",
                     f"skos:notation {lit(c['notation'])}"]
            if c["broader"]:
                lines.append(f"skos:broader <{by[c['broader']]['iri']}>")
            else:
                lines.append(f"skos:topConceptOf <{sj['iri']}>")
            for m in c["match"]:
                lines.append(f"skos:{m['property']} <{m['target']}>")
            out.append(" ;\n    ".join(lines) + " .")
            out.append("")
    return "\n".join(out).rstrip("\n") + "\n"


def run() -> int:
    pages = load_pages()
    if not pages:
        print("vocab: no parsed JSON in data/derived/json/ (run 'python main.py parse' first)")
        return 0
    schemes, types = build(pages)
    doc = OrderedDict((sid, schemes[sid].to_json()) for sid in sorted(schemes))
    write_json(VOCAB_JSON, doc)
    write_json(TYPES_JSON, types)
    VOCAB_TTL.parent.mkdir(parents=True, exist_ok=True)
    with open(VOCAB_TTL, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(to_turtle(schemes))
    total = sum(len(s["concepts"]) for s in doc.values())
    print(f"vocab: {len(doc)} schemes, {total} concepts, {len(types)} object types parsed")
    unparsed = [pid for pid, t in types.items() if not t["assignments"]]
    for pid in unparsed:
        print(f"WARN {pid}: object type not understood: {types[pid]['raw']!r}")
    for sid in sorted(doc):
        n = len(doc[sid]["concepts"])
        print(f"  {sid:22s} {n:3d} concepts")
    return 0


if __name__ == "__main__":
    sys.exit(run())
