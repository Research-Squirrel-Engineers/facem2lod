"""Build the RDF graphs (offline, no clock, byte-stable).

Reads   data/derived/json/*.json                   (step_parse.py)
        data/derived/vocab/vocabularies.json, object_types.json   (step_vocab.py)
        data/curated/wikidata_candidates.csv       (your decisions, step_wikidata.py)
        data/raw/harvest_manifest.json             (hash and address of every stored page)
        shapes/*.ttl                               (SHACL shapes, written by hand)
Writes  dist/facem-ontology.ttl    classes and properties (table in facem_model.py)
        dist/facem.ttl             the instance graph: sherds, samples, observations, places, ...
        dist/facem-dcat.ttl        catalogue, series, datasets, distributions (DCAT 3, mandatory fields of DCAT-AP)
and checks all of it with SHACL (pyshacl); a violation is an error.

Model: PRIMER section S6 and docs/visuals/img/s6-model-draft.png. Nothing is invented: what FACEM does not say is not said here.
"""
from __future__ import annotations

import csv
import re
import sys
from collections import OrderedDict, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from facem_model import (CRM, CRMSCI, DCT, ENT, GEO, GEOLOD, NS, PREFIXES, SKOS, SOSA, anchors_ok, ontology_graph)
from facem_utils import DATA_DERIVED, DERIVED_JSON, DIST, HARVEST_MANIFEST, NS_BASE, RELEASE, ROOT, read_json
from ttl import Graph, I, L, RDF_TYPE, XSD
from values import (CHRON_RULE, CHRON_RULE_TEXT, derive_interval, parse_dimensions, parse_inclusion_row,
                    parse_inclusions, parse_porosity, split_chronology)

VOCAB_NS = NS_BASE + "vocab/"
VOCAB_JSON = DATA_DERIVED / "vocab" / "vocabularies.json"
TYPES_JSON = DATA_DERIVED / "vocab" / "object_types.json"
WIKIDATA_CSV = ROOT / "data" / "curated" / "wikidata_candidates.csv"
SHAPES_DIR = ROOT / "shapes"
VOCAB_TTL = DIST / "facem-vocabularies.ttl"
OUT_ONTOLOGY = DIST / "facem-ontology.ttl"
OUT_GRAPH = DIST / "facem.ttl"
OUT_DCAT = DIST / "facem-dcat.ttl"
RAW_BASE = "https://raw.githubusercontent.com/Research-Squirrel-Engineers/facem2lod/main/dist/"
WD = "http://www.wikidata.org/entity/"
GEONAMES = "https://sws.geonames.org/"
UNITS = {"cm": "CentiM", "mm": "MilliM", "m": "M", "%": "PERCENT"}
RDFS = PREFIXES["rdfs"]
SECTION_OBJECT = "object data > Object"
SECTION_CONTEXT = "object data > Context Data"
SECTION_BREAK = "fabric description > Visual Examination of Fresh Break"
SECTION_BINO = "fabric description > Observation Under the Binocular Stereoscope (all lengths in mm)"
SECTION_INCL = "fabric description > Inclusions (all lengths in mm)"
METHOD_VISUAL, METHOD_BINO, METHOD_THIN = "Visual Examination of Fresh Break", "Observation Under the Binocular Stereoscope", "thin section"


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def ent(kind: str, key: str) -> str:
    return f"{ENT}{kind}_{slug(key)}"


class Builder:
    def __init__(self):
        self.g = Graph(PREFIXES)
        self.warnings = []
        self.notes = []
        self.vocab = read_json(VOCAB_JSON)
        self.types = read_json(TYPES_JSON)
        self.concepts = {sid: {c["notation"]: c["iri"] for c in v["concepts"]} for sid, v in self.vocab.items()}
        self.manifest = read_json(HARVEST_MANIFEST) if HARVEST_MANIFEST.exists() else {}
        self.places = OrderedDict()        # label -> {"lat", "long"}
        self.fabrics = defaultdict(list)   # fabric label -> [sample slug]
        self.fabric_pages = defaultdict(list)
        self.persons = {}
        self.publications = {}

    # ------------------------------------------------------------- helpers
    def warn(self, msg: str):
        self.warnings.append(msg)
        print(f"WARN {msg}")

    def concept(self, scheme: str, term: str, page: str):
        iri = self.concepts.get(scheme, {}).get(term)
        if iri is None:
            self.warn(f"{page}: '{term}' is not in the scheme '{scheme}' (run python main.py vocab)")
        return I(iri) if iri else None

    @staticmethod
    def fv(page, section, key):
        for s in page["sections"]:
            label = " > ".join(x for x in (s.get("tab"), s["name"]) if x)
            if label == section:
                for f in s["fields"]:
                    if f["key"] == key:
                        return f.get("value")
        return None

    @staticmethod
    def section_fields(page, section):
        for s in page["sections"]:
            if " > ".join(x for x in (s.get("tab"), s["name"]) if x) == section:
                return s["fields"]
        return []

    def label(self, iri, text, lang="en"):
        self.g.add(iri, RDFS + "label", L(text, lang=lang))

    def person(self, name: str, note: str = ""):
        iri = ent("person", name)
        if iri not in self.persons:
            self.persons[iri] = name
            self.g.type(iri, NS + "Person")
            self.label(iri, name, None)
        if note:
            self.g.add(iri, CRM + "P3_has_note", L(note, lang="en"))
        return iri

    def publication(self, work: str):
        iri = ent("publication", work)
        if iri not in self.publications:
            self.publications[iri] = work
            self.g.type(iri, NS + "Publication")
            self.label(iri, work, None)
        return iri

    def decimal(self, v):
        return L(str(v), dt=XSD + "decimal")

    def year(self, y: int):
        return L(f"{y:04d}", dt=XSD + "gYear")

    # ------------------------------------------------------------- places and wikidata
    def collect_places(self, pages):
        for page in pages:
            for mp in page.get("map_points", []):
                self.places.setdefault(mp["label"], {"lat": mp["lat"], "long": mp["long"]})
            for key, sec in (("supposed site of production", "object data"), ("site of discovery", SECTION_CONTEXT)):
                v = self.fv(page, sec, key)
                if v:
                    self.places.setdefault(v, {})
        decided = defaultdict(list)
        if WIKIDATA_CSV.exists():
            with open(WIKIDATA_CSV, encoding="utf-8", newline="") as fh:
                for r in csv.DictReader(fh):
                    if r["kind"] == "place" and r["confirmed"] == "ja" and r["match"]:
                        decided[r["source_label"]].append(r)
        for label in sorted(self.places):
            iri = ent("place", label)
            xy = self.places[label]
            self.g.type(iri, NS + "Place")
            self.label(iri, label, None)
            if "lat" in xy:
                geom = ent("geometry", label)
                self.g.type(geom, GEO + "Geometry")
                self.g.add(iri, GEO + "hasGeometry", I(geom))
                self.g.add(geom, GEO + "asWKT", L(f"POINT({xy['long']} {xy['lat']})", dt=GEO + "wktLiteral"))
                self.g.add(iri, CRM + "P168_place_is_defined_by", I(geom))
            else:
                self.warn(f"place '{label}': no coordinate on any map")
            for r in sorted(decided.get(label, []), key=lambda r: (r["match"], r["qid"])):
                if r["match"] == "exactMatch":
                    self.g.add(iri, SKOS + "exactMatch", I(WD + r["qid"]))
                elif r["match"] == "within":
                    self.g.add(iri, CRM + "P89_falls_within", I(WD + r["qid"]))
                    if r["geonames_id"]:
                        self.g.add(iri, CRM + "P89_falls_within", I(f"{GEONAMES}{r['geonames_id']}/"))
                else:
                    self.warn(f"place '{label}': match '{r['match']}' is not handled")

    # ------------------------------------------------------------- one page
    def sherd(self, page):
        pid, g = page["id"], self.g
        raw = self.fv(page, "object data", "sample no") or ""
        m = re.match(r"^(M \d+/\d+)(?:\s+representative sample for\s+(.+))?$", raw)
        if not m:
            self.warn(f"{pid}: sample no not understood: {raw!r}")
            return
        sample_no, rep = m.group(1), m.group(2)
        sid = slug(sample_no)
        sherd, sample, taking, record = ent("sherd", sid), ent("sample", sid), ent("sampletaking", sid), ent("record", pid)
        fabric_label = self.fv(page, "object data", "fabric type")
        fabric = self.concept("fabric", fabric_label, pid)
        self.fabrics[fabric_label].append(sid)
        self.fabric_pages[fabric_label].append(pid)

        # source record
        g.type(record, NS + "SourceRecord")
        self.label(record, f"FACEM page {page['title']}", None)
        g.add(record, NS + "sourcePage", L(page["source_url"], dt=XSD + "anyURI"))
        mf = self.manifest.get(f"html/{pid}.html", {})
        if mf.get("sha256"):
            g.add(record, NS + "sha256", L(mf["sha256"]))
        else:
            self.warn(f"{pid}: no hash in the harvest manifest")

        # sherd
        g.type(sherd, NS + "Sherd")
        self.label(sherd, f"Sherd {sample_no}", None)
        g.add(sherd, NS + "primarySource", I(record))
        if fabric:
            g.add(sherd, CRM + "P45_consists_of", fabric)
        for key, prop, scheme in (("ware", "ware", "ware"), ("object form", "objectForm", "object-form"),
                                  ("surface description", "surface", "surface")):
            v = self.fv(page, SECTION_OBJECT, key)
            if v:
                g.add(sherd, NS + prop, self.concept(scheme, v, pid))
        for term in re.split(r"\s+and\s+", self.fv(page, SECTION_OBJECT, "fragmentation") or ""):
            if term.strip():
                g.add(sherd, NS + "fragmentation", self.concept("fragmentation", term.strip(), pid))
        for key, prop in (("decoration", "decoration"),):
            v = self.fv(page, SECTION_OBJECT, key)
            if v:
                g.add(sherd, NS + prop, L(v, lang="en"))
        v = self.fv(page, SECTION_CONTEXT, "registration code of object")
        if v:
            g.add(sherd, NS + "registrationCode", L(v))
        pub = self.fv(page, SECTION_OBJECT, "publication")
        if pub:
            work, _, ref = [x.strip() for x in pub.partition(",")]
            g.add(self.publication(work), CRM + "P70_documents", I(sherd))
            if ref:
                g.add(sherd, NS + "catalogueReference", L(ref))

        # sample and sample taking
        g.type(sample, NS + "Sample")
        self.label(sample, f"Sample {sample_no}", None)
        g.add(sample, SOSA + "isSampleOf", I(sherd))
        if rep and fabric:
            g.add(sample, NS + "representativeSampleOf", fabric)
        g.type(taking, NS + "SampleTaking")
        self.label(taking, f"Sample taking {sample_no}", None)
        g.add(taking, CRMSCI + "O5_removed", I(sample))
        by = self.fv(page, "object data", "sample taken") or ""
        if by:
            g.add(taking, CRM + "P14_carried_out_by", I(self.person(re.sub(r"^by\s+", "", by).strip())))

        self.production_and_survey(page, sherd, sample_no, fabric_label)
        self.dating(page, sherd, sid, sample_no)
        self.type_assignments(page, sherd, sid, sample_no)
        self.dimensions(page, sherd, sid)
        self.observations(page, sample, sid, sample_no)
        self.images(page, sherd, sample)

    def production_and_survey(self, page, sherd, sample_no, fabric_label):
        pid, g = page["id"], self.g
        site = self.fv(page, "object data", "supposed site of production")
        if site and fabric_label:
            prod = ent("production", fabric_label)
            g.type(prod, NS + "Production")
            self.label(prod, f"Production of the fabric {fabric_label}", None)
            g.add(prod, CRM + "P108_has_produced", I(sherd))
            g.add(prod, NS + "supposedPlace", I(ent("place", site)))
        found = self.fv(page, SECTION_CONTEXT, "site of discovery")
        code = self.fv(page, SECTION_CONTEXT, "registration code of context") or ""
        ym = re.search(r"survey\w*\s+in\s+(\d{4})", code)
        if found:
            key = f"{found} {ym.group(1)}" if ym else f"{found} {pid}"
            sv = ent("survey", key)
            g.type(sv, NS + "Survey")
            self.label(sv, f"Survey at {found}" + (f", {ym.group(1)}" if ym else ""), None)
            g.add(sv, CRM + "P12_occurred_in_the_presence_of", I(sherd))
            g.add(sv, CRM + "P7_took_place_at", I(ent("place", found)))
            if code:
                g.add(sv, NS + "contextRegistration", L(code))
            if ym:
                ts = ent("timespan", f"survey-{ym.group(1)}")
                g.type(ts, CRM + "E52_Time-Span")
                self.label(ts, ym.group(1), None)
                g.add(ts, CRM + "P82a_begin_of_the_begin", self.year(int(ym.group(1))))
                g.add(ts, CRM + "P82b_end_of_the_end", self.year(int(ym.group(1))))
                g.add(sv, CRM + "P4_has_time-span", I(ts))
            else:
                self.warn(f"{pid}: no survey year in {code!r}")
            interp = self.fv(page, SECTION_CONTEXT, "interpretation of context")
            if interp:
                g.add(sv, NS + "contextInterpretation", self.concept("context-interpretation", interp, pid))

    def dating(self, page, sherd, sid, sample_no):
        pid, g = page["id"], self.g
        text = self.fv(page, SECTION_OBJECT, "chronology of object")
        if not text:
            return
        periods = list(self.concepts.get("period", {}))
        label, rest = split_chronology(text, periods)
        da = ent("dating", sid)
        g.type(da, NS + "DatingAssignment")
        self.label(da, f"Dating of {sample_no}", None)
        g.add(da, CRM + "P140_assigned_attribute_to", I(sherd))
        g.add(da, CRM + "P177_assigned_property_of_type", I(CRM + "P4_has_time-span"))
        g.add(da, NS + "chronologyText", L(text, lang="en"))
        if label:
            g.add(da, NS + "periodConcept", self.concept("period", label, pid))
        else:
            self.warn(f"{pid}: no period concept in {text!r}")
        iv = derive_interval(rest)
        if iv is None:
            self.warn(f"{pid}: no interval derived from {rest!r}")
            return
        ts = ent("timespan", f"dating-{sid}")
        g.type(ts, CRM + "E52_Time-Span")
        self.label(ts, f"{iv[0]} to {iv[1]} CE (derived from the text of FACEM)", None)
        g.add(ts, CRM + "P82a_begin_of_the_begin", self.year(iv[0]))
        g.add(ts, CRM + "P82b_end_of_the_end", self.year(iv[1]))
        g.add(ts, NS + "intervalRule", L(CHRON_RULE))
        if iv[2]:
            g.add(ts, NS + "intervalUncertain", L("true", dt=XSD + "boolean"))
        g.add(da, CRM + "P141_assigned", I(ts))

    def type_assignments(self, page, sherd, sid, sample_no):
        pid, g = page["id"], self.g
        t = self.types.get(pid)
        if not t or not t["assignments"]:
            return
        previous = None
        for i, a in enumerate(t["assignments"], 1):
            ta = ent("typeassignment", f"{sid}-{i}")
            g.type(ta, NS + "TypeAssignment")
            self.label(ta, f"Type of {sample_no}: {a['typology']} {a['code']}" + (f" {a['variant']}" if a["variant"] else ""), None)
            g.add(ta, CRM + "P140_assigned_attribute_to", I(sherd))
            g.add(ta, CRM + "P177_assigned_property_of_type", I(CRM + "P2_has_type"))
            g.add(ta, CRM + "P141_assigned", I(VOCAB_NS + a["concept"]))
            g.add(ta, NS + "typeCertainty", self.concept("certainty", a["certainty"], pid))
            for q in a["qualifiers"]:
                g.add(ta, NS + "typeQualifier", self.concept("qualifier", q, pid))
            if previous and a["relation"] == "alternative":
                g.add(ta, NS + "alternativeTo", I(previous))
            elif previous and a["relation"] == "concordance":
                g.add(ta, NS + "concordantWith", I(previous))
            if i == 1 and t["note"]:
                g.add(ta, CRM + "P3_has_note", L(t["note"], lang="en"))
            for work in t["literature"]:
                g.add(self.publication(work), CRM + "P70_documents", I(ta))
            previous = ta

    def dimensions(self, page, sherd, sid):
        pid, g = page["id"], self.g
        text = self.fv(page, SECTION_OBJECT, "dimensions")
        if not text:
            return
        dims = parse_dimensions(text)
        if not dims:
            self.warn(f"{pid}: dimensions not understood: {text!r}")
        for n, d in enumerate(dims, 1):
            iri = ent("dimension", f"{sid}-{n}")
            g.type(iri, NS + "Dimension")
            self.label(iri, f"{d['kind']}" + (f" ({d['part']})" if d["part"] else "") + f" {d['value']} {d['unit']}", None)
            g.add(sherd, CRM + "P43_has_dimension", I(iri))
            g.add(iri, NS + "dimensionKind", self.concept("dimension", d["kind"], pid))
            if d["part"]:
                g.add(iri, NS + "dimensionPart", self.concept("dimension-part", d["part"], pid))
            g.add(iri, CRM + "P90_has_value", self.decimal(d["value"]))
            g.add(iri, CRM + "P91_has_unit", I(PREFIXES["unit"] + UNITS[d["unit"]]))

    # ------------------------------------------------------------- observations
    def observation(self, sample, sid, sample_no, pid, prop_scheme, prop_term, method):
        iri = ent("obs", f"{sid}-{slug(method)}-{slug(prop_term)}")
        g = self.g
        g.type(iri, NS + "Observation")
        self.label(iri, f"{prop_term} of {sample_no}", None)
        g.add(iri, SOSA + "hasFeatureOfInterest", I(sample))
        g.add(iri, SOSA + "observedProperty", self.concept(prop_scheme, prop_term, pid))
        g.add(iri, SOSA + "usedProcedure", self.concept("method", method, pid))
        return iri

    def range_result(self, obs, rng, pct=False, size=None):
        g = self.g
        for name, r in (("value", rng), ("size", size)):
            if r and r[0] > r[1]:      # FACEM writes the end smaller than the start: no number is invented
                self.notes.append(f"{obs.rsplit('/', 1)[-1]}: {name} range {r[0]}-{r[1]} runs backwards in FACEM, left out")
                print(f"NOTE {self.notes[-1]}")
                g.add(obs, RDFS + "comment", L(f"{name} as written in FACEM: {r[0]}-{r[1]} (end smaller than start, not interpreted)", lang="en"))
                if name == "value":
                    rng = None
                else:
                    size = None
        if rng:
            g.add(obs, NS + "valueMin", self.decimal(rng[0]))
            g.add(obs, NS + "valueMax", self.decimal(rng[1]))
            if pct:
                g.add(obs, NS + "valueUnit", I(PREFIXES["unit"] + "PERCENT"))
        if size:
            g.add(obs, NS + "sizeMin", self.decimal(size[0]))
            g.add(obs, NS + "sizeMax", self.decimal(size[1]))

    def observations(self, page, sample, sid, sample_no):
        pid, g = page["id"], self.g
        for key in ("visual examination of fresh break", "colour of fresh break"):
            v = self.fv(page, SECTION_BREAK, key)
            if v:
                o = self.observation(sample, sid, sample_no, pid, "property", key, METHOD_VISUAL)
                g.add(o, SOSA + "hasSimpleResult", L(v, lang="en"))
        for key, scheme in (("texture of fresh break", "fracture-texture"), ("hardness", "hardness")):
            v = self.fv(page, SECTION_BREAK, key)
            if v:
                o = self.observation(sample, sid, sample_no, pid, "property", key, METHOD_VISUAL)
                g.add(o, NS + "resultConcept", self.concept(scheme, v, pid))
        v = self.fv(page, SECTION_BINO, "sorting")
        if v:
            o = self.observation(sample, sid, sample_no, pid, "property", "sorting", METHOD_BINO)
            g.add(o, NS + "resultConcept", self.concept("sorting", v, pid))
        v = self.fv(page, SECTION_BINO, "porosity")
        if v:
            o = self.observation(sample, sid, sample_no, pid, "property", "porosity", METHOD_BINO)
            p = parse_porosity(v)
            if not p["percent"]:
                self.warn(f"{pid}: porosity not understood: {v!r}")
                g.add(o, SOSA + "hasSimpleResult", L(v, lang="en"))
            self.range_result(o, p["percent"], True, p["void_length"])
            for form in p["void_forms"]:
                g.add(o, NS + "voidForm", self.concept("void-form", form, pid))
        v = self.fv(page, SECTION_BINO, "inclusions")
        if v:
            o = self.observation(sample, sid, sample_no, pid, "property", "inclusions", METHOD_BINO)
            p = parse_inclusions(v)
            if not p["percent"]:
                self.warn(f"{pid}: inclusions not understood: {v!r}")
                g.add(o, SOSA + "hasSimpleResult", L(v, lang="en"))
            self.range_result(o, p["percent"], True, p["length"])
        for f in self.section_fields(page, SECTION_INCL):
            row = parse_inclusion_row(f.get("value") or "")
            o = self.observation(sample, sid, sample_no, pid, "inclusion-kind", f["key"], METHOD_BINO)
            for part, prop in (("frequency", "frequency"), ("roundness", "roundness"), ("shape", "shape"),
                               ("colour", "colour")):
                for term in row.get(part, []):
                    g.add(o, NS + prop, self.concept("inclusion-" + part, term, pid))
            self.range_result(o, None, False, row["size"])
        method = self.fv(page, "archaeometry", "applied method for sample analysis")
        if method:
            o = ent("obs", f"{sid}-{slug(method)}")
            g.type(o, NS + "Observation")
            self.label(o, f"Analysis ({method}) of {sample_no}", None)
            g.add(o, SOSA + "hasFeatureOfInterest", I(sample))
            g.add(o, SOSA + "usedProcedure", self.concept("method", method, pid))
            author = self.fv(page, "archaeometry", "analysis author") or self.fv(page, "petrography", "name of author")
            if author:
                inst = self.fv(page, "petrography", "institution of author") or ""
                g.add(o, CRM + "P14_carried_out_by", I(self.person(author, inst)))
            pub = self.fv(page, "archaeometry", "publication (of analysis)")
            if pub:
                g.add(self.publication(pub), CRM + "P70_documents", I(o))

    def images(self, page, sherd, sample):
        g = self.g
        for im in page["images"]:
            if im["role"] not in ("macro", "thin_section", "drawing"):
                continue
            iri = ent("image", re.sub(r"\.[a-z]+$", "", im["src"].rsplit("/", 1)[-1], flags=re.I))
            g.type(iri, NS + "Image")
            self.label(iri, im["caption"] or im["alt"] or im["src"].rsplit("/", 1)[-1], None)
            g.add(iri, NS + "imageUrl", L(im["src"], dt=XSD + "anyURI"))
            g.add(iri, NS + "imageRole", L(im["role"]))
            g.add(iri, CRM + "P138_represents", I(sample if im["role"] == "thin_section" else sherd))

    # ------------------------------------------------------------- DCAT
    def dcat(self, pages) -> Graph:
        d = Graph(PREFIXES)
        dcat, foaf = PREFIXES["dcat"], PREFIXES["foaf"]
        pub, creator = ent("agent", "research-squirrel-engineers"), ent("agent", "facem-project")
        for a, name, home in ((pub, "Research Squirrel Engineers", "https://github.com/Research-Squirrel-Engineers"),
                              (creator, "FACEM project, University of Vienna", "https://facem.at/")):
            d.type(a, foaf + "Agent")
            d.add(a, foaf + "name", L(name, lang="en"))
            d.add(a, foaf + "homepage", I(home))
            d.type(home, foaf + "Document")
        series, cat = ent("series", "facem-ars-release-10"), ent("catalog", "facem")
        d.type(series, dcat + "DatasetSeries")
        d.type(series, dcat + "Dataset")
        d.add(series, DCT + "title", L("FACEM, African Red Slip Ware fabrics, release 10", lang="en"))
        d.add(series, DCT + "description", L("The fabrics of African Red Slip Ware in release 10 of the FACEM database (facem.at), "
                                            "one dataset per fabric.", lang="en"))
        d.add(series, DCT + "publisher", I(pub))
        d.add(series, DCT + "creator", I(creator))
        d.type(cat, dcat + "Catalog")
        d.add(cat, DCT + "title", L("FACEM as Linked Open Data", lang="en"))
        d.add(cat, DCT + "description", L("Linked Open Data made from the public pages of facem.at: ontology, vocabularies and "
                                         "one dataset per fabric.", lang="en"))
        d.add(cat, DCT + "publisher", I(pub))
        d.add(cat, foaf + "homepage", I("https://github.com/Research-Squirrel-Engineers/facem2lod"))
        d.type("https://github.com/Research-Squirrel-Engineers/facem2lod", foaf + "Document")
        d.add(cat, dcat + "dataset", I(series))
        # generated files belong to the series
        for name, title, fmt in (("facem.ttl", "Instance graph of the FACEM fabrics (Turtle)", "text/turtle"),
                                 ("facem-ontology.ttl", "Ontology facem: (Turtle)", "text/turtle"),
                                 ("facem-vocabularies.ttl", "SKOS vocabularies of FACEM (Turtle)", "text/turtle"),
                                 ("facem-dcat.ttl", "DCAT description of the datasets (Turtle)", "text/turtle")):
            dist = ent("distribution", name)
            d.type(dist, dcat + "Distribution")
            d.add(dist, DCT + "title", L(title, lang="en"))
            d.add(dist, dcat + "accessURL", I(RAW_BASE + name))
            d.add(dist, dcat + "downloadURL", I(RAW_BASE + name))
            self.media(d, dist, "http://www.iana.org/assignments/media-types/" + fmt, "RDF_TURTLE")
            d.add(series, dcat + "distribution", I(dist))
        sources = {p["id"]: p for p in pages}
        for fabric in sorted(self.fabrics):
            ds = ent("dataset", fabric)
            d.type(ds, dcat + "Dataset")
            d.add(ds, DCT + "title", L(f"FACEM fabric {fabric}", lang="en"))
            n = len(self.fabrics[fabric])
            d.add(ds, DCT + "description", L(f"The fabric {fabric} with its {n} samples and sherds, described in the thin-section "
                                            f"and binocular observations of FACEM.", lang="en"))
            d.add(ds, DCT + "identifier", L(fabric))
            d.add(ds, DCT + "publisher", I(pub))
            d.add(ds, DCT + "creator", I(creator))
            d.add(ds, dcat + "inSeries", I(series))
            for kw in ("African Red Slip Ware", "fabric", "pottery", "thin section"):
                d.add(ds, dcat + "keyword", L(kw, lang="en"))
            d.add(cat, dcat + "dataset", I(ds))
            for pid in sorted(self.fabric_pages[fabric]):
                dist = ent("distribution", pid)
                d.type(dist, dcat + "Distribution")
                d.add(dist, DCT + "title", L(f"FACEM page {sources[pid]['title']}", lang="en"))
                d.add(dist, dcat + "accessURL", I(sources[pid]["source_url"]))
                self.media(d, dist, "http://www.iana.org/assignments/media-types/text/html", "HTML")
                d.add(ds, dcat + "distribution", I(dist))
        return d

    @staticmethod
    def media(d, dist, media_type, file_type):
        """Media type and file type are typed resources, as DCAT-AP 3 asks (dct:MediaType, dct:MediaTypeOrExtent)."""
        ft = "http://publications.europa.eu/resource/authority/file-type/" + file_type
        d.add(dist, PREFIXES["dcat"] + "mediaType", I(media_type))
        d.add(dist, DCT + "format", I(ft))
        d.type(media_type, DCT + "MediaType")
        d.type(ft, DCT + "MediaTypeOrExtent")

    def memberships(self):
        for fabric, sids in self.fabrics.items():
            for sid in sids:
                self.g.add(ent("sherd", sid), DCT + "isPartOf", I(ent("dataset", fabric)))


# ---------------------------------------------------------------- run

HEADER_GRAPH = ("facem2lod instance graph, generated by py/step_rdf.py. Do not edit.\n"
                "Ontology: dist/facem-ontology.ttl, vocabularies: dist/facem-vocabularies.ttl, catalogue: dist/facem-dcat.ttl\n"
                f"Chronology intervals: rule {CHRON_RULE}. " + CHRON_RULE_TEXT)


def write(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)


def shacl(data_files: list, shapes_files: list) -> list:
    """-> list of violation messages (empty if all conform). Needs pyshacl."""
    import rdflib
    from pyshacl import validate
    data = rdflib.Graph()
    for f in data_files:
        data.parse(str(f), format="turtle")
    shapes = rdflib.Graph()
    for f in shapes_files:
        shapes.parse(str(f), format="turtle")
    conforms, report, text = validate(data, shacl_graph=shapes, inference="none", abort_on_first=False)
    if conforms:
        return []
    SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")
    out = []
    for r in report.subjects(rdflib.RDF.type, SH.ValidationResult):
        out.append(f"{report.value(r, SH.focusNode)} | {report.value(r, SH.resultPath)} | {report.value(r, SH.resultMessage)}")
    return sorted(out)


def run() -> int:
    pages = [read_json(p) for p in sorted(DERIVED_JSON.glob("*.json"))
             if p.name != "index.json" and re.fullmatch(r"[a-z]{1,2}-(?:ars-)?\d+(?:-\d+)?", p.stem)]
    if not pages or not VOCAB_JSON.exists():
        print("rdf: no parsed JSON or vocabularies (run 'python main.py parse' and 'vocab' first)")
        return 0
    bad = anchors_ok()
    if bad:
        print(f"ERROR classes without a CIDOC-CRM anchor: {bad}")
        return 1
    b = Builder()
    b.collect_places(pages)
    for page in pages:
        b.sherd(page)
    b.memberships()
    dcat = b.dcat(pages)
    onto = ontology_graph()
    write(OUT_ONTOLOGY, onto.serialize("facem2lod ontology, generated by py/step_rdf.py from py/facem_model.py. Do not edit."))
    write(OUT_GRAPH, b.g.serialize(HEADER_GRAPH))
    write(OUT_DCAT, dcat.serialize("facem2lod DCAT description, generated by py/step_rdf.py. Do not edit."))
    print(f"rdf: {len(b.g)} triples in facem.ttl, {len(onto)} in the ontology, {len(dcat)} in the DCAT graph; "
          f"{len(b.fabrics)} fabrics, {sum(len(v) for v in b.fabrics.values())} sherds, {len(b.places)} places")
    # SHACL gate
    try:
        import pyshacl  # noqa: F401
    except ImportError:
        print("WARN pyshacl is not installed, the SHACL gate was skipped (pip install pyshacl)")
        return 0
    errs = shacl([OUT_GRAPH, OUT_DCAT, VOCAB_TTL, OUT_ONTOLOGY], sorted(SHAPES_DIR.glob("*.ttl")))
    for e in errs[:40]:
        print(f"ERROR SHACL {e}")
    if errs:
        print(f"ERROR rdf: {len(errs)} SHACL violations")
        return 1
    print("rdf: SHACL conforms (instance graph, DCAT graph)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
