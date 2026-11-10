"""Build the static pages in docs/ (offline, no clock, byte-stable).

Reads   dist/facem.ttl, dist/facem-ontology.ttl, dist/facem-vocabularies.ttl   (step_rdf.py, step_vocab.py)
        data/derived/vocab/vocabularies.json
Writes  docs/index.html, search.html, model.html, vocabularies.html, example.html, data.html, docs/assets/

The search page is one HTML file with the records embedded, so it also works from a local checkout. Images are not copied:
the cards load them from facem.at (the licence of the images is not yet clarified, PRIMER D1).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import rdflib

sys.path.insert(0, str(Path(__file__).resolve().parent))

import site_kit as kit
from facem_model import CLASSES, ENT, NS, P, PREFIXES
from facem_utils import DIST, ROOT, read_json
from site_kit import esc
from ttl import Graph

DOCS = ROOT / "docs"
IMG = DOCS / "visuals" / "img"
VOCAB_JSON = ROOT / "data" / "derived" / "vocab" / "vocabularies.json"
RAW = "https://raw.githubusercontent.com/Research-Squirrel-Engineers/facem2lod/main/dist/"
REPO = "https://github.com/Research-Squirrel-Engineers/facem2lod"
EXAMPLE = "m-200-10"

R = rdflib.URIRef
RDF_TYPE = rdflib.RDF.type
RDFS_LABEL = rdflib.RDFS.label
CRM = PREFIXES["crm"]
SOSA = PREFIXES["sosa"]


def f(name):
    return R(NS + name)


def c(name):
    return R(CRM + name)


class Data:
    def __init__(self):
        self.g = rdflib.Graph()
        self.g.parse(str(DIST / "facem.ttl"), format="turtle")
        self.vocab = read_json(VOCAB_JSON)
        self.label = {}
        self.scheme_of = {}
        for sid, v in self.vocab.items():
            for cc in v["concepts"]:
                self.label[cc["iri"]] = cc["label"]
                self.scheme_of[cc["iri"]] = sid

    def one(self, s, p):
        return next(iter(sorted(self.g.objects(s, p))), None)

    def many(self, s, p):
        return sorted(self.g.objects(s, p))

    def text(self, s, p):
        o = self.one(s, p)
        return str(o) if o is not None else ""

    def concept_labels(self, s, p):
        return [self.label.get(str(o), str(o)) for o in self.many(s, p)]


def records(d: Data) -> list:
    g = d.g
    out = []
    reps = {str(sa): str(fab) for sa in g.subjects(f("representativeSampleOf"), None)
            for fab in g.objects(sa, f("representativeSampleOf"))}
    for sh in sorted(g.subjects(RDF_TYPE, f("Sherd"))):
        sid = str(sh).rsplit("sherd_", 1)[1]
        sample = R(ENT + "sample_" + sid)
        rec = {"id": sid, "no": d.text(sh, RDFS_LABEL).replace("Sherd ", ""), "iri": str(sh),
               "fabric": "", "form": ", ".join(d.concept_labels(sh, f("objectForm"))),
               "part": ", ".join(d.concept_labels(sh, f("fragmentation"))),
               "surface": ", ".join(d.concept_labels(sh, f("surface"))),
               "decoration": d.text(sh, f("decoration")), "code": d.text(sh, f("registrationCode")),
               "rep": str(sample) in reps}
        fab = d.one(sh, c("P45_consists_of"))
        rec["fabric"] = d.label.get(str(fab), "") if fab else ""
        rec["page"] = ""
        src = d.one(sh, f("primarySource"))
        if src is not None:
            rec["page"] = d.text(src, f("sourcePage"))
        # types
        types = []
        for ta in sorted(g.subjects(c("P140_assigned_attribute_to"), sh)):
            if (ta, RDF_TYPE, f("TypeAssignment")) not in g:
                continue
            con = d.one(ta, c("P141_assigned"))
            types.append({"label": d.label.get(str(con), str(con)), "system": d.scheme_of.get(str(con), ""),
                          "certainty": (d.concept_labels(ta, f("typeCertainty")) or [""])[0]})
        rec["types"] = sorted(types, key=lambda t: (t["system"], t["label"]))
        # dating
        for da in g.subjects(c("P140_assigned_attribute_to"), sh):
            if (da, RDF_TYPE, f("DatingAssignment")) in g:
                rec["chron"] = d.text(da, f("chronologyText"))
                rec["period"] = (d.concept_labels(da, f("periodConcept")) or [""])[0]
                ts = d.one(da, c("P141_assigned"))
                if ts is not None:
                    rec["from"] = int(d.text(ts, c("P82a_begin_of_the_begin")))
                    rec["to"] = int(d.text(ts, c("P82b_end_of_the_end")))
        # place and survey
        for pr in g.subjects(c("P108_has_produced"), sh):
            pl = d.one(pr, f("supposedPlace"))
            rec["place"] = d.text(pl, RDFS_LABEL) if pl is not None else ""
        for sv in g.subjects(c("P12_occurred_in_the_presence_of"), sh):
            rec["survey"] = d.text(sv, RDFS_LABEL)
        rec["dims"] = "; ".join(d.text(x, RDFS_LABEL) for x in d.many(sh, c("P43_has_dimension")))
        pubs = sorted(d.text(p_, RDFS_LABEL) for p_ in g.subjects(c("P70_documents"), sh))
        ref = d.text(sh, f("catalogueReference"))
        rec["pub"] = ", ".join(pubs) + (", " + ref if ref else "")
        # observations of the sample
        kinds, porosity, incl, texture, hard, sorting = [], "", "", "", "", ""
        for ob in g.subjects(R(SOSA + "hasFeatureOfInterest"), sample):
            prop = d.one(ob, R(SOSA + "observedProperty"))
            name = d.label.get(str(prop), "") if prop else ""
            sch = d.scheme_of.get(str(prop), "") if prop else ""
            lo, hi = d.one(ob, f("valueMin")), d.one(ob, f("valueMax"))
            rng = "" if lo is None else (str(lo) if str(lo) == str(hi) else f"{lo}–{hi}") + " %"
            res = (d.concept_labels(ob, f("resultConcept")) or [""])[0]
            if sch == "inclusion-kind":
                kinds.append(name)
            elif name == "porosity":
                porosity = rng
            elif name == "inclusions":
                incl = rng
            elif name == "texture of fresh break":
                texture = res
            elif name == "hardness":
                hard = res
            elif name == "sorting":
                sorting = res
        rec.update({"kinds": sorted(kinds), "porosity": porosity, "inclusions": incl, "texture": texture, "hardness": hard,
                    "sorting": sorting})
        # images
        imgs = {"macro": [], "thin_section": [], "drawing": []}
        for im in list(g.subjects(c("P138_represents"), sh)) + list(g.subjects(c("P138_represents"), sample)):
            role = d.text(im, f("imageRole"))
            imgs.setdefault(role, []).append(d.text(im, f("imageUrl")))
        rec["img"] = {k: sorted(v) for k, v in imgs.items() if v}
        out.append(rec)
    return sorted(out, key=lambda r: [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", r["id"])])


def fabric_cards(recs) -> list:
    fabs = {}
    for r in recs:
        fabs.setdefault(r["fabric"], {"fabric": r["fabric"], "n": 0, "img": "", "place": r.get("place", "")})
        fabs[r["fabric"]]["n"] += 1
        if r["rep"] and r["img"].get("macro"):
            fabs[r["fabric"]]["img"] = r["img"]["macro"][0]
        elif not fabs[r["fabric"]]["img"] and r["img"].get("macro"):
            fabs[r["fabric"]]["img"] = r["img"]["macro"][0]
    return [fabs[k] for k in sorted(fabs)]


# ---------------------------------------------------------------- pages

SEARCH_JS = r"""<script>
(function(){
var D=JSON.parse(document.getElementById("data").textContent),R=D.records,F=D.fabrics;
var st={fab:{},form:{},sys:{},place:{},q:"",from:"",to:"",img:false};
function uniq(fn){var m={};R.forEach(function(r){[].concat(fn(r)).forEach(function(v){if(v)m[v]=(m[v]||0)+1})});return Object.keys(m).sort().map(function(k){return[k,m[k]]})}
function group(id,title,items,key){var h='<section><h3>'+title+'</h3>';items.forEach(function(it){h+='<label><input type="checkbox" data-g="'+key+'" value="'+it[0].replace(/"/g,"&quot;")+'"><span>'+it[0]+'</span><span class="c">'+it[1]+'</span></label>'});document.getElementById(id).innerHTML=h+'</section>'}
group("g-fab","Fabric",uniq(function(r){return r.fabric}),"fab");
group("g-form","Form",uniq(function(r){return r.form}),"form");
group("g-sys","Typology",uniq(function(r){return r.types.map(function(t){return t.system})}),"sys");
group("g-place","Place",uniq(function(r){return r.place}),"place");
document.getElementById("fabs").innerHTML=F.map(function(f){return '<button type="button" class="fab" data-f="'+f.fabric+'"><div class="ph">'+(f.img?'<img loading="lazy" referrerpolicy="no-referrer" src="'+f.img+'" alt="" onerror="this.parentNode.textContent=\'image not available\'">':'no image')+'</div><div class="b"><b>'+f.fabric+'</b><br><span class="m">'+f.n+' sherds</span></div></button>'}).join("");
function blob(r){return(r.no+" "+r.fabric+" "+r.form+" "+r.part+" "+r.surface+" "+r.decoration+" "+r.code+" "+r.chron+" "+r.place+" "+r.pub+" "+r.kinds.join(" ")+" "+r.types.map(function(t){return t.label}).join(" ")).toLowerCase()}
R.forEach(function(r){r.b=blob(r)});
function on(m){return Object.keys(m).filter(function(k){return m[k]})}
function ok(r){
 var a=on(st.fab);if(a.length&&a.indexOf(r.fabric)<0)return false;
 a=on(st.form);if(a.length&&a.indexOf(r.form)<0)return false;
 a=on(st.place);if(a.length&&a.indexOf(r.place)<0)return false;
 a=on(st.sys);if(a.length&&!r.types.some(function(t){return a.indexOf(t.system)>=0}))return false;
 if(st.q&&st.q.toLowerCase().split(/\s+/).some(function(w){return w&&r.b.indexOf(w)<0}))return false;
 if(st.from!==""&&r.to<Number(st.from))return false;
 if(st.to!==""&&r.from>Number(st.to))return false;
 if(st.img&&!(r.img.macro||r.img.thin_section||r.img.drawing))return false;
 return true}
function card(r){var u=(r.img.macro||r.img.drawing||r.img.thin_section||[])[0];
 var ty=r.types.map(function(t){return t.label+(t.certainty&&t.certainty!=="certain"?" ("+t.certainty+")":"")}).join(" / ");
 return '<div class="card"><div class="ph">'+(u?'<img loading="lazy" referrerpolicy="no-referrer" src="'+u+'" alt="" onerror="this.parentNode.textContent=\'image not available\'">':'no image')+'</div><div class="b"><div class="t">'+r.no+' · '+r.fabric+'</div>'
 +'<div>'+[r.form,r.part].filter(Boolean).join(", ")+'</div><div>'+ty+'</div><div class="m">'+(r.chron||"")+'</div><div class="m">'+(r.place||"")+(r.dims?" · "+r.dims:"")+'</div>'
 +'<div class="m">'+(r.kinds.length?"inclusions: "+r.kinds.join(", "):"")+'</div>'
 +'<div><a href="'+r.page+'">FACEM page</a></div></div></div>'}
function draw(){var rs=R.filter(ok);document.getElementById("count").textContent=rs.length+" of "+R.length+" sherds";
 document.getElementById("res").innerHTML=rs.map(card).join("");
 document.querySelectorAll(".fab").forEach(function(b){b.classList.toggle("on",!!st.fab[b.dataset.f])})}
document.addEventListener("change",function(e){var t=e.target,g=t.dataset.g;if(g){st[g][t.value]=t.checked;draw()}else if(t.id==="img"){st.img=t.checked;draw()}});
document.addEventListener("input",function(e){var t=e.target;if(t.id==="q"){st.q=t.value;draw()}if(t.id==="from"){st.from=t.value;draw()}if(t.id==="to"){st.to=t.value;draw()}});
document.addEventListener("click",function(e){var b=e.target.closest(".fab");if(!b)return;st.fab[b.dataset.f]=!st.fab[b.dataset.f];
 document.querySelectorAll('input[data-g="fab"]').forEach(function(i){i.checked=!!st.fab[i.value]});draw()});
draw();
})();
</script>"""


def search_page(recs) -> str:
    data = {"records": recs, "fabrics": fabric_cards(recs)}
    blob = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).replace("</", "<\\/")
    body = (f'<script id="data" type="application/json">{blob}</script>\n'
            '<div id="fabs" class="fabrics"></div>\n'
            '<div class="finder"><div class="filters">'
            '<section><h3>Search</h3><input type="text" id="q" placeholder="Hayes 67, quartz, rim, Oudna ..."></section>'
            '<section><h3>Dated between (CE)</h3><div class="years"><input type="number" id="from" placeholder="from">'
            '<input type="number" id="to" placeholder="to"></div></section>'
            '<div id="g-fab"></div><div id="g-form"></div><div id="g-sys"></div><div id="g-place"></div>'
            '<section><label><input type="checkbox" id="img"><span>only with images</span></label></section></div>'
            '<div><p class="count" id="count"></p><div class="cards" id="res"></div></div></div>\n'
            '<blockquote><p>The images are loaded from facem.at and are not copied here; the terms of use of the images are not '
            'yet clarified. The dating filter uses the interval derived from the FACEM text (rule facem-chron-1), the original '
            'text is on every card.</p></blockquote>')
    return kit.page("search.html", "Search the sherds",
                    "All 60 sherds of the 12 FACEM fabrics. Click a fabric above or combine the filters; everything runs in your browser.",
                    body, SEARCH_JS)


def stats(g: rdflib.Graph, recs) -> list:
    n = lambda cls: sum(1 for _ in g.subjects(RDF_TYPE, f(cls)))
    return [("sherds", len(recs)), ("fabrics", len(fabric_cards(recs))), ("observations", n("Observation")),
            ("type assignments", n("TypeAssignment")), ("places", n("Place")), ("triples", len(g))]


def index_page(d: Data, recs) -> str:
    st = "".join(f"<div><b>{v:,}</b><span>{esc(k)}</span></div>" for k, v in stats(d.g, recs))
    body = (f'<div class="stat">{st}</div>\n<section class="text"><h2>What this is</h2>'
            '<p>FACEM (<a href="https://facem.at">facem.at</a>) describes the fabrics of African Red Slip Ware found in Tunisia: '
            'for each fabric a representative sample and further samples, each with the sherd it was taken from, its form, type, '
            'dating, dimensions and the observations of the fabric under the binocular and in thin section. facem2lod turns the '
            'public pages into Linked Open Data: an ontology anchored in CIDOC-CRM, SKOS vocabularies for every controlled value, '
            'an instance graph, and a DCAT 3 description of the datasets.</p>'
            '<h2>Where to start</h2><ul>'
            '<li><a href="search.html">Search</a> the sherds by fabric, form, type, period and place.</li>'
            '<li>The <a href="model.html">data model</a>: classes, anchors and properties.</li>'
            '<li>The <a href="vocabularies.html">vocabularies</a> with their concepts and counts.</li>'
            '<li>One <a href="example.html">sherd</a> as the triples that describe it.</li>'
            f'<li>The files in <a href="data.html">Downloads</a>, the code on <a href="{REPO}">GitHub</a>.</li></ul>'
            '<blockquote><p>Work in progress. The terms of use of the FACEM data are being clarified with the editors; '
            'until then the generated files are published for review, not as a release.</p></blockquote></section>')
    return kit.page("index.html", "FACEM as Linked Open Data",
                    "Fabrics of African Red Slip Ware from facem.at, as CIDOC-CRM, SKOS and DCAT.", body)


def model_page() -> str:
    rows = "".join(
        f"<tr><td><code>facem:{esc(n)}</code></td><td>{esc(com)}</td><td>"
        + "<br>".join(f"<code>{esc(short(a))}</code>" for a in anch) + "</td></tr>"
        for n, (lab, com, anch) in CLASSES.items())
    prow = "".join(
        f"<tr><td><code>facem:{esc(n)}</code><small>{esc(lab)}</small></td><td>{esc(com)}</td>"
        f"<td><code>{esc(short(dom))}</code> → <code>{esc(short(rng))}</code></td>"
        f"<td>{'<br>'.join('<code>%s</code>' % esc(short(x)) for x in sup)}</td></tr>"
        for n, (lab, com, dom, rng, kind, sup) in P.items())
    body = (kit.fig_card(IMG, "s6-model-draft", "Model", "Classes, anchors and vocabularies of facem2lod",
                         "Colours: orange facem class, green real-world object, purple SKOS vocabulary, white other vocabulary, amber property.")
            + '<section class="text"><h2>Classes</h2><div class="tbl"><table><tr><th>Class</th><th>Meaning</th><th>Anchors</th></tr>'
            + rows + "</table></div><h2>Properties</h2><div class=\"tbl\"><table><tr><th>Property</th><th>Meaning</th><th>Domain → range</th>"
            "<th>Sub-property of</th></tr>" + prow + "</table></div></section>")
    return kit.page("model.html", "Data model",
                    "Every class is anchored in CIDOC-CRM or one of its extensions; terms of other vocabularies are reused by IRI.", body)


def short(iri: str) -> str:
    best = None
    for pre, ns in PREFIXES.items():
        if iri.startswith(ns) and (best is None or len(ns) > len(best[1])):
            best = (pre, ns)
    return f"{best[0]}:{iri[len(best[1]):]}" if best else iri


def vocab_page(d: Data) -> str:
    rows, det = [], []
    for sid in sorted(d.vocab):
        v = d.vocab[sid]
        rows.append(f"<tr><td><code>{esc(sid)}</code></td><td>{esc(v['title'])}</td><td>{len(v['concepts'])}</td></tr>")
        items = "".join(f"<tr><td>{esc(cc['label'])}</td><td><code>{esc(cc['notation'])}</code></td><td>{cc['count']}</td>"
                        f"<td>{esc(', '.join(m['property'] + ' ' + m['target'].rsplit('/', 1)[-1] for m in cc['match']))}</td></tr>"
                        for cc in v["concepts"])
        det.append(f"<details><summary>{esc(v['title'])} ({len(v['concepts'])})</summary><div class=\"tbl\"><table>"
                   f"<tr><th>Label</th><th>Notation</th><th>Pages</th><th>Links</th></tr>{items}</table></div></details>")
    body = ('<section class="text"><h2>Schemes</h2><div class="tbl"><table><tr><th>Scheme</th><th>Title</th><th>Concepts</th></tr>'
            + "".join(rows) + "</table></div><h2>Concepts</h2>" + "".join(det)
            + "<p class=\"cr\">Labels are the wording of FACEM, nothing is corrected. IRIs: "
              "<code>http://w3id.org/facem/vocab/&lt;scheme&gt;/&lt;notation&gt;</code> (not yet registered).</p></section>")
    return kit.page("vocabularies.html", "Vocabularies",
                    f"{len(d.vocab)} SKOS concept schemes: the typologies, the fabrics and every other controlled value.", body)


def example_page(d: Data) -> str:
    g = d.g
    gg = Graph(PREFIXES)
    seen, todo, order = set(), [R(ENT + "sherd_" + EXAMPLE)], []
    while todo:
        s = todo.pop(0)
        if s in seen:
            continue
        seen.add(s)
        order.append(s)
        for p_, o in sorted(g.predicate_objects(s)):
            if isinstance(o, rdflib.URIRef) and str(o).startswith(ENT) and o not in seen and "place_" not in str(o) \
                    and "publication_" not in str(o) and "person_" not in str(o) and "dataset_" not in str(o):
                todo.append(o)
        # things that point at the sherd or its sample
        if str(s).endswith("sherd_" + EXAMPLE) or str(s).endswith("sample_" + EXAMPLE):
            for p_ in (c("P140_assigned_attribute_to"), c("P108_has_produced"), c("P12_occurred_in_the_presence_of"),
                       R(SOSA + "isSampleOf"), R(SOSA + "hasFeatureOfInterest"), c("O5_removed"), c("P138_represents")):
                for sub in sorted(g.subjects(p_, s)):
                    if sub not in seen and str(sub).startswith(ENT):
                        todo.append(sub)
            for sub in sorted(g.subjects(R(PREFIXES["crmsci"] + "O5_removed"), s)):
                todo.append(sub)
    blocks = []
    for s in order:
        rows = []
        for p_, o in sorted(g.predicate_objects(s), key=lambda po: (str(po[0]) != str(RDF_TYPE), short(str(po[0])), str(po[1]))):
            ov = short(str(o)) if isinstance(o, rdflib.URIRef) else str(o)
            if isinstance(o, rdflib.Literal) and o.datatype:
                ov += "  [" + short(str(o.datatype)) + "]"
            rows.append(f"<tr><td><code>{esc('a' if p_ == RDF_TYPE else short(str(p_)))}</code></td><td>{esc(ov)}</td></tr>")
        blocks.append(f"<h3><code>{esc(short(str(s)))}</code></h3><div class=\"tbl\"><table>{''.join(rows)}</table></div>")
    body = '<section class="text">' + "".join(blocks) + "</section>"
    return kit.page("example.html", f"Example: sherd {EXAMPLE.upper().replace('-', ' ', 1).replace('-', '/')}",
                    "Everything the graph says about one sherd and its sample, one block per node. Vocabulary terms are written as IRIs; "
                    "<code>ent:</code> is <code>http://w3id.org/facem/entity/</code>.", body)


def data_page() -> str:
    files = [("facem.ttl", "Instance graph: sherds, samples, observations, assignments, places"),
             ("facem-ontology.ttl", "Ontology: classes and properties"),
             ("facem-vocabularies.ttl", "SKOS vocabularies"),
             ("facem-dcat.ttl", "DCAT 3 description: catalogue, series, datasets, distributions")]
    rows = "".join(f'<tr><td><a href="{RAW}{n}">{n}</a></td><td>{esc(t)}</td></tr>' for n, t in files)
    body = ('<section class="text"><div class="tbl"><table><tr><th>File</th><th>Content</th></tr>' + rows + "</table></div>"
            f'<p>All files are Turtle, generated by <code>python main.py</code>; the source is on <a href="{REPO}">GitHub</a>. '
            "They are written without timestamps, so the same input gives the same bytes.</p></section>")
    return kit.page("data.html", "Downloads", "The generated graphs as Turtle.", body)


def run() -> int:
    if not (DIST / "facem.ttl").exists() or not VOCAB_JSON.exists():
        print("site: dist/facem.ttl is missing (run 'python main.py' first)")
        return 0
    d = Data()
    recs = records(d)
    kit.write_assets(DOCS)
    pages = {"index.html": index_page(d, recs), "search.html": search_page(recs), "model.html": model_page(),
             "vocabularies.html": vocab_page(d), "example.html": example_page(d), "data.html": data_page()}
    for name, text in pages.items():
        kit.write_page(DOCS / name, text)
    no_img = [r["no"] for r in recs if not r["img"]]
    print(f"site: {len(pages)} pages, {len(recs)} records in the search index, {len(no_img)} sherds without an image")
    return 0


if __name__ == "__main__":
    sys.exit(run())
