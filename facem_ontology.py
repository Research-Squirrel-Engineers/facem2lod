"""
facem_ontology.py
-----------------
Generates the FACEM ontology as a Turtle file (facem_ontology.ttl).

Run directly:
    python facem_ontology.py

Or import from facem_pipeline.py / facem_run.py to share namespace constants.
Default output: next to this script file.
Callers (facem_run.py) can pass out_dir= to redirect.
"""

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

ONTOLOGY_FILE = "facem_ontology.ttl"  # written next to this script by default

# Canonical namespace URI strings — imported by facem_pipeline.py / facem_run.py
NS_FACEM  = "http://facem.at/"
NS_FACEMV = "http://facem.at/vocab/"
NS_CRM    = "http://www.cidoc-crm.org/cidoc-crm/"
NS_CRMSCI = "http://www.ics.forth.gr/isl/CRMsci/"
NS_BFO    = "http://purl.obolibrary.org/obo/"

# ──────────────────────────────────────────────────────────────────────────────

from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL, XSD, SKOS, DCTERMS

FACEM  = Namespace(NS_FACEM)
FACEMV = Namespace(NS_FACEMV)
CRM    = Namespace(NS_CRM)
CRMSCI = Namespace(NS_CRMSCI)
BFO    = Namespace(NS_BFO)
PROV   = Namespace("http://www.w3.org/ns/prov#")

ONTOLOGY_URI = URIRef("http://facem.at/ontology/")


def build_ontology() -> Graph:
    g = Graph()
    g.bind("owl",     OWL)
    g.bind("skos",    SKOS)
    g.bind("dcterms", DCTERMS)
    g.bind("crm",     CRM)
    g.bind("crmsci",  CRMSCI)
    g.bind("bfo",     BFO)
    g.bind("prov",    PROV)
    g.bind("facem",   FACEM)
    g.bind("facemv",  FACEMV)

    # ── Ontology header ───────────────────────────────────────────────────────
    g.add((ONTOLOGY_URI, RDF.type,        OWL.Ontology))
    g.add((ONTOLOGY_URI, RDFS.label,      Literal("FACEM Ontology", lang="en")))
    g.add((ONTOLOGY_URI, RDFS.comment,    Literal(
        "Ontology for the FACEM (Provenance Studies on Ancient Pottery in the "
        "Mediterranean) database. Maps fabric-type descriptions, archaeometric "
        "analyses, and petrographic observations to CIDOC-CRM, CRMsci, PROV-O "
        "and BFO.", lang="en")))
    g.add((ONTOLOGY_URI, OWL.versionInfo, Literal("0.2")))
    g.add((ONTOLOGY_URI, DCTERMS.creator, Literal("FACEM RDF Pipeline")))
    g.add((ONTOLOGY_URI, DCTERMS.license,
           URIRef("https://creativecommons.org/licenses/by/4.0/")))
    for ns in ["http://www.cidoc-crm.org/cidoc-crm/",
               "http://www.ics.forth.gr/isl/CRMsci/",
               "http://www.w3.org/ns/prov-o#"]:
        g.add((ONTOLOGY_URI, RDFS.seeAlso, URIRef(ns)))

    # ── Helpers ───────────────────────────────────────────────────────────────

    def cls(uri, label, comment, subclass_of=None):
        g.add((uri, RDF.type,         OWL.Class))
        g.add((uri, RDFS.label,       Literal(label, lang="en")))
        g.add((uri, RDFS.comment,     Literal(comment, lang="en")))
        g.add((uri, RDFS.isDefinedBy, ONTOLOGY_URI))
        parents = subclass_of if isinstance(subclass_of, list) else ([subclass_of] if subclass_of else [])
        for sc in parents:
            g.add((uri, RDFS.subClassOf, sc))

    def prop(uri, label, comment, domain=None, range_=None,
             subprop_of=None, prop_type=OWL.ObjectProperty):
        g.add((uri, RDF.type,         prop_type))
        g.add((uri, RDFS.label,       Literal(label, lang="en")))
        g.add((uri, RDFS.comment,     Literal(comment, lang="en")))
        g.add((uri, RDFS.isDefinedBy, ONTOLOGY_URI))
        if domain:     g.add((uri, RDFS.domain,        domain))
        if range_:     g.add((uri, RDFS.range,         range_))
        if subprop_of: g.add((uri, RDFS.subPropertyOf, subprop_of))

    def dprop(uri, label, comment, domain=None, range_=XSD.string, subprop_of=None):
        prop(uri, label, comment, domain=domain, range_=range_,
             subprop_of=subprop_of, prop_type=OWL.DatatypeProperty)

    # ── Classes ───────────────────────────────────────────────────────────────

    cls(FACEMV.FabricType,
        "Fabric Type",
        "A fabric type as defined in the FACEM classification system, "
        "representing the characteristic clay body composition of pottery "
        "produced at a specific site. Subclass of crm:E57_Material and "
        "bfo:BFO_0000040 (Material Entity).",
        subclass_of=[CRM.E57_Material, BFO["BFO_0000040"]])

    cls(FACEMV.CeramicObject,
        "Ceramic Object",
        "A human-made ceramic vessel or fragment studied in FACEM. "
        "Subclass of crm:E22_Human_Made_Object.",
        subclass_of=CRM.E22_Human_Made_Object)

    cls(FACEMV.CeramicSample,
        "Ceramic Sample",
        "A physical sample taken from a ceramic object for archaeometric or "
        "petrographic analysis. Subclass of crmsci:S10_Material_Substantial "
        "and bfo:BFO_0000040.",
        subclass_of=[CRMSCI.S10_Material_Substantial, BFO["BFO_0000040"]])

    cls(FACEMV.VisualExamination,
        "Visual Examination",
        "A macroscopic examination of the fresh break of a ceramic sample "
        "carried out with the naked eye. Subclass of crmsci:S4_Observation.",
        subclass_of=CRMSCI.S4_Observation)

    cls(FACEMV.BinocularObservation,
        "Binocular Stereoscope Observation",
        "An observation of a ceramic sample carried out under a binocular "
        "stereoscope, recording porosity, inclusion percentage, sorting and "
        "individual inclusion types. Subclass of crmsci:S4_Observation.",
        subclass_of=CRMSCI.S4_Observation)

    cls(FACEMV.PorosityMeasurement,
        "Porosity Measurement",
        "A measurement of the porosity of a ceramic sample body, expressed "
        "as a percentage with optional void-form and void-length description. "
        "Subclass of crmsci:S21_Measurement.",
        subclass_of=CRMSCI.S21_Measurement)

    cls(FACEMV.InclusionsMeasurement,
        "Inclusions Percentage Measurement",
        "A measurement of the total volume percentage of inclusions in a "
        "ceramic sample body. Subclass of crmsci:S21_Measurement.",
        subclass_of=CRMSCI.S21_Measurement)

    cls(FACEMV.ArchaeometricAnalysis,
        "Archaeometric Analysis",
        "An analytical activity (e.g. thin section, heavy mineral analysis) "
        "carried out on a ceramic sample to determine its material composition "
        "and provenance. Subclass of crm:E13_Attribute_Assignment and prov:Activity.",
        subclass_of=[CRM.E13_Attribute_Assignment, PROV.Activity])

    cls(FACEMV.PetrographicAnalysis,
        "Petrographic Analysis",
        "A petrographic and mineralogical examination of a ceramic sample, "
        "typically involving thin sections and heavy mineral analysis. "
        "Subclass of crmsci:S4_Observation and prov:Activity.",
        subclass_of=[CRMSCI.S4_Observation, PROV.Activity])

    cls(FACEMV.Inclusion,
        "Inclusion",
        "A non-plastic inclusion identified in the ceramic body during "
        "binocular stereoscope observation, described by type, frequency, "
        "shape, colour, and size range.")

    cls(FACEMV.PetrographicType,
        "Petrographic-Mineralogical Type",
        "A classification code assigned to a fabric type on the basis of "
        "petrographic-mineralogical analysis (e.g. PG-C4, RPG 03). "
        "Subclass of crm:E55_Type.",
        subclass_of=CRM.E55_Type)

    cls(FACEMV.SamplingEvent,
        "Sampling Event",
        "The activity of taking a physical sample from a ceramic object. "
        "Subclass of crm:E7_Activity and prov:Activity.",
        subclass_of=[CRM.E7_Activity, PROV.Activity])

    # ── Object properties ─────────────────────────────────────────────────────

    prop(FACEMV.hasFabricType,
         "has fabric type",
         "Associates a ceramic object with its assigned FACEM fabric type.",
         domain=FACEMV.CeramicObject, range_=FACEMV.FabricType,
         subprop_of=CRM.P45_consists_of)

    prop(FACEMV.hasSample,
         "has sample",
         "Associates a ceramic object with a physical sample taken from it.",
         domain=FACEMV.CeramicObject, range_=FACEMV.CeramicSample,
         subprop_of=CRM["P46_is_composed_of"])

    prop(FACEMV.hasInclusion,
         "has inclusion",
         "Associates a binocular observation with an identified inclusion.",
         domain=FACEMV.BinocularObservation, range_=FACEMV.Inclusion)

    prop(FACEMV.measuredProperty,
         "measured property",
         "Points to the property being measured (e.g. facemv:porosity).",
         domain=CRMSCI.S21_Measurement, range_=RDF.Property)

    # ── Datatype properties ───────────────────────────────────────────────────

    dprop(FACEMV.fabricTypeCode,
          "fabric type code",
          "The FACEM identifier code for a fabric type (e.g. BNAP-A-11).",
          domain=FACEMV.FabricType, subprop_of=SKOS.notation)

    dprop(FACEMV.ware, "ware",
          "The ware category of the ceramic object "
          "(e.g. Transport Amphorae, Terra Sigillata, Coarse Wares).",
          domain=FACEMV.CeramicObject)

    dprop(FACEMV.objectForm, "object form",
          "The morphological form of the ceramic object (e.g. Amphora, Cup, Bowl).",
          domain=FACEMV.CeramicObject)

    dprop(FACEMV.objectType, "object type",
          "The typological classification of the ceramic object "
          "(e.g. Dressel 2-4, Conspectus B 4.14).",
          domain=FACEMV.CeramicObject)

    dprop(FACEMV.fragmentation, "fragmentation",
          "Fragmentation state of the ceramic object "
          "(e.g. rim, base, fragmentation not specified).",
          domain=FACEMV.CeramicObject)

    dprop(FACEMV.surfaceDescription, "surface description",
          "Description of the surface treatment or slip of the ceramic object.",
          domain=FACEMV.CeramicObject)

    dprop(FACEMV.decoration, "decoration",
          "Description of the decorative features of the ceramic object.",
          domain=FACEMV.CeramicObject)

    dprop(FACEMV.contextRegistrationCode, "context registration code",
          "The excavation or context registration code for the find context.",
          domain=FACEMV.CeramicObject)

    dprop(FACEMV.munsellColour, "Munsell colour",
          "Munsell colour notation of the ceramic fresh break (e.g. 2.5YR 5/6).",
          domain=FACEMV.VisualExamination)

    dprop(FACEMV.texture, "texture",
          "Texture of the ceramic fresh break (e.g. granular, fine).",
          domain=FACEMV.VisualExamination)

    dprop(FACEMV.hardness, "hardness",
          "Hardness of the ceramic body as assessed macroscopically.",
          domain=FACEMV.VisualExamination)

    dprop(FACEMV.sorting, "sorting",
          "Sorting of inclusions in the ceramic matrix as observed under "
          "the binocular stereoscope.",
          domain=FACEMV.BinocularObservation)

    dprop(FACEMV.analysisMethod, "analysis method",
          "The analytical method applied in an archaeometric analysis "
          "(e.g. thin section and heavy mineral analysis).",
          domain=FACEMV.ArchaeometricAnalysis)

    dprop(FACEMV.inclusionType, "inclusion type",
          "The mineralogical or descriptive type of an inclusion "
          "(e.g. quartz, black inclusions, foraminifera).",
          domain=FACEMV.Inclusion)

    dprop(FACEMV.inclusionValue, "inclusion value (raw)",
          "The raw string value from FACEM describing an inclusion "
          "(frequency, shape, colour, size range).",
          domain=FACEMV.Inclusion)

    # ── Named individuals for measured properties ─────────────────────────────

    for ind, label, comment in [
        (FACEMV.porosity,
         "porosity",
         "Porosity of the ceramic body, expressed as a percentage."),
        (FACEMV.inclusionsPercent,
         "inclusions percentage",
         "Total volume percentage of non-plastic inclusions in the ceramic body."),
    ]:
        g.add((ind, RDF.type,     OWL.NamedIndividual))
        g.add((ind, RDF.type,     RDF.Property))
        g.add((ind, RDFS.label,   Literal(label, lang="en")))
        g.add((ind, RDFS.comment, Literal(comment, lang="en")))

    return g


def build_and_save(out_dir: "str | Path | None" = None) -> Path:
    """
    Build ontology and write to disk. Returns the output Path.
    Default (no argument): writes next to this script file.
    Callers (facem_run.py) can pass out_dir to redirect output.
    """
    target = Path(out_dir) if out_dir else Path(__file__).parent
    target.mkdir(parents=True, exist_ok=True)
    out_file = target / ONTOLOGY_FILE
    out_file.write_text(build_ontology().serialize(format="turtle"), encoding="utf-8")
    return out_file


if __name__ == "__main__":
    out_file = build_and_save()
    print(f"Ontology written → {out_file}  ({len(build_ontology())} triples)")
