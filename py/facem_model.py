"""The facem ontology as data: classes with their anchors, properties with domain and range.

One table is the single source for dist/facem-ontology.ttl, for the anchor test (every class reaches CIDOC-CRM) and for the
documentation. Namespace http://w3id.org/facem/ontology/ (prefix facem:). External terms are only reused by IRI, no owl:imports.
Names of the CRMsci properties still have to be compared with the specification (PRIMER, D10a); they are marked below.
"""
from __future__ import annotations

from ttl import Graph, I, L, XSD

NS = "http://w3id.org/facem/ontology/"
ENT = "http://w3id.org/facem/entity/"
PREFIXES = {
    "facem": NS, "ent": ENT, "vocab": "http://w3id.org/facem/vocab/",
    "crm": "http://www.cidoc-crm.org/cidoc-crm/", "crmsci": "http://www.ics.forth.gr/isl/CRMsci/",
    "sosa": "http://www.w3.org/ns/sosa/", "geo": "http://www.opengis.net/ont/geosparql#",
    "geolod": "http://w3id.org/geo-lod/", "skos": "http://www.w3.org/2004/02/skos/core#",
    "dcat": "http://www.w3.org/ns/dcat#", "dct": "http://purl.org/dc/terms/", "foaf": "http://xmlns.com/foaf/0.1/",
    "owl": "http://www.w3.org/2002/07/owl#", "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "xsd": XSD, "prov": "http://www.w3.org/ns/prov#",
    "qudt": "http://qudt.org/schema/qudt/", "unit": "http://qudt.org/vocab/unit/",
}
CRM, CRMSCI, SOSA, GEO, GEOLOD = (PREFIXES[k] for k in ("crm", "crmsci", "sosa", "geo", "geolod"))
SKOS, DCT = PREFIXES["skos"], PREFIXES["dct"]

# name -> (label, comment, anchors); anchors are the superclasses, the first one is the CIDOC-CRM (or extension) anchor
CLASSES = {
    "Sherd": ("Sherd", "A sherd of pottery with its FACEM description: form, type, dimensions, chronology.",
              [CRM + "E22_Human-Made_Object"]),
    "Sample": ("Sample", "A sample taken from a sherd for the analysis of the fabric.",
               [CRMSCI + "S13_Sample", SOSA + "Sample"]),
    "SampleTaking": ("Sample taking", "The removal of a sample from a sherd.", [CRMSCI + "S2_Sample_Taking"]),
    "Production": ("Production", "The supposed production of the sherds of one fabric at one place.", [CRM + "E12_Production"]),
    "Survey": ("Survey", "A survey in which the sherds were found, with the place and the year.", [CRM + "E7_Activity"]),
    "Place": ("Place", "A place of the FACEM map, with a coordinate and, where decided, a link to Wikidata.",
              [CRM + "E53_Place", GEO + "Feature"]),
    "TypeAssignment": ("Type assignment", "The assignment of a sherd to a concept of a typology (Hayes, Atlante, El Mahrine, Fulford, PM).",
                       [CRM + "E13_Attribute_Assignment"]),
    "DatingAssignment": ("Dating assignment", "The dating of a sherd: the original text, a period concept and a derived interval.",
                         [CRM + "E13_Attribute_Assignment"]),
    "Observation": ("Observation", "An observation of a sample: visual examination, binocular observation or thin section.",
                    [CRMSCI + "S4_Observation", SOSA + "Observation"]),
    "ObservableProperty": ("Observable property", "A property that is observed on a sample (colour of the fresh break, a kind of inclusion).",
                           [CRMSCI + "S9_Property_Type", SOSA + "ObservableProperty"]),
    "Dimension": ("Dimension", "A measured dimension of a sherd (diameter, height) with value and unit.", [CRM + "E54_Dimension"]),
    "Image": ("Image", "A photograph, thin-section image or drawing on facem.at.", [CRM + "E36_Visual_Item"]),
    "Publication": ("Publication", "A publication that documents the sherd, an assignment or an analysis.", [CRM + "E31_Document"]),
    "Person": ("Person", "A person named in the FACEM records (who took a sample, who analysed it).", [CRM + "E21_Person"]),
    "SourceRecord": ("Source record", "The FACEM page a statement comes from, with its address and the hash of the stored copy.",
                     [CRM + "E73_Information_Object", GEOLOD + "DataSource"]),
}

# name -> (label, comment, domain, range, kind, superproperties)   kind: 'o' object, 'd' datatype
P = {}


def prop(name, label, comment, domain, rng, kind="o", sup=()):
    P[name] = (label, comment, domain, rng, kind, list(sup))


F = lambda n: NS + n
prop("supposedPlace", "supposed place", "The supposed place of a production; FACEM calls it 'supposed site of production'.",
     F("Production"), F("Place"), sup=[CRM + "P7_took_place_at"])
prop("representativeSampleOf", "representative sample of", "Marks the sample that FACEM calls the representative sample of a fabric.",
     F("Sample"), SKOS + "Concept")
prop("ware", "ware", "The ware of a sherd.", F("Sherd"), SKOS + "Concept", sup=[CRM + "P2_has_type"])
prop("objectForm", "object form", "The form of the vessel.", F("Sherd"), SKOS + "Concept", sup=[CRM + "P2_has_type"])
prop("fragmentation", "fragmentation", "The preserved part of the vessel.", F("Sherd"), SKOS + "Concept", sup=[CRM + "P2_has_type"])
prop("surface", "surface", "The surface treatment.", F("Sherd"), SKOS + "Concept", sup=[CRM + "P2_has_type"])
prop("decoration", "decoration", "The description of the decoration, as written in FACEM.", F("Sherd"), XSD + "string", "d")
prop("registrationCode", "registration code", "The registration code of the object in the survey.", F("Sherd"), XSD + "string", "d")
prop("catalogueReference", "catalogue reference", "The place in the publication, for example 'cat. Bd.040, pl. 3'.", F("Sherd"), XSD + "string", "d")
prop("primarySource", "primary source", "The FACEM page the description of the sherd comes from.", F("Sherd"), F("SourceRecord"),
     sup=[CRM + "P70i_is_documented_in"])
prop("contextRegistration", "registration of the context", "The text of FACEM, for example 'surveyed in 1968'.", F("Survey"), XSD + "string", "d")
prop("contextInterpretation", "interpretation of the context", "How FACEM interprets the context (production).", F("Survey"), SKOS + "Concept")
prop("chronologyText", "chronology text", "The chronology exactly as written in FACEM.", F("DatingAssignment"), XSD + "string", "d")
prop("periodConcept", "period", "The period concept named in the chronology.", F("DatingAssignment"), SKOS + "Concept")
prop("intervalRule", "interval rule", "Identifier of the rule that turned the chronology text into the begin and end years of the time-span.",
     CRM + "E52_Time-Span", XSD + "string", "d")
prop("intervalUncertain", "interval uncertain", "True if the chronology text carries a question mark or a plus.", CRM + "E52_Time-Span", XSD + "boolean", "d")
prop("typeCertainty", "certainty", "How certain the assignment is, in the words of FACEM (certain, possible, uncertain, similar).",
     F("TypeAssignment"), SKOS + "Concept")
prop("typeQualifier", "qualifier", "A qualifier of the type, such as 'late' or 'no. 1'.", F("TypeAssignment"), SKOS + "Concept")
prop("alternativeTo", "alternative to", "Two assignments written with a slash inside one typology (Hayes 61A/B4).",
     F("TypeAssignment"), F("TypeAssignment"))
prop("concordantWith", "concordant with", "Two assignments written with a slash across typologies (Hayes 76/EM 10.1).",
     F("TypeAssignment"), F("TypeAssignment"))
prop("dimensionKind", "kind of dimension", "What was measured.", F("Dimension"), SKOS + "Concept")
prop("dimensionPart", "part measured", "The part of the sherd the dimension refers to (rim, base).", F("Dimension"), SKOS + "Concept")
prop("resultConcept", "result (concept)", "The result of an observation that is a vocabulary term.", F("Observation"), SKOS + "Concept")
prop("valueMin", "lowest value", "The lowest value of a range result.", F("Observation"), XSD + "decimal", "d")
prop("valueMax", "highest value", "The highest value of a range result (the same as the lowest if there is one value).",
     F("Observation"), XSD + "decimal", "d")
prop("valueUnit", "unit", "The unit of valueMin and valueMax, a QUDT unit.", F("Observation"), "http://qudt.org/schema/qudt/Unit")
prop("sizeMin", "lowest size", "Lowest length of the inclusions, in millimetres.", F("Observation"), XSD + "decimal", "d")
prop("sizeMax", "highest size", "Highest length of the inclusions, in millimetres.", F("Observation"), XSD + "decimal", "d")
prop("voidForm", "void form", "The form of the voids.", F("Observation"), SKOS + "Concept")
prop("frequency", "frequency", "How frequent the inclusions are.", F("Observation"), SKOS + "Concept")
prop("roundness", "roundness", "The roundness of the inclusions.", F("Observation"), SKOS + "Concept")
prop("shape", "shape", "The shape of the inclusions.", F("Observation"), SKOS + "Concept")
prop("colour", "colour", "The colour of the inclusions.", F("Observation"), SKOS + "Concept")
prop("imageUrl", "image address", "The address of the image on facem.at.", F("Image"), XSD + "anyURI", "d")
prop("imageRole", "image role", "What the image shows: macro, thin_section or drawing.", F("Image"), XSD + "string", "d")
prop("sourcePage", "source page", "The address of the FACEM page.", F("SourceRecord"), XSD + "anyURI", "d")
prop("sha256", "sha256", "The hash of the stored copy of the page (data/raw/harvest_manifest.json).", F("SourceRecord"), XSD + "string", "d")

CRM_LABELS = {  # labels for the external terms used, so that the file can be read on its own
    CRM + "E22_Human-Made_Object": "E22 Human-Made Object", CRM + "E12_Production": "E12 Production",
    CRM + "E7_Activity": "E7 Activity", CRM + "E53_Place": "E53 Place", CRM + "E13_Attribute_Assignment": "E13 Attribute Assignment",
    CRM + "E54_Dimension": "E54 Dimension", CRM + "E36_Visual_Item": "E36 Visual Item", CRM + "E31_Document": "E31 Document",
    CRM + "E21_Person": "E21 Person", CRM + "E73_Information_Object": "E73 Information Object", CRM + "E52_Time-Span": "E52 Time-Span",
    CRM + "P7_took_place_at": "P7 took place at", CRM + "P2_has_type": "P2 has type",
    CRM + "P70i_is_documented_in": "P70i is documented in",
    CRMSCI + "S13_Sample": "S13 Sample", CRMSCI + "S2_Sample_Taking": "S2 Sample Taking", CRMSCI + "S4_Observation": "S4 Observation",
    CRMSCI + "S9_Property_Type": "S9 Property Type", SOSA + "Sample": "Sample", SOSA + "Observation": "Observation",
    SOSA + "ObservableProperty": "Observable Property", GEO + "Feature": "Feature", GEOLOD + "DataSource": "geo-lod data source",
}


def anchors_ok() -> list:
    """Classes that do not reach CIDOC-CRM or an extension through their first anchor."""
    return [n for n, (_, _, a) in CLASSES.items()
            if not (a and (a[0].startswith(CRM) or a[0].startswith(CRMSCI)))]


def ontology_graph() -> Graph:
    g = Graph(PREFIXES)
    o = I(NS)
    g.type(o, PREFIXES["owl"] + "Ontology")
    g.add(o, DCT + "title", L("facem2lod ontology", lang="en"))
    g.add(o, DCT + "description", L("Classes and properties for the FACEM fabrics of African Red Slip Ware as Linked Open Data. "
                                    "Every class is anchored in CIDOC-CRM or one of its extensions; terms of other vocabularies "
                                    "are reused by IRI, nothing is imported.", lang="en"))
    g.add(o, PREFIXES["owl"] + "versionInfo", L("0.1"))
    g.add(o, PREFIXES["rdfs"] + "seeAlso", I("https://github.com/Research-Squirrel-Engineers/facem2lod"))
    for name, (label, comment, anchors) in CLASSES.items():
        c = NS + name
        g.type(c, PREFIXES["owl"] + "Class")
        g.add(c, PREFIXES["rdfs"] + "label", L(label, lang="en"))
        g.add(c, PREFIXES["rdfs"] + "comment", L(comment, lang="en"))
        g.add(c, PREFIXES["rdfs"] + "isDefinedBy", I(NS))
        for a in anchors:
            g.add(c, PREFIXES["rdfs"] + "subClassOf", I(a))
    for name, (label, comment, dom, rng, kind, sup) in P.items():
        p = NS + name
        g.type(p, PREFIXES["owl"] + ("ObjectProperty" if kind == "o" else "DatatypeProperty"))
        g.add(p, PREFIXES["rdfs"] + "label", L(label, lang="en"))
        g.add(p, PREFIXES["rdfs"] + "comment", L(comment, lang="en"))
        g.add(p, PREFIXES["rdfs"] + "isDefinedBy", I(NS))
        g.add(p, PREFIXES["rdfs"] + "domain", I(dom))
        g.add(p, PREFIXES["rdfs"] + "range", I(rng))
        for s in sup:
            g.add(p, PREFIXES["rdfs"] + "subPropertyOf", I(s))
    for iri, label in CRM_LABELS.items():
        g.add(iri, PREFIXES["rdfs"] + "label", L(label, lang="en"))
    return g
