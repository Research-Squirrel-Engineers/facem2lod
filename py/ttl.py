"""A small Turtle writer with a fixed order, so that the same data gives the same bytes.

Nodes are plain Python values:  I("http://...") is an IRI, L("text") a literal (with lang= or dt=), a str starting with
"_:" is not supported (the graphs here have no blank nodes). A Graph collects (s, p, o) and writes them sorted:
prefixes by name, subjects by IRI, rdf:type first, then predicates and objects by their written form.
rdflib is only used afterwards, to read the result back as a check.
"""
from __future__ import annotations

import re
from collections import OrderedDict

XSD = "http://www.w3.org/2001/XMLSchema#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
LOCAL = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")


class I(str):
    """An IRI."""
    __slots__ = ()


class L:
    __slots__ = ("value", "lang", "dt")

    def __init__(self, value, lang=None, dt=None):
        self.value, self.lang, self.dt = str(value), lang, dt

    def key(self):
        return (self.value, self.lang or "", self.dt or "")

    def __eq__(self, o):
        return isinstance(o, L) and self.key() == o.key()

    def __hash__(self):
        return hash(self.key())


def esc(text: str) -> str:
    s = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "").replace("\t", "\\t")
    return f'"{s}"'


class Graph:
    def __init__(self, prefixes: dict):
        self.prefixes = dict(prefixes)          # name -> namespace
        self.t = {}                             # s -> p -> set(o)

    def add(self, s, p, o):
        if o is None:
            return
        self.t.setdefault(str(s), {}).setdefault(str(p), set()).add(o)

    def type(self, s, cls):
        self.add(s, RDF_TYPE, I(cls))

    def __len__(self):
        return sum(len(v) for po in self.t.values() for v in po.values())

    # ---- writing
    def name(self, iri: str) -> str:
        best = None
        for pre, ns in self.prefixes.items():
            if iri.startswith(ns) and LOCAL.match(iri[len(ns):]):
                if best is None or len(ns) > len(best[1]):
                    best = (pre, ns)
        return f"{best[0]}:{iri[len(best[1]):]}" if best else f"<{iri}>"

    def term(self, o) -> str:
        if isinstance(o, I):
            return self.name(str(o))
        if o.lang:
            return f"{esc(o.value)}@{o.lang}"
        if o.dt:
            if o.dt in (XSD + "integer", XSD + "boolean") or (o.dt == XSD + "decimal" and "." in o.value):
                return o.value
            return f"{esc(o.value)}^^{self.name(o.dt)}"
        return esc(o.value)

    def serialize(self, header: str = "") -> str:
        out = []
        if header:
            out += [f"# {line}".rstrip() for line in header.splitlines()] + [""]
        for pre in sorted(self.prefixes):
            out.append(f"@prefix {pre}: <{self.prefixes[pre]}> .")
        out.append("")
        for s in sorted(self.t):
            po = self.t[s]
            lines = []
            preds = sorted(po, key=lambda p: (p != RDF_TYPE, self.name(p)))
            for p in preds:
                pn = "a" if p == RDF_TYPE else self.name(p)
                objs = sorted((self.term(o) for o in po[p]))
                lines.append(f"    {pn} " + " ,\n        ".join(objs))
            out.append(f"{self.name(s)}\n" + " ;\n".join(lines) + " .\n")
        return "\n".join(out).rstrip("\n") + "\n"
