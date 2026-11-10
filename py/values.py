"""Parsers for the structured values inside the FACEM fields (pure functions, no I/O, no clock).

    parse_dimensions   'diameter: 19cm (rim), 9cm (base); height: 5cm'
    parse_porosity     '2.5-7.5%; void form: channel;vughy; void length: 0.025-0.25'
    parse_inclusions   '20-25% %, length: 0.025-0.75'
    parse_inclusion_row 'singular, subrounded, subspherical, black, 0.025-0.125'
    derive_interval    'second half of 5th century CE' -> (450, 500)   (rule facem-chron-1)

The original text always stays with the data; what is derived here is added next to it, never in place of it.
"""
from __future__ import annotations

import re

CHRON_RULE = "facem-chron-1"
CHRON_RULE_TEXT = ("Century N spans the years (N-1)*100 to N*100. Within a century: first half = first 50 years, second half = "
                   "last 50, first third = first 33, second quarter = years 25 to 50, end or late = last 25 years, "
                   "beginning or early = first 25 years, mid = the year 50. A range takes the start of its first part and the "
                   "end of its last part; a slash joins both parts. A year number stands for itself. '?', '(?)' and '(+)' "
                   "are dropped and reported as uncertain.")

NUM = r"\d+(?:\.\d+)?"


def _num(text: str) -> float:
    v = float(text)
    return int(v) if v == int(v) else v


# ---------------------------------------------------------------- dimensions

def parse_dimensions(text: str) -> list:
    """-> [{'kind': 'diameter', 'part': 'rim' or '', 'value': 19, 'unit': 'cm'}, ...] (empty list if not understood)."""
    out = []
    for block in [b.strip() for b in text.split(";") if b.strip()]:
        if ":" not in block:
            return []
        kind, rest = [x.strip() for x in block.split(":", 1)]
        for piece in [p.strip() for p in rest.split(",") if p.strip()]:
            m = re.fullmatch(rf"({NUM})\s*(cm|mm|m)\s*(?:\(([^)]*)\))?", piece)
            if not m:
                return []
            out.append({"kind": kind.lower(), "part": (m.group(3) or "").strip().lower(),
                        "value": _num(m.group(1)), "unit": m.group(2)})
    return out


# ---------------------------------------------------------------- ranges

def parse_range(text: str):
    """'2.5-7.5' or '10' -> (min, max); None if not a number or range."""
    m = re.fullmatch(rf"\s*({NUM})\s*(?:[-–]\s*({NUM}))?\s*", text)
    if not m:
        return None
    lo = _num(m.group(1))
    return (lo, _num(m.group(2)) if m.group(2) else lo)


def parse_porosity(text: str) -> dict:
    """-> {'percent': (lo, hi) | None, 'void_forms': [...], 'void_length': (lo, hi) | None}"""
    parts = [p.strip() for p in text.split("; void form:", 1)]
    pct = re.match(rf"^({NUM}(?:\s*[-–]\s*{NUM})?)\s*%", parts[0])
    forms, length = [], None
    if len(parts) > 1:
        rest = parts[1]
        m = re.search(r"void length:\s*(.+)$", rest)
        if m:
            length = parse_range(m.group(1))
            rest = rest[:m.start()]
        forms = [t.strip() for t in rest.strip().rstrip(";").split(";") if t.strip()]
    return {"percent": parse_range(pct.group(1)) if pct else None, "void_forms": forms, "void_length": length}


def parse_inclusions(text: str) -> dict:
    """-> {'percent': (lo, hi) | None, 'length': (lo, hi) | None}"""
    pct = re.match(rf"^({NUM}(?:\s*[-–]\s*{NUM})?)\s*%", text)
    m = re.search(r"length:\s*(.+)$", text)
    return {"percent": parse_range(pct.group(1)) if pct else None,
            "length": parse_range(m.group(1)) if m else None}


def parse_inclusion_row(text: str) -> dict:
    """'frequency, roundness, shape, colour(;colour), size' -> parts; a missing size or colour stays empty."""
    cells = [c.strip() for c in text.split(",")]
    size = None
    if cells and parse_range(cells[-1]) is not None:
        size = parse_range(cells.pop())
    names = ["frequency", "roundness", "shape", "colour"]
    row = {n: [t.strip() for t in c.split(";") if t.strip()] for n, c in zip(names, cells)}
    row["size"] = size
    return row


# ---------------------------------------------------------------- chronology

QUAL = {  # qualifier -> (from, to) as offsets inside the century
    "first half": (0, 50), "second half": (50, 100), "first third": (0, 33), "second quarter": (25, 50),
    "end": (75, 100), "late": (75, 100), "beginning": (0, 25), "early": (0, 25), "mid": (50, 50),
}
PART = re.compile(r"^(?:(?P<q>first half|second half|first third|second quarter|end|late|beginning|early|mid)"
                  r"(?:\s+of\s+|-|\s+))?(?P<n>\d+)(?P<ord>st|nd|rd|th)?$")


def _part(text: str):
    m = PART.match(text.strip())
    if not m:
        return None
    n = int(m.group("n"))
    if not m.group("ord"):                       # a plain number is a year
        return (n, n) if not m.group("q") else None
    base = (n - 1) * 100
    lo, hi = QUAL[m.group("q")] if m.group("q") else (0, 100)
    return (base + lo, base + hi)


def split_chronology(text: str, period_labels) -> tuple:
    """'Roman-Late Antiquity second half of 5th century CE' -> ('Roman-Late Antiquity', 'second half of 5th century CE')"""
    for label in sorted(period_labels, key=len, reverse=True):
        if text.startswith(label):
            return label, text[len(label):].strip()
    return "", text.strip()


def derive_interval(text: str):
    """-> (start, end, uncertain) or None. Years CE, convention in CHRON_RULE_TEXT."""
    t = text.strip()
    uncertain = bool(re.search(r"\?|\(\+\)", t))
    t = re.sub(r"\(\+\)|\(\?\)|\?", "", t)
    t = re.sub(r"[()]", "", t)
    t = re.sub(r"\b(?:century|centuries|CE|AD)\b", "", t)
    t = re.sub(r"^\s*c\.\s*", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return None
    sides = [s.strip() for s in re.split(r"[–]", t)]
    if len(sides) > 2:
        return None
    spans = []
    for side in sides:
        parts = [_part(x) for x in side.split("/")]
        if any(p is None for p in parts):
            return None
        spans.append((min(p[0] for p in parts), max(p[1] for p in parts)))
    start, end = spans[0][0], spans[-1][1]
    if end < start:
        return None
    return start, end, uncertain
