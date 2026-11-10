"""Figure: the S6 model draft of facem2lod (classes, anchors, vocabularies), drawn on a fixed grid.

    python visuals/model_draft.py

Writes docs/visuals/img/s6-model-draft.svg and .png. The layout is fixed by hand: columns are the roles (events, objects,
assignments, vocabularies), every line is horizontal or vertical or has right angles, property labels sit above the line.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fig_kit import CLS, OWL, REAL, TERM, W, H, Fig, MUTED

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "docs" / "visuals" / "img"

# columns: left x, width
A = (40, 260)
B = (470, 280)
C = (920, 280)
D = (1400, 300)
BH = 54                                  # height of an ordinary box


def cx(col):
    return col[0] + col[1] / 2


def right(col):
    return col[0] + col[1]


def left(col):
    return col[0]


def build() -> Fig:
    f = Fig("s6-model-draft", "facem2lod data model: classes, anchors and vocabularies")
    f.text(40, 40, "facem2lod: data model", 30, True, anchor="start", maxw=420)
    f.legend(40, [(CLS, "class of facem:"), (REAL, "real-world object"), (TERM, "SKOS vocabulary"),
                  (OWL, "other vocabulary")], 900)

    def box(col, yc, title, sub, st, h=BH):
        f.box(col[0], yc - h / 2, col[1], h, title, sub, st)

    # ------------------------------------------------------------------ people, top
    Py = 112
    box((cx(B) - 140, 280), Py, "facem:Person", "crm:E21 Person", CLS)
    # ------------------------------------------------------------------ row 1: sample
    y1 = 222
    box(A, y1, "facem:SampleTaking", "crmsci:S2 Sample Taking", CLS)
    box(B, y1, "facem:Sample", "crmsci:S13, sosa:Sample", REAL)
    box(C, y1, "facem:Observation", "crmsci:S4, sosa:Observation", CLS)
    box(D, y1 - 30, "vocab: properties", "porosity, inclusions, colour ...", TERM, 48)
    box(D, y1 + 38, "vocab: method", "thin section, binocular ...", TERM, 48)
    f.line(right(A), y1, left(B), y1)
    f.chip((right(A) + left(B)) / 2, y1 - 24, "O5 removed")
    f.line(left(C), y1, right(B), y1)
    f.chip((left(C) + right(B)) / 2, y1 - 24, "feature of interest")
    f.path([(right(C), y1 - 10), (1300, y1 - 10), (1300, y1 - 30), (left(D), y1 - 30)])
    f.chip(1318, y1 - 58, "observed property")
    f.path([(right(C), y1 + 10), (1340, y1 + 10), (1340, y1 + 38), (left(D), y1 + 38)])
    f.chip(1290, y1 + 70, "procedure")
    # person edges: up and across
    f.path([(cx(A), y1 - BH / 2), (cx(A), Py), (cx(B) - 140, Py)])
    f.chip(cx(A) + 80, Py - 24, "P14 carried out by")
    f.path([(cx(C), y1 - BH / 2), (cx(C), Py), (cx(B) + 140, Py)])
    f.chip(cx(C) - 80, Py - 24, "P14 carried out by")

    # ------------------------------------------------------------------ sherd, the hub
    sy0, sy1 = 330, 790
    f.box(B[0], sy0, B[1], sy1 - sy0, "facem:Sherd", "crm:E22 Human-Made Object", REAL, ts=26, ss=18)
    f.line(cx(B), y1 + BH / 2, cx(B), sy0)
    f.chip(cx(B) + 66, (y1 + BH / 2 + sy0) / 2, "taken from")

    # left: events and fabric
    yp, yf, ys = 400, 540, 680
    box(A, yp, "facem:Production", "crm:E12 Production", CLS)
    box(A, yf, "vocab: fabric", "12 FACEM fabrics", TERM)
    box(A, ys, "facem:Survey", "crm:E7 Activity, type survey", CLS)
    f.line(right(A), yp, left(B), yp)
    f.chip((right(A) + left(B)) / 2, yp - 24, "P108 produced")
    f.line(left(B), yf, right(A), yf)
    f.chip((right(A) + left(B)) / 2, yf - 24, "P45 consists of")
    f.line(right(A), ys, left(B), ys)
    f.chip((right(A) + left(B)) / 2, ys - 24, "P12 present at")

    # place, below the sherd
    pyc = 852
    box(B, pyc, "facem:Place", "crm:E53 Place, geo:Feature", REAL, 76)
    # survey -> place (down, then right), production -> place (round the left edge)
    f.path([(cx(A), ys + BH / 2), (cx(A), pyc - 16), (left(B), pyc - 16)])
    f.chip(cx(A) + 90, pyc - 40, "P7 took place at")
    f.path([(left(A), yp), (22, yp), (22, pyc + 16), (left(B), pyc + 16)])
    f.chip(cx(A) + 90, pyc + 42, "P7 supposed place")

    # right: assignments and attachments of the sherd
    rows = [("facem:TypeAssignment", "crm:E13 Attribute Assignment", CLS),
            ("facem:DatingAssignment", "crm:E13, E52 Time-Span", CLS),
            ("facem:Dimension", "crm:E54 Dimension", CLS),
            ("facem:Publication", "crm:E31 Document", CLS),
            ("facem:Image", "crm:E36 Visual Item", CLS),
            ("facem:SourceRecord", "geolod:DataSource, E73", CLS)]
    ys_r = [376 + 72 * i for i in range(len(rows))]
    labels = ["P140 assigned to", "P140 assigned to", "P43 dimension", "P70 documents",
              "P138 represents", "primary source"]
    for (t, sub, st), y, lab in zip(rows, ys_r, labels):
        box(C, y, t, sub, st)
        if t in ("facem:TypeAssignment", "facem:DatingAssignment"):
            f.line(left(C), y, right(B), y)
        elif t == "facem:Dimension":
            f.line(right(B), y, left(C), y)
        else:
            f.line(left(C), y, right(B), y)
        f.chip((right(B) + left(C)) / 2, y - 24, lab)
    # onwards to vocabularies and units
    box(D, ys_r[0], "vocab: typologies", "hayes, atlante, el-mahrine ...", TERM)
    f.line(right(C), ys_r[0], left(D), ys_r[0])
    f.chip((right(C) + left(D)) / 2, ys_r[0] - 24, "P141 assigned")
    box(D, ys_r[1], "vocab: period", "Roman-Late Antiquity", TERM)
    f.line(right(C), ys_r[1], left(D), ys_r[1])
    f.chip((right(C) + left(D)) / 2, ys_r[1] - 24, "P141 assigned")
    box(D, ys_r[2], "qudt:Unit", "mm, cm, %", OWL)
    f.line(right(C), ys_r[2], left(D), ys_r[2])
    f.chip((right(C) + left(D)) / 2, ys_r[2] - 24, "P91 has unit")

    # place -> geometry
    box(C, pyc, "geo:Geometry", "WKT point", OWL, 54)
    f.line(right(B), pyc, left(C), pyc)
    f.chip((right(B) + left(C)) / 2, pyc - 24, "P168 defined by")

    # ------------------------------------------------------------------ DCAT band
    by = 962
    f.rect(40, 922, 1670, 72, "none", "#9a3412", 10, 1.6, True)
    f.text(56, 938, "DCAT 3 / DCAT-AP", 16, True, color="#9a3412", anchor="start")
    bx = [(200, 200, "dcat:DatasetSeries", "Release 10, ARS"),
          (560, 180, "dcat:Catalog", "FACEM"),
          (920, 220, "dcat:Dataset", "one per fabric"),
          (1320, 250, "dcat:Distribution", "source page, JSON, Turtle")]
    for x, w, t, sub in bx:
        f.box(x, by - 22, w, 44, t, sub, OWL, ts=19, ss=14)
    f.line(560, by, 400, by)
    f.line(740, by, 920, by)
    f.line(1140, by, 1320, by)
    f.text(480, by - 14, "dcat:seriesMember", 14, color=MUTED, maxw=150)
    f.text(830, by - 14, "dcat:dataset", 14, color=MUTED, maxw=150)
    f.text(1230, by - 14, "dcat:distribution", 14, color=MUTED, maxw=150)
    return f


def main() -> int:
    f = build()
    f.save(OUT)
    print(f"model draft: {OUT / 's6-model-draft.png'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
