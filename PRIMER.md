# PRIMER – facem2lod

Arbeitsplan für facem2lod: die FACEM-Datenbank (facem.at, Keramik-Fabrics) als Linked Open Data, mit Brücke zu geo-lod. Wird zu Beginn jedes Chats komplett hochgeladen und am Ende zurückgeschrieben. Teil A gilt immer, Teil B ist die Übersicht, Teil C sind die Schritte, Teil D sind offene Punkte.

---

## Teil A – Immer gültig

### A1 Ausgangslage

| Was | Wo | Rolle |
|---|---|---|
| Quelle | `https://facem.at` | Keramik-Fabrics, Release 10 (November 2026): African Red Slip Ware aus Tunesien |
| Erster Versuch | `facem_run.py`, `facem_ontology.py`, `facem_pipeline.py`, `output/` | Legacy, bleibt unverändert bis S6 |
| Skeleton | `main.py`, `py/facem_utils.py` | Orchestrator, Konstanten, kanonische Writer |
| Harvest | `py/step_harvest.py` | einziger Schritt mit Netzwerk; legt HTML unverändert in `data/raw/html/` ab |
| Parser | `py/step_parse.py` | HTML → JSON in `data/derived/json/`, Feldkatalog in `data/derived/field_catalogue.json` |
| Tests | `tests/` | Parser gegen synthetische Fixture |
| Schwesterprojekt | `GeoScience-FAIRification-LOD` (geo-lod) | gleiche Muster: CRM, CRMsci, SOSA, QUDT, GeoSPARQL, SHACL |

**Befunde** (aus dem, was tatsächlich gesehen wurde):

- 2026-11-10: Das Repo hat einen Commit (6f04b3e, 2026-03-21), ein leeres README und drei Skripte mit zusammen rund 900 Zeilen.
- 2026-11-10: `output/facem_records.json` enthält für `bnap-a-11` den Wert `tab_count: 0`, alle vier Blöcke leer. Das `facem.ttl` besteht deshalb nur aus Platzhaltern (`BNAP-A-11-sample`). Der erste Lauf hat also keine Tabellen gefunden. Ursache ungeklärt: fehlender `lxml`-Parser oder geänderte Markup-Struktur.
- 2026-11-10: facem.at wurde 2024–2026 neu gebaut (Wiener Universitäts-IT, FAIR-Ausrichtung). Auf den Seiten ist weder ein Export (JSON, CSV, API) noch eine Lizenzangabe sichtbar; der Zitiervorschlag lautet „FACEM - http://facem.at/<id>". Geprüft über die gerenderte Textdarstellung, nicht über das rohe HTML.
- 2026-11-10: Die Suchseite nennt für ARS zwölf Fabrics: `BD-ARS-1..3` (Bordj el Djerbi), `HB-ARS-1..3` (Henchir el Biar), `OU-ARS-1..2` (Oudna), `PH-ARS-1..4` (Pheradi Maius). Die Fabric-IDs folgen dem Muster `ORT-KATEGORIE-NUMMER`.
- 2026-11-10: `OU-ARS-1` zeigt (gerenderter Text) Felder wie Hayes-Typ (Hayes 99B), Chronologie als Zeitspanne (ca. 530–580 n. Chr.), Publikation (Hasenzagl 2019, Kat. Ou.023), Binokular-Werte (Porosität 7,5 %, Magerung 25 %) und Dünnschliff-Analyse (Claudio Capelli). Einen petrographischen Typ nennt die Seite nicht; das Feld kann also fehlen.
- 2026-11-10: Die Karte führt Produktionsorte über `map/production_site.php?id=…`, Filter über `map/?type=production` und `map/?type=discovery`. Ob Koordinaten im HTML stehen, ist offen.
- 2026-11-10: Aus der Cloud-Sandbox ist facem.at nicht erreichbar (Proxy antwortet 403). Der Harvest läuft deshalb lokal bei dir. Der Parser ist bisher nur gegen eine synthetische Fixture getestet, nicht gegen echtes Markup.

### A2 Zielbild

```
facem.at ──harvest──► data/raw/html/*.html (unverändert, mit harvest_manifest.json)
                           │
                         parse
                           ▼
              data/derived/json/*.json + field_catalogue.json
                           │   (lokal ansehen: S3)
                  Wikidata-Kandidaten (S4) · SKOS-Vokabulare (S5)
                           ▼
              Ontologie + RDF-Builder (S6) ──► dist/*.ttl (SHACL-geprüft)
                           │
                  Brücke zu geo-lod (S7) · Doku- und Figure-Seiten (S8)
```

Eigenschaften, die das Ergebnis haben muss:

1. Jede Rohdatei unter `data/raw/` ist unverändert, wie der Server sie lieferte; `harvest_manifest.json` hält URL und SHA-256.
2. `python main.py` zweimal hintereinander: `git status` bleibt beim zweiten Mal sauber.
3. Ohne Netzwerk läuft alles außer `harvest`.
4. Jedes Feld der Quelle taucht im Feldkatalog auf; was nicht modelliert wird, ist als verworfen vermerkt (S3).
5. Jede erzeugte IRI liegt unter `http://w3id.org/facem/` und hat ein `rdfs:seeAlso` auf die FACEM-Seite.

### A3 Querschnittsregeln

- Rohdaten liegen unter `data/raw/`, unverändert. Was ein Skript daraus macht, geht nach `data/derived/` oder `dist/`.
- Keine Uhr in der Ausgabe: kein `datetime.now()`, Datum nur über `RELEASE` in `py/facem_utils.py`.
- Zweimal laufen lassen, `git status` bleibt beim zweiten Mal sauber.
- Netzwerk nur im Schritt `harvest`, mit eigenem Verb, nie im Standardlauf.
- Parameter stehen im CONFIG-Block am Skriptanfang, nicht als Kommandozeilenargumente.
- Wiederverwendung heißt kopieren, nicht referenzieren; Ausnahme ist das geteilte Core-Vokabular, das per w3id-IRI importiert wird.
- Alles Nach-außen-Sichtbare (Code, Kommentare, README, Ontologie) ist British English; `PRIMER.md` ist Deutsch.
- Befehle für Windows `cmd`, je Zeile ein Befehl, zusammengehörige Befehle mit `&&` verkettet.
- Ein Thema pro Chat, ein Repo pro Chat.
- Lieferung als Patch-ZIP (Skill `patch-zip-delivery`), nur Quellen, erzeugte Dateien werden per Befehl neu gebaut.

### A4 Beschlusslage

| Frage | Beschluss | seit |
|---|---|---|
| Crawl-Umfang | zunächst nur die 12 ARS-Fabrics; Liste und optionale Such-URL im CONFIG-Block von `step_harvest.py` | 2026-11-10 |
| Rohdaten | HTML-Snapshot (`data/raw/html/`) plus geparstes JSON (`data/derived/json/`) | 2026-11-10 |
| Parser | generisch: Tabellen, Überschriften, Links, Bilder und Textzeilen komplett ins JSON, keine festen Feldnamen | 2026-11-10 |
| Namespace | eigenständig unter `w3id.org/facem/` | 2026-11-10 |
| IRI-Schema | `http://w3id.org/facem/entity/<typ>_<id>` (http, wie bei geo-lod) | Vorschlag 2026-11-10 |
| Orte und Perioden | Wikidata-Kandidaten per Skript vorschlagen, du bestätigst lokal in einer CSV | 2026-11-10 |
| GeoNames und OSM | nicht direkt gesucht, sondern über Wikidata-Properties (GeoNames-ID P1566, OSM-Relation P402), wo vorhanden | Vorschlag 2026-11-10 |
| Hayes-Typen | als SKOS-Konzeptschema statt Freitext | 2026-11-10 |
| Zeitperioden | als eigene SKOS-Konzepte (Zeitspannen) mit Wikidata-Link, wo möglich | 2026-11-10 |
| Legacy-Skripte | `facem_*.py` und `output/` bleiben bis S6 unverändert | Vorschlag 2026-11-10 |
| Brücke zu geo-lod | Core-Vokabular per IRI importieren; Messungen nach SOSA/QUDT; Produktionsorte als GeoSPARQL-FeatureCollection | Vorschlag 2026-11-10 |
| Abrufdatum | kein Datum im Manifest (wäre eine Uhr); der Abruf ist über den Commit datiert | Vorschlag 2026-11-10 |
| Lizenz der Quelle | ungeklärt, die Seiten nennen keine; vor Veröffentlichung der Daten bei den Herausgebenden nachfragen | offen |

### A5 Was in welchem Chat hochgeladen wird

`PRIMER.md` plus ein Bundle der Quellen. Die HTML-Snapshots gehören dazu, weil sich Parser-Probleme nur an echtem Markup zeigen:

```cmd
powershell -NoProfile -Command "Compress-Archive -Path py,tests,data,main.py,PRIMER.md,requirements.txt,facem_run.py,facem_ontology.py,facem_pipeline.py -DestinationPath bundle.zip -Force"
```

Nicht hochladen: `.venv/`, `.git/`, `bundle.zip`.

### A6 IRI-Landkarte

| Pfad | Inhalt | Ziel des Redirects | Status |
|---|---|---|---|
| `http://w3id.org/facem/ontology/` | Ontologie | GitHub Pages dieses Repos | beschlossen (nicht registriert) |
| `http://w3id.org/facem/entity/` | Instanzen (Fabric, Probe, Objekt, Ort, Analyse) | GitHub Pages | geplant |
| `http://w3id.org/facem/vocab/hayes/` | Hayes-Typen (SKOS) | GitHub Pages | geplant |
| `http://w3id.org/facem/vocab/period/` | Zeitperioden (SKOS) | GitHub Pages | geplant |

Offen: w3id-Registrierung (Pull Request auf `perma-id/w3id.org`), Content Negotiation für Turtle und HTML, Verhältnis zu `geo-lod` (selbst noch nicht registriert).

---

## Teil B – Schrittübersicht

| ID | Schritt | Repo | hängt ab von | Status |
|---|---|---|---|---|
| S0 | Festlegungen: Umfang, Rohdaten, Namespace, Wikidata | facem2lod | – | erledigt 2026-11-10 |
| S1 | Skeleton: `main.py`, `facem_utils.py`, Requirements, README, Citation | facem2lod | S0 | erledigt 2026-11-10 |
| S2 | Scraper: `harvest` und `parse` für die 12 ARS-Fabrics | facem2lod | S1 | offen |
| S3 | JSONs lokal ansehen, Modellierungsliste mit Feldkatalog | facem2lod | S2 | offen |
| S4 | Wikidata-Kandidaten für Orte und Perioden (CSV) | facem2lod | S3 | offen |
| S5 | SKOS-Vokabulare: Hayes-Typen, Perioden, Einschlüsse | facem2lod | S3, S4 | offen |
| S6 | Ontologie und RDF-Builder neu (offline aus JSON), SHACL | facem2lod | S5 | offen |
| S7 | Brücke zu geo-lod: Core-Import, Messmuster, Orte als FeatureCollection | facem2lod, geo-lod | S6 | offen |
| S8 | Doku- und Figure-Seiten (Skill `figure-pages-site`) | facem2lod | S6 | offen |
| S9 | w3id-Registrierung und Redirects | facem2lod | S6 | offen |

Status ist `offen`, `erledigt <Datum>` oder `hinfällig`. Schritt-IDs werden nie umnummeriert.

S4 und S5 sind nach S3 weitgehend unabhängig voneinander; S8 kann parallel zu S7 laufen. Alles ab S3 hängt davon ab, dass du `harvest` einmal lokal laufen lässt.

---

## Teil C – Die Schritte

## S2 – Scraper: harvest und parse

**Ziel:** Die 12 ARS-Fabric-Seiten (plus verlinkte Kartenseiten) liegen unverändert unter `data/raw/html/`, und zu jeder Seite gibt es ein JSON mit allem, was der Parser sehen konnte, dazu ein Feldkatalog über alle Seiten.

**Uploads:** Nach dem lokalen Lauf das Bundle aus A5 (enthält `data/`), damit ich gegen echtes Markup nachbessern kann.

**Ablauf (lokal, Windows cmd, eine Zeile):**

```cmd
python -m pip install -r requirements.txt && python main.py harvest && python main.py
```

- `harvest` holt die Seiten (1,5 s Pause je Request) und schreibt `data/raw/harvest_manifest.json`. Bereits vorhandene Dateien werden übersprungen, `FORCE = True` im CONFIG-Block holt neu.
- `python main.py` (Standardlauf, offline) ruft `parse` auf und schreibt `dist/pipeline_report.txt`.
- Der Parser meldet `WARN <id>: no table sections found`, wenn eine Fabric-Seite keine Tabellen liefert. `python main.py --strict` bricht dann ab.

**Abnahme:** `harvest` meldet `12 fetched` und `0 failed`; `python main.py` meldet 12 Fabric-Seiten, davon 0 ohne Sektionen; zweimal `python main.py`, danach `git status` ohne Änderungen an `data/derived/` und `dist/`; `field_catalogue.json` enthält Abschnitte mit den Feldern, die auf der Seite zu sehen sind (Probe, Fabric-Typ, Ort, Ware, Objekttyp, Chronologie, Binokular, Einschlüsse, Analyse).

## S3 – JSONs ansehen, Modellierungsliste

**Ziel:** Eine Liste in Teil A (Befunde) und eine Tabelle „Feld → Modellierung → verworfen/zurückgestellt", abgeleitet aus `field_catalogue.json`.

**Uploads:** Bundle nach S2.

**Abnahme:** Jedes (Abschnitt, Feld)-Paar des Feldkatalogs hat eine Zeile; Felder mit freiem Text (Einschlüsse, Chronologie) haben einen Parsing-Vorschlag mit Beispielen aus den echten Werten.

## S4 – Wikidata-Kandidaten

**Ziel:** Eine CSV mit je einem Kandidaten pro Produktions-/Fundort und Zeitperiode (Wikidata-ID, Label, Treffer-Qualität), daneben GeoNames-ID und OSM-ID aus den Wikidata-Properties. Das Skript läuft lokal (Netzwerk, eigener Verb), du bestätigst die Zeilen.

**Abnahme:** Jede Zeile hat `confirmed` = ja, nein oder offen; bestätigte Zeilen werden von S6 gelesen, nichts wird ungeprüft übernommen.

## S5 – SKOS-Vokabulare

**Ziel:** Hayes-Typen, Zeitperioden und (falls S3 es nahelegt) Einschlussarten als SKOS-Konzeptschemata mit `skos:notation` und Wikidata-/Getty-Links, wo bestätigt.

**Abnahme:** Jeder in den JSONs vorkommende Hayes-Wert und jede Periode hat ein Konzept oder steht in einer Liste „nicht abgebildet".

## S6 – Ontologie und RDF-Builder neu

**Ziel:** Ontologie und Instanzgraph werden offline aus den JSONs gebaut, mit SHACL-Gate und byte-stabiler Ausgabe; die Legacy-Skripte werden dabei abgelöst.

**Abnahme:** Zweimal bauen, `git status` sauber; SHACL ohne Verstöße; jede Klasse hat einen CRM-Anker.

## S7 – Brücke zu geo-lod

**Ziel:** Gemeinsames Core-Vokabular per IRI importiert; Binokular- und Dünnschliffanalysen als Beobachtungen nach dem geo-lod-Muster (CRMsci, SOSA, QUDT; CRMhs als in geo-lod vorgemerkter Kandidat); Produktionsorte als GeoSPARQL-FeatureCollection, Hayes-Datierung als weitere Chronologie.

**Abnahme:** Eine SPARQL-Abfrage über beide Graphen findet Fabric und Ort gemeinsam; das Core-Vokabular wird importiert, nicht kopiert.

## S8 – Doku- und Figure-Seiten

**Ziel:** GitHub-Pages-Seite im Stil der anderen visuals-Repos (Skill `figure-pages-site`): Modellübersicht, ein Beispielobjekt, Brücke zu geo-lod.

**Abnahme:** Seiten bauen aus `python main.py`; zweimal bauen, byte-identisch.

## S9 – w3id

**Ziel:** Registrierung `w3id.org/facem/` und Redirects laut A6.

**Abnahme:** Jede IRI aus A6 mit Status `aktiv` löst auf die erwartete Seite auf.

---

## Teil D – Offene Punkte

- **D1** Lizenz und Nachnutzungsbedingungen der FACEM-Daten bei den Herausgebenden erfragen (Babette Bechtold, Barbara Borgers, Verena Gassner, Carina Hasenzagl), bevor RDF veröffentlicht wird. Ein Export oder eine API wäre bequemer als Scraping, und eine Rückfrage kostet wenig.
- **D2** Ob Koordinaten der Produktionsorte im HTML der Kartenseiten stehen, zeigt erst der lokale Harvest.
- **D3** Der Parser ist bisher nur gegen eine synthetische Fixture geprüft; echte Befunde stehen nach S2 hier.
- **D4** Ob `facem2lod` ein eigenständiger Producer bleibt oder in die geo-lod-Familie zieht (Decision Log von geo-lod: dort ist geo-lod alleiniger Producer seiner Artefakte).
