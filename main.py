"""Orchestrator for facem2lod. The only entry point of the pipeline.

    python main.py                 offline steps, in order
    python main.py harvest         fetch pages from facem.at (network, own verb)
    python main.py --list          print the steps and exit
    python main.py --only parse    one step
    python main.py --from parse    this step and everything after
    python main.py --skip parse    everything but this step
    python main.py --dry-run       print the plan, run nothing
    python main.py --strict        warnings become errors

Every step also runs standalone, for example: python py/step_parse.py
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = ROOT / "py"
REPORT = ROOT / "dist" / "pipeline_report.txt"

# name, script, network?, description
STEPS = [
    ("harvest", "step_harvest.py", True,  "fetch fabric and sample pages into data/raw/html/"),
    ("parse",   "step_parse.py",   False, "raw HTML -> data/derived/json/ and field catalogue"),
    ("wikidata", "step_wikidata.py", True, "Wikidata candidates for places and periods -> data/curated/wikidata_candidates.csv"),
]
NAMES = [s[0] for s in STEPS]
NETWORK = {s[0] for s in STEPS if s[2]}


def plan(args) -> list:
    default = [s for s in STEPS if not s[2]]
    verb = args.verb or args.only
    if verb:
        chosen = [s for s in STEPS if s[0] == verb]
    elif args.from_step:
        pool = STEPS if args.from_step in NETWORK else default
        names = [s[0] for s in pool]
        chosen = pool[names.index(args.from_step):]
    else:
        chosen = default
    if args.skip:
        chosen = [s for s in chosen if s[0] not in args.skip]
    return chosen


def run_step(step, lines: list) -> int:
    name, script, _, _ = step
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    proc = subprocess.Popen(
        [sys.executable, str(PY / script)], cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
    )
    for line in proc.stdout:
        line = line.rstrip("\n")
        print(f"  [{name}] {line}")
        lines.append(f"[{name}] {line}")
    return proc.wait()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="facem2lod pipeline")
    ap.add_argument("verb", nargs="?", choices=sorted(NETWORK),
                    help="network step to run on its own")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--only", choices=NAMES)
    ap.add_argument("--from", dest="from_step", choices=NAMES)
    ap.add_argument("--skip", action="append", choices=NAMES, default=[])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--strict", action="store_true")
    args = ap.parse_args()

    if args.list:
        for name, script, network, desc in STEPS:
            flag = "network, own verb" if network else "offline"
            print(f"{name:8s} {script:18s} {flag:18s} {desc}")
        return 0

    chosen = plan(args)
    if args.dry_run:
        print("plan:", " -> ".join(s[0] for s in chosen) or "(nothing)")
        return 0

    lines, timings, failed, warned = [], [], False, False
    for step in chosen:
        print(f"== {step[0]}")
        t0 = time.perf_counter()
        rc = run_step(step, lines)
        timings.append((step[0], time.perf_counter() - t0))
        if rc != 0:
            print(f"step {step[0]} failed with exit code {rc}")
            failed = True
            break
    warned = any(" WARN " in f" {l}" for l in lines)

    # Timings go to the terminal only; the report must stay byte-stable.
    total = sum(t for _, t in timings) or 1.0
    print("-- timing")
    for name, t in timings:
        print(f"   {name:8s} {t:6.1f} s  {100 * t / total:4.0f} %")

    if not failed and chosen and not (len(chosen) == 1 and chosen[0][0] in NETWORK):
        REPORT.parent.mkdir(parents=True, exist_ok=True)
        with open(REPORT, "w", encoding="utf-8", newline="\n") as fh:
            fh.write("\n".join(lines) + ("\n" if lines else ""))

    if failed or (args.strict and warned):
        if warned and args.strict:
            print("strict mode: warnings present")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
