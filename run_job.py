#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_job.py : pilote exécuté depuis l'hôte.

Ne calcule rien, construit la commande docker exec + spark-submit et
l'exécute dans le conteneur spark-master (driver en mode client).

Commandes
---------
  ./run_job.py top100   2026-06
  ./run_job.py monument 2026-06 Tour_Eiffel
  ./run_job.py circadian 2026-06
  ./run_job.py check                     # état de la stack
  ./run_job.py up 2w                     # docker compose -f docker-compose.2w.yml up -d
  ./run_job.py down                      # arrête la variante courante
  ./run_job.py campaign 1w 2026-06       # les 4 runs imposés d'une variante

Options communes : --dry-run, --count-in, --extra "--conf k=v"
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
MASTER_CONTAINER = os.environ.get("CRUNCH_MASTER", "crunch-spark-master")
SPARK_MASTER_URL = "spark://spark-master:7077"
APP_DIR = "/opt/app"

JOB_SCRIPTS = {
    "top100": "top100.py",
    "monument": "monument.py",
    "circadian": "circadian.py",
}

VARIANTS = {
    "1w": ("docker-compose.1w.yml", 1),
    "2w": ("docker-compose.2w.yml", 2),
    "4w": ("docker-compose.4w.yml", 4),
}

STATE_FILE = os.path.join(ROOT, ".crunch-variant")

OK, KO = "  OK ", "  KO "


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------

def run(cmd: list[str], dry: bool = False, check: bool = True) -> int:
    print("$ " + " ".join(shlex.quote(c) for c in cmd), flush=True)
    if dry:
        return 0
    proc = subprocess.run(cmd)
    if check and proc.returncode != 0:
        raise SystemExit(f"[run_job] échec (code {proc.returncode})")
    return proc.returncode


def http_json(url: str, timeout: float = 3.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def http_ok(url: str, timeout: float = 3.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Soumission
# ---------------------------------------------------------------------------

def build_submit_cmd(job: str, job_args: list[str], extra: list[str]) -> list[str]:
    """docker exec ... spark-submit ..."""
    script = JOB_SCRIPTS[job]
    return [
        "docker", "exec",
        "-e", "PYTHONIOENCODING=utf-8",
        "-e", "PYTHONUNBUFFERED=1",
        MASTER_CONTAINER,
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER_URL,
        "--deploy-mode", "client",
        "--name", f"crunch-{job}",
        "--py-files", f"{APP_DIR}/lib.py",
        *extra,
        f"{APP_DIR}/{script}",
        *job_args,
    ]


def submit(job: str, job_args: list[str], dry: bool, count_in: bool, extra: list[str]) -> int:
    args = list(job_args)
    if count_in:
        args.append("--count-in")
    t0 = time.monotonic()
    rc = run(build_submit_cmd(job, args, extra), dry=dry, check=False)
    print(f"[run_job] {job} terminé en {time.monotonic() - t0:.1f}s (code {rc})")
    if rc != 0:
        raise SystemExit(rc)
    return rc


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------

def cmd_check(_args) -> int:
    ok = True

    print("== Conteneurs ==")
    out = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
        capture_output=True, text=True,
    ).stdout
    names = {}
    for line in out.strip().splitlines():
        if "\t" in line:
            n, s = line.split("\t", 1)
            names[n] = s
    expected = ["crunch-spark-master", "crunch-prometheus", "crunch-grafana",
                "crunch-cadvisor", "crunch-spark-history"]
    workers = sorted(n for n in names if n.startswith("crunch-spark-worker"))
    for n in expected:
        flag = OK if n in names else KO
        ok &= n in names
        print(f"{flag} {n:<26} {names.get(n, 'absent')}")
    for n in workers:
        print(f"{OK} {n:<26} {names[n]}")
    print(f"     workers démarrés : {len(workers)}")

    print("\n== Spark master (localhost:8080) ==")
    try:
        st = http_json("http://localhost:8080/json/")
        alive = [w for w in st.get("workers", []) if w.get("state") == "ALIVE"]
        cores = sum(w.get("cores", 0) for w in alive)
        print(f"{OK} status={st.get('status')} workers ALIVE={len(alive)} "
              f"cœurs={cores} apps actives={len(st.get('activeapps', []))}")
        if len(alive) != len(workers):
            print(f"{KO} {len(workers)} conteneurs worker mais {len(alive)} enregistrés")
            ok = False
    except Exception as exc:
        print(f"{KO} master injoignable : {exc}")
        ok = False

    print("\n== Cibles Prometheus (localhost:9090) ==")
    try:
        targets = http_json("http://localhost:9090/api/v1/targets")["data"]["activeTargets"]
        for t in targets:
            job = t["labels"].get("job", "?")
            inst = t["labels"].get("instance", "?")
            health = t["health"]
            # la cible spark-driver n'existe que pendant un job
            soft = job == "spark-driver"
            flag = OK if health == "up" else ("  ~  " if soft else KO)
            print(f"{flag} {job:<16} {inst:<28} {health}")
            if health != "up" and not soft:
                ok = False
        if not targets:
            print(f"{KO} aucune cible")
            ok = False
    except Exception as exc:
        print(f"{KO} Prometheus injoignable : {exc}")
        ok = False

    print("\n== Interfaces ==")
    for name, url in [
        ("Grafana", "http://localhost:3000/api/health"),
        ("cAdvisor", "http://localhost:8085/healthz"),
        ("History Server", "http://localhost:18080/"),
    ]:
        up = http_ok(url)
        ok &= up
        print(f"{OK if up else KO} {name:<16} {url}")

    print("\n== Données ==")
    for month in sorted(os.listdir(os.path.join(ROOT, "data", "raw"))) if \
            os.path.isdir(os.path.join(ROOT, "data", "raw")) else []:
        d = os.path.join(ROOT, "data", "raw", month)
        if not os.path.isdir(d):
            continue
        n = len([f for f in os.listdir(d) if f.endswith(".gz")])
        size = sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d)) / 1e9
        flag = OK if n == 168 else KO
        print(f"{flag} raw/{month} : {n} fichiers ({size:.2f} Go) — attendu 168")
        ok &= n == 168

    runs = os.path.join(ROOT, "data", "out", "runs.csv")
    if os.path.exists(runs):
        with open(runs, encoding="utf-8") as fh:
            n = sum(1 for _ in fh) - 1
        print(f"{OK} runs.csv : {n} run(s) enregistré(s)")
    else:
        print("     runs.csv : pas encore de run")

    print("\n=>", "stack prête" if ok else "PROBLÈMES DÉTECTÉS")
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# up / down / campaign
# ---------------------------------------------------------------------------

def compose_file(variant: str) -> str:
    if variant not in VARIANTS:
        raise SystemExit(f"variante inconnue : {variant} (1w, 2w ou 4w)")
    return VARIANTS[variant][0]


def cmd_up(args) -> int:
    f = compose_file(args.variant)
    run(["docker", "compose", "-f", os.path.join(ROOT, f), "up", "-d"], dry=args.dry_run)
    if not args.dry_run:
        with open(STATE_FILE, "w") as fh:
            fh.write(args.variant)
        print("[run_job] attente de l'enregistrement des workers…")
        expected = VARIANTS[args.variant][1]
        for _ in range(60):
            try:
                st = http_json("http://localhost:8080/json/")
                alive = [w for w in st.get("workers", []) if w.get("state") == "ALIVE"]
                if len(alive) >= expected:
                    print(f"[run_job] {len(alive)} worker(s) ALIVE, "
                          f"{sum(w['cores'] for w in alive)} cœurs")
                    return 0
            except Exception:
                pass
            time.sleep(2)
        print("[run_job] workers non enregistrés après 120s — voir `check`")
        return 1
    return 0


def cmd_down(args) -> int:
    variant = args.variant
    if not variant and os.path.exists(STATE_FILE):
        variant = open(STATE_FILE).read().strip()
    if not variant:
        raise SystemExit("précisez la variante : ./run_job.py down 1w")
    run(["docker", "compose", "-f", os.path.join(ROOT, compose_file(variant)), "down"],
        dry=args.dry_run)
    return 0


CAMPAIGN = {
    "1w": [("top100", []), ("monument", ["Tour_Eiffel"]),
           ("monument", ["Sagrada_Família"]), ("top100", [])],
    "2w": [("top100", []), ("monument", ["Sagrada_Família"]),
           ("monument", ["Mont-Saint-Michel"]), ("top100", [])],
    "4w": [("top100", []), ("monument", ["Mont-Saint-Michel"]),
           ("monument", ["Tour_Eiffel"]), ("top100", [])],
}


def cmd_campaign(args) -> int:
    """Les 4 runs d'une variante, dans l'ordre imposé."""
    plan = CAMPAIGN[args.variant]
    print(f"[run_job] campagne {args.variant} — {len(plan)} runs sur {args.month}")
    for i, (job, extra_args) in enumerate(plan, 1):
        print(f"\n----- run {i}/{len(plan)} : {job} {' '.join(extra_args)} -----")
        submit(job, [args.month, *extra_args], args.dry_run, args.count_in, [])
        if i < len(plan):
            time.sleep(args.pause)
    print("\n[run_job] campagne terminée — voir data/out/runs.csv")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="run_job.py", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="affiche sans exécuter")
    sub = p.add_subparsers(dest="cmd", required=True)

    for job in ("top100", "circadian"):
        sp = sub.add_parser(job)
        sp.add_argument("month")
        sp.add_argument("--count-in", action="store_true")
        sp.add_argument("--extra", default="", help="options spark-submit en plus")
        sp.set_defaults(func=lambda a, j=job: submit(
            j, [a.month], a.dry_run, a.count_in, shlex.split(a.extra)))

    sp = sub.add_parser("monument")
    sp.add_argument("month")
    sp.add_argument("title")
    sp.add_argument("--count-in", action="store_true")
    sp.add_argument("--force", action="store_true")
    sp.add_argument("--extra", default="")
    sp.set_defaults(func=lambda a: submit(
        "monument",
        [a.month, a.title] + (["--force"] if a.force else []),
        a.dry_run, a.count_in, shlex.split(a.extra)))

    sp = sub.add_parser("check", help="état de la stack")
    sp.set_defaults(func=cmd_check)

    sp = sub.add_parser("up")
    sp.add_argument("variant", choices=sorted(VARIANTS))
    sp.set_defaults(func=cmd_up)

    sp = sub.add_parser("down")
    sp.add_argument("variant", nargs="?", choices=sorted(VARIANTS))
    sp.set_defaults(func=cmd_down)

    sp = sub.add_parser("campaign", help="les 4 runs imposés d'une variante")
    sp.add_argument("variant", choices=sorted(CAMPAIGN))
    sp.add_argument("month", nargs="?", default="2026-06")
    sp.add_argument("--pause", type=int, default=20,
                    help="pause entre deux runs, en secondes (défaut 20)")
    sp.add_argument("--count-in", action="store_true")
    sp.set_defaults(func=cmd_campaign)

    args = p.parse_args(argv)
    return args.func(args) or 0


if __name__ == "__main__":
    raise SystemExit(main())
