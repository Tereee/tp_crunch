#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyse_runs.py — exploite data/out/runs.csv pour le rapport.

    python3 tools/analyse_runs.py              # tableaux en Markdown
    python3 tools/analyse_runs.py --plot       # + rapport/figures/speedup.png

Produit :
  * le détail des 12 runs
  * la synthèse par (job, parallélisme) : médiane, écart entre répétitions
  * le speedup et l'efficacité par rapport à la variante 1 worker
  * la fraction séquentielle estimée par la loi d'Amdahl

Aucune dépendance obligatoire (matplotlib seulement pour --plot).
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics as st
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNS = os.path.join(ROOT, "data", "out", "runs.csv")
FIGDIR = os.path.join(ROOT, "rapport", "figures")


def load(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        r["parallelism"] = int(r["parallelism"])
        r["duration_s"] = float(r["duration_s"])
        r["rows_out"] = int(r["rows_out"])
        r["rows_in"] = int(r["rows_in"])
        # top100 / monument_Tour_Eiffel / circadian -> famille de job
        r["family"] = r["job"].split("_", 1)[0]
    return rows


def md_table(headers: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", default=RUNS)
    ap.add_argument("--plot", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(args.runs):
        raise SystemExit(f"{args.runs} introuvable : lancez d'abord la campagne")

    rows = load(args.runs)
    rows.sort(key=lambda r: r["ts_utc"])

    # ---------------------------------------------------------- détail ----
    print("## Détail des runs\n")
    print(md_table(
        ["#", "ts_utc", "job", "cœurs", "durée (s)", "rows_in", "rows_out"],
        [[i, r["ts_utc"], r["job"], r["parallelism"],
          f"{r['duration_s']:.1f}", r["rows_in"], r["rows_out"]]
         for i, r in enumerate(rows, 1)],
    ))

    # ------------------------------------------------------- synthèse -----
    groups: dict[tuple, list[float]] = defaultdict(list)
    for r in rows:
        groups[(r["family"], r["parallelism"])].append(r["duration_s"])

    print("\n\n## Synthèse par job et par taille de cluster\n")
    synth = []
    for (fam, par), durs in sorted(groups.items()):
        med = st.median(durs)
        spread = (max(durs) - min(durs)) / med * 100 if len(durs) > 1 else 0.0
        synth.append([fam, par, len(durs), f"{med:.1f}",
                      f"{min(durs):.1f}", f"{max(durs):.1f}", f"{spread:.1f} %"])
    print(md_table(
        ["job", "cœurs", "n", "médiane (s)", "min", "max", "dispersion"], synth))

    # ------------------------------------------- speedup / efficacité -----
    print("\n\n## Scaling (référence : variante 1 worker)\n")
    scal = []
    for fam in sorted({f for f, _ in groups}):
        pars = sorted(p for f, p in groups if f == fam)
        if not pars:
            continue
        base_par = pars[0]
        base = st.median(groups[(fam, base_par)])
        for p in pars:
            med = st.median(groups[(fam, p)])
            speedup = base / med if med else float("nan")
            eff = speedup / (p / base_par) if p else float("nan")
            # Amdahl : S = 1 / (f + (1-f)/n)  ->  f = (n/S - 1) / (n - 1)
            n = p / base_par
            f_seq = ((n / speedup) - 1) / (n - 1) if n > 1 and speedup else 0.0
            scal.append([fam, p, f"{med:.1f}", f"{speedup:.2f}x",
                         f"{eff * 100:.0f} %",
                         "—" if n == 1 else f"{max(f_seq, 0) * 100:.0f} %"])
    print(md_table(
        ["job", "cœurs", "médiane (s)", "speedup", "efficacité",
         "part séquentielle (Amdahl)"], scal))

    print("\n> Lecture : une efficacité de 100 % signifierait que doubler les "
          "cœurs divise exactement le temps par deux. La part séquentielle "
          "estimée agrège tout ce qui ne parallélise pas : démarrage des "
          "exécuteurs, plan Spark, collecte du résultat, écriture du CSV "
          "unique (coalesce(1)) et contention d'E/S disque sur l'hôte.")

    # ----------------------------------------------------------- plot -----
    if args.plot:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            raise SystemExit("pip install matplotlib pour utiliser --plot")

        os.makedirs(FIGDIR, exist_ok=True)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))

        for fam in sorted({f for f, _ in groups}):
            pars = sorted(p for f, p in groups if f == fam)
            meds = [st.median(groups[(fam, p)]) for p in pars]
            ax1.plot(pars, meds, marker="o", label=fam)
            base = meds[0]
            ax2.plot(pars, [base / m for m in meds], marker="o", label=fam)

        pars_all = sorted({p for _, p in groups})
        ax2.plot(pars_all, [p / pars_all[0] for p in pars_all],
                 "k--", alpha=.5, label="idéal (linéaire)")

        ax1.set_xlabel("cœurs totaux"); ax1.set_ylabel("durée médiane (s)")
        ax1.set_title("Durée en fonction de la taille du cluster")
        ax1.set_xticks(pars_all); ax1.grid(alpha=.3); ax1.legend()

        ax2.set_xlabel("cœurs totaux"); ax2.set_ylabel("speedup")
        ax2.set_title("Speedup vs variante 1 worker")
        ax2.set_xticks(pars_all); ax2.grid(alpha=.3); ax2.legend()

        fig.tight_layout()
        out = os.path.join(FIGDIR, "speedup.png")
        fig.savefig(out, dpi=140)
        print(f"\n[analyse] figure écrite : {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
