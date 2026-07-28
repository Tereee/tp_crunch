#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job "monument" : vues quotidiennes d'un titre précis.

    spark-submit --master spark://spark-master:7077 \
        /opt/app/monument.py <mois> <Titre_Exact>

Desktop + mobile, une ligne par jour, triées par date. Le titre doit être
exact, underscores et accents compris (Sagrada_Família). Les dumps
contiennent parfois la forme percent-encodée, on cherche les deux.

Sortie : data/out/monument_<Titre>_<ts>_p<par>/result.csv
         date, views, views_desktop, views_mobile
"""

import argparse
import os
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F

import lib

MONUMENTS_CSV = os.path.join(lib.DATA_ROOT, "monuments.csv")


def known_titles() -> set:
    """Titres valides de data/monuments.csv."""
    import csv

    if not os.path.exists(MONUMENTS_CSV):
        return set()
    with open(MONUMENTS_CSV, encoding="utf-8") as fh:
        return {r["title"].strip() for r in csv.DictReader(fh) if r.get("title")}


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="monument")
    p.add_argument("month", help="mois traité, ex. 2026-06")
    p.add_argument("title", help="titre exact, ex. Sagrada_Família")
    p.add_argument("--count-in", action="store_true")
    p.add_argument(
        "--force",
        action="store_true",
        help="accepte un titre absent de data/monuments.csv",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    valid = known_titles()
    if valid and args.title not in valid and not args.force:
        raise SystemExit(
            f"[monument] titre inconnu : {args.title!r}\n"
            f"           voir {MONUMENTS_CSV} (ou --force)"
        )

    # les deux écritures possibles dans les dumps
    variants = {args.title, quote(args.title, safe="")}

    spark = lib.build_spark(f"crunch-monument-{args.title}-{args.month}")

    with lib.JobRun(spark, f"monument_{args.title}", args.month) as run:
        raw = lib.read_pageviews(spark, args.month)
        run.rows_in = lib.maybe_count(raw, args.count_in)

        hits = lib.add_platform(
            lib.only_fr(raw).where(F.col("title").isin(*variants))
        )

        daily = (
            hits.groupBy("date")
            .agg(
                F.sum("views").alias("views"),
                F.sum(
                    F.when(F.col("platform") == "desktop", F.col("views")).otherwise(0)
                ).alias("views_desktop"),
                F.sum(
                    F.when(F.col("platform") == "mobile", F.col("views")).otherwise(0)
                ).alias("views_mobile"),
            )
            .orderBy("date")
        )

        run.rows_out = lib.write_result_csv(daily, run.outdir)

        if run.rows_out == 0:
            print(
                f"[monument] AUCUNE vue pour {args.title!r} : "
                "vérifiez l'orthographe exacte (underscores, accents)."
            )

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
