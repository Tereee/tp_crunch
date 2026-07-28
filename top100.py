#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job "top100" : les 100 pages fr.wikipedia les plus vues sur 7 jours.

    spark-submit --master spark://spark-master:7077 /opt/app/top100.py <mois>

Desktop + mobile, hors pages spéciales, namespaces et page d'accueil.

Sortie : data/out/top100_<ts>_p<par>/result.csv
         rank, title, views, views_desktop, views_mobile, share_pct
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import Window
from pyspark.sql import functions as F

import lib


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="top100")
    p.add_argument("month", help="mois traité, ex. 2026-06")
    p.add_argument(
        "--count-in",
        action="store_true",
        help="compte les lignes lues (passe supplémentaire sur les 5 Go)",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    spark = lib.build_spark(f"crunch-top100-{args.month}")

    with lib.JobRun(spark, "top100", args.month) as run:
        raw = lib.read_pageviews(spark, args.month)
        run.rows_in = lib.maybe_count(raw, args.count_in)

        pages = lib.add_platform(lib.clean_fr_pages(raw))

        # une seule agrégation pour le total et la ventilation
        agg = pages.groupBy("title").agg(
            F.sum("views").alias("views"),
            F.sum(F.when(F.col("platform") == "desktop", F.col("views")).otherwise(0))
            .alias("views_desktop"),
            F.sum(F.when(F.col("platform") == "mobile", F.col("views")).otherwise(0))
            .alias("views_mobile"),
        )

        top = agg.orderBy(F.desc("views"), F.asc("title")).limit(100)

        total = top.agg(F.sum("views")).collect()[0][0] or 1
        ranked = (
            top.withColumn(
                "rank",
                F.row_number().over(Window.orderBy(F.desc("views"), F.asc("title"))),
            )
            .withColumn("share_pct", F.round(100.0 * F.col("views") / F.lit(total), 3))
            .select(
                "rank", "title", "views", "views_desktop", "views_mobile", "share_pct"
            )
            .orderBy("rank")
        )

        run.rows_out = lib.write_result_csv(ranked, run.outdir)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
