#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job "circadian" : profil horaire du trafic fr.wikipedia.

    spark-submit --master spark://spark-master:7077 \
        /opt/app/circadian.py <mois>

Répartition des vues par heure UTC (0-23) sur les 7 jours, avec la
ventilation desktop / mobile.

Sortie : data/out/circadian_<ts>_p<par>/result.csv
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F

import lib


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="circadian")
    p.add_argument("month", help="mois traité, ex. 2026-06")
    p.add_argument("--count-in", action="store_true")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    spark = lib.build_spark(f"crunch-circadian-{args.month}")

    with lib.JobRun(spark, "circadian", args.month) as run:
        raw = lib.read_pageviews(spark, args.month)
        run.rows_in = lib.maybe_count(raw, args.count_in)

        pages = lib.add_platform(lib.clean_fr_pages(raw))

        by_hour = pages.groupBy("hour").agg(
            F.sum("views").alias("views_total"),
            F.countDistinct("date").alias("n_days"),
            F.sum(
                F.when(F.col("platform") == "desktop", F.col("views")).otherwise(0)
            ).alias("views_desktop"),
            F.sum(
                F.when(F.col("platform") == "mobile", F.col("views")).otherwise(0)
            ).alias("views_mobile"),
        )

        grand_total = by_hour.agg(F.sum("views_total")).collect()[0][0] or 1

        result = (
            by_hour.withColumn(
                "views_avg_day",
                F.round(F.col("views_total") / F.greatest(F.col("n_days"), F.lit(1))),
            )
            .withColumn(
                "mobile_share_pct",
                F.round(100.0 * F.col("views_mobile") / F.col("views_total"), 2),
            )
            .withColumn(
                "share_pct",
                F.round(100.0 * F.col("views_total") / F.lit(grand_total), 3),
            )
            .select(
                "hour",
                "views_total",
                "views_avg_day",
                "views_desktop",
                "views_mobile",
                "mobile_share_pct",
                "share_pct",
            )
            .orderBy("hour")
        )

        run.rows_out = lib.write_result_csv(result, run.outdir)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
