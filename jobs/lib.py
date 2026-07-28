#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bibliothèque commune des jobs Spark (lecture, filtres, sorties, manifeste).
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import shutil
import time
from datetime import datetime, timezone
from urllib.parse import quote

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Volume monté à l'identique dans tous les conteneurs Spark.
DATA_ROOT = os.environ.get("CRUNCH_DATA_ROOT", "/opt/data")
RAW_ROOT = os.path.join(DATA_ROOT, "raw")
OUT_ROOT = os.path.join(DATA_ROOT, "out")
RUNS_CSV = os.path.join(OUT_ROOT, "runs.csv")

RUNS_COLUMNS = [
    "run_id",
    "ts_utc",
    "job",
    "month",
    "parallelism",
    "duration_s",
    "rows_in",
    "rows_out",
]

# pageviews-20260601-070000.gz -> 20260601, 07
FILENAME_RE = r"pageviews-(\d{8})-(\d{2})0000\.gz$"

# Liste blanche : les suffixes .b, .d, .n... sont d'autres projets.
FR_PROJECTS = ("fr", "fr.m")

NAMESPACE_PREFIXES = [
    "Spécial", "Special", "Spezial",
    "Wikipédia", "Wikipedia",
    "Catégorie", "Category",
    "Utilisateur", "Utilisatrice", "User",
    "Fichier", "File", "Image",
    "MediaWiki",
    "Modèle", "Template",
    "Aide", "Help",
    "Portail", "Portal",
    "Projet", "Project",
    "Référence",
    "Module",
    "Sujet",
    "Gadget", "Gadget_definition",
    "TimedText",
    "Média", "Media",
]

HOME_PAGES = [
    "-",
    "Accueil_principal",
    "Wikipédia:Accueil_principal",
    "Wikipedia:Accueil_principal",
    "Main_Page",
    "Page_d'accueil",
    "Portail:Accueil",
    "Special:Search",
    "Spécial:Recherche",
]


def _with_encoded_variants(values):
    """Ajoute la forme percent-encodée de chaque valeur.

    Les dumps contiennent les deux (Spécial: et Sp%C3%A9cial:).
    """
    out = []
    for v in values:
        out.append(v)
        enc = quote(v, safe="")
        if enc != v:
            out.append(enc)
    return out


def namespace_regex() -> str:
    """Regex des titres appartenant à un namespace (séparateur : ou %3A)."""
    sep = r"(?::|%3[Aa])"
    parts = [re.escape(p) for p in _with_encoded_variants(NAMESPACE_PREFIXES)]
    # Discussion, Discussion_utilisateur, Discussion_Wikipédia : une seule
    # alternative générique les couvre.
    return (
        r"^(?:Discussion[^:%]*" + sep + r"|(?:" + "|".join(parts) + r")" + sep + r")"
    )


HOME_PAGES_ALL = _with_encoded_variants(HOME_PAGES)


def build_spark(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    """SparkSession. Le master vient de spark-submit, local[*] est refusé."""
    builder = SparkSession.builder.appName(app_name)
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()

    master = spark.sparkContext.master
    if master.startswith("local"):
        spark.stop()
        raise SystemExit(
            f"[lib] master='{master}' : mode local interdit, "
            "soumettre avec --master spark://spark-master:7077"
        )
    spark.sparkContext.setLogLevel(os.environ.get("CRUNCH_LOG_LEVEL", "WARN"))
    return spark


def raw_glob(month: str) -> str:
    return "file://" + os.path.join(RAW_ROOT, month, "*.gz")


def count_raw_files(month: str) -> int:
    return len(glob.glob(os.path.join(RAW_ROOT, month, "*.gz")))


def read_pageviews(spark: SparkSession, month: str) -> DataFrame:
    """Lit les fichiers horaires d'un mois.

    Colonnes : project, title, views, day, hour, date.
    La date et l'heure ne sont pas dans les lignes, on les prend dans le nom
    du fichier. Un .gz n'étant pas splittable, Spark fait une tâche par
    fichier (168 pour 7 jours).
    Les lignes qui ne donnent pas exactement 4 champs sont écartées.
    """
    fields = F.split(F.col("value"), " ")

    df = (
        spark.read.text(raw_glob(month))
        .withColumn("src", F.input_file_name())
        .withColumn("f", fields)
        .where(F.size("f") == 4)
        .select(
            F.col("f").getItem(0).alias("project"),
            F.col("f").getItem(1).alias("title"),
            F.col("f").getItem(2).cast("int").alias("views"),
            F.regexp_extract("src", FILENAME_RE, 1).alias("day"),
            F.regexp_extract("src", FILENAME_RE, 2).cast("int").alias("hour"),
        )
        .where(F.col("views").isNotNull() & (F.col("day") != ""))
        .withColumn("date", F.to_date("day", "yyyyMMdd"))
    )
    return df


def only_fr(df: DataFrame) -> DataFrame:
    return df.where(F.col("project").isin(*FR_PROJECTS))


def drop_special_pages(df: DataFrame) -> DataFrame:
    return df.where(
        (~F.col("title").isin(*HOME_PAGES_ALL))
        & (~F.col("title").rlike(namespace_regex()))
        & (F.length("title") > 0)
    )


def clean_fr_pages(df: DataFrame) -> DataFrame:
    return drop_special_pages(only_fr(df))


def add_platform(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "platform",
        F.when(F.col("project") == "fr.m", F.lit("mobile")).otherwise(F.lit("desktop")),
    )


def write_result_csv(df: DataFrame, outdir: str, name: str = "result.csv") -> int:
    """Écrit un seul CSV dans outdir et renvoie le nombre de lignes de données.

    Le comptage se fait sur le fichier produit : un df.count() après
    l'écriture relancerait tout le DAG.
    """
    tmp = os.path.join(outdir, "_spark")
    (
        df.coalesce(1)
        .write.mode("errorifexists")
        .option("header", True)
        .option("encoding", "UTF-8")
        .csv("file://" + tmp)
    )

    parts = sorted(glob.glob(os.path.join(tmp, "part-*.csv")))
    if not parts:
        raise RuntimeError(f"aucun fichier part-* produit dans {tmp}")
    target = os.path.join(outdir, name)
    shutil.move(parts[0], target)
    shutil.rmtree(tmp, ignore_errors=True)

    with open(target, "r", encoding="utf-8") as fh:
        rows = sum(1 for _ in fh) - 1
    return max(rows, 0)


def _append_runs_row(row: dict) -> None:
    """Ajoute une ligne à runs.csv, avec verrou pour les écritures concurrentes."""
    os.makedirs(OUT_ROOT, exist_ok=True)
    new_file = not os.path.exists(RUNS_CSV)
    with open(RUNS_CSV, "a", encoding="utf-8", newline="") as fh:
        try:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        except Exception:
            pass
        writer = csv.DictWriter(fh, fieldnames=RUNS_COLUMNS)
        if new_file:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in RUNS_COLUMNS})


class JobRun:
    """Dossier de sortie horodaté, chrono, écriture du manifeste."""

    def __init__(self, spark: SparkSession, job: str, month: str):
        self.spark = spark
        self.job = job
        self.month = month
        self.started = datetime.now(timezone.utc)
        stamp = self.started.strftime("%Y%m%dT%H%M%SZ")
        self.ts_utc = self.started.isoformat(timespec="seconds")
        self.parallelism = spark.sparkContext.defaultParallelism
        self.run_id = f"{job}_{stamp}"
        self.outdir = os.path.join(OUT_ROOT, f"{job}_{stamp}_p{self.parallelism}")
        if os.path.exists(self.outdir):
            raise RuntimeError(f"{self.outdir} existe déjà, écrasement interdit")
        os.makedirs(self.outdir)
        self.rows_in = -1
        self.rows_out = -1
        self._t0 = time.monotonic()
        print(
            f"[run] {self.run_id} | parallelism={self.parallelism} "
            f"| app={spark.sparkContext.applicationId}"
        )

    def __enter__(self) -> "JobRun":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        duration = round(time.monotonic() - self._t0, 3)
        if exc_type is not None:
            print(f"[run] {self.run_id} échec après {duration}s : {exc}")
            return False

        row = {
            "run_id": self.run_id,
            "ts_utc": self.ts_utc,
            "job": self.job,
            "month": self.month,
            "parallelism": self.parallelism,
            "duration_s": duration,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
        }
        _append_runs_row(row)

        meta = dict(row)
        meta.update(
            {
                "app_id": self.spark.sparkContext.applicationId,
                "master": self.spark.sparkContext.master,
                "executors": len(
                    self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
                )
                - 1,
                "shuffle_partitions": self.spark.conf.get("spark.sql.shuffle.partitions"),
                "input_files": count_raw_files(self.month),
            }
        )
        with open(os.path.join(self.outdir, "meta.json"), "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)

        print(f"[run] {self.run_id} ok en {duration}s -> {self.outdir}")
        print(f"[run] rows_in={self.rows_in} rows_out={self.rows_out}")
        return False


def maybe_count(df: DataFrame, enabled: bool) -> int:
    """rows_in : une passe complète sur les 5 Go, donc optionnelle (-1 sinon)."""
    return df.count() if enabled else -1
