# crunch — pipeline batch Spark et observabilité

M2 Big Data · IPSSI · module Spark & Monitoring.

Traitement de 7 jours de pageviews Wikipédia (168 fichiers gzip, ~5 Go) par un
cluster Spark standalone dockerisé, en 1, 2 puis 4 workers, avec Prometheus,
cAdvisor et Grafana, et un manifeste de runs qui rend la campagne rejouable.

## Arborescence

```
crunch/
├── docker-compose.1w.yml        1 worker  · 2 cœurs
├── docker-compose.2w.yml        2 workers · 4 cœurs
├── docker-compose.4w.yml        4 workers · 8 cœurs
├── run_job.py                   pilote hôte : submit / check / up / down / campaign
├── jobs/
│   ├── lib.py                   bibliothèque commune
│   ├── top100.py
│   ├── monument.py
│   └── circadian.py
├── conf/
│   ├── spark/spark-defaults.conf
│   ├── spark/metrics.properties
│   ├── prometheus/prometheus.{1w,2w,4w}.yml
│   └── grafana/                 datasource + dashboard provisionnés
├── scripts/download_data.sh
├── tools/analyse_runs.py
├── data/
│   ├── monuments.csv
│   ├── raw/<mois>/*.gz          non versionné
│   ├── events/                  event logs Spark
│   └── out/                     résultats horodatés + runs.csv
└── rapport/rapport.md
```

## Prérequis

- Docker >= 24 avec le plugin `compose`
- Python 3.9+ sur l'hôte (pour `run_job.py`, aucune dépendance externe)
- ~15 Go de disque libre, et au moins 10 Go de RAM Docker pour la variante 4w

## Données

```bash
./scripts/download_data.sh 2026-06 2026-06-01 7
./scripts/download_data.sh --verify 2026-06
```

Le script saute les fichiers déjà présents et gzip-valides, on peut donc le
relancer après une coupure. 4 connexions parallèles, `--retry 5` (les miroirs
Wikimédia renvoient souvent des 503).

Format d'une ligne :

```
fr Tour_Eiffel 231 0
```

code projet (`fr` = desktop, `fr.m` = mobile), titre, vues sur l'heure, octets
ignorés. La date et l'heure ne sont pas dans les lignes : elles viennent du nom
du fichier, récupérées avec `input_file_name()` + regex dans `lib.read_pageviews`.

## Démarrer une variante

```bash
./run_job.py up 1w
./run_job.py check
./run_job.py down 1w
```

| Interface | URL |
|---|---|
| Spark master | http://localhost:8080 |
| Spark UI (application en cours) | http://localhost:4040 |
| History Server | http://localhost:18080 |
| Prometheus | http://localhost:9090 |
| Grafana (admin/admin) | http://localhost:3000 |
| cAdvisor | http://localhost:8085 |

Les trois compose sont identiques au nombre de workers près, chaque worker
recevant `--cores 2 --memory 2g`.

## Lancer les jobs

```bash
./run_job.py top100    2026-06
./run_job.py monument  2026-06 Tour_Eiffel
./run_job.py circadian 2026-06
```

`run_job.py` construit et exécute :

```
docker exec crunch-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 --deploy-mode client \
    --py-files /opt/app/lib.py /opt/app/top100.py 2026-06
```

`local[*]` est refusé par `lib.build_spark()`. Options : `--dry-run`,
`--count-in`, `--extra "--conf k=v"`.

## Dashboard Grafana

Provisionné depuis `conf/grafana/` : dossier crunch, dashboard
« crunch · Spark & conteneurs ».

| Panel | Requête | Type |
|---|---|---|
| Workers vivants | `sum(up{job="spark-workers"})` | Stat |
| Applications en cours | `metrics_master_apps_Value` | Stat |
| CPU par conteneur | `sum by (name) (rate(container_cpu_usage_seconds_total{name=~"crunch-.+"}[$rate_window]))` | Séries temporelles |
| Mémoire par conteneur | `sum by (name) (container_memory_working_set_bytes{name=~"crunch-.+"})` | Séries temporelles |
| Réseau des workers | `± sum by (name) (rate(container_network_{receive,transmit}_bytes_total{name=~"crunch-spark-.+"}[$rate_window]))` | Séries temporelles |

Plus deux panels de contexte : cœurs occupés/disponibles et exécuteurs
enregistrés.

Tous les compteurs passent par `rate()`. `container_cpu_usage_seconds_total`
est monotone croissant, sa dérivée donne des cœurs consommés (unité
`percentunit`, 1,00 = un cœur saturé). La fenêtre de `rate()` est une variable
de dashboard (`$rate_window`) : avec un scrape à 5 s, 30 s montre le détail des
phases, 1 min lisse le bruit. Le TX réseau est tracé en négatif pour lire le
shuffle d'un coup d'œil.

Les noms de métriques du master se vérifient avec :

```bash
curl -s localhost:8080/metrics/master/prometheus | head -40
```

## Manifeste

Chaque exécution ajoute une ligne à `data/out/runs.csv` et écrit dans
`data/out/<job>_<timestampUTC>_p<parallélisme>/` :

```
top100_20260713T142530Z_p8/
├── result.csv
├── _SUCCESS
└── meta.json
```

Colonnes : `run_id, ts_utc, job, month, parallelism, duration_s, rows_in, rows_out`.

- `parallelism` = `defaultParallelism`, donc les cœurs vus par Spark (2/4/8),
  pas le nombre de workers
- écraser une sortie est impossible : horodatage dans le nom, `JobRun` refuse
  un dossier existant, écriture Spark en `errorifexists`
- `rows_out` est compté en relisant le CSV produit, un `df.count()` après
  écriture relancerait tout le DAG
- `rows_in` vaut -1 par défaut, `--count-in` l'active (une passe complète sur
  les 5 Go double la durée mesurée)

```bash
python3 tools/analyse_runs.py           # tableaux pour le rapport
python3 tools/analyse_runs.py --plot    # + rapport/figures/speedup.png
```

## Campagne

```bash
./run_job.py up 1w && ./run_job.py campaign 1w 2026-06 && ./run_job.py down 1w
./run_job.py up 2w && ./run_job.py campaign 2w 2026-06 && ./run_job.py down 2w
./run_job.py up 4w && ./run_job.py campaign 4w 2026-06 && ./run_job.py down 4w
```

| Variante | Runs |
|---|---|
| 1w | top100 · monument Tour_Eiffel · monument Sagrada_Família · top100 |
| 2w | top100 · monument Sagrada_Família · monument Mont-Saint-Michel · top100 |
| 4w | top100 · monument Mont-Saint-Michel · monument Tour_Eiffel · top100 |

Pause de 20 s entre deux runs (`--pause`), et `down` puis `up` entre deux
variantes plutôt qu'un scale à chaud, pour ne pas fausser la comparaison avec
le cache page de l'hôte.

## Choix techniques

**Image** `apache/spark:3.5.5-python3`, lancée via `spark-class` en premier
plan : pas de démonisation, pas d'image tierce.

**Volume `data/` monté partout.** Le driver est dans `spark-master` mais les
tâches de lecture et d'écriture tournent sur les exécuteurs, dans les
conteneurs worker. Sans montage identique côté worker, le `result.csv`
atterrirait dans le système de fichiers du conteneur exécuteur.

**AQE désactivé, `spark.sql.shuffle.partitions` figé à 16.** Sinon les
partitions seraient recoalescées différemment selon la variante et les trois
mesures ne seraient plus comparables.

**168 tâches de lecture.** Un `.gz` n'est pas splittable, Spark fait une tâche
par fichier horaire. Le job reste donc largement au-dessus du seuil où le
scaling a un sens.

**Titres percent-encodés.** Les dumps contiennent `Sagrada_Família` comme
`Sagrada_Fam%C3%ADlia`. Le job `monument` cherche les deux formes, et les
filtres de namespace couvrent `Spécial:`, `Sp%C3%A9cial:` et `Sp%C3%A9cial%3A`.

**Lignes malformées** (découpage != 4 champs, compteur non numérique) écartées
silencieusement, elles sont marginales.

## Dépannage

| Symptôme | Piste |
|---|---|
| workers absents du master | attendre 10-20 s après `up`, puis `docker logs crunch-spark-worker-1` |
| cible `spark-driver` DOWN | normal au repos, l'UI 4040 n'existe que pendant un job |
| `Initial job has not accepted any resources` | RAM Docker insuffisante, baisser `spark.executor.memory` |
| pas de métrique `metrics_master_*` | vérifier le montage de `metrics.properties` |
| panels vides | fenêtre de temps trop courte, ou `name=~"crunch-.+"` ne matche pas (`docker ps --format '{{.Names}}'`) |
| 1er run très lent | cache page froid, c'est ce que le run répété sert à mesurer |
