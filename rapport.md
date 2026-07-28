# crunch — pipeline batch Spark et observabilité

Nom Prénom — M2 Big Data, IPSSI
Dépôt : `<url>` · Commit : `<sha>` · Date : `<date>`

## 1. Contexte

| Élément | Valeur |
|---|---|
| Jeu de données | pageviews Wikimédia, `<mois>`, du `<date>` au `<date>` |
| Volume | 168 fichiers .gz, `<X>` Go compressés |
| Cluster | Spark 3.5.5 standalone, 1/2/4 workers, 2 cœurs et 2 Go par worker |
| Jobs | top100, monument, circadian |
| Machine hôte | `<CPU, RAM, disque, OS>` |

Remarque sur l'hôte : avec `<N>` cœurs physiques, la variante 4 workers en
demande 8. Si l'hôte en a moins, le speedup plafonne pour une raison qui n'a
rien à voir avec Spark.

## 2. Architecture

`<schéma : hôte → conteneurs → réseau crunch-net>`

- pourquoi `data/` est monté dans tous les conteneurs Spark
- pourquoi `local[*]` est interdit et ce que ça change
- ce qui a été figé pour rendre les mesures comparables (shuffle partitions,
  AQE, allocation statique)

## 3. Les jobs

### 3.1 Bibliothèque commune

Extraction date/heure depuis le nom de fichier, filtres mutualisés.

### 3.2 top100

`<10 premières lignes du result.csv>`

Commentaire : `<ce qui arrive en tête et pourquoi, part du mobile, effet du
filtre de namespaces>`

### 3.3 monument

`<les 7 lignes pour Tour_Eiffel>`

Commentaire : `<effet week-end, pic éventuel, écart desktop/mobile>`

### 3.4 circadian

`<tableau ou courbe des 24 heures>`

Commentaire : `<creux nocturne, pic du soir, évolution de la part mobile>`

## 4. Campagne de mesures

### 4.1 Protocole

Ordre imposé, `down` puis `up` entre deux variantes, pause de 20 s entre deux
runs. `<cache page vidé ou non>`

### 4.2 Les 12 runs

`<sortie « Détail des runs » de tools/analyse_runs.py>`

### 4.3 Scaling

`<tableaux « Synthèse » et « Scaling »>`

![Durée et speedup](figures/speedup.png)

Figure 1 — durée médiane et speedup selon le nombre de cœurs.

### 4.4 Analyse

- linéarité du speedup, et où part la différence (démarrage des exécuteurs,
  plan Spark, `coalesce(1)`, contention disque de l'hôte)
- part séquentielle estimée par Amdahl, cohérence entre top100 et monument
  (à recouper avec le shuffle write dans la Spark UI)
- variance entre les deux top100 d'une même variante = barre d'erreur
- premier run de chaque variante plus lent ?

## 5. Observabilité

### 5.1 Dashboard

`<capture pendant un run top100 en 4 workers>`

Figure 2 — dashboard Grafana pendant `<run_id>`.

Panel par panel : `<workers vivants / applications / CPU / mémoire / réseau>`

### 5.2 Corrélation avec la Spark UI

`<captures : DAG, timeline des stages, shuffle read/write>`

`<à t = ..., pic réseau de ... Mo/s ; la Spark UI donne un shuffle write de
... Mo sur le stage ... : les deux concordent>`

### 5.3 Ce que le monitoring a révélé

`<un fait invisible sans les panels>`

## 6. Limites

- une seule machine : workers en concurrence sur les mêmes cœurs et le même
  disque
- deux répétitions par point, suffisant pour un ordre de grandeur
- `coalesce(1)` sérialise l'écriture finale et pénalise les grosses variantes
- pistes : conversion en Parquet, variation de `shuffle.partitions` à cluster
  constant, mesure avec AQE activé

## 7. Conclusion

`<3 à 5 phrases>`

## Annexes

- A. `data/out/runs.csv`
- B. commandes exactes de la campagne
- C. requêtes PromQL du dashboard
