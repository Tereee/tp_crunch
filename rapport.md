# crunch — pipeline batch Spark et observabilité

Florian Dardy — M2 Big Data, IPSSI
Dépôt : (À compléter) · Commit : (À compléter) · Date : 28/07/2026

## 1. Contexte

| Élément | Valeur |
|---|---|
| Jeu de données | pageviews Wikimédia, juillet 2026, du 26/07 au 27/07 (échantillon) |
| Volume | 25 fichiers .gz, ~1.4 Go compressés |
| Cluster | Spark 3.5.5 standalone, 1/2/4 workers, 2 cœurs et 2 Go par worker |
| Jobs | top100, monument, circadian |
| Machine hôte | Intel i7-12700K (12 cœurs physiques) |

Remarque sur l'hôte : avec 12 cœurs physiques, la variante 4 workers en demande 8, la machine est donc capable d'encaisser la charge.

## 2. Architecture

*(Pensez à insérer votre schéma : hôte → conteneurs → réseau crunch-net)*

- **Pourquoi `data/` est monté dans tous les conteneurs Spark** : Sans système de fichiers distribué comme HDFS, chaque exécuteur doit pouvoir lire les fichiers directement depuis le système de fichiers local. Monter le volume dans chaque conteneur permet de distribuer la lecture des fichiers gz sur tous les workers de façon transparente.
- **Pourquoi `local[*]` est interdit** : Ce mode exécute tout (driver et workers) dans la même JVM locale sur la machine maître. Pour évaluer de vraies performances d'un cluster distribué et observer l'impact du réseau, de la mémoire et du scaling par worker, nous devons impérativement soumettre les jobs au mode Standalone (`spark://...`).
- **Ce qui a été figé** : Le nombre de partitions lors des shuffles (`spark.sql.shuffle.partitions`), l'allocation dynamique (`AQE` désactivé) et l'allocation de ressources (nombre de cœurs fixes) ont été contraints pour assurer que la seule variable entre chaque test soit la taille de l'infrastructure, garantissant la fiabilité des mesures.

## 3. Les jobs

### 3.1 Bibliothèque commune

Extraction de la date et de l'heure depuis le nom de fichier via Regex, et filtres mutualisés (filtrer uniquement sur FR, suppression des namespaces inutiles). L'écriture dans `runs.csv` est gérée de façon centralisée par une classe s'assurant des fermetures (verrou pour la concurrence).

### 3.2 top100

| rank | title | views | views_desktop | views_mobile | share_pct |
|---|---|---|---|---|---|
| 1 | Cookie_(informatique) | 493838 | 491017 | 2821 | 26.35 |
| 2 | Tadej_Pogačar | 54756 | 5986 | 48770 | 2.922 |
| 3 | Marie-Paule_Belle | 53114 | 9930 | 43184 | 2.834 |
| 4 | Mathieu_van_der_Poel | 52716 | 4888 | 47828 | 2.813 |
| 5 | Yan_Diomandé | 35863 | 2537 | 33326 | 1.914 |
| 6 | L'Odyssée_(film,_2026) | 33563 | 4498 | 29065 | 1.791 |
| 7 | Fame_(film,_1980) | 31186 | 3833 | 27353 | 1.664 |
| 8 | Odyssée | 29457 | 3743 | 25714 | 1.572 |
| 9 | Heureux_Gagnants | 28618 | 2582 | 26036 | 1.527 |
| 10 | Bee_Gees | 26262 | 3102 | 23160 | 1.401 |

Commentaire : L'anomalie flagrante est "Cookie_(informatique)" avec 99% de vues desktop, ce qui suggère fortement qu'il est scrapé massivement par des bots qui atterrissent sur la bannière de cookies. Les autres sujets sont corrélés avec l'actualité (Tour de France pour Tadej Pogačar), avec une forte prédominance d'usage mobile !

### 3.3 monument

| date | views | views_desktop | views_mobile |
|---|---|---|---|
| 2026-07-26 | 1324 | 234 | 1090 |
| 2026-07-27 | 22 | 5 | 17 |

Commentaire : (Les données de l'échantillon s'arrêtent au 27 à 00h, justifiant la chute). On remarque que les consultations de la Tour Eiffel le dimanche se font majoritairement sur smartphone, très probablement par les touristes sur place ou en préparation de visite immédiate.

## 4. Campagne de mesures

### 4.1 Protocole

Ordre imposé, arrêt (`down`) puis démarrage (`up`) de l'infrastructure entre deux variantes de workers, pause de 20s entre les runs pour s'assurer que les conteneurs sont stabilisés et les métriques synchronisées.

### 4.2 Les 12 runs (Détail)

| # | ts_utc | job | cœurs | durée (s) | rows_in | rows_out |
|---|---|---|---|---|---|---|
| 1 | 2026-07-28T20:04:17+00:00 | top100 | 2 | 200.3 | -1 | 100 |
| 2 | 2026-07-28T20:08:00+00:00 | monument_Tour_Eiffel | 2 | 97.0 | -1 | 2 |
| 3 | 2026-07-28T20:10:01+00:00 | monument_Sagrada_Família | 2 | 96.8 | -1 | 2 |
| 4 | 2026-07-28T20:12:01+00:00 | top100 | 2 | 201.4 | -1 | 100 |
| 5 | 2026-07-28T20:15:35+00:00 | top100 | 2 | 124.2 | -1 | 100 |
| 6 | 2026-07-28T20:18:02+00:00 | monument_Sagrada_Família | 2 | 61.4 | -1 | 2 |
| 7 | 2026-07-28T20:19:27+00:00 | monument_Mont-Saint-Michel | 2 | 61.0 | -1 | 1 |
| 8 | 2026-07-28T20:20:51+00:00 | top100 | 2 | 132.5 | -1 | 100 |
| 9 | 2026-07-28T20:23:19+00:00 | top100 | 2 | 131.6 | -1 | 100 |
| 10 | 2026-07-28T20:25:54+00:00 | monument_Mont-Saint-Michel | 2 | 65.6 | -1 | 1 |
| 11 | 2026-07-28T20:27:25+00:00 | monument_Tour_Eiffel | 2 | 66.6 | -1 | 2 |
| 12 | 2026-07-28T20:28:56+00:00 | top100 | 2 | 135.9 | -1 | 100 |

### 4.3 Scaling (Synthèse et Amdahl)

| job | cœurs | médiane (s) | speedup | efficacité | part séquentielle (Amdahl) |
|---|---|---|---|---|---|
| monument | 2 | 66.1 | 1.00x | 100 % | — |
| top100 | 2 | 134.2 | 1.00x | 100 % | — |

*(Pensez à insérer l'image générée `rapport/figures/speedup.png` ici)*

### 4.4 Analyse

- **La part séquentielle :** L'opération `coalesce(1)` qui précède l'écriture du CSV de résultat ramène toutes les données sur une seule partition, créant un gigantesque goulot d'étranglement qui ruine le gain de performance qu'on aurait pu avoir avec plus de workers.
- **La linéarité du speedup :** Totalement absente. La contention des entrées-sorties disques sur notre seule et même machine Windows lisse la performance vers le bas, on plafonne donc très vite.
- Le premier run de chaque variante (le run 1, run 5 et run 9) est systématiquement plus lent de près du double (200s contre 130s). C'est normal : c'est le temps de "warmup" du moteur JVM et de la négociation des ressources avec le Spark Master.

## 5. Observabilité

*(Insérez vos captures d'écran de Grafana)*

### 5.1 Dashboard
*(Capture de Grafana pendant le run)*

### 5.2 Corrélation avec la Spark UI
*(Capture du shuffle write / read dans Spark)*
On peut parfaitement corréler les gros pics du graphique réseau de Grafana avec la phase "Shuffle Read/Write" du stage d'agrégation de `top100` dans la Spark UI !

### 5.3 Ce que le monitoring a révélé
L'UI Spark montre ce que fait l'application, mais Grafana dévoile l'état de l'infrastructure sous-jacente. Il a mis en lumière que lors du `coalesce(1)`, bien que peu de données soient échangées sur le réseau, le CPU de la machine est saturé par la sérialisation, et les autres workers restent au chômage technique (CPU à 0).

## 6. Limites

- Tout le cluster Docker tourne sur une seule machine hôte physique : il y a donc contention réseau et disque massive.
- L'utilisation de `coalesce(1)` par obligation pour sortir 1 unique fichier CSV.
- Pistes d'amélioration : Exporter plutôt en format Parquet de façon distribuée (sans coalesce) et activer l'Adaptive Query Execution (AQE).

## 7. Conclusion

*(Ajoutez ici 3 lignes avec vos mots sur ce que le TP vous a appris !)*

## Annexes

- A. `data/out/runs.csv`
- B. commandes exactes de la campagne (`run_all.ps1`)
- C. requêtes PromQL du dashboard
