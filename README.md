# `deduplication`

Tout ce qui est lié à notre mécanisme de déduplication se trouvera dans ce dossier.
Il est pensé indépendamment du pipeline à ce jour; l'objectif est de pouvoir expérimenter
facilement sur sa propre machine ou en CI.

Notre algorithme est pour l'instant inspiré de:
https://dedupeio.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html


## Utilisation
Ce dossier est considéré comme un projet Python à part entière.

A ce titre, il est important de lui créer son propre environnement virtuel:

```bash
python3 -m venv .venv --prompt di-dedup

source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -r requirements/dev-requirements.txt

pip install -e .
```

Par ailleurs, nous avons besoin d'un accès à une base de données
PostgreSQL (via la variable d'environnement `PGDATABASE`)

Cette base de données sera utilisée:
- en lecture, pour la table `public.api__structure`
- en écriture, pour les tables générées par les outils de déduplication.


### Preprocessing
Le script `src/00_preprocess_structures.py` permet de générer une table
`public.preprocessed_structures` qui contient des données optimisées
pour l'entraînement de notre algorithme de déduplication.

```bash
python src/00_preprocess_structures.py
```


### Entraînement
L'entraînement se fait via le script `src/01_interactive_train.py`.

Ce dernier prend un certain temps à démarrer, car il commence par échantilloner
la table `preprocessed_structures` pour en sortir des blocs à présenter à
l'utilisateur, ce qui est gourmand en ressources.

L'étape suivante consiste à marquer, manuellement, les enregistrements
considérés comme doublons ou non. Attention ! Notre stratégie ici est
de marquer comme doublons ceux dont nous sommes "absolument certains" qu'il
s'agit de doublons.

Nous considérons comme doublon:
- toutes structures dont les champs nom, adresse, ville, etc sont proches
- ...à l'erreur de saisie près, ou changement de nom connu (PE=>FT par exemple)
- lorsque la structure A est contenue dans la structure B
- lorsque la structure A peut etre considérée comme un service annexe de la structure B

Nous essayons de viser pour la précision maximum et le moins de "faux positifs" possibles.


```bash
python src/01_interactive_train.py
[...]
# quelques sorties intermédiaires
[...]
# inputs utilisateurs pour choisir ce qui est un doublon ou non
[...]
```

L'apprentissage génère deux fichiers, qui sont commités avec le code:
- un fichier binaire `.deduper.settings` qui contient le modèle entraîné final;
- un fichier JSON `.deduper.training.json` qui contient les choix de doublons de l'utilisateur.

Le fichier JSON est éditable et est repris à chaque entraînement pour l'enrichir de nouveaux cas,
proposés par `dedupe`. Ce dernier s'efforce de trouver des cas "complexes" qui nous mettent
en difficulté.

Il est aussi possible est de supprimer les fichiers `.dedupe.*` pour
reprendre l'entraînement de zéro.


### Clustering
Le clustering (via `src/02_create_clusters.py`) permet, à partir du fichier `.deduper.settings`
et de la table `preprocessed_structures`, de générer une table `dedupe_clusters` contenant
les doublons regroupés en clusters.

```sql
> SELECT * FROM dedupe_clusters;
-[ RECORD 1 ]-+-----------------------------------------------------
cluster_id    | 1
structure_id  | action-logement-0ac6835a-2d0f-41ee-8590-9ccda367efa6
cluster_score | 0.8844631
cardinality   | 2
-[ RECORD 2 ]-+-----------------------------------------------------
cluster_id    | 1
structure_id  | soliguide-41997
cluster_score | 0.8844631
cardinality   | 2
```

### Test
Afin de s'assurer au mieux que notre algorithme ne fait pas de retour en arrière à chaque
modification, intégration de nouvelles données, etc; nous nous attachons à tester
systématiquement qu'il est toujours capable:
- de "retrouver" un certain nombre de doublons,
- de ne pas identifier comme doublons des structures "uniques"
- de ne pas baisser en termes de score de proximité de doublons (sauf exception)

Ce test s'appuie sur le fichier `dedupe_inputs.csv` (extrait de `preprocessed_structures` sur
un certain nombre d'ID structure) et s'attache à retrouver en sortie, un fichier JSON qui lui
aussi peut être régénéré.

```python
python tests/gen_inputs.py  # optionnel, génère dedupe_inputs.csv
pytest  # lance le test
pytest --update-duplicates  # MAJ le fichier dedupe_outputs.json
```


### Métriques
Nous ne sommes pas encore certains de la métrique de déduplication à suivre.

Un indicateur simple pourrait être de suivre le nombre de "clusters" versus le nombre de structures
total; de même, on pourrait suivre la moyenne et la médiane des scores des différents membres de
chaque cluster pour suivre la précision de notre algorithme au cours du temps.

Mais cela a ses limites : cherche-t-on vraiment à maximiser le nombre de clusters ?
Que dire si l'une de nos sources ajoute subitement de nombreux enregistrements "uniques", notre
indicateur baissera sans que l'on puisse en conclure quelque chose.

De même, les scores peuvent être en moyenne plus bas alors que la taille des clusters augmente;
ceci signifierait par exemple que l'on identifie subitement plus de doublons, avec marginalement
moins de précision, ce qui n'est pas forcément une mauvaise chose.

Nous devrions chercher à réduire le nombre de "doublons non détectés" idéalement. Or, presque
par définition, ces derniers sont difficiles à décompter.

Par ailleurs, il faudrait mettre en place une mesure pour détecter les "faux positifs", ie les
structures dans un cluster qui en réalité, ne devraient pas y être.
