# `deduplication`

Ce dossier contient les éléments d'entraînement et d'écaluation de notre modèle
de déduplication, lequel est ensuite utilisé en production dans notre pipeline.

Le tout tient dans un simple Notebook Jupyter.

## Utilisation
Ce dossier est considéré comme un projet Python à part entière.

A ce titre, il est important de lui créer son propre environnement virtuel:

```bash
python3 -m venv .venv --prompt deduplication

source .venv/bin/activate
pip install -e .

jupyter-notebook main.ipynb
```

Le notebook concentre les différentes étapes liées à la gestion de notre
algorithme de déduplication.


### Preprocessing
Nous faisons un traitement préliminaire sur les données issues des dumps
au format Parquet de nos structures afin d'avoir les données les mieux
formatées possible.

### Entraînement
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

L'apprentissage génère un fichier `labels.json` qui enregistre les couples d'identifiants
DI pour lesquels nous avons annoté le caractère "doublon" ou non; nous enregistrons
également les cas où un doute subsiste.

Ce fichier devrait être enrichi à chaque nouveau doublon découvert en production.

### Entraînement
Le fichier de labels est utilisé pour entraîner un modèle capable de mesurer une distance
entre ddexu structures. Ce modèle est un simple fichier binaire (`model.bin`) qui est
ensuite réutilisé dans le pipeline pour reconnaître les doublons en production.

### Test
Afin de s'assurer au mieux que notre algorithme reste pertinent, nous utilisons une
collection d'outils de mesure de performance statistiques qui nous permettent de
vérifier que notre tradeoff entre précision et "rappel" (faible taux de faux positifs,
grand nombre de doublons effectifs trouvés) reste optimal.

Pour cela le Notebook évalue un certain nombre de valeurs possibles des hyperparamètres
de la bibliothèque `dedupe` (paramètres `threshold` et `recall`) sur des sous-ensembles
de validation choisis par `sklearn.train_test_split()` et nous renseigne sur le
choix de paramètres optimaux.

Quelques références à ce sujet:
- [F-1 score](https://serokell.io/blog/a-guide-to-f1-score)
- [Confusion matrices](https://freedium.cfd/https://smuhabdullah.medium.com/confusion-matrices-and-classification-reports-a-guide-to-evaluating-machine-learning-models-385496cf7cee)
- [ROC Curve](https://en.wikipedia.org/wiki/Receiver_operating_characteristic)
- [Cross-validation](https://scikit-learn.org/stable/modules/cross_validation.html#cross-validation)
