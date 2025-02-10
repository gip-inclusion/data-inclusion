### URL de base

L'API est utilisable à partir de l'URL de base suivante:

```
{{ base_url }}
```

Par exemple, l'adresse complète (URL) correspondant à une recherche de services,
sans paramètres additionnels, est:

```
{{ base_url }}/api/v0/search/services
```



### Token

Un token est nécessaire pour accéder aux données.

Les demandes de tokens s'effectuent via [ce formulaire](https://tally.so/r/mYjJ85). L'équipe data·inclusion prendra contact avec vous.

Le token doit être renseigné dans chaque requête via un header:
`Authorization: Bearer <VOTRE_TOKEN>`.

### Schéma des données

Les données utilisent le schéma data·inclusion. Ce schéma comprend deux modèles principaux :

* les structures proposant des services
* les services proposés par ces structures

Ces deux modèles utilisent des référentiels faisant également partie du schéma data·inclusion : les types de structures et de services, les thématiques, etc.

Plus d'informations sur le
[dépôt](https://github.com/gip-inclusion/data-inclusion-schema) versionnant le schéma,
sur la [documentation officielle](https://data.inclusion.gouv.fr/outils-et-documentation/schema-de-donnees/)
ou sur la page [schema.gouv](https://schema.data.gouv.fr/gip-inclusion/data-inclusion-schema/) du schéma.

### Sources des données

Les données data·inclusion sont issues d'un ensemble de sources (emplois de l'inclusion, France Travail, etc.).

Le endpoint `/sources` permet de lister les sources disponibles.


### Filtrer géographiquement les données

Les données renvoyées par certains endpoints peuvent être filtrées géographiquement.

Les codes communes, départements et régions utilisés sont issus de l'API [découpage administratif](https://geo.api.gouv.fr/decoupage-administratif).

L'[api de la base adresse nationale](https://adresse.data.gouv.fr/api-doc/adresse) est utilisée afin d'automatiser l'identification de codes insee associés à partir d'adresses ou de parties d'adresses (e.g. nom de commune, code postal).


### Nous contacter

#### via notre [formulaire de contact](https://data.inclusion.gouv.fr/nous-contacter/)

#### par mail à [data-inclusion@inclusion.gouv.fr](mailto:data-inclusion@inclusion.gouv.fr)
