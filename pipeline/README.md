# `data-inclusion-scripts`

Ce dépôt contient des workflows (airflow) pour le traitement des données de l'inclusion.

## Pipeline

```mermaid
graph TD;
    subgraph legend
        direction LR
        legend-airflow-left[ ] -- "airflow" --> legend-airflow-right[ ]
        legend-dbt-left[ ] -- "airflow+dbt" --> legend-dbt-right[ ]
    end

    emplois-de-linclusion[emplois de l'inclusion] -- extraction --> s3
    soliguide -- extraction --> s3[datalake fa:fa-link]
    autre[...] -- extraction --> s3
    s3 -- chargement --> source
    subgraph datawarehouse
    source -- 1. nettoyage --> staging
    staging -- 2. remodélisation \n 3. géocodage --> intermediate
    end
    intermediate -- "🕙 quotidien" --> api
    intermediate --> metabase
    intermediate -- "🕙 chaque lundi" --> opendata[open data]
    api -- en flux --> dora

    click s3 "#datalake"
    click metabase "#metabase"
    click opendata "#open-data"

    linkStyle 0,2,3,4,5,8,9,10 stroke:#4287f5,stroke-width:4;
    linkStyle 1,6,7 stroke:orange,stroke-width:4;
```

## Outils

### airflow

|      |                                                                               |
|------|-------------------------------------------------------------------------------|
| dev  | http://localhost:8080                                                         |
| prod | [lien 🔗](https://data-inclusion-scripts-staging.osc-secnum-fr1.scalingo.io/) |

### datalake

|      |                                                                                                    |
|------|----------------------------------------------------------------------------------------------------|
| dev  | Utiliser la cli `mc`                                                                               |
| prod | [lien 🔗](https://console.scaleway.com/object-storage/buckets/fr-par/data-inclusion-lake/explorer) |

### metabase

|      |                                                          |
|------|----------------------------------------------------------|
| dev  | ❌                                                        |
| prod | [lien 🔗](https://metabase.data.inclusion.gouv.fr/) |

### open data

[lien 🔗](https://www.data.gouv.fr/fr/datasets/referentiel-de-loffre-dinsertion-liste-des-structures-et-services-dinsertion/)

### api

|      |                                                                                                    |
|------|----------------------------------------------------------------------------------------------------|
| dev  | http://localhost:8000/api/v0/docs                                                                               |
| prod | [lien 🔗](https://api.data.inclusion.gouv.fr/api/v0/docs) |

## [Contribuer](CONTRIBUTING.md)
