# sources

{% for source in sources %}

## {{ source.nom }}

Identifiant de la source dans les données (colonne `source`) : `{{ source.id }}`

### Description

{{ source.description|default('*Pas encore de description*', true) }}

### Qualité

| Fréquence de récupération | Date dernière récupération | Thématiques principales |
| -------- | -------- | -------- |
| {{ source.frequence_recuperation }} | {{ source.date_derniere_recuperation|default('', true) }} | {{ source.thematiques|default('', true) }} |

### Ressources

{% for ressource in source.ressources %}

#### {{ ressource.id }}

{{ ressource.description }}

| Lien données producteur                 | Types de données                                  | Dans l'API          | En Open Data |
|-----------------------------------------|-----------------------------------------|---------------------|--------------|
|  {{ ressource.lien_donnees_producteurs }} | {{ ressource.types_donnees }} | {{ ressource.api }} | {{ ressource.open_data }}         |
| **Siretisation automatisable**       | **Historisation**                       | **Correspondances** |
| {{ ressource.siretisation_automatisable }} | {{ ressource.historisation }} | {{ ressource.correspondances }} |

{% endfor %}

{% endfor %}