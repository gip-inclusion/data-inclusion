# sources

{% for source in sources %}

## {{ source.nom }}

{{ source.description|default('*Pas encore de description*', true) }}

|                                                              |                                                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------------------ |
| Type d'usagers accompagnés                                   | {{ source.type_usager }}                                                        |
| Lien vers le site/outil/portail                              | {% if source.lien_source                                                 | length %}[lien]({{ source.lien_source }}){% endif %}          |
| Thématiques principales abordées                             | {{ source.thematiques                                                    | default('', true) }}                                          |
| Lien vers les statistiques publiques de la source            | {% if source.lien_stats_publiques                                        | length %}[lien]({{ source.lien_stats_publiques }}){% endif %} |
| Identifiant de la source dans les données (colonne `source`) | `{{ source.id }}`                                                        |
| Date/fréquence de récupération par data.inclusion            | {{ source.frequence_recuperation }} {{ source.date_derniere_recuperation | default('', true) }}                                          |

{% for ressource in source.ressources %}

#### {{ source.nom }} : {{ ressource.id }}

{{ ressource.description }}

|                                                                    |                                            |
| ------------------------------------------------------------------ | ------------------------------------------ |
| Lien vers les données d'origine                                    | {{ ressource.lien_donnees_producteurs }}   |
| Types de données                                                   | {{ ressource.types_donnees }}              |
| Ces données sont disponibles dans [l'API data.inclusion](https://www.data.inclusion.beta.gouv.fr/api/lapi-data-inclusion)             | {{ ressource.api }}                        |
| Ces données sont disponibles en [Open Data](https://www.data.inclusion.beta.gouv.fr/open-data/acceder-aux-donnees) | {{ ressource.open_data }}                  |
| Ces données sont disponibles dans [notre outil de siretisation](https://www.data.inclusion.beta.gouv.fr/schemas-de-donnees-de-loffre/siretisation)      | {{ ressource.siretisation_automatisable }} |
| Ces données sont disponibles dans notre outil de correspondance    | {{ ressource.correspondances }}            |
| Nous historicisons les données d'origine                                     | {{ ressource.historisation }}              |

{% endfor %}

_______
{% endfor %}