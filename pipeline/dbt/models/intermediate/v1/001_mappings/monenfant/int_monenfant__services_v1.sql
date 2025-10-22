{% set description_avip %}
'## À Vocation d’Insertion Professionnelle

Les crèches à vocation d’insertion professionnelle, dites crèches Avip, facilitent l’accès à l’emploi des parents de jeunes enfants, notamment les mères isolées, en accueillant leurs jeunes enfants. En effet, ce dispositif, créé par Pôle emploi et la Cnaf, propose aux demandeurs d’emploi un service ponctuel pour faire garder leurs enfants de 0 à 3 ans afin qu’ils puissent mener à bien leurs démarches de recherche d’emploi (entretien d’embauche et de sélection, formation, rendez-vous avec Pôle emploi, période d’essai, stage, création d’entreprise…).

Les crèches Avip accueillent les enfants comme toutes les autres crèches. Elles appliquent le même tarif et répondent aux mêmes exigences de qualité. Mais elles portent une attention particulière aux parents éloignés de l’emploi et volontaires pour s’engager dans une démarche active de recherche d’emploi. Un effort spécifique est fait pour les parents qui élèvent seuls leurs enfants et ceux qui vivent dans un quartier classé en zone dite «politique de la ville». Les crèches Avip s’adressent aussi aux parents de jeunes enfants qui ont des horaires de travail en décalé puisqu’elles proposent d’accueillir les enfants sur des plages horaires plus larges.

Les **crèches à vocation d’insertion professionnelle (AVIP)** ont une double mission :

- Réserver une place en crèche de jeunes enfants (zéro à trois ans) de parents sans emploi
- Les accompagner vers l’emploi ou la formation professionnelle'
{% endset %}

WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

zones_eligibilite AS (
    SELECT
        communes_desservies.structure_id,
        ARRAY_AGG(DISTINCT communes.code) AS "zone_eligibilite"
    FROM {{ ref('stg_monenfant__communes_desservies') }} AS communes_desservies
    INNER JOIN {{ ref('stg_decoupage_administratif__communes') }} AS communes
        -- array operator is required to leverage the GIN index on codes_postaux
        ON ARRAY[communes_desservies.code_postal] && communes.codes_postaux
    GROUP BY communes_desservies.structure_id
),

final AS (
    SELECT
        'monenfant'                                                             AS "source",
        'monenfant--' || creches.structure_id                                   AS "id",
        'monenfant--' || creches.structure_id                                   AS "adresse_id",
        'monenfant--' || creches.structure_id                                   AS "structure_id",
        ARRAY_TO_STRING(
            ARRAY[
                'Garde et accueil des enfants',
                CASE
                    WHEN creches.service_commun__avip
                        THEN ' À Vocation d’Insertion Professionnelle'
                END
            ],
            ', '
        )                                                                       AS "nom",
        ARRAY_TO_STRING(
            ARRAY[
                CASE WHEN creches.service_commun__avip THEN {{ description_avip }} END,
                '## Accueil des enfants en situation d’handicap' || E'\n\n' || creches.service_accueil__handicap,
                '## Le projet pédagogique :' || E'\n\n' || creches.description__projet
            ],
            E'\n\n'
        )                                                                       AS "description",
        FORMAT(
            'https://monenfant.fr/que-recherchez-vous/%s/%s/%s',
            creches.structure_name,
            creches.structure_type,
            creches.structure_id
        )                                                                       AS "lien_source",
        creches.modified_date                                                   AS "date_maj",
        'accompagnement'                                                        AS "type",
        ARRAY['famille--garde-denfants']                                        AS "thematiques",
        'payant'                                                                AS "frais",
        creches.description__modalites_tarifaires                               AS "frais_precisions",
        ARRAY['familles']                                                       AS "publics",
        NULL                                                                    AS "publics_precisions",
        creches.description__conditions_admission                               AS "conditions_acces",
        creches.coordonnees__telephone                                          AS "telephone",
        creches.coordonnees__adresse_mail                                       AS "courriel",
        NULL                                                                    AS "contact_nom_prenom",
        ARRAY['en-presentiel']                                                  AS "modes_accueil",
        zones_eligibilite.zone_eligibilite                                      AS "zone_eligibilite",
        NULL                                                                    AS "zone_eligibilite_type",
        NULL                                                                    AS "lien_mobilisation",
        NULLIF(ARRAY_REMOVE(
            ARRAY[
                CASE WHEN creches.coordonnees__telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN creches.coordonnees__adresse_mail IS NOT NULL THEN 'envoyer-un-courriel' END
            ],
            NULL
        ), '{}')                                                                AS "modes_mobilisation",
        ARRAY['usagers', 'professionnels']                                      AS "mobilisable_par",
        creches.description__modalites_inscription                              AS "mobilisation_precisions",
        NULL                                                                    AS "volume_horaire_hebdomadaire",
        NULL                                                                    AS "nombre_semaines",
        processings.monenfant_opening_hours(creches.service_commun__calendrier) AS "horaires_accueil"
    FROM creches
    LEFT JOIN zones_eligibilite ON creches.structure_id = zones_eligibilite.structure_id
)

SELECT * FROM final
