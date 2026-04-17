{% set description_cmp | trim | replace('\n', ' ') %}
Le centre médico- psychologique (CMP) est un lieu de soins public
qui offre des consultations médico-psychologiques et sociales
à toute personne en difficulté psychique.
{% endset %}

{% set mobilisation_precisions | trim | replace('\n', ' ') %}
Il existe deux types de centres médico-psychologiques :
ceux destinés aux enfants et adolescents (CMPEA),
et ceux réservés aux adultes (CMP).
Se référer au nom de la structure pour confirmer le public.
{% endset %}

WITH etablissements AS (
    SELECT * FROM {{ ref('stg_finess__etablissements') }}
),

final AS (
    SELECT
        'finess'                                                                                                AS "source",
        'finess--' || numero_finess_et                                                                          AS "structure_id",
        'finess--' || numero_finess_et || '--cmp'                                                               AS "id",
        'finess--' || numero_finess_et                                                                          AS "adresse_id",
        NULL                                                                                                    AS "courriel",
        NULL                                                                                                    AS "contact_nom_prenom",
        date_maj_structure                                                                                      AS "date_maj",
        'Consultations médico-psychologiques et sociales'                                                       AS "nom",
        '{{ description_cmp }}'                                                                                 AS "description",
        NULL                                                                                                    AS "lien_source",
        'accompagnement'                                                                                        AS "type",
        ARRAY['sante--sante-mentale', 'sante--addictions', 'famille--soutien-a-la-parentalite-et-a-leducation'] AS "thematiques",
        'gratuit'                                                                                               AS "frais",
        'Prise en charge gratuite et intégralement remboursée par la sécurité sociale'                          AS "frais_precisions",
        CASE
            WHEN raison_sociale ~ 'ADOS?\M' THEN ARRAY['jeunes']
            WHEN raison_sociale ~ 'ENFANT' THEN ARRAY['jeunes']
            WHEN raison_sociale ~ 'CMPEA' THEN ARRAY['jeunes']
            ELSE ARRAY['tous-publics']
        END                                                                                                     AS "publics",
        NULL                                                                                                    AS "publics_precisions",
        NULL                                                                                                    AS "conditions_acces",
        telephone                                                                                               AS "telephone",
        ARRAY['telephoner', 'se-presenter']                                                                     AS "modes_mobilisation",
        NULL                                                                                                    AS "lien_mobilisation",
        ARRAY['usagers', 'professionnels']                                                                      AS "mobilisable_par",
        '{{ mobilisation_precisions }}'                                                                         AS "mobilisation_precisions",
        ARRAY['en-presentiel']                                                                                  AS "modes_accueil",
        ARRAY[departement]                                                                                      AS "zone_eligibilite",
        NULL                                                                                                    AS "volume_horaire_hebdomadaire",
        NULL                                                                                                    AS "nombre_semaines",
        NULL                                                                                                    AS "horaires_accueil"
    FROM etablissements
)

SELECT * FROM final
