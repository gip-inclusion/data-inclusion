{% set cmp = namespace() %}

{% set cmp.description | trim | replace('\n', ' ') %}
Le centre médico- psychologique (CMP) est un lieu de soins public
qui offre des consultations médico-psychologiques et sociales
à toute personne en difficulté psychique.
{% endset %}

{% set cmp.mobilisation_precisions | trim | replace('\n', ' ') %}
Il existe deux types de centres médico-psychologiques :
ceux destinés aux enfants et adolescents (CMPEA),
et ceux réservés aux adultes (CMP).
Se référer au nom de la structure pour confirmer le public.
{% endset %}

{% set mdph = namespace() %}

{% set mdph.description | trim | replace('\n', ' ') %}
Les maisons départementales des personnes handicapées (MDPH) sont,
dans chaque département, le guichet unique d’accès simplifié aux
droits et prestations pour les personnes handicapées (enfants et adultes).
\n\n
La MDPH peut :\n
- vous accueillir vous et votre famille\n
- vous informer\n
- vous aider pour expliquer votre situation et vos besoins\n
- regarder votre dossier de demande d’aide.\n
\n\n
Beaucoup de maisons départementales des personnes handicapées proposent
un téléservice de demande de prestations à la MDPH qui permet d’effectuer
en ligne la saisie du formulaire et le dépôt des documents obligatoires.
{% endset %}

{% set mdph.mobilisation_precisions | trim | replace('\n', ' ') %}
La MDPH compétente pour traiter le dossier est celle du département
où se trouve la résidence principale de la personne.
{% endset %}

WITH etablissements AS (
    SELECT * FROM {{ ref('stg_finess__etablissements') }}
),

cmp AS (
    SELECT
        'finess'                                                                                                AS "source",
        'finess--' || numero_finess_et                                                                          AS "structure_id",
        'finess--' || numero_finess_et || '--cmp'                                                               AS "id",
        'finess--' || numero_finess_et                                                                          AS "adresse_id",
        NULL                                                                                                    AS "courriel",
        NULL                                                                                                    AS "contact_nom_prenom",
        date_maj_structure                                                                                      AS "date_maj",
        'Consultations médico-psychologiques et sociales'                                                       AS "nom",
        '{{ cmp.description }}'                                                                                 AS "description",
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
        '{{ cmp.mobilisation_precisions }}'                                                                     AS "mobilisation_precisions",
        ARRAY['en-presentiel']                                                                                  AS "modes_accueil",
        ARRAY[departement]                                                                                      AS "zone_eligibilite",
        NULL                                                                                                    AS "volume_horaire_hebdomadaire",
        NULL                                                                                                    AS "nombre_semaines",
        NULL                                                                                                    AS "horaires_accueil"
    FROM etablissements
    WHERE categorie = '156'
),

mdph AS (
    SELECT
        'finess'                                                                                           AS "source",
        'finess--' || numero_finess_et                                                                     AS "structure_id",
        'finess--' || numero_finess_et || '--mdph'                                                         AS "id",
        'finess--' || numero_finess_et                                                                     AS "adresse_id",
        NULL                                                                                               AS "courriel",
        NULL                                                                                               AS "contact_nom_prenom",
        date_maj_structure                                                                                 AS "date_maj",
        'Guichet unique d’accès simplifié aux droits et prestations pour les personnes handicapées (MDPH)' AS "nom",
        '{{ mdph.description }}'                                                                           AS "description",
        NULL                                                                                               AS "lien_source",
        'accompagnement'                                                                                   AS "type",
        ARRAY[
            'difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-aux-droits',
            'difficultes-administratives-ou-juridiques--beneficier-dune-mesure-daccompagnement-adapte',
            'sante--constituer-un-dossier-mdph-invalidite'
        ]                                                                                                  AS "thematiques",
        'gratuit'                                                                                          AS "frais",
        NULL                                                                                               AS "frais_precisions",
        ARRAY['personnes-en-situation-de-handicap']                                                        AS "publics",
        NULL                                                                                               AS "publics_precisions",
        NULL                                                                                               AS "conditions_acces",
        telephone                                                                                          AS "telephone",
        ARRAY['telephoner', 'se-presenter', 'utiliser-lien-mobilisation', 'envoyer-un-courriel']           AS "modes_mobilisation",
        'https://mdphenligne.cnsa.fr/'                                                                     AS "lien_mobilisation",
        ARRAY['usagers', 'professionnels']                                                                 AS "mobilisable_par",
        '{{ mdph.mobilisation_precisions }}'                                                               AS "mobilisation_precisions",
        ARRAY['en-presentiel']                                                                             AS "modes_accueil",
        ARRAY[departement]                                                                                 AS "zone_eligibilite",
        NULL                                                                                               AS "volume_horaire_hebdomadaire",
        NULL                                                                                               AS "nombre_semaines",
        NULL                                                                                               AS "horaires_accueil"
    FROM etablissements
    WHERE categorie = '609'
),

final AS (
    SELECT * FROM cmp
    UNION ALL
    SELECT * FROM mdph
)

SELECT * FROM final
