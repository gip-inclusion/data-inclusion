{% set description_cmp | trim | replace('\n', ' ') %}
Le Centre Médico-Psychologique (CMP) est un lieu de soins
public qui offre des consultations médico-psychologiques
et sociales à toute personne en difficulté psychique.
\n\n
Il existe deux types de centres médico-psychologiques : ceux
destinés aux enfants et adolescents (CMPEA), et ceux réservés
aux adultes (CMP). Se référer au nom de la structure pour
confirmer le public.
{% endset %}

{% set description_esat | trim | replace('\n', ' ') %}
L’Établissement et Service d’Accompagnement par le Travail (ESAT)
est une structure qui permet aux personnes en situation de handicap
d’exercer une activité professionnelle tout en bénéficiant d’un
soutien médico-social et éducatif dans un milieu protégé. Cette
structure accueille des personnes qui n’ont pas acquis assez
d’autonomie pour travailler en milieu ordinaire ou en entreprise.
{% endset %}

{% set description_mdph | trim | replace('\n', ' ') %}
Les Maisons Départementales des Personnes Handicapées (MDPH) accompagnent
au quotidien les personnes en situation de handicap dans tous les domaines
de leur vie, quels que soient leur âge et leur situation.
\n\n
Une MDPH a une mission d’accueil, d’information, d’accompagnement
et de conseil des personnes en situation de handicap et de leur
famille, ainsi que de sensibilisation des citoyens au handicap.
{% endset %}

WITH etablissements AS (
    SELECT * FROM {{ ref('stg_finess__etablissements') }}
),

final AS (
    SELECT
        'finess'                                        AS "source",
        'finess--' || numero_finess_et                  AS "id",
        'finess--' || numero_finess_et                  AS "adresse_id",
        COALESCE(raison_sociale_longue, raison_sociale) AS "nom",
        CASE
            WHEN categorie = '156' THEN E'{{ description_cmp }}'
            WHEN categorie = '246' THEN E'{{ description_esat }}'
            WHEN categorie = '609' THEN E'{{ description_mdph }}'
        END                                             AS "description",
        NULL                                            AS "lien_source",
        siret                                           AS "siret",
        date_maj_structure                              AS "date_maj",
        telephone                                       AS "telephone",
        NULL                                            AS "courriel",
        NULL                                            AS "site_web",
        NULL                                            AS "horaires_accueil",
        NULL                                            AS "accessibilite_lieu",
        CASE
            WHEN categorie = '156' THEN ARRAY['cmp']
            WHEN categorie = '246' THEN ARRAY['esat']
            WHEN categorie = '609' THEN ARRAY['mdph']
        END                                             AS "reseaux_porteurs"
    FROM etablissements
)

SELECT * FROM final
