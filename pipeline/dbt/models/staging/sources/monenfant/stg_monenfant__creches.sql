WITH source AS (
    {{ stg_source_header('monenfant', 'creches') }}
),

creches AS (
    SELECT
        _di_source_id,
        NULLIF(TRIM(data ->> 'structureType'), '')                                        AS "structure_type",
        NULLIF(TRIM(data ->> 'structureId'), '')                                          AS "structure_id",
        NULLIF(TRIM(data ->> 'structureName'), '')                                        AS "structure_name",
        TO_DATE(data ->> 'modifiedDate', 'DD/MM/YYYY')                                    AS "modified_date",
        NULLIF(TRIM(data ->> 'gestionnaire'), '')                                         AS "gestionnaire",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'numeroVoie'), '')                          AS "coordonnees__numero_voie",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'typeVoie'), '')                            AS "coordonnees__type_voie",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'nomVoie'), '')                             AS "coordonnees__nom_voie",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'codePostal'), '')                          AS "coordonnees__code_postal",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'commune'), '')                             AS "coordonnees__commune",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'telephone'), '')                           AS "coordonnees__telephone",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'telephone2'), '')                          AS "coordonnees__telephone2",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'adresseMail'), '')                         AS "coordonnees__adresse_mail",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'siteInternet'), '')                        AS "coordonnees__site_internet",
        NULLIF(TRIM(data -> 'description' ->> 'description1'), '')                        AS "description__modalites_tarifaires",
        NULLIF(TRIM(data -> 'description' ->> 'description2'), '')                        AS "description__projet",
        NULLIF(TRIM(data -> 'description' ->> 'description3'), '')                        AS "description__gestionnaire",
        NULLIF(TRIM(data -> 'description' ->> 'description4'), '')                        AS "description__equipe",
        NULLIF(TRIM(data -> 'description' ->> 'description5'), '')                        AS "description__conditions_admission",
        NULLIF(TRIM(data -> 'description' ->> 'description6'), '')                        AS "description__modalites_inscription",
        CAST(data -> 'serviceCommun' -> 'avip' AS BOOLEAN)                                AS "service_commun__avip",
        CAST(data -> 'serviceCommun' -> 'calendrier' -> 'rendezVous' AS BOOLEAN)          AS "service_commun__calendrier__rendez_vous",
        NULLIF(TRIM(data -> 'serviceCommun' -> 'calendrier' ->> 'joursHorairesText'), '') AS "service_commun__calendrier__jours_horaires_text"
    FROM source
),

final AS (
    SELECT DISTINCT ON (structure_id) *
    FROM creches
)

SELECT * FROM final
