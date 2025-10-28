WITH source AS (
    {{ stg_source_header('monenfant', 'creches') }}),

creches AS (
    SELECT
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
        NULLIF(NULLIF(TRIM(data -> 'coordonnees' ->> 'telephone'), ''), '0000000000')     AS "coordonnees__telephone",
        NULLIF(NULLIF(TRIM(data -> 'coordonnees' ->> 'telephone2'), ''), '0000000000')    AS "coordonnees__telephone2",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'adresseMail'), '')                         AS "coordonnees__adresse_mail",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'siteInternet'), '')                        AS "coordonnees__site_internet",
        NULLIF(TRIM(data -> 'description' ->> 'description1'), '')                        AS "description__modalites_tarifaires",
        CASE
            WHEN LENGTH(TRIM(data -> 'description' ->> 'description2')) >= 8  -- junk values observed on shorter entries
                THEN TRIM(data -> 'description' ->> 'description2')
        END                                                                               AS "description__projet",
        NULLIF(TRIM(data -> 'description' ->> 'description3'), '')                        AS "description__gestionnaire",
        NULLIF(TRIM(data -> 'description' ->> 'description4'), '')                        AS "description__equipe",
        NULLIF(TRIM(data -> 'description' ->> 'description5'), '')                        AS "description__conditions_admission",
        NULLIF(TRIM(data -> 'description' ->> 'description6'), '')                        AS "description__modalites_inscription",
        CAST(data -> 'serviceCommun' -> 'avip' AS BOOLEAN)                                AS "service_commun__avip",
        NULLIF(TRIM(data -> 'serviceCommun' -> 'calendrier' ->> 'joursHorairesText'), '') AS "service_commun__calendrier__jours_horaires_text",
        data -> 'serviceCommun' -> 'calendrier'                                           AS "service_commun__calendrier",
        NULLIF(TRIM(data -> 'serviceAccueil' ->> 'handicap'), '')                         AS "service_accueil__handicap"
    FROM source
),

final AS (
    -- our scraping can create duplicates : the search results overlaps between two nearby cities
    SELECT DISTINCT ON (structure_id) *
    FROM creches
)

SELECT * FROM final
