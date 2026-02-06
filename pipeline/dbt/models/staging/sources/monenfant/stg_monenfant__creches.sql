WITH source AS (
    {{ stg_source_header('monenfant', 'creches') }}),

/*
- Description fields can have junk values : very short strings, strings with only non-word characters
*/

creches AS (
    SELECT
        NULLIF(TRIM(data ->> 'structureType'), '')                                                                                     AS "structure_type",
        NULLIF(TRIM(data ->> 'structureId'), '')                                                                                       AS "structure_id",
        NULLIF(TRIM(data ->> 'structureName'), '')                                                                                     AS "structure_name",
        TO_DATE(data ->> 'modifiedDate', 'DD/MM/YYYY')                                                                                 AS "modified_date",
        NULLIF(TRIM(data ->> 'gestionnaire'), '')                                                                                      AS "gestionnaire",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'numeroVoie'), '')                                                                       AS "coordonnees__numero_voie",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'typeVoie'), '')                                                                         AS "coordonnees__type_voie",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'nomVoie'), '')                                                                          AS "coordonnees__nom_voie",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'codePostal'), '')                                                                       AS "coordonnees__code_postal",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'commune'), '')                                                                          AS "coordonnees__commune",
        NULLIF(NULLIF(TRIM(data -> 'coordonnees' ->> 'telephone'), ''), '0000000000')                                                  AS "coordonnees__telephone",
        NULLIF(NULLIF(TRIM(data -> 'coordonnees' ->> 'telephone2'), ''), '0000000000')                                                 AS "coordonnees__telephone2",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'adresseMail'), '')                                                                      AS "coordonnees__adresse_mail",
        NULLIF(TRIM(data -> 'coordonnees' ->> 'siteInternet'), '')                                                                     AS "coordonnees__site_internet",
        NULLIF(REGEXP_REPLACE(processings.html_to_markdown(TRIM(data -> 'description' ->> 'description1')), '^\W*$', ''), '')          AS "description__modalites_tarifaires",
        NULLIF(REGEXP_REPLACE(processings.html_to_markdown(TRIM(data -> 'description' ->> 'description2')), '^(\W*|.{1,8})$', ''), '') AS "description__projet",
        NULLIF(REGEXP_REPLACE(processings.html_to_markdown(TRIM(data -> 'description' ->> 'description3')), '^\W*$', ''), '')          AS "description__gestionnaire",
        NULLIF(REGEXP_REPLACE(processings.html_to_markdown(TRIM(data -> 'description' ->> 'description4')), '^\W*$', ''), '')          AS "description__equipe",
        NULLIF(REGEXP_REPLACE(processings.html_to_markdown(TRIM(data -> 'description' ->> 'description5')), '^\W*$', ''), '')          AS "description__conditions_admission",
        NULLIF(REGEXP_REPLACE(processings.html_to_markdown(TRIM(data -> 'description' ->> 'description6')), '^\W*$', ''), '')          AS "description__modalites_inscription",
        CAST(data -> 'serviceCommun' -> 'avip' AS BOOLEAN)                                                                             AS "service_commun__avip",
        NULLIF(TRIM(data -> 'serviceCommun' -> 'calendrier' ->> 'joursHorairesText'), '')                                              AS "service_commun__calendrier__jours_horaires_text",
        data -> 'serviceCommun' -> 'calendrier'                                                                                        AS "service_commun__calendrier",
        processings.html_to_markdown(NULLIF(REGEXP_REPLACE(TRIM(data -> 'serviceAccueil' ->> 'handicap'), '^\W*$', ''), ''))           AS "service_accueil__handicap"
    FROM source
),

final AS (
    -- our scraping can create duplicates : the search results overlaps between two nearby cities
    SELECT DISTINCT ON (structure_id) *
    FROM creches
)

SELECT * FROM final
