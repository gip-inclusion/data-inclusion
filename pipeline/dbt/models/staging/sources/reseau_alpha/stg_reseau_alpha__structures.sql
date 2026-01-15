WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'id'), '')                                        AS "id",
        NULLIF(TRIM(data ->> 'acronyme'), '')                                  AS "acronyme",
        processings.html_to_markdown(NULLIF(TRIM(data ->> 'description'), '')) AS "description",
        CAST(data ->> 'estCoordination' AS BOOLEAN)                            AS "est_coordination",
        NULLIF(TRIM(data ->> 'logo'), '')                                      AS "logo",
        NULLIF(TRIM(data ->> 'nom'), '')                                       AS "nom",
        NULLIF(TRIM(data ->> 'slug'), '')                                      AS "slug",
        NULLIF(TRIM(data ->> 'type'), '')                                      AS "type",
        NULLIF(TRIM(data ->> 'url'), '')                                       AS "url",
        NULLIF(TRIM(data ->> 'urlCoordination'), '')                           AS "url_coordination"
    FROM source
)

SELECT * FROM final
