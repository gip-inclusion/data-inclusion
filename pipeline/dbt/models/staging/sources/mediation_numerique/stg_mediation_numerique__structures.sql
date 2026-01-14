WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

final AS (
    SELECT
        source.data ->> 'id'                                                     AS "id",
        NULLIF(TRIM(source.data ->> 'nom'), '')                                  AS "nom",
        CASE
            WHEN LENGTH(source.data ->> 'pivot') = 14 AND source.data ->> 'pivot' != '00000000000000'
                THEN source.data ->> 'pivot'
        END                                                                      AS "siret",
        NULLIF(TRIM(source.data -> 'courriels' ->> 0), '')                       AS "courriel",
        NULLIF(TRIM(source.data ->> 'telephone'), '')                            AS "telephone",
        NULLIF(TRIM(source.data ->> 'site_web'), '')                             AS "site_web",
        NULLIF(TRIM(source.data ->> 'horaires'), '')                             AS "horaires",
        NULLIF(TRIM(source.data ->> 'presentation_detail'), '')                  AS "presentation_detail",
        NULLIF(TRIM(source.data ->> 'presentation_resume'), '')                  AS "presentation_resume",
        CAST(NULLIF(source.data ->> 'date_maj', '') AS TIMESTAMP WITH TIME ZONE) AS "date_maj",
        CAST(NULLIF(source.data ->> 'latitude', '') AS FLOAT)                    AS "latitude",
        CAST(NULLIF(source.data ->> 'longitude', '') AS FLOAT)                   AS "longitude",
        NULLIF(TRIM(source.data -> 'adresse' ->> 'commune'), '')                 AS "adresse__commune",
        NULLIF(TRIM(source.data -> 'adresse' ->> 'code_postal'), '')             AS "adresse__code_postal",
        NULLIF(TRIM(source.data -> 'adresse' ->> 'code_insee'), '')              AS "adresse__code_insee",
        NULLIF(TRIM(source.data -> 'adresse' ->> 'nom_voie'), '')                AS "adresse__nom_voie",
        NULLIF(TRIM(source.data -> 'adresse' ->> 'numero_voie'), '')             AS "adresse__numero_voie",
        NULLIF(TRIM(source.data -> 'adresse' ->> 'repetition'), '')              AS "adresse__repetition",
        NULLIF(TRIM(source.data ->> 'prise_rdv'), '')                            AS "prise_rdv"
    FROM source
    WHERE source.data ->> 'source' NOT IN ('dora', 'fredo', 'soliguide')
)

SELECT * FROM final
