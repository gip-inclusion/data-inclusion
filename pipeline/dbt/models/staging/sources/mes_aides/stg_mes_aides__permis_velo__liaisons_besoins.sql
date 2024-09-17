WITH source AS (
    {{ stg_source_header('mes_aides', 'permis_velo') }}
),

final AS (
    SELECT
        data #>> '{fields,ID}'                 AS "service_id",
        TRIM(TRIM(liaisons_besoins, ' '), '"') AS "value"
    FROM
        source,
        LATERAL UNNEST(
            REGEXP_SPLIT_TO_ARRAY(
                -- Ignore commas inside double quotes. Example: "foo, bar", baz
                data #>> '{fields,Liaisons Besoins}',
                ',(?=(?:[^"]*"[^"]*")*[^"]*$)'
            )
        ) AS liaisons_besoins
    WHERE data #>> '{fields,Liaisons Besoins}' IS NOT NULL
)

SELECT * FROM final
