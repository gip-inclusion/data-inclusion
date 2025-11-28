WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

raw AS (
    SELECT
        data ->> 'ID'                                 AS "aide_id",
        UNNEST(STRING_TO_ARRAY(data ->> 'EPCI', ',')) AS "value"
    FROM source
),

final AS (
    SELECT DISTINCT ON (raw.aide_id, raw.value)
        raw.aide_id,
        raw.value,
        epcis.code
    FROM raw
    LEFT JOIN {{ ref('stg_decoupage_administratif__epcis') }} AS epcis ON raw.value % epcis.nom
    ORDER BY
        raw.aide_id,
        raw.value,
        WORD_SIMILARITY(raw.value, epcis.nom) DESC
)

SELECT * FROM final
