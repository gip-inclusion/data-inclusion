WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

raw AS (
    SELECT
        data ->> 'ID'                                     AS "aide_id",
        UNNEST(STRING_TO_ARRAY(data ->> 'Régions', ',')) AS "value"
    FROM source
),

final AS (
    SELECT DISTINCT ON (raw.aide_id, raw.value)
        raw.aide_id,
        raw.value,
        regions.code
    FROM raw
    LEFT JOIN {{ ref('stg_decoupage_administratif__regions') }} AS regions ON raw.value % regions.nom
    ORDER BY
        raw.aide_id,
        raw.value,
        WORD_SIMILARITY(raw.value, regions.nom) DESC
)

SELECT * FROM final
