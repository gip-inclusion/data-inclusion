WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT DISTINCT ON (1, 2)
        source.data ->> 'id' AS "aide_id",
        raw.item             AS "nom",
        departements.code    AS "code"
    FROM source,
        JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'departements') AS "raw" (item)
    LEFT JOIN {{ ref('stg_decoupage_administratif__departements') }} AS departements
        ON raw.item % departements.nom
    ORDER BY
        source.data ->> 'id',
        raw.item,
        SIMILARITY(raw.item, departements.nom) DESC
)

SELECT * FROM final
