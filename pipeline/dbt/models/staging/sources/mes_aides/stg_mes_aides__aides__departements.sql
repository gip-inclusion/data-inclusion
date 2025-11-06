WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        data ->> 'ID'                      AS "aide_id",
        SPLIT_PART(departements, ' - ', 1) AS "code",
        SPLIT_PART(departements, ' - ', 2) AS "nom"
    FROM source,
        UNNEST(STRING_TO_ARRAY(data ->> 'Départements', ',')) AS departements
)

SELECT * FROM final
