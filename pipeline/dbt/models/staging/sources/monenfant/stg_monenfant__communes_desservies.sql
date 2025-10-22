WITH source AS (
    {{ stg_source_header('monenfant', 'creches') }}),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'structureId'), '')             AS "structure_id",
        NULLIF(TRIM(communes_desservies.data ->> 'commune'), '')    AS "commune",
        NULLIF(TRIM(communes_desservies.data ->> 'codePostal'), '') AS "code_postal"
    FROM source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.serviceCommun.communesDesservies[*]') AS communes_desservies (data)
)

SELECT * FROM final
