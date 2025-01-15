WITH source AS (
    {{ stg_source_header('monenfant', 'creches') }}
),

communes_desservies AS (
    SELECT
        source._di_source_id,
        NULLIF(TRIM(source.data ->> 'structureId'), '')             AS "structure_id",
        NULLIF(TRIM(communes_desservies.data ->> 'commune'), '')    AS "service_commun__communes_desservies__commune",
        NULLIF(TRIM(communes_desservies.data ->> 'codePostal'), '') AS "service_commun__communes_desservies__code_postal"
    FROM source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.serviceCommun.communesDesservies[*]') AS communes_desservies (data)
),

final AS (
    SELECT DISTINCT ON (structure_id) *
    FROM communes_desservies
)

SELECT * FROM final
