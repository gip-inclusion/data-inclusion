WITH source AS (
    {{ stg_source_header('monenfant', 'creches') }}),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'structureId'), '')                                                                               AS "structure_id",
        -- FIXME: monenfant has swapped the commune and code postal fields.. simply use the fields without regex once it has been fixed at source
        TRIM(SUBSTRING((communes_desservies.data ->> 'commune') || ' ' || (communes_desservies.data ->> 'codePostal') FROM '[^\d]+')) AS "commune",
        SUBSTRING((communes_desservies.data ->> 'commune') || ' ' || (communes_desservies.data ->> 'codePostal') FROM '\d{5}')        AS "code_postal"
    FROM source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.serviceCommun.communesDesservies[*]') AS communes_desservies (data)
)

SELECT * FROM final
