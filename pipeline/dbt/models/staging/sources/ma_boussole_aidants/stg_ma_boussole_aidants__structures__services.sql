WITH source AS (
    {{ stg_source_header('ma_boussole_aidants', 'structures') }}),

final AS (
    SELECT
        source.data ->> 'idStructure'                                AS "id_structure",
        CAST(service.data -> 'isDistanciel' AS BOOLEAN)              AS "is_distanciel",
        CAST(service.data -> 'idSousthematiqueSolutions' AS INTEGER) AS "id_sous_thematique_solutions"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.services[*]') AS service (data)
)

SELECT * FROM final
