WITH source AS (
    {{ stg_source_header('ma_boussole_aidants', 'structures') }}),

final AS (
    SELECT
        source.data ->> 'idStructure'     AS "id_structure",
        situation.data ->> 'idSituation'  AS "id_situation",
        situation.data ->> 'nomSituation' AS "nom_situation",
        situation.data ->> 'idProfil'     AS "id_profil"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.situations[*]') AS situation (data)
)

SELECT * FROM final
