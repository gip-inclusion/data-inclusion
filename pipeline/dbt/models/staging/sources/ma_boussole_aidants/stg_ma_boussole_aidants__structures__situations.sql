WITH source AS (
    {{ stg_source_header('ma_boussole_aidants', 'structures') }}),

final AS (
    SELECT
        source.data ->> 'idStructure'                 AS "id_structure",
        CAST(situation.data ->> 'idSituation' AS INT) AS "id_situation",
        situation.data ->> 'nomSituation'             AS "nom_situation",
        CAST(situation.data ->> 'idProfil' AS INT)    AS "id_profil"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.situations[*]') AS situation (data)
)

SELECT * FROM final
