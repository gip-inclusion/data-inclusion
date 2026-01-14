WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

mediateurs AS (
    SELECT
        source.data ->> 'id'                              AS "structure_id",
        NULLIF(TRIM(mediateur.data ->> 'prenom'), '')     AS "prenom",
        NULLIF(TRIM(mediateur.data ->> 'nom'), '')        AS "nom",
        NULLIF(TRIM(mediateur.data ->> 'email'), '')      AS "email"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.mediateurs[*]') AS mediateur (data)
),

final AS (
    SELECT mediateurs.*
    FROM mediateurs
    INNER JOIN structures ON mediateurs.structure_id = structures.id
)

SELECT * FROM final
