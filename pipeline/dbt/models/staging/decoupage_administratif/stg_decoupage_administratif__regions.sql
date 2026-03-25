WITH source AS (
    {{ stg_source_header('decoupage_administratif', 'regions') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

final AS (
    SELECT
        source.code AS "code",
        source.nom  AS "nom",
        ARRAY(
            SELECT d.code
            FROM departements AS d
            WHERE d.code_region = source.code
            ORDER BY d.code
        )           AS "departements"
    FROM source
    ORDER BY source.code
)

SELECT * FROM final
