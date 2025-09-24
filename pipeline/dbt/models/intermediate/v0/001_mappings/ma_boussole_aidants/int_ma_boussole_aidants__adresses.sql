WITH structures AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

final AS (
    SELECT
        structures.id_structure           AS "id",
        structures.adresse__code_postal   AS "code_postal",
        structures.adresse__ligne_adresse AS "adresse",
        structures.adresse__latitude      AS "latitude",
        structures.adresse__longitude     AS "longitude",
        communes.nom                      AS "commune",
        communes.code                     AS "code_insee",
        NULL                              AS "complement_adresse",
        'ma-boussole-aidants'             AS "source"
    FROM structures
    LEFT JOIN communes ON structures.ville_code_commune = communes.code
)

SELECT * FROM final
