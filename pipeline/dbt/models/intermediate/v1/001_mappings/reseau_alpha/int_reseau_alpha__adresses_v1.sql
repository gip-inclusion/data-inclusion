WITH structures_adresses AS (
    SELECT DISTINCT ON (structure_id) *
    FROM {{ ref('stg_reseau_alpha__structures__adresses') }}
    ORDER BY structure_id, voie IS NOT NULL DESC
),

formations_adresses AS (
    SELECT DISTINCT ON (formation_id) *
    FROM {{ ref('stg_reseau_alpha__formations__adresses') }}
    ORDER BY formation_id, voie IS NOT NULL DESC
),

final AS (
    SELECT
        'reseau-alpha'                                   AS "source",
        'reseau-alpha--' || 'structure-' || structure_id AS "id",
        ville                                            AS "commune",
        NULL                                             AS "code_insee",
        longitude                                        AS "longitude",
        latitude                                         AS "latitude",
        code_postal                                      AS "code_postal",
        ARRAY_TO_STRING(ARRAY[numero, voie], ',')        AS "adresse",
        NULL                                             AS "complement_adresse"
    FROM structures_adresses
    UNION ALL
    SELECT
        'reseau-alpha'                                   AS "source",
        'reseau-alpha--' || 'formation-' || formation_id AS "id",
        ville                                            AS "commune",
        NULL                                             AS "code_insee",
        longitude                                        AS "longitude",
        latitude                                         AS "latitude",
        code_postal                                      AS "code_postal",
        ARRAY_TO_STRING(ARRAY[numero, voie], ',')        AS "adresse",
        NULL                                             AS "complement_adresse"
    FROM formations_adresses
)

SELECT * FROM final
