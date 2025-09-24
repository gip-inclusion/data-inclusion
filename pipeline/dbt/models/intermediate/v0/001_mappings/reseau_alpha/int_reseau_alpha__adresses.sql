WITH structures AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__structures') }}
),

formations AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations') }}
),

structure_adresses AS (
    SELECT
        'reseau-alpha'              AS "source",
        adresses__longitude         AS "longitude",
        adresses__latitude          AS "latitude",
        content__complement_adresse AS "complement_adresse",
        adresses__ville             AS "commune",
        content__adresse            AS "adresse",
        adresses__code_postal       AS "code_postal",
        NULL                        AS "code_insee",
        'structure--' || id         AS "id"
    FROM structures
),

formation_adresses AS (
    SELECT
        'reseau-alpha'                                AS "source",
        adresses__longitude                           AS "longitude",
        adresses__latitude                            AS "latitude",
        NULL                                          AS "complement_adresse",
        adresses__ville                               AS "commune",
        content__lieux_et_horaires_formation__adresse AS "adresse",
        adresses__code_postal                         AS "code_postal",
        NULL                                          AS "code_insee",
        'service--' || id                             AS "id"
    FROM formations
),

final AS (
    SELECT * FROM structure_adresses
    UNION ALL
    SELECT * FROM formation_adresses
)

SELECT * FROM final
