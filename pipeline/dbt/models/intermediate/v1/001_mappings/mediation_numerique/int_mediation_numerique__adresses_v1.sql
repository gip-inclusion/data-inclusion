WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

final AS (
    SELECT
        'mediation-numerique'         AS "source",
        'mediation-numerique--' || id AS "id",
        adresse__code_postal          AS "code_postal",
        NULLIF(
            TRIM(
                CONCAT_WS(
                    ' ',
                    adresse__numero_voie,
                    adresse__repetition,
                    adresse__nom_voie
                )
            ),
            ''
        )                             AS "adresse",
        latitude                      AS "latitude",
        longitude                     AS "longitude",
        adresse__commune              AS "commune",
        adresse__code_insee           AS "code_insee",
        NULL                          AS "complement_adresse"
    FROM structures
)

SELECT * FROM final
