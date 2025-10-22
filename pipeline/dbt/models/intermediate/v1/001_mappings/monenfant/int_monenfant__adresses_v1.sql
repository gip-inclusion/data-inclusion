WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        'monenfant'                                 AS "source",
        'monenfant--' || structure_id               AS "id",
        coordonnees__commune                        AS "commune",
        NULL                                        AS "code_insee",
        CAST(NULL AS FLOAT)                         AS "longitude",
        CAST(NULL AS FLOAT)                         AS "latitude",
        coordonnees__code_postal                    AS "code_postal",
        ARRAY_TO_STRING(
            ARRAY[
                coordonnees__numero_voie,
                coordonnees__type_voie,
                SPLIT_PART(coordonnees__nom_voie, ' - ', 1)
            ],
            ' '
        )                                           AS "adresse",
        SPLIT_PART(coordonnees__nom_voie, ' - ', 2) AS "complement_adresse"
    FROM creches
)

SELECT * FROM final
