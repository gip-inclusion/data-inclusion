WITH creches AS (
    SELECT * FROM {{ ref('stg_monenfant__creches') }}
),

final AS (
    SELECT
        structure_id             AS "id",
        coordonnees__commune     AS "commune",
        NULL                     AS "code_insee",
        CAST(NULL AS FLOAT)      AS "longitude",
        CAST(NULL AS FLOAT)      AS "latitude",
        'monenfant'              AS "source",
        coordonnees__code_postal AS "code_postal",
        ARRAY_TO_STRING(
            ARRAY_REMOVE(
                ARRAY[
                    coordonnees__numero_voie,
                    coordonnees__type_voie,
                    coordonnees__nom_voie
                ],
                NULL
            ),
            ' '
        )                        AS "adresse",
        NULL                     AS "complement_adresse"
    FROM creches
)

SELECT * FROM final
