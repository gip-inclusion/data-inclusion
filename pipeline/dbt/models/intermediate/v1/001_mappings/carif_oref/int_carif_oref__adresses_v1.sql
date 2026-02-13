WITH adresses AS (
    SELECT * FROM {{ ref('stg_carif_oref__adresses') }}
),

final AS (
    SELECT
        'carif-oref--' || adresses.hash_      AS "id",
        adresses.ville                        AS "commune",
        adresses.code_insee_commune           AS "code_insee",
        adresses.geolocalisation__longitude   AS "longitude",
        adresses.geolocalisation__latitude    AS "latitude",
        'carif-oref'                          AS "source",
        adresses.codepostal                   AS "code_postal",
        ARRAY_TO_STRING(adresses.ligne, ', ') AS "adresse",
        NULL                                  AS "complement_adresse"
    FROM adresses
)

SELECT * FROM final
