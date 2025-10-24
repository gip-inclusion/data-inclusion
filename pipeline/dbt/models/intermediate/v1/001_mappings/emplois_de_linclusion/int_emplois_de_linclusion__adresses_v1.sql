WITH structures AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__siaes') }}
    UNION
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

final AS (
    SELECT
        'emplois-de-linclusion--' || structures.id AS "id",
        'emplois-de-linclusion'                    AS "source",
        structures.longitude                       AS "longitude",
        structures.latitude                        AS "latitude",
        structures.complement_adresse              AS "complement_adresse",
        structures.adresse                         AS "adresse",
        structures.commune                         AS "commune",
        structures.code_postal                     AS "code_postal",
        NULL                                       AS "code_insee"

    FROM structures
)

SELECT * FROM final
