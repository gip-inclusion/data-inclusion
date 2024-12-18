WITH structures AS (
    SELECT * FROM {{ ref('stg_imilo__structures') }}
),

final AS (
    SELECT
        _di_source_id           AS "source",
        id                      AS "id",
        NULL                    AS "siret",
        NULL                    AS "rna",
        courriel                AS "courriel",
        antenne                 AS "antenne",
        horaires_ouverture      AS "horaires_ouverture",
        site_web                AS "site_web",
        NULL                    AS "lien_source",
        NULL                    AS "accessibilite",
        telephone               AS "telephone",
        typologie               AS "typologie",
        nom                     AS "nom",
        ARRAY[labels_nationaux] AS "labels_nationaux",
        CAST(NULL AS TEXT [])   AS "labels_autres",
        presentation_resume     AS "presentation_resume",
        presentation_detail     AS "presentation_detail",
        id                      AS "adresse_id",
        CAST(NULL AS TEXT [])   AS "thematiques",
        CAST(date_maj AS DATE)  AS "date_maj"
    FROM structures

)

SELECT * FROM final
