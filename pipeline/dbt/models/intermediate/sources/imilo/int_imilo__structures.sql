WITH structures AS (
    SELECT * FROM {{ ref('stg_imilo__structures') }}
),

final AS (
    SELECT
        _di_source_id           AS "source",
        id_structure            AS "id",
        siret                   AS "siret",
        NULL                    AS "rna",
        email                   AS "courriel",
        CAST(NULL AS BOOLEAN)   AS "antenne",
        horaires                AS "horaires_ouverture",
        site_web                AS "site_web",
        NULL                    AS "lien_source",
        NULL                    AS "accessibilite",
        telephone               AS "telephone",
        typologie               AS "typologie",
        nom_structure           AS "nom",
        ARRAY[labels_nationaux] AS "labels_nationaux",
        CAST(NULL AS TEXT [])   AS "labels_autres",
        NULL                    AS "presentation_resume",
        presentation            AS "presentation_detail",
        id_structure            AS "adresse_id",
        CAST(NULL AS TEXT [])   AS "thematiques",
        CAST(date_maj AS DATE)  AS "date_maj"
    FROM structures

)

SELECT * FROM final
