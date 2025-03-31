WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        id                                 AS "id",
        id                                 AS "adresse_id",
        siret                              AS "siret",
        CAST(NULL AS BOOLEAN)              AS "antenne",
        NULL                               AS "rna",
        RTRIM(SUBSTRING(nom, 1, 150), '.') AS "nom",
        telephone                          AS "telephone",
        email                              AS "courriel",
        url                                AS "site_web",
        _di_source_id                      AS "source",
        NULL                               AS "lien_source",
        NULL                               AS "horaires_ouverture",
        NULL                               AS "accessibilite",
        CAST(NULL AS TEXT [])              AS "labels_nationaux",
        CAST(NULL AS TEXT [])              AS "labels_autres",
        NULL                               AS "typologie",
        NULL                               AS "presentation_resume",
        NULL                               AS "presentation_detail",
        CAST(modifie_le AS DATE)           AS "date_maj",
        ARRAY['mobilite']                  AS "thematiques"
    FROM garages
    WHERE
        en_ligne
)

SELECT * FROM final
