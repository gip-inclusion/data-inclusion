WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        garages.id                                             AS "id",
        garages.id                                             AS "adresse_id",
        garages.siret                                          AS "siret",
        NULL                                                   AS "rna",
        garages.nom                                            AS "nom",
        SUBSTRING(garages.telephone FROM '\+?\d[\d\.\-\s]*\d') AS "telephone",
        garages.email                                          AS "courriel",
        garages.url                                            AS "site_web",
        garages._di_source_id                                  AS "source",
        NULL                                                   AS "lien_source",
        NULL                                                   AS "horaires_ouverture",
        NULL                                                   AS "accessibilite",
        CAST(NULL AS TEXT [])                                  AS "labels_nationaux",
        CAST(NULL AS TEXT [])                                  AS "labels_autres",
        NULL                                                   AS "typologie",
        NULL                                                   AS "presentation_resume",
        NULL                                                   AS "presentation_detail",
        CAST(garages.modifie_le AS DATE)                       AS "date_maj",
        ARRAY['mobilite']                                      AS "thematiques"
    FROM garages
    WHERE
        garages.en_ligne
)

SELECT * FROM final
