WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

final AS (
    SELECT
        id                         AS "id",
        id                         AS "adresse_id",
        NULL::BOOLEAN              AS "antenne",
        NULL                       AS "rna",
        _di_source_id              AS "source",
        NULL                       AS "horaires_ouverture",
        NULL                       AS "accessibilite",
        NULL::TEXT []              AS "labels_nationaux",
        NULL::TEXT []              AS "labels_autres",
        NULL::TEXT []              AS "thematiques",
        typology                   AS "typologie",
        modification_date          AS "date_maj",
        NULLIF(siret, '')          AS "siret",
        NULLIF(name, '')           AS "nom",
        NULLIF(link_on_source, '') AS "lien_source",
        NULLIF(short_desc, '')     AS "presentation_resume",
        NULLIF(full_desc, '')      AS "presentation_detail",
        NULLIF(phone, '')          AS "telephone",
        NULLIF(url, '')            AS "site_web",
        NULLIF(email, '')          AS "courriel"
    FROM structures
)

SELECT * FROM final
