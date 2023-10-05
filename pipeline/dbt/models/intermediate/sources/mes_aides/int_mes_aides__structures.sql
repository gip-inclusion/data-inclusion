WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        id                AS "id",
        id                AS "adresse_id",
        NULL              AS "siret",
        NULL::BOOLEAN     AS "antenne",
        NULL              AS "rna",
        nom               AS "nom",
        telephone         AS "telephone",
        email             AS "courriel",
        url               AS "site_web",
        _di_source_id     AS "source",
        NULL              AS "lien_source",
        NULL              AS "horaires_ouverture",
        NULL              AS "accessibilite",
        NULL::TEXT []     AS "labels_nationaux",
        NULL::TEXT []     AS "labels_autres",
        NULL              AS "typologie",
        NULL              AS "presentation_resume",
        NULL              AS "presentation_detail",
        modifie_le::DATE  AS "date_maj",
        ARRAY['mobilite'] AS "thematiques"
    FROM garages
)

SELECT * FROM final
