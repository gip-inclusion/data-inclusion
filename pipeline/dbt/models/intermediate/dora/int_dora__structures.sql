{{
    config(
        post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
    )
}}

WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

final AS (
    SELECT
        id                AS "id",
        siret             AS "siret",
        NULL::BOOLEAN     AS "antenne",
        NULL              AS "rna",
        name              AS "nom",
        city              AS "commune",
        postal_code       AS "code_postal",
        city_code         AS "code_insee",
        address_1         AS "adresse",
        address_2         AS "complement_adresse",
        longitude         AS "longitude",
        latitude          AS "latitude",
        phone             AS "telephone",
        email             AS "courriel",
        url               AS "site_web",
        'dora'            AS "source",
        link_on_source    AS "lien_source",
        NULL              AS "horaires_ouverture",
        NULL              AS "accessibilite",
        NULL::TEXT[]      AS "labels_nationaux",
        NULL::TEXT[]      AS "labels_autres",
        NULL::TEXT[]      AS "thematiques",
        typology          AS "typologie",
        short_desc        AS "presentation_resume",
        full_desc         AS "presentation_detail",
        modification_date AS "date_maj"
    FROM structures
)

SELECT * FROM final
