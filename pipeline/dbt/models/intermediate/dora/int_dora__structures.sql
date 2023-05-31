WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

final AS (
    SELECT
        id                         AS "id",
        NULL::BOOLEAN              AS "antenne",
        NULL                       AS "rna",
        longitude                  AS "longitude",
        latitude                   AS "latitude",
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
        NULLIF(address_2, '')      AS "complement_adresse",
        NULLIF(city, '')           AS "commune",
        NULLIF(address_1, '')      AS "adresse",
        NULLIF(short_desc, '')     AS "presentation_resume",
        NULLIF(full_desc, '')      AS "presentation_detail",
        NULLIF(phone, '')          AS "telephone",
        NULLIF(url, '')            AS "site_web",
        NULLIF(postal_code, '')    AS "code_postal",
        NULLIF(city_code, '')      AS "code_insee",
        NULLIF(email, '')          AS "courriel"
    FROM structures
    -- FIXME: exclude data that have been imported by dora from les emplois
    --  because it decreases the average dataset quality.
    WHERE source != 'di-emplois-de-linclusion'
)

SELECT * FROM final
