WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

final AS (
    SELECT
        id                        AS "id",
        id                        AS "adresse_id",
        siret_structure           AS "siret",
        CAST(NULL AS BOOLEAN)     AS "antenne",
        NULL                      AS "rna",
        nom                       AS "nom",
        contact_telephone         AS "telephone",
        contact_email             AS "courriel",
        site                      AS "site_web",
        _di_source_id             AS "source",
        url_mes_aides             AS "lien_source",
        NULL                      AS "horaires_ouverture",
        NULL                      AS "accessibilite",
        CAST(NULL AS TEXT [])     AS "labels_nationaux",
        CAST(NULL AS TEXT [])     AS "labels_autres",
        NULL                      AS "typologie",
        NULL                      AS "presentation_resume",
        NULL                      AS "presentation_detail",
        CAST(modifiee_le AS DATE) AS "date_maj",
        ARRAY['mobilite']         AS "thematiques"
    FROM permis_velo
)

SELECT * FROM final
