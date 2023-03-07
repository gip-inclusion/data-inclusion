WITH organisations AS (
    SELECT * FROM {{ ref('stg_cd35__organisations') }}
),

final AS (
    SELECT
        id                                  AS "id",
        NULL                                AS "siret",
        NULL::BOOLEAN                       AS "antenne",
        NULL                                AS "rna",
        org_nom                             AS "nom",
        org_ville                           AS "commune",
        org_cp                              AS "code_postal",
        NULL                                AS "code_insee",
        org_adres                           AS "adresse",
        NULL                                AS "complement_adresse",
        org_longitude                       AS "longitude",
        org_latitude                        AS "latitude",
        org_tel                             AS "telephone",
        org_mail                            AS "courriel",
        org_web                             AS "site_web",
        _di_source_id                       AS "source",
        url                                 AS "lien_source",
        org_horaire                         AS "horaires_ouverture",
        NULL                                AS "accessibilite",
        NULL::TEXT[]                        AS "labels_nationaux",
        NULL::TEXT[]                        AS "labels_autres",
        NULL::TEXT[]                        AS "thematiques",
        CASE org_sigle
            WHEN 'CCAS' THEN 'CCAS'
            WHEN 'MAIRIE' THEN 'MUNI'
        END                                 AS "typologie",
        CASE LENGTH(org_desc) <= 280
            WHEN TRUE THEN org_desc
            WHEN FALSE THEN LEFT(org_desc, 279) || '…'
        END                                 AS "presentation_resume",
        CASE LENGTH(org_desc) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN org_desc
        END                                 AS "presentation_detail",
        COALESCE(org_datemaj, org_datecrea) AS "date_maj"
    FROM organisations
)

SELECT * FROM final
