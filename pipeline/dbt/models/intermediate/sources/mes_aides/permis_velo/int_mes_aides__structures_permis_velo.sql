WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

final AS (
    SELECT
        siret_structure             AS "id",
        MAX(id)                     AS "adresse_id",
        siret_structure             AS "siret",
        CAST(NULL AS BOOLEAN)       AS "antenne",
        NULL                        AS "rna",
        MAX(nom)                    AS "nom",
        MAX(telephone)              AS "telephone",
        MAX(courriel)               AS "courriel",
        MAX(page_web)               AS "site_web",
        MAX(_di_source_id)          AS "source",
        MAX(lien_source)            AS "lien_source",
        NULL                        AS "horaires_ouverture",
        NULL                        AS "accessibilite",
        CAST(NULL AS TEXT [])       AS "labels_nationaux",
        CAST(NULL AS TEXT [])       AS "labels_autres",
        NULL                        AS "typologie",
        NULL                        AS "presentation_resume",
        NULL                        AS "presentation_detail",
        MAX(CAST(date_maj AS DATE)) AS "date_maj",
        ARRAY['mobilite']           AS "thematiques"
    FROM permis_velo
    WHERE siret_structure IS NOT NULL
    GROUP BY siret_structure
    UNION ALL
    SELECT
        id                     AS "id",
        id                     AS "adresse_id",
        NULL                   AS "siret",
        CAST(NULL AS BOOLEAN)  AS "antenne",
        NULL                   AS "rna",
        nom                    AS "nom",
        telephone              AS "telephone",
        courriel               AS "courriel",
        page_web               AS "site_web",
        _di_source_id          AS "source",
        lien_source            AS "lien_source",
        NULL                   AS "horaires_ouverture",
        NULL                   AS "accessibilite",
        CAST(NULL AS TEXT [])  AS "labels_nationaux",
        CAST(NULL AS TEXT [])  AS "labels_autres",
        NULL                   AS "typologie",
        NULL                   AS "presentation_resume",
        NULL                   AS "presentation_detail",
        CAST(date_maj AS DATE) AS "date_maj",
        ARRAY['mobilite']      AS "thematiques"
    FROM permis_velo
    WHERE siret_structure IS NULL
)

SELECT * FROM final
