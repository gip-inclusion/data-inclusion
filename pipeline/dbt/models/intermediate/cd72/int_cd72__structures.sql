WITH raw_rows AS (
    SELECT * FROM {{ ref('stg_cd72__rows') }}
),

rows_with_id AS (
    SELECT *
    FROM raw_rows
    WHERE id IS NOT NULL
),

final AS (
    SELECT
        id                                               AS "id",
        siret                                            AS "siret",
        NULL::BOOLEAN                                    AS "antenne",
        NULL                                             AS "rna",
        nom_structure                                    AS "nom",
        ville                                            AS "commune",
        code_postal                                      AS "code_postal",
        NULL                                             AS "code_insee",
        adresse                                          AS "adresse",
        NULL                                             AS "complement_adresse",
        NULL::FLOAT                                      AS "longitude",
        NULL::FLOAT                                      AS "latitude",
        email_accueil                                    AS "courriel",
        site_internet                                    AS "site_web",
        'cd72'                                           AS "source",
        NULL                                             AS "lien_source",
        horaires                                         AS "horaires_ouverture",
        NULL                                             AS "accessibilite",
        NULL::TEXT[]                                     AS "labels_autres",
        NULL::TEXT[]                                     AS "thematiques",
        NULL                                             AS "typologie",
        mise_a_jour_le::DATE                             AS "date_maj",
        COALESCE(telephone_accueil, telephone_principal) AS "telephone",
        CASE
            WHEN typologie_structure ~ 'AFPA' THEN ARRAY['afpa']
            WHEN typologie_structure ~ 'Mission Locale' THEN ARRAY['mission-locale']
        END                                              AS "labels_nationaux",
        CASE LENGTH(description) <= 280
            WHEN TRUE THEN description
            WHEN FALSE THEN LEFT(description, 279) || '…'
        END                                              AS "presentation_resume",
        CASE LENGTH(description) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN description
        END                                              AS "presentation_detail"
    FROM rows_with_id
)

SELECT * FROM final
