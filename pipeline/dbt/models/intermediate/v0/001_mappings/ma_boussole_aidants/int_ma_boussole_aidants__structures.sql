WITH structures AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures') }}
),

final AS (
    SELECT
        'ma-boussole-aidants'            AS "source",
        structures.id_structure          AS "id",
        structures.id_structure          AS "adresse_id",
        structures.nom_structure         AS "nom",
        structures.telephone_1           AS "telephone",
        structures.email                 AS "courriel",
        structures.last_modified_date    AS "date_maj",
        structures.structure_mere__siret AS "siret",
        structures.site_web              AS "site_web",
        NULL                             AS "horaires_ouverture",
        NULL                             AS "accessibilite",
        NULL                             AS "rna",
        CASE structures.id_type_structure
            WHEN '15' THEN 'CCAS'
            WHEN '53' THEN 'MDA'
            WHEN '127' THEN 'MDS'
        END                              AS "typologie",
        NULL                             AS "presentation_resume",
        structures.description_structure AS "presentation_detail",
        NULL                             AS "antenne",
        NULL                             AS "lien_source",
        CAST(NULL AS TEXT [])            AS "labels_nationaux",
        CAST(NULL AS TEXT [])            AS "labels_autres"
    FROM structures
)

SELECT * FROM final
