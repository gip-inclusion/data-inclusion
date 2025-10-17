WITH structures AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

final AS (
    SELECT
        'emplois-de-linclusion' AS "source",
        structures.id           AS "id",
        structures.id           AS "adresse_id",
        structures.kind         AS typologie,
        structures.date_maj     AS "date_maj",
        structures.siret        AS "siret",
        structures.nom          AS "nom",
        structures.lien_source  AS "lien_source",
        structures.description  AS "presentation_resume",
        structures.description  AS "presentation_detail",
        structures.telephone    AS "telephone",
        structures.courriel     AS "courriel",
        structures.site_web     AS "site_web",
        NULL                    AS "horaires_ouverture",
        NULL                    AS "accessibilite",
        NULL                    AS "rna",
        CAST(NULL AS TEXT [])   AS "labels_nationaux",
        CAST(NULL AS TEXT [])   AS "labels_autres",
        CAST(NULL AS TEXT [])   AS "thematiques"
    FROM structures
)

SELECT * FROM final
