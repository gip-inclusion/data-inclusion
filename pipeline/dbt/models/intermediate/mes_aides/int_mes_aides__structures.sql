WITH garages AS (
    SELECT * FROM {{ ref('stg_mes_aides__garages') }}
),

final AS (
    SELECT
        id                AS "id",
        NULL              AS "siret",
        NULL::BOOLEAN     AS "antenne",
        NULL              AS "rna",
        nom               AS "nom",
        ville_nom         AS "commune",
        code_postal       AS "code_postal",
        NULL              AS "code_insee",
        adresse           AS "adresse",
        NULL              AS "complement_adresse",
        ville_longitude   AS "longitude",
        ville_latitude    AS "latitude",
        telephone         AS "telephone",
        email             AS "courriel",
        url               AS "site_web",
        'mes-aides'       AS "source",
        NULL              AS "lien_source",
        NULL              AS "horaires_ouverture",
        NULL              AS "accessibilite",
        NULL::TEXT[]      AS "labels_nationaux",
        NULL::TEXT[]      AS "labels_autres",
        NULL              AS "typologie",
        NULL              AS "presentation_resume",
        NULL              AS "presentation_detail",
        modifie_le::DATE  AS "date_maj",
        ARRAY['mobilite'] AS "thematiques"
    FROM garages
)

SELECT * FROM final
