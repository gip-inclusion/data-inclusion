WITH organisations AS (
    SELECT * FROM {{ ref('stg_cd35__organisations') }}
),

final AS (
    SELECT
        'cd35'              AS "source",
        'cd35--' || id      AS "id",
        'cd35--' || id      AS "adresse_id",
        nom                 AS "nom",
        date_maj            AS "date_maj",
        lien_source         AS "lien_source",
        NULL                AS "siret",
        telephone           AS "telephone",
        courriel            AS "courriel",
        site_web            AS "site_web",
        presentation_detail AS "description",
        horaires_ouvertures AS "horaires_accueil",
        NULL                AS "accessibilite_lieu",
        CASE sigle
            WHEN 'CCAS' THEN ARRAY['ccas-cias']
            WHEN 'MAIRIE' THEN ARRAY['communes']
        END                 AS "reseaux_porteurs"
    FROM organisations
)

SELECT * FROM final
