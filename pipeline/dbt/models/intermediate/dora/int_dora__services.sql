WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

-- Temporary mapping of dora thematiques (for mobilité)
-- dora still use some thematiques that are not in the data.inclusion schema.
di_thematique_by_dora_subcategory AS (
    SELECT x.*
    FROM (
        VALUES
        ('mobilite--accompagnement-parcours-mobilite', 'mobilite--etre-accompagne-dans-son-parcours-mobilite'),
        ('mobilite--se-deplacer-sans-permis-ou-vehicule', 'handicap'),
        ('mobilite--louer-acheter-vehicule', 'mobilite--louer-un-vehicule'),
        ('mobilite--louer-acheter-vehicule', 'mobilite--acheter-un-vehicule-motorise'),
        ('mobilite--reprendre-emploi-formation', 'mobilite--aides-a-la-reprise-demploi-ou-a-la-formation'),
        ('mobilite--preparer-permis', 'mobilite--preparer-son-permis-de-conduire-se-reentrainer-a-la-conduite'),
        ('mobilite--entretenir-vehicule', 'mobilite--entretenir-reparer-son-vehicule')
    ) AS x(subcategory, thematique)
),

final AS (
    SELECT
        id,
        _di_source_id                                           AS "source",
        name                                                    AS "nom",
        short_desc                                              AS "presentation_resume",
        kinds                                                   AS "types",
        online_form                                             AS "prise_rdv",
        NULL::TEXT[]                                            AS "frais",
        fee_details                                             AS "frais_autres",
        NULL::TEXT[]                                            AS "profils",
        SPLIT_PART(TRIM('/' FROM structure), '/structures/', 2) AS "structure_id",
        (categories || ARRAY(
            SELECT di_thematique_by_dora_subcategory.thematique
            FROM di_thematique_by_dora_subcategory
            WHERE di_thematique_by_dora_subcategory.subcategory = ANY(services.subcategories)
        )::TEXT[])                                              AS "thematiques"
    FROM services
)

SELECT * FROM final
