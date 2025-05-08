WITH services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int_agefiph__structures') }}
),

-- https://www.agefiph.fr/jsonapi/taxonomy_term/type_aide_service
di_type_by_agefiph_type AS (
    SELECT x.*
    FROM (
        VALUES
        ('aec45130-893a-45ba-84e0-41ff7bd99815', 'aide-financiere'),
        ('f7e83615-cb00-4ddd-91ee-9586d86ccf23', 'accompagnement')
    ) AS x (agefiph_type, type_)
),

final AS (
    SELECT
        structures.id                                                AS "adresse_id",
        TRUE                                                         AS "contact_public",
        NULL                                                         AS "contact_nom_prenom",
        CASE
            WHEN services.attributes__field_lien_aide_uri IS NULL THEN structures.courriel
        END                                                          AS "courriel",
        services.attributes__field_lien_aide_uri                     AS "formulaire_en_ligne",
        NULL                                                         AS "frais_autres",
        services.attributes__title                                   AS "nom",
        CASE
            WHEN LENGTH(services.attributes__field_solution_detail__processed) <= 280 THEN services.attributes__field_solution_detail__processed
            ELSE LEFT(services.attributes__field_solution_detail__processed, 279) || '…'
        END                                                          AS "presentation_resume",
        NULL                                                         AS "prise_rdv",
        NULL                                                         AS "recurrence",
        services._di_source_id                                       AS "source",
        structures.id                                                AS "structure_id",
        CASE
            WHEN services.attributes__field_lien_aide_uri IS NULL THEN structures.telephone
        END                                                          AS "telephone",
        NULL                                                         AS "zone_diffusion_code",
        NULL                                                         AS "zone_diffusion_nom",
        'region'                                                     AS "zone_diffusion_type",
        structures.id || '-' || services.id                          AS "id",
        NULL                                                         AS "page_web",
        NULL                                                         AS "modes_orientation_accompagnateur_autres",
        NULL                                                         AS "modes_orientation_beneficiaire_autres",
        CAST(NULL AS TEXT [])                                        AS "justificatifs",
        CAST(NULL AS TEXT [])                                        AS "pre_requis",
        CAST(NULL AS BOOLEAN)                                        AS "cumulable",
        CAST(NULL AS DATE)                                           AS "date_suspension",
        'https://www.agefiph.fr' || services.attributes__path__alias AS "lien_source",
        CAST(services.attributes__created AS DATE)                   AS "date_creation",
        CAST(services.attributes__changed AS DATE)                   AS "date_maj",
        CASE
            WHEN services.attributes__field_montant_aide IS NULL THEN services.attributes__field_solution_detail__processed
            ELSE services.attributes__field_solution_detail__processed || '\nAide d’un montant de ' || services.attributes__field_montant_aide
        END                                                          AS "presentation_detail",
        -- WIP (cf https://www.notion.so/gip-inclusion/24610bd08f8a412c83c09f6b36a1a44f?v=34cdd4c049e44f49aec060657c72c9b0&p=1d75f321b60480d9aca0d55129ea310e&pm=s)
        CAST(NULL AS TEXT [])                                        AS "thematiques",
        ARRAY['a-distance']                                          AS "modes_accueil",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NOT NULL THEN 'completer-le-formulaire-dadhesion' END
            ],
            NULL
        )                                                            AS "modes_orientation_accompagnateur",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NULL AND structures.courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN services.attributes__field_lien_aide_uri IS NOT NULL THEN 'completer-le-formulaire-dadhesion' END
            ],
            NULL
        )                                                            AS "modes_orientation_beneficiaire",
        ARRAY['personnes-en-situation-de-handicap']                  AS "profils",
        NULL                                                         AS "profils_precisions",
        ARRAY(
            SELECT di_type_by_agefiph_type.type_
            FROM di_type_by_agefiph_type
            WHERE services.relationships__field_type_de_solution__data__id = di_type_by_agefiph_type.agefiph_type
        )                                                            AS "types",
        ARRAY['gratuit']                                             AS "frais"
    FROM
        structures
    CROSS JOIN services
    INNER JOIN di_type_by_agefiph_type
        ON services.relationships__field_type_de_solution__data__id = di_type_by_agefiph_type.agefiph_type
    WHERE
        -- filter on profils to retrieve only 'personnes-en-situation-de-handicap'
        services.relationships__field_profil_associe__data__id = '0d0b63b6-4043-4b2d-a3f6-d7c85f335070'
        AND services.attributes__field_solution_partenaire = 'false'
)

SELECT * FROM final
