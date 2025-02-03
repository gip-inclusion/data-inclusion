{% set presentation %}
structures.nom || ' propose des services : ' || ARRAY_TO_STRING(
    ARRAY(
        SELECT LOWER(di_thematiques.label)
        FROM UNNEST(services.thematiques) AS t (value)
        INNER JOIN di_thematiques ON t.value = di_thematiques.value
    ),
', ') || '.'
{% endset %}

WITH services AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__services') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

di_thematiques AS (
    SELECT * FROM {{ ref('thematiques') }}
),

final AS (
    SELECT
        services.id                                                                                    AS "id",
        structures.id                                                                                  AS "adresse_id",
        services.nom                                                                                   AS "nom",
        services.prise_rdv                                                                             AS "prise_rdv",
        services.frais                                                                                 AS "frais",
        services.profils                                                                               AS "profils",
        NULL                                                                                           AS "profils_precisions",
        services.structure_id                                                                          AS "structure_id",
        services.thematiques                                                                           AS "thematiques",
        services._di_source_id                                                                         AS "source",
        NULL                                                                                           AS "formulaire_en_ligne",
        NULL                                                                                           AS "recurrence",
        structures.telephone                                                                           AS "telephone",
        structures.courriel                                                                            AS "courriel",
        TRUE                                                                                           AS "contact_public",
        NULL                                                                                           AS "contact_nom_prenom",
        NULL                                                                                           AS "zone_diffusion_nom",
        NULL                                                                                           AS "modes_orientation_accompagnateur_autres",
        NULL                                                                                           AS "modes_orientation_beneficiaire_autres",
        NULL                                                                                           AS "lien_source",
        'departement'                                                                                  AS "zone_diffusion_type",
        CAST(NULL AS TEXT [])                                                                          AS "pre_requis",
        CAST(NULL AS BOOLEAN)                                                                          AS "cumulable",
        CAST(NULL AS TEXT [])                                                                          AS "justificatifs",
        CAST(NULL AS DATE)                                                                             AS "date_suspension",
        CAST(structures.date_maj AS DATE)                                                              AS "date_maj",
        CAST(NULL AS DATE)                                                                             AS "date_creation",
        CASE
            WHEN structures.code_insee LIKE '97%' THEN LEFT(structures.code_insee, 3)
            ELSE LEFT(structures.code_insee, 2)
        END                                                                                            AS "zone_diffusion_code",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN structures.telephone IS NOT NULL THEN 'telephoner' END,
                CASE WHEN structures.courriel IS NOT NULL THEN 'envoyer-un-mail' END
            ],
            NULL
        )                                                                                              AS "modes_orientation_accompagnateur",
        ARRAY_REMOVE(ARRAY[CASE WHEN structures.telephone IS NOT NULL THEN 'telephoner' END], NULL)    AS "modes_orientation_beneficiaire",
        -- TODO (hlecuyer): do the mapping
        ARRAY['professionnels']                                                                        AS "mobilisable_par",
        CAST(NULL AS TEXT)                                                                             AS "frais_autres",
        CASE WHEN CARDINALITY(services.types) > 0 THEN services.types ELSE ARRAY['accompagnement'] END AS "types",
        ARRAY['en-presentiel']                                                                         AS "modes_accueil",
        {{ truncate_text(presentation) }}                                                              AS "presentation_resume",
        {{ presentation }}                                                                             AS "presentation_detail",
        NULL                                                                                           AS "page_web"
    FROM services
    LEFT JOIN structures ON services.structure_id = structures.id
)

SELECT * FROM final
