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
        NULL                                                                                           AS "lien_mobilisation",
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
        NULL                                                                                           AS "contact_nom_prenom",
        NULL                                                                                           AS "zone_diffusion_nom",
        NULL                                                                                           AS "modes_orientation_accompagnateur_autres",
        NULL                                                                                           AS "modes_orientation_beneficiaire_autres",
        NULL                                                                                           AS "lien_source",
        'departement'                                                                                  AS "zone_diffusion_type",
        CAST(NULL AS TEXT [])                                                                          AS "pre_requis",
        CAST(NULL AS TEXT [])                                                                          AS "justificatifs",
        NULL                                                                                           AS "conditions_acces",
        CAST(structures.date_maj AS DATE)                                                              AS "date_maj",
        CAST(NULL AS FLOAT)                                                                            AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                                                              AS "nombre_semaines",
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
        CAST(NULL AS TEXT [])                                                                          AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                                                                          AS "mobilisable_par",
        NULL                                                                                           AS "mobilisation_precisions",
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
