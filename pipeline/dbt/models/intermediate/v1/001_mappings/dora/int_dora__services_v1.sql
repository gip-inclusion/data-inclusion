WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_dora__adresses_v1') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

publics AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "publics"
    FROM {{ ref('stg_dora__services__publics') }}
    GROUP BY service_id
),

thematiques AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "thematiques"
    FROM {{ ref('stg_dora__services__thematiques') }}
    GROUP BY service_id
),

types AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "types"
    FROM {{ ref('stg_dora__services__types') }}
    GROUP BY service_id
),

modes_accueil AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "modes_accueil"
    FROM {{ ref('stg_dora__services__modes_accueil') }}
    GROUP BY service_id
),

pre_requis AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "pre_requis"
    FROM {{ ref('stg_dora__services__pre_requis') }}
    GROUP BY service_id
),

justificatifs AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "justificatifs"
    FROM {{ ref('stg_dora__services__justificatifs') }}
    GROUP BY service_id
),

modes_orientation_accompagnateur AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "modes_orientation_accompagnateur"
    FROM {{ ref('stg_dora__services__modes_orientation_accompagnateur') }}
    GROUP BY service_id
),

modes_orientation_beneficiaire AS (
    SELECT
        service_id,
        ARRAY_AGG(item ORDER BY item) AS "modes_orientation_beneficiaire"
    FROM {{ ref('stg_dora__services__modes_orientation_beneficiaire') }}
    GROUP BY service_id
),

modes_mobilisation_from_modes_orientation_beneficiaire AS (
    SELECT
        modes_orientation.service_id,
        mapping.mode_mobilisation
    FROM {{ ref('stg_dora__services__modes_orientation_beneficiaire') }} AS modes_orientation
    INNER JOIN {{ ref('_map_dora__modes_mobilisation') }} AS "mapping"
        ON
            mapping.type = 'beneficiaire'
            AND modes_orientation.item = mapping.mode_orientation
),

modes_mobilisation_from_modes_orientation_accompagnateur AS (
    SELECT
        modes_orientation.service_id,
        mapping.mode_mobilisation
    FROM {{ ref('stg_dora__services__modes_orientation_accompagnateur') }} AS modes_orientation
    INNER JOIN {{ ref('_map_dora__modes_mobilisation') }} AS "mapping"
        ON
            mapping.type = 'accompagnateur'
            AND modes_orientation.item = mapping.mode_orientation
),

modes_mobilisation_from_liens AS (
    SELECT
        id                           AS service_id,
        'utiliser-lien-mobilisation' AS mode_mobilisation
    FROM services
    WHERE prise_rdv IS NOT NULL OR formulaire_en_ligne IS NOT NULL
),

modes_mobilisation AS (
    SELECT
        service_id,
        ARRAY_AGG(DISTINCT mode_mobilisation ORDER BY mode_mobilisation) AS "modes_mobilisation"
    FROM (
        SELECT * FROM modes_mobilisation_from_modes_orientation_beneficiaire
        UNION
        SELECT * FROM modes_mobilisation_from_modes_orientation_accompagnateur
        UNION
        SELECT * FROM modes_mobilisation_from_liens
    ) AS all_modes_mobilisation
    GROUP BY service_id
),

final AS (
    SELECT
        'dora'                                                                                                                      AS "source",
        'dora--' || services.id                                                                                                     AS "id",
        'dora--' || services.structure_id                                                                                           AS "structure_id",
        adresses.id                                                                                                                 AS "adresse_id",
        COALESCE(services.prise_rdv, services.formulaire_en_ligne)                                                                  AS "lien_mobilisation",
        /* from v0: "We decided against using dora's recurrence field in the new horaires_accueil field." */
        NULL                                                                                                                        AS "horaires_accueil",
        services.lien_source                                                                                                        AS "lien_source",
        NULLIF(ARRAY_TO_STRING(pre_requis.pre_requis || justificatifs.justificatifs, '\n'), '')                                     AS "conditions_acces",
        CAST(services.date_maj AS DATE)                                                                                             AS "date_maj",
        CASE
            WHEN LENGTH(services.presentation_detail) >= 10000
                THEN LEFT(services.presentation_detail, 9999) || '…'
            ELSE COALESCE(services.presentation_detail, services.presentation_resume)
        END                                                                                                                         AS "description",
        thematiques.thematiques                                                                                                     AS "thematiques",
        modes_accueil.modes_accueil                                                                                                 AS "modes_accueil",
        modes_mobilisation.modes_mobilisation                                                                                       AS "modes_mobilisation",
        NULLIF(ARRAY_REMOVE(
            ARRAY[
                CASE WHEN CARDINALITY(modes_orientation_beneficiaire.modes_orientation_beneficiaire) > 0 THEN 'usagers' END,
                CASE WHEN CARDINALITY(modes_orientation_accompagnateur.modes_orientation_accompagnateur) > 0 THEN 'professionnels' END
            ],
            NULL
        ), '{}')                                                                                                                    AS "mobilisable_par",
        NULLIF(TRIM(services.modes_orientation_beneficiaire_autres || ' ' || services.modes_orientation_accompagnateur_autres), '') AS "mobilisation_precisions",
        publics.publics                                                                                                             AS "publics",
        services.publics_precisions                                                                                                 AS "publics_precisions",
        CASE
            WHEN
                ARRAY['aide-financiere', 'financement'] && types.types
                THEN 'aide-financiere'
            WHEN
                ARRAY['autonomie', 'aide-materielle'] && types.types
                THEN 'aide-materielle'
            WHEN 'formation' = ANY(types.types) THEN 'formation'
            WHEN
                ARRAY['atelier', 'numerique'] && types.types
                THEN 'atelier'
            WHEN 'accompagnement' = ANY(types.types) THEN 'accompagnement'
            WHEN
                ARRAY['accueil', 'information'] && types.types
                THEN 'information'
        END                                                                                                                         AS "type",
        services.frais                                                                                                              AS "frais",
        services.frais_autres                                                                                                       AS "frais_precisions",
        services.temps_passe_semaines                                                                                               AS "nombre_semaines",
        services.temps_passe_duree_hebdomadaire                                                                                     AS "volume_horaire_hebdomadaire",
        CASE
            WHEN
                services.zone_diffusion_type = 'region'
                THEN (
                    SELECT ARRAY_AGG(departements.code)
                    FROM departements
                    WHERE departements.code_region = services.zone_diffusion_code
                )
            WHEN services.zone_diffusion_type = 'pays' THEN ARRAY['france']
            WHEN services.zone_diffusion_code IS NOT NULL THEN ARRAY[services.zone_diffusion_code]
        END                                                                                                                         AS "zone_eligibilite",
        services.contact_nom_prenom                                                                                                 AS "contact_nom_prenom",
        services.courriel                                                                                                           AS "courriel",
        services.telephone                                                                                                          AS "telephone",
        CASE
            WHEN LENGTH(services.nom) <= 150 THEN services.nom
            ELSE LEFT(services.nom, 149) || '…'
        END                                                                                                                         AS "nom"
    FROM services
    LEFT JOIN adresses ON ('dora--' || services.id) = adresses.id
    LEFT JOIN publics ON services.id = publics.service_id
    LEFT JOIN thematiques ON services.id = thematiques.service_id
    LEFT JOIN types ON services.id = types.service_id
    LEFT JOIN modes_accueil ON services.id = modes_accueil.service_id
    LEFT JOIN pre_requis ON services.id = pre_requis.service_id
    LEFT JOIN justificatifs ON services.id = justificatifs.service_id
    LEFT JOIN modes_orientation_accompagnateur ON services.id = modes_orientation_accompagnateur.service_id
    LEFT JOIN modes_orientation_beneficiaire ON services.id = modes_orientation_beneficiaire.service_id
    LEFT JOIN modes_mobilisation ON services.id = modes_mobilisation.service_id
)

SELECT * FROM final
