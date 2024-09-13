WITH services AS (
    SELECT * FROM {{ ref('int__union_services') }}
),

structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__union_adresses__enhanced') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

-- TODO: Refactoring needed to be able to do geocoding per source and then use the result in the mapping
services_with_zone_diffusion AS (
    SELECT
        {{ dbt_utils.star(from=ref('int__union_services'), relation_alias='services', except=["zone_diffusion_code", "zone_diffusion_nom"]) }},
        CASE
            WHEN services.source = ANY(ARRAY['monenfant', 'soliguide']) THEN adresses.code_insee
            WHEN services.source = ANY(ARRAY['reseau-alpha', 'action-logement']) THEN LEFT(adresses.code_insee, 2)
            ELSE services.zone_diffusion_code
        END AS "zone_diffusion_code",
        CASE
            WHEN services.source = ANY(ARRAY['monenfant', 'soliguide']) THEN adresses.commune
            WHEN services.source = ANY(ARRAY['reseau-alpha', 'action-logement']) THEN (SELECT departements."nom" FROM departements WHERE departements."code" = LEFT(adresses.code_insee, 2))
            WHEN services.source = 'mediation-numerique' THEN (SELECT departements."nom" FROM departements WHERE departements."code" = services.zone_diffusion_code)
            ELSE services.zone_diffusion_nom
        END AS "zone_diffusion_nom"
    FROM
        services
    LEFT JOIN adresses ON services._di_adresse_surrogate_id = adresses._di_surrogate_id
),

services_with_valid_structure AS (
    SELECT services_with_zone_diffusion.*
    FROM services_with_zone_diffusion
    INNER JOIN structures ON services_with_zone_diffusion._di_structure_surrogate_id = structures._di_surrogate_id
),

valid_services AS (
    SELECT services_with_valid_structure.*
    FROM services_with_valid_structure
    LEFT JOIN
        LATERAL
        LIST_SERVICE_ERRORS(
            services_with_valid_structure.contact_public,
            services_with_valid_structure.contact_nom_prenom,
            services_with_valid_structure.courriel,
            services_with_valid_structure.cumulable,
            services_with_valid_structure.date_creation,
            services_with_valid_structure.date_maj,
            services_with_valid_structure.date_suspension,
            services_with_valid_structure.frais,
            services_with_valid_structure.frais_autres,
            services_with_valid_structure.id,
            services_with_valid_structure.justificatifs,
            services_with_valid_structure.lien_source,
            services_with_valid_structure.modes_accueil,
            services_with_valid_structure.modes_orientation_accompagnateur,
            services_with_valid_structure.modes_orientation_accompagnateur_autres,
            services_with_valid_structure.modes_orientation_beneficiaire,
            services_with_valid_structure.modes_orientation_beneficiaire_autres,
            services_with_valid_structure.nom,
            services_with_valid_structure.page_web,
            services_with_valid_structure.presentation_detail,
            services_with_valid_structure.presentation_resume,
            services_with_valid_structure.prise_rdv,
            services_with_valid_structure.profils,
            services_with_valid_structure.recurrence,
            services_with_valid_structure.source,
            services_with_valid_structure.structure_id,
            services_with_valid_structure.telephone,
            services_with_valid_structure.thematiques,
            services_with_valid_structure.types,
            services_with_valid_structure.zone_diffusion_code,
            services_with_valid_structure.zone_diffusion_nom,
            services_with_valid_structure.zone_diffusion_type,
            services_with_valid_structure.pre_requis
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
),

final AS (
    SELECT
        valid_services.*,
        adresses.longitude          AS "longitude",
        adresses.latitude           AS "latitude",
        adresses.complement_adresse AS "complement_adresse",
        adresses.commune            AS "commune",
        adresses.adresse            AS "adresse",
        adresses.code_postal        AS "code_postal",
        adresses.code_insee         AS "code_insee"
    FROM
        valid_services
    LEFT JOIN adresses ON valid_services._di_adresse_surrogate_id = adresses._di_surrogate_id
)

SELECT * FROM final
