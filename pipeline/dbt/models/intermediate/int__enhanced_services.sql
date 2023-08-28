WITH services AS (
    SELECT * FROM {{ ref('int__services') }}
),

structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

adresses_geocoded AS (
    SELECT * FROM {{ ref('int__adresses_geocoded') }}
),

-- TODO: Refactoring needed to be able to do geocoding per source and then use the result in the mapping
services_with_zone_diffusion AS (
    SELECT
        {{ dbt_utils.star(from=ref('int__services'), relation_alias='services', except=["zone_diffusion_code", "zone_diffusion_nom"]) }},
        CASE services.source = ANY(ARRAY['monenfant', 'soliguide'])
            WHEN TRUE THEN adresses_geocoded.result_citycode
            ELSE services.zone_diffusion_code
        END AS "zone_diffusion_code",
        CASE services.source = ANY(ARRAY['monenfant', 'soliguide'])
            WHEN TRUE THEN adresses_geocoded.commune
            ELSE services.zone_diffusion_nom
        END AS "zone_diffusion_nom"
    FROM
        services
    LEFT JOIN adresses_geocoded ON services._di_adresse_surrogate_id = adresses_geocoded._di_surrogate_id
),

services_with_valid_structure AS (
    SELECT services_with_zone_diffusion.*
    FROM services_with_zone_diffusion
    INNER JOIN structures ON services_with_zone_diffusion._di_structure_surrogate_id = structures._di_surrogate_id
),

valid_services AS (
    SELECT services_with_valid_structure.*
    FROM services_with_valid_structure
    LEFT JOIN LATERAL
        LIST_SERVICE_ERRORS(
            contact_public,
            contact_nom_prenom,
            courriel,
            cumulable,
            date_creation,
            date_maj,
            date_suspension,
            frais,
            frais_autres,
            id,
            justificatifs,
            lien_source,
            modes_accueil,
            modes_orientation_accompagnateur,
            modes_orientation_beneficiaire,
            nom,
            presentation_detail,
            presentation_resume,
            prise_rdv,
            profils,
            recurrence,
            source,
            structure_id,
            telephone,
            thematiques,
            types,
            zone_diffusion_code,
            zone_diffusion_nom,
            zone_diffusion_type,
            pre_requis
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
),

final AS (
    SELECT
        valid_services.*,
        adresses_geocoded.longitude          AS "longitude",
        adresses_geocoded.latitude           AS "latitude",
        adresses_geocoded.complement_adresse AS "complement_adresse",
        adresses_geocoded.commune            AS "commune",
        adresses_geocoded.adresse            AS "adresse",
        adresses_geocoded.code_postal        AS "code_postal",
        adresses_geocoded.code_insee         AS "code_insee",
        adresses_geocoded.result_score       AS "_di_geocodage_score",
        adresses_geocoded.result_citycode    AS "_di_geocodage_code_insee"
    FROM
        valid_services
    LEFT JOIN adresses_geocoded ON valid_services._di_adresse_surrogate_id = adresses_geocoded._di_surrogate_id
)

SELECT * FROM final
