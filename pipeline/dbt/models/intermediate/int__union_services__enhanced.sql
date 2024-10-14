WITH services AS (
    SELECT * FROM {{ ref('int__union_services') }}
),

structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__union_adresses__enhanced') }}
),

adresses_with_code_departement AS (
    SELECT
        adresses.*,
        CASE
            WHEN LEFT(adresses.code_insee, 2) = '97' THEN LEFT(adresses.code_insee, 3)
            ELSE LEFT(adresses.code_insee, 2)
        END AS "code_departement"
    FROM adresses
),

services_with_valid_structure AS (
    SELECT services.*
    FROM services
    INNER JOIN structures
        ON services._di_structure_surrogate_id = structures._di_surrogate_id
),

-- For some providers, zone_diffusion_code can not be set at the source mapping level for lack of proper codification.
-- Now that the data has been geocoded, it can be set, according to the mapped zone_diffusion_type.
-- FIXME(vperron) : ODSPEP services have such a catastrophic adress columns quality
-- that trying to reuse them for the zone diffusion makes the situation worse.
zones_diffusion AS (
    SELECT
        services._di_surrogate_id    AS "_di_surrogate_id",
        services.zone_diffusion_type AS "zone_diffusion_type",
        CASE
            WHEN NOT (services.source = ANY(ARRAY['monenfant', 'action-logement', 'soliguide', 'reseau-alpha', 'mediation-numerique']))
                THEN services.zone_diffusion_code
            WHEN services.zone_diffusion_type = 'commune' AND adresses.code_insee IS NOT NULL
                THEN adresses.code_insee
            WHEN services.zone_diffusion_type = 'departement' AND adresses.code_departement IS NOT NULL
                THEN adresses.code_departement
            ELSE services.zone_diffusion_code
        END                          AS "zone_diffusion_code",
        CASE
            WHEN NOT (services.source = ANY(ARRAY['monenfant', 'action-logement', 'soliguide', 'reseau-alpha', 'mediation-numerique']))
                THEN services.zone_diffusion_nom
            WHEN services.zone_diffusion_type = 'commune' AND adresses.commune IS NOT NULL
                THEN adresses.commune
            WHEN services.zone_diffusion_type = 'departement' AND departements.nom IS NOT NULL
                THEN departements.nom
            ELSE services.zone_diffusion_nom
        END                          AS "zone_diffusion_nom"
    FROM services_with_valid_structure AS services
    LEFT JOIN adresses_with_code_departement AS adresses
        ON services._di_adresse_surrogate_id = adresses._di_surrogate_id
    LEFT JOIN departements
        ON adresses.code_departement = departements.code
),

services_with_zone_diffusion AS (
    SELECT
        {{
            dbt_utils.star(
                from=ref('int__union_services'),
                relation_alias='services',
                except=["zone_diffusion_code", "zone_diffusion_nom"]
            )
        }},
        zones_diffusion.zone_diffusion_code AS "zone_diffusion_code",
        zones_diffusion.zone_diffusion_nom  AS "zone_diffusion_nom"
    FROM services_with_valid_structure AS services
    LEFT JOIN zones_diffusion
        ON services._di_surrogate_id = zones_diffusion._di_surrogate_id
),

valid_services AS (
    SELECT services.*
    FROM services_with_zone_diffusion AS services
    LEFT JOIN
        LATERAL
        LIST_SERVICE_ERRORS(
            services.contact_public,
            services.contact_nom_prenom,
            services.courriel,
            services.cumulable,
            services.date_creation,
            services.date_maj,
            services.date_suspension,
            services.frais,
            services.frais_autres,
            services.id,
            services.justificatifs,
            services.lien_source,
            services.modes_accueil,
            services.modes_orientation_accompagnateur,
            services.modes_orientation_accompagnateur_autres,
            services.modes_orientation_beneficiaire,
            services.modes_orientation_beneficiaire_autres,
            services.nom,
            services.page_web,
            services.presentation_detail,
            services.presentation_resume,
            services.prise_rdv,
            services.profils,
            services.recurrence,
            services.source,
            services.structure_id,
            services.telephone,
            services.thematiques,
            services.types,
            services.zone_diffusion_code,
            services.zone_diffusion_nom,
            services.zone_diffusion_type,
            services.pre_requis
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
),

final AS (
    SELECT
        services.*,
        adresses.longitude          AS "longitude",
        adresses.latitude           AS "latitude",
        adresses.complement_adresse AS "complement_adresse",
        adresses.commune            AS "commune",
        adresses.adresse            AS "adresse",
        adresses.code_postal        AS "code_postal",
        adresses.code_insee         AS "code_insee"
    FROM
        valid_services AS services
    LEFT JOIN adresses_with_code_departement AS adresses
        ON services._di_adresse_surrogate_id = adresses._di_surrogate_id
)

SELECT * FROM final
