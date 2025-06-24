WITH services AS (
    SELECT * FROM {{ ref('int__union_services') }}
),

structures AS (
    SELECT * FROM {{ ref('int__structures') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

contacts AS (
    SELECT * FROM {{ ref('int__contacts') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__adresses') }}
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
            WHEN NOT (services.source = ANY(ARRAY['monenfant', 'action-logement', 'soliguide', 'reseau-alpha', 'mediation-numerique', 'mission-locale']))
                THEN services.zone_diffusion_code
            WHEN services.zone_diffusion_type = 'commune' AND adresses.code_insee IS NOT NULL
                THEN adresses.code_insee
            WHEN services.zone_diffusion_type = 'departement' AND adresses.code_departement IS NOT NULL
                THEN adresses.code_departement
            ELSE services.zone_diffusion_code
        END                          AS "zone_diffusion_code",
        CASE
            WHEN NOT (services.source = ANY(ARRAY['monenfant', 'action-logement', 'soliguide', 'reseau-alpha', 'mediation-numerique', 'mission-locale']))
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

valid_site_web AS (
    SELECT
        input_url,
        "url"
    FROM {{ ref('int__urls') }}
    WHERE status_code > 0
)

SELECT
    services._di_surrogate_id                                                        AS "_di_surrogate_id",
    services._di_structure_surrogate_id                                              AS "_di_structure_surrogate_id",
    services.formulaire_en_ligne                                                     AS "formulaire_en_ligne",
    services.frais_autres                                                            AS "frais_autres",
    services.justificatifs                                                           AS "justificatifs",
    services.presentation_resume                                                     AS "presentation_resume",
    services.prise_rdv                                                               AS "prise_rdv",
    COALESCE(valid_prise_rdv.url, valid_formulaire_en_ligne.url, valid_page_web.url) AS "lien_mobilisation",
    services.recurrence                                                              AS "recurrence",
    services.source                                                                  AS "source",
    services.structure_id                                                            AS "structure_id",
    services.zone_diffusion_type                                                     AS "zone_diffusion_type",
    services.pre_requis                                                              AS "pre_requis",
    services.lien_source                                                             AS "lien_source",
    services.date_maj                                                                AS "date_maj",
    services.id                                                                      AS "id",
    services.presentation_detail                                                     AS "presentation_detail",
    CASE
        WHEN LENGTH(services.presentation_detail) >= 2000
            THEN LEFT(services.presentation_detail, 1999) || '…'
        ELSE COALESCE(services.presentation_detail, services.presentation_resume)
    END                                                                              AS "description",
    services.thematiques                                                             AS "thematiques",
    services.modes_accueil                                                           AS "modes_accueil",
    services.modes_orientation_accompagnateur                                        AS "modes_orientation_accompagnateur",
    services.modes_orientation_accompagnateur_autres                                 AS "modes_orientation_accompagnateur_autres",
    services.modes_orientation_beneficiaire                                          AS "modes_orientation_beneficiaire",
    services.modes_orientation_beneficiaire_autres                                   AS "modes_orientation_beneficiaire_autres",
    ARRAY(
        -- Mapping https://www.notion.so/gip-inclusion/24610bd08f8a412c83c09f6b36a1a44f?v=34cdd4c049e44f49aec060657c72c9b0&p=1fa5f321b604805a9ba5d0c7c2386dc2&pm=s
        SELECT x FROM
            UNNEST(ARRAY[
                -- envoyer-un-courriel
                CASE
                    WHEN
                        ARRAY['envoyer-un-mail', 'envoyer-un-mail-avec-une-fiche-de-prescription', 'prendre-rdv'] && services.modes_orientation_accompagnateur
                        OR ARRAY['envoyer-un-mail', 'prendre-rdv'] && services.modes_orientation_beneficiaire
                        THEN 'envoyer-un-courriel'
                END,
                -- se-presenter
                CASE
                    WHEN 'se-presenter' = ANY(services.modes_orientation_beneficiaire)
                        THEN 'se-presenter'
                END,
                -- telephoner
                CASE
                    WHEN
                        'telephoner' = ANY(services.modes_orientation_accompagnateur)
                        OR 'telephoner' = ANY(services.modes_orientation_beneficiaire)
                        THEN 'telephoner'
                END,
                -- utiliser-lien-mobilisation
                CASE
                    WHEN
                        ARRAY['completer-le-formulaire-dadhesion', 'prendre-rdv'] && services.modes_orientation_accompagnateur
                        OR ARRAY['completer-le-formulaire-dadhesion', 'prendre-rdv'] && services.modes_orientation_beneficiaire
                        -- TODO: change this when migrate to the `lien_mobilisation` field
                        OR services.prise_rdv IS NOT NULL
                        OR services.formulaire_en_ligne IS NOT NULL
                        OR services.page_web IS NOT NULL
                        THEN 'utiliser-lien-mobilisation'
                END
            ]) AS x
        WHERE x IS NOT NULL
    )                                                                                AS "modes_mobilisation",
    ARRAY(
        SELECT x FROM
            UNNEST(ARRAY[
                CASE WHEN ARRAY_LENGTH(services.modes_orientation_beneficiaire, 1) > 0 THEN 'usagers' END,
                CASE WHEN ARRAY_LENGTH(services.modes_orientation_accompagnateur, 1) > 0 THEN 'professionnels' END
            ]) AS x
        WHERE x IS NOT NULL
    )                                                                                AS "mobilisable_par",
    CASE
        WHEN ARRAY_LENGTH(services.modes_orientation_beneficiaire, 1) > 0 AND ARRAY_LENGTH(services.modes_orientation_accompagnateur, 1) > 0
            THEN COALESCE(
                services.modes_orientation_accompagnateur_autres,
                services.modes_orientation_beneficiaire_autres
            )
        WHEN ARRAY_LENGTH(services.modes_orientation_beneficiaire, 1) > 0 THEN services.modes_orientation_beneficiaire_autres
        WHEN ARRAY_LENGTH(services.modes_orientation_accompagnateur, 1) > 0 THEN services.modes_orientation_accompagnateur_autres
    END                                                                              AS "mobilisation_precisions",
    services.profils                                                                 AS "profils",
    services.profils_precisions                                                      AS "profils_precisions",
    services.types                                                                   AS "types",
    services.frais                                                                   AS "frais",
    services.page_web                                                                AS "page_web",
    services.nombre_semaines                                                         AS "nombre_semaines",
    services.volume_horaire_hebdomadaire                                             AS "volume_horaire_hebdomadaire",
    zones_diffusion.zone_diffusion_code                                              AS "zone_diffusion_code",
    zones_diffusion.zone_diffusion_nom                                               AS "zone_diffusion_nom",
    contacts.contact_nom_prenom                                                      AS "contact_nom_prenom",
    contacts.courriel                                                                AS "courriel",
    contacts.telephone                                                               AS "telephone",
    CASE
        WHEN LENGTH(services.nom) <= 150 THEN services.nom
        ELSE LEFT(services.nom, 149) || '…'
    END                                                                              AS "nom",
    adresses.longitude                                                               AS "longitude",
    adresses.latitude                                                                AS "latitude",
    adresses.complement_adresse                                                      AS "complement_adresse",
    adresses.commune                                                                 AS "commune",
    adresses.adresse                                                                 AS "adresse",
    adresses.code_postal                                                             AS "code_postal",
    adresses.code_insee                                                              AS "code_insee"
FROM services_with_valid_structure AS services
LEFT JOIN zones_diffusion
    ON services._di_surrogate_id = zones_diffusion._di_surrogate_id
LEFT JOIN contacts
    ON services._di_surrogate_id = contacts._di_surrogate_id
LEFT JOIN adresses_with_code_departement AS adresses
    ON services._di_adresse_surrogate_id = adresses._di_surrogate_id
LEFT JOIN valid_site_web AS valid_prise_rdv
    ON services.prise_rdv = valid_prise_rdv.input_url
LEFT JOIN valid_site_web AS valid_formulaire_en_ligne
    ON services.formulaire_en_ligne = valid_formulaire_en_ligne.input_url
LEFT JOIN valid_site_web AS valid_page_web
    ON services.page_web = valid_page_web.input_url
