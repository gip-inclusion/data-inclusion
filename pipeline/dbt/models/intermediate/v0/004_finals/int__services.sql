WITH services AS (
    SELECT * FROM {{ ref('int__union_services') }}
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

adresses_with_code_region AS (
    SELECT
        adresses.*,
        departements.code_region AS "code_region"
    FROM adresses_with_code_departement AS adresses
    LEFT JOIN departements
        ON adresses.code_departement = departements.code
),

-- For some providers, zone_diffusion_code can not be set at the source mapping level for lack of proper codification.
-- Now that the data has been geocoded, it can be set, according to the mapped zone_diffusion_type.
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
    FROM services
    LEFT JOIN adresses_with_code_departement AS adresses
        ON services._di_adresse_surrogate_id = adresses._di_surrogate_id
    LEFT JOIN departements
        ON adresses.code_departement = departements.code
),

urls AS (
    SELECT
        input_url,
        "url"
    FROM {{ ref('int__urls') }}
    WHERE status_code > 0
)

SELECT
    services._di_surrogate_id                        AS "_di_surrogate_id",
    services._di_structure_surrogate_id              AS "_di_structure_surrogate_id",
    urls_formulaire_en_ligne.url                     AS "formulaire_en_ligne",
    services.frais_autres                            AS "frais_autres",
    services.justificatifs                           AS "justificatifs",
    services.presentation_resume                     AS "presentation_resume",
    urls_prise_rdv.url                               AS "prise_rdv",
    services.recurrence                              AS "recurrence",
    services.source                                  AS "source",
    services.structure_id                            AS "structure_id",
    services.zone_diffusion_type                     AS "zone_diffusion_type",
    services.pre_requis                              AS "pre_requis",
    services.lien_source                             AS "lien_source",
    services.date_maj                                AS "date_maj",
    services.id                                      AS "id",
    services.presentation_detail                     AS "presentation_detail",
    services.thematiques                             AS "thematiques",
    services.modes_accueil                           AS "modes_accueil",
    services.modes_orientation_accompagnateur        AS "modes_orientation_accompagnateur",
    services.modes_orientation_accompagnateur_autres AS "modes_orientation_accompagnateur_autres",
    services.modes_orientation_beneficiaire          AS "modes_orientation_beneficiaire",
    services.modes_orientation_beneficiaire_autres   AS "modes_orientation_beneficiaire_autres",
    services.profils                                 AS "profils",
    services.profils_precisions                      AS "profils_precisions",
    services.types                                   AS "types",
    services.frais                                   AS "frais",
    urls_page_web.url                                AS "page_web",
    services.nombre_semaines                         AS "nombre_semaines",
    services.volume_horaire_hebdomadaire             AS "volume_horaire_hebdomadaire",
    zones_diffusion.zone_diffusion_code              AS "zone_diffusion_code",
    zones_diffusion.zone_diffusion_nom               AS "zone_diffusion_nom",
    contacts.contact_nom_prenom                      AS "contact_nom_prenom",
    contacts.courriel                                AS "courriel",
    contacts.telephone                               AS "telephone",
    CASE
        WHEN LENGTH(services.nom) <= 150 THEN services.nom
        ELSE LEFT(services.nom, 149) || '…'
    END                                              AS "nom",
    adresses.longitude                               AS "longitude",
    adresses.latitude                                AS "latitude",
    adresses.complement_adresse                      AS "complement_adresse",
    adresses.commune                                 AS "commune",
    adresses.adresse                                 AS "adresse",
    adresses.code_postal                             AS "code_postal",
    adresses.code_insee                              AS "code_insee"
FROM services
LEFT JOIN zones_diffusion
    ON services._di_surrogate_id = zones_diffusion._di_surrogate_id
LEFT JOIN contacts
    ON services._di_surrogate_id = contacts._di_surrogate_id
LEFT JOIN adresses_with_code_region AS adresses
    ON services._di_adresse_surrogate_id = adresses._di_surrogate_id
LEFT JOIN urls AS urls_prise_rdv
    ON services.prise_rdv = urls_prise_rdv.input_url
LEFT JOIN urls AS urls_formulaire_en_ligne
    ON services.formulaire_en_ligne = urls_formulaire_en_ligne.input_url
LEFT JOIN urls AS urls_page_web
    ON services.page_web = urls_page_web.input_url
