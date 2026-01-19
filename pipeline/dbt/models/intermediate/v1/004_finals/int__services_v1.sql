WITH services AS (
    SELECT * FROM {{ ref('int__union_services_v1') }}
),

services_v0_to_v1 AS (
    SELECT * FROM {{ ref('int__services_v0_to_v1') }}
),

contacts AS (
    SELECT * FROM {{ ref('int__contacts_v1') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__adresses_v1') }}
),

urls AS (
    SELECT
        input_url,
        "url"
    FROM {{ ref('int__urls') }}
    WHERE status_code > 0
)

SELECT
    services.adresse_id                  AS "adresse_id",
    urls.url                             AS "lien_mobilisation",
    services.horaires_accueil            AS "horaires_accueil",
    services.source                      AS "source",
    services.structure_id                AS "structure_id",
    services.lien_source                 AS "lien_source",
    CASE
        WHEN services.publics && ARRAY['locataires']
            THEN 'Le bénéficiaire doit être locataire.' || COALESCE('\n' || NULLIF(services.conditions_acces, ''), '')
        WHEN services.publics && ARRAY['proprietaires']
            THEN 'Le bénéficiaire doit être propriétaire.' || COALESCE('\n' || NULLIF(services.conditions_acces, ''), '')
        ELSE services.conditions_acces
    END                                  AS "conditions_acces",
    services.date_maj                    AS "date_maj",
    services.id                          AS "id",
    CASE
        WHEN LENGTH(services.description) > 10000 THEN LEFT(services.description, 9999) || '…'
        ELSE services.description
    END                                  AS "description",
    CASE
        WHEN services.nom ILIKE '%vélo%'
            THEN services.thematiques || ARRAY['mobilite--mobilite-douce-partagee-collective']
        ELSE services.thematiques
    END                                  AS "thematiques",
    services.modes_accueil               AS "modes_accueil",
    services.modes_mobilisation          AS "modes_mobilisation",
    services.mobilisable_par             AS "mobilisable_par",
    services.mobilisation_precisions     AS "mobilisation_precisions",
    services.publics                     AS "publics",
    services.publics_precisions          AS "publics_precisions",
    services.type                        AS "type",
    services.frais                       AS "frais",
    services.frais_precisions            AS "frais_precisions",
    services.nombre_semaines             AS "nombre_semaines",
    services.volume_horaire_hebdomadaire AS "volume_horaire_hebdomadaire",
    CASE
        WHEN services.zone_eligibilite IS NOT NULL
            THEN services.zone_eligibilite
        WHEN services.zone_eligibilite_type = 'commune' AND adresses.code_insee IS NOT NULL
            THEN ARRAY[adresses.code_insee]
        WHEN services.zone_eligibilite_type = 'epci'
            THEN services.zone_eligibilite
        WHEN services.zone_eligibilite_type = 'departement' AND adresses.code_departement IS NOT NULL
            THEN ARRAY[adresses.code_departement]
        WHEN services.zone_eligibilite_type = 'region' AND adresses.code_region IS NOT NULL
            THEN ARRAY[adresses.code_region]
        WHEN services.zone_eligibilite_type = 'pays'
            THEN ARRAY['france']
    END                                  AS "zone_eligibilite",
    contacts.contact_nom_prenom          AS "contact_nom_prenom",
    contacts.courriel                    AS "courriel",
    contacts.telephone                   AS "telephone",
    CASE
        WHEN LENGTH(services.nom) <= 150 THEN services.nom
        ELSE LEFT(services.nom, 149) || '…'
    END                                  AS "nom",
    adresses.longitude                   AS "longitude",
    adresses.latitude                    AS "latitude",
    adresses.complement_adresse          AS "complement_adresse",
    adresses.commune                     AS "commune",
    adresses.adresse                     AS "adresse",
    adresses.code_postal                 AS "code_postal",
    adresses.code_insee                  AS "code_insee",
    services.extra                       AS "extra"
FROM services
LEFT JOIN contacts
    ON services.id = contacts.id
LEFT JOIN adresses
    ON services.adresse_id = adresses.id
LEFT JOIN urls
    ON services.lien_mobilisation = urls.input_url
UNION ALL
SELECT
    services_v0_to_v1.adresse_id,
    services_v0_to_v1.lien_mobilisation,
    services_v0_to_v1.horaires_accueil,
    services_v0_to_v1.source,
    services_v0_to_v1.structure_id,
    services_v0_to_v1.lien_source,
    services_v0_to_v1.conditions_acces,
    services_v0_to_v1.date_maj,
    services_v0_to_v1.id,
    services_v0_to_v1.description,
    services_v0_to_v1.thematiques,
    services_v0_to_v1.modes_accueil,
    services_v0_to_v1.modes_mobilisation,
    services_v0_to_v1.mobilisable_par,
    services_v0_to_v1.mobilisation_precisions,
    services_v0_to_v1.publics,
    services_v0_to_v1.publics_precisions,
    services_v0_to_v1.type,
    services_v0_to_v1.frais,
    services_v0_to_v1.frais_precisions,
    services_v0_to_v1.nombre_semaines,
    services_v0_to_v1.volume_horaire_hebdomadaire,
    services_v0_to_v1.zone_eligibilite,
    services_v0_to_v1.contact_nom_prenom,
    services_v0_to_v1.courriel,
    services_v0_to_v1.telephone,
    services_v0_to_v1.nom,
    services_v0_to_v1.longitude,
    services_v0_to_v1.latitude,
    services_v0_to_v1.complement_adresse,
    services_v0_to_v1.commune,
    services_v0_to_v1.adresse,
    services_v0_to_v1.code_postal,
    services_v0_to_v1.code_insee,
    NULL::JSONB AS "extra"
FROM services_v0_to_v1
