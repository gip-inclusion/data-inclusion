{% set check_structure_str = "Veuillez vérifier sur le site internet de la structure" %}

WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

liaisons_besoins AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo__liaisons_besoins') }}
),

types AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo__types') }}
),

natures AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo__natures') }}
),

methodes AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo__methodes') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

regions AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__regions') }}
),

mapping_thematiques AS (
    SELECT * FROM {{ ref('_map_permis_velo_thematiques') }}
),

zone_code AS (
    SELECT
        permis_velo.id,
        communes.code,
        communes.code_epci,
        communes.code_departement,
        communes.code_region
    FROM permis_velo
    LEFT JOIN communes ON permis_velo.liaisons_villes_code_postal = ANY(communes.codes_postaux) AND communes.nom LIKE permis_velo.liaisons_villes_nom
),

types_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('Aide financière', 'aide-financiere'),
        ('Prestation', 'aide-materielle'),
        ('Accompagnement', 'accompagnement')
    ) AS x (types_mes_aides, types_di)
),

zone_de_diffusion_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('Échelle communale', 'commune'),
        ('Échelle intercommunale', 'epci'),
        ('Échelle régionale', 'region'),
        ('Échelle départementale', 'departement'),
        ('France métropolitaine', 'pays'),
        ('Échelle nationale', 'pays')
    ) AS x (zone_mes_aides, zone_di)
),

frais_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('payant', ARRAY['Somme d''argent', 'Prise en charge partielle', 'Tarif préférentiel']),
        ('gratuit', ARRAY['Prise en charge totale', 'Prêt remboursement']),
        ('gratuit-sous-conditions', ARRAY['Facilitation de paiement', 'Prise en charge partielle ou totale'])
    ) AS x (frais_di, frais_mes_aides)
),

zone_diffusion AS (
    SELECT
        zone_de_diffusion_mapping.zone_di,
        permis_velo.id
    FROM permis_velo
    INNER JOIN zone_de_diffusion_mapping ON permis_velo.zone_geographique = zone_de_diffusion_mapping.zone_mes_aides
),

modes_orientation_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('La démarche nécessite un passage ou une prise de contact directe avec votre organisme', ARRAY['envoyer-un-mail', 'telephoner'], ARRAY['envoyer-un-mail', 'telephoner', 'se-presenter']),
        ('La démarche nécessite une connexion sur le site de votre organisme', ARRAY['completer-le-formulaire-dadhesion'], ARRAY['completer-le-formulaire-dadhesion']),
        ('La démarche peut être faite par mail', ARRAY['envoyer-un-mail'], ARRAY['envoyer-un-mail'])
    ) AS x (demarche, modes_orientation_accompagnateur, modes_orientation_beneficiaire)
),

modes_orientation_transformed AS (
    SELECT
        permis_velo.id                                           AS service_id,
        ARRAY_AGG(DISTINCT moa.modes_orientation_accompagnateur) AS modes_orientation_accompagnateur,
        ARRAY_AGG(DISTINCT mob.modes_orientation_beneficiaire)   AS modes_orientation_beneficiaire
    FROM
        permis_velo
    INNER JOIN methodes ON permis_velo.id = methodes.service_id
    INNER JOIN modes_orientation_mapping AS mom ON methodes.value = mom.demarche
    LEFT JOIN LATERAL UNNEST(mom.modes_orientation_accompagnateur) AS moa (modes_orientation_accompagnateur) ON TRUE
    LEFT JOIN LATERAL UNNEST(mom.modes_orientation_beneficiaire) AS mob (modes_orientation_beneficiaire) ON TRUE
    GROUP BY permis_velo.id
),

thematiques AS (
    SELECT
        ARRAY_AGG(mapping_thematiques.correspondance_di) AS thematiques,
        permis_velo.id                                   AS service_id
    FROM permis_velo
    INNER JOIN liaisons_besoins ON permis_velo.id = liaisons_besoins.service_id
    INNER JOIN mapping_thematiques ON liaisons_besoins.value = mapping_thematiques.besoins
    GROUP BY permis_velo.id
),

transformed_types AS (
    SELECT
        permis_velo.id,
        ARRAY_AGG(types_mapping.types_di) AS transformed_types
    FROM permis_velo
    INNER JOIN types ON permis_velo.id = types.service_id
    INNER JOIN types_mapping ON types.value = types_mapping.types_mes_aides
    GROUP BY permis_velo.id
),

mes_aides_natures AS (
    SELECT
        permis_velo.id                                  AS service_id,
        ARRAY_AGG(frais_mapping.frais_di)               AS frais,
        ARRAY_TO_STRING(ARRAY_AGG(natures.value), '; ') AS frais_autres
    FROM permis_velo
    INNER JOIN natures ON permis_velo.id = natures.service_id
    INNER JOIN frais_mapping ON natures.value = ANY(frais_mapping.frais_mes_aides)
    GROUP BY permis_velo.id
),

final AS (
    SELECT
        permis_velo.id                                                     AS "adresse_id",
        permis_velo.modifiee_le                                            AS "date_maj",
        permis_velo.formulaire_url                                         AS "formulaire_en_ligne",
        mes_aides_natures.frais_autres                                     AS "frais_autres",
        permis_velo.id                                                     AS "id",
        CASE
            WHEN permis_velo.autres_justificatifs IS NOT NULL THEN ARRAY[permis_velo.autres_justificatifs]
            ELSE CAST(NULL AS TEXT [])
        END                                                                AS "justificatifs",
        permis_velo.url_mes_aides                                          AS "lien_source",
        CASE
            WHEN permis_velo.en_ligne = TRUE THEN ARRAY['a-distance']
            ELSE ARRAY['en-presentiel']
        END                                                                AS "modes_accueil",
        COALESCE(modes_orientation_transformed.modes_orientation_accompagnateur, CASE
            WHEN permis_velo.contact_telephone IS NOT NULL THEN ARRAY['telephoner']
            WHEN permis_velo.contact_email IS NOT NULL THEN ARRAY['envoyer-un-mail']
            WHEN permis_velo.formulaire_url IS NOT NULL THEN ARRAY['completer-le-formulaire-dadhesion']
        END)                                                               AS "modes_orientation_accompagnateur",
        NULL                                                               AS "modes_orientation_accompagnateur_autres",
        COALESCE(modes_orientation_transformed.modes_orientation_beneficiaire, CASE
            WHEN permis_velo.contact_telephone IS NOT NULL THEN ARRAY['telephoner']
            WHEN permis_velo.contact_email IS NOT NULL THEN ARRAY['envoyer-un-mail']
            WHEN permis_velo.formulaire_url IS NOT NULL THEN ARRAY['completer-le-formulaire-dadhesion']
        END)                                                               AS "modes_orientation_beneficiaire",
        permis_velo.demarche                                               AS "modes_orientation_beneficiaire_autres",
        permis_velo.nom                                                    AS "nom",
        CASE
            WHEN LENGTH(permis_velo.description) > 280 THEN SUBSTRING(permis_velo.description FROM 1 FOR 277) || '...'
            ELSE permis_velo.description
        END                                                                AS "presentation_resume",
        permis_velo.description
        || COALESCE(E'\n\n' || permis_velo.bon_a_savoir, '')
        || COALESCE(E'\n\n' || permis_velo.modalite_versement, '')         AS "presentation_detail",
        NULL                                                               AS "prise_rdv",
        CAST(NULL AS TEXT [])                                              AS "profils",
        LEFT(permis_velo.autres_conditions, 500)                           AS "profils_precisions",
        NULL                                                               AS "recurrence",
        permis_velo._di_source_id                                          AS "source",
        permis_velo.id                                                     AS "structure_id",
        thematiques.thematiques                                            AS "thematiques",
        transformed_types.transformed_types                                AS "types",
        CASE
            WHEN zone_diffusion.zone_di = 'commune' THEN zone_code.code
            WHEN zone_diffusion.zone_di = 'epci' THEN zone_code.code_epci
            WHEN zone_diffusion.zone_di = 'region' THEN regions.code
            WHEN zone_diffusion.zone_di = 'departement' THEN departements.code
            WHEN zone_diffusion.zone_di = 'pays' THEN 'FR'
        END                                                                AS "zone_diffusion_code",
        CASE
            WHEN zone_diffusion.zone_di = 'commune' THEN permis_velo.liaisons_villes_nom
            WHEN zone_diffusion.zone_di = 'epci' THEN permis_velo.liaisons_villes_nom
            WHEN zone_diffusion.zone_di = 'region' THEN permis_velo.liaisons_region
            WHEN zone_diffusion.zone_di = 'departement' THEN departements.nom
            WHEN zone_diffusion.zone_di = 'pays' THEN 'France entière'
        END                                                                AS "zone_diffusion_nom",
        zone_diffusion.zone_di                                             AS "zone_diffusion_type",
        CASE
            WHEN permis_velo.autres_conditions IS NULL THEN CAST(NULL AS TEXT [])
            ELSE ARRAY[permis_velo.autres_conditions]
        END                                                                AS "pre_requis",
        NULL                                                               AS "contact_nom_prenom",
        permis_velo.contact_email                                          AS "courriel",
        SUBSTRING(permis_velo.contact_telephone FROM '\+?\d[\d\.\-\s]*\d') AS "telephone",
        mes_aides_natures.frais                                            AS "frais",
        permis_velo.site                                                   AS "page_web"
    FROM permis_velo
    LEFT JOIN transformed_types ON permis_velo.id = transformed_types.id
    LEFT JOIN zone_diffusion ON permis_velo.id = zone_diffusion.id
    LEFT JOIN zone_code ON permis_velo.id = zone_code.id
    LEFT JOIN departements ON permis_velo.num_departement = departements.code
    LEFT JOIN regions ON permis_velo.liaisons_region = regions.nom
    LEFT JOIN thematiques ON permis_velo.id = thematiques.service_id
    LEFT JOIN mes_aides_natures ON permis_velo.id = mes_aides_natures.service_id
    LEFT JOIN modes_orientation_transformed ON permis_velo.id = modes_orientation_transformed.service_id
    WHERE permis_velo.slug_organisme NOT IN ('action-logement', 'france-travail')
)

SELECT * FROM final
