WITH services AS (
    SELECT services.*
    FROM {{ ref('int__services') }} AS services
    LEFT JOIN {{ ref('int__sources_v0_to_v1') }} AS sources
        ON services.source = sources.source
    WHERE sources.source IS NULL
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

map_frais AS (SELECT * FROM {{ ref('_map_frais') }}),

frais AS (
    SELECT DISTINCT ON (1)
        services._di_surrogate_id,
        map_frais.frais_v1 AS frais
    FROM
        services,
        UNNEST(services.frais) AS item
    INNER JOIN map_frais ON item = map_frais.frais_v0
    WHERE map_frais.frais_v1 IS NOT NULL
    ORDER BY services._di_surrogate_id, map_frais.frais_v1 = 'payant' DESC
),

mapping_publics AS (SELECT * FROM {{ ref('_map_publics') }}),

publics_v1 AS (
    SELECT
        services._di_surrogate_id,
        CASE
            WHEN 'tous-publics' = ANY(services.profils)
                THEN ARRAY['tous-publics']
            WHEN
                services.thematiques && ARRAY[
                    'handicap',
                    'handicap--accompagnement-par-une-structure-specialisee',
                    'handicap--adaptation-au-poste-de-travail',
                    'handicap--adapter-son-logement',
                    'handicap--aide-a-la-personne',
                    'handicap--connaissance-des-droits-des-travailleurs',
                    'handicap--faire-reconnaitre-un-handicap',
                    'handicap--favoriser-le-retour-et-le-maintien-dans-lemploi',
                    'handicap--gerer-le-depart-a-la-retraite-des-personnes-en-situation-de-handicap',
                    'handicap--mobilite-des-personnes-en-situation-de-handicap'
                ] AND NOT 'personnes-en-situation-de-handicap' = ANY(ARRAY_AGG(mapping_publics.public_v1))
                THEN ARRAY_AGG(DISTINCT mapping_publics.public_v1) || ARRAY['personnes-en-situation-de-handicap']
            ELSE ARRAY_AGG(DISTINCT mapping_publics.public_v1)
        END AS publics
    FROM
        services,
        UNNEST(services.profils) AS profil
    LEFT JOIN mapping_publics ON profil = mapping_publics.profil_v0
    WHERE mapping_publics.public_v1 IS NOT NULL
    GROUP BY services._di_surrogate_id, services.profils, services.thematiques
),

mapping_thematiques AS (SELECT * FROM {{ ref('_map_thematiques') }}),

thematiques_v1 AS (
    SELECT
        services._di_surrogate_id,
        ARRAY_AGG(DISTINCT mapping_thematiques.v1) AS thematiques
    FROM
        services,
        UNNEST(services.thematiques) AS thematique_v0
    LEFT JOIN mapping_thematiques ON thematique_v0 = mapping_thematiques.v0
    WHERE mapping_thematiques.v1 IS NOT NULL
    GROUP BY services._di_surrogate_id
)

SELECT
    services.source || '--' || services.id                                                                    AS "id",
    services.source || '--' || REPLACE(services._di_structure_surrogate_id, services.source || '-', '')       AS "structure_id",
    COALESCE(services.prise_rdv, services.formulaire_en_ligne, services.page_web)                             AS "lien_mobilisation",
    services.recurrence                                                                                       AS "horaires_accueil",
    services.source                                                                                           AS "source",
    services.lien_source                                                                                      AS "lien_source",
    CASE
        WHEN services.profils && ARRAY['locataires'] THEN 'Le bénéficiaire doit être locataire.'
        WHEN services.profils && ARRAY['proprietaires'] THEN 'Le bénéficiaire doit être propriétaire.'
    END                                                                                                       AS "conditions_acces",
    services.date_maj                                                                                         AS "date_maj",
    CASE
        WHEN LENGTH(services.presentation_detail) >= 2000
            THEN LEFT(services.presentation_detail, 1999) || '…'
        ELSE COALESCE(services.presentation_detail, services.presentation_resume)
    END                                                                                                       AS "description",
    thematiques_v1.thematiques                                                                                AS "thematiques",
    services.modes_accueil                                                                                    AS "modes_accueil",
    NULLIF(ARRAY(
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
    ), '{}')                                                                                                  AS "modes_mobilisation",
    NULLIF(ARRAY(
        SELECT x FROM
            UNNEST(ARRAY[
                CASE WHEN ARRAY_LENGTH(services.modes_orientation_beneficiaire, 1) > 0 THEN 'usagers' END,
                CASE WHEN ARRAY_LENGTH(services.modes_orientation_accompagnateur, 1) > 0 THEN 'professionnels' END
            ]) AS x
        WHERE x IS NOT NULL
    ), '{}')                                                                                                  AS "mobilisable_par",
    services.modes_orientation_beneficiaire_autres || ' ' || services.modes_orientation_accompagnateur_autres AS "mobilisation_precisions",
    publics_v1.publics                                                                                        AS "publics",
    services.profils_precisions                                                                               AS "publics_precisions",
    CASE
        WHEN
            services.types && ARRAY['aide-financiere']
            OR services.types && ARRAY['financement']
            THEN 'aide-financiere'
        WHEN
            services.types && ARRAY['autonomie']
            OR services.types && ARRAY['aide-materielle']
            THEN 'aide-materielle'
        WHEN services.types && ARRAY['formation'] THEN 'formation'
        WHEN
            services.types && ARRAY['atelier']
            OR services.types && ARRAY['numerique']
            THEN 'atelier'
        WHEN services.types && ARRAY['accompagnement'] THEN 'accompagnement'
        WHEN
            services.types && ARRAY['accueil']
            OR services.types && ARRAY['information']
            THEN 'information'
    END                                                                                                       AS "type",
    frais.frais                                                                                               AS "frais",
    NULLIF(services.frais_autres, '')                                                                         AS "frais_precisions",
    services.nombre_semaines                                                                                  AS "nombre_semaines",
    services.volume_horaire_hebdomadaire                                                                      AS "volume_horaire_hebdomadaire",
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
    END                                                                                                       AS "zone_eligibilite",
    services.contact_nom_prenom                                                                               AS "contact_nom_prenom",
    services.courriel                                                                                         AS "courriel",
    services.telephone                                                                                        AS "telephone",
    CASE
        WHEN LENGTH(services.nom) <= 150 THEN services.nom
        ELSE LEFT(services.nom, 149) || '…'
    END                                                                                                       AS "nom",
    services.longitude                                                                                        AS "longitude",
    services.latitude                                                                                         AS "latitude",
    services.complement_adresse                                                                               AS "complement_adresse",
    services.commune                                                                                          AS "commune",
    services.adresse                                                                                          AS "adresse",
    services.code_postal                                                                                      AS "code_postal",
    services.code_insee                                                                                       AS "code_insee"
FROM services
LEFT JOIN publics_v1
    ON services._di_surrogate_id = publics_v1._di_surrogate_id
LEFT JOIN frais
    ON services._di_surrogate_id = frais._di_surrogate_id
LEFT JOIN thematiques_v1
    ON services._di_surrogate_id = thematiques_v1._di_surrogate_id
