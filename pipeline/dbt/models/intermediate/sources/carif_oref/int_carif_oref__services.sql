WITH actions AS (
    SELECT * FROM {{ ref('stg_carif_oref__actions') }}
),

actions__publics AS (
    SELECT * FROM {{ ref('stg_carif_oref__actions__publics') }}
),

organismes_formateurs AS (
    SELECT * FROM {{ ref('stg_carif_oref__organismes_formateurs') }}
),

organismes_formateurs__contacts AS (
    SELECT * FROM {{ ref('stg_carif_oref__organismes_formateurs__contacts') }}
),

formations AS (
    SELECT * FROM {{ ref('stg_carif_oref__formations') }}
),

formations__contacts AS (
    SELECT * FROM {{ ref('stg_carif_oref__formations__contacts') }}
),

coordonnees AS (
    SELECT * FROM {{ ref('stg_carif_oref__coordonnees') }}
),

adresses AS (
    SELECT * FROM {{ ref('stg_carif_oref__adresses') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

profils_mapping AS (
    SELECT x.*
    FROM (
        VALUES
        ('80001', 'demandeurs-demploi'),
        ('80003', 'demandeurs-demploi'),
        ('80004', 'demandeurs-demploi'),
        ('80006', 'demandeurs-demploi'),
        ('80007', 'demandeurs-demploi'),
        ('80008', 'demandeurs-demploi'),
        ('80009', 'demandeurs-demploi'),
        ('80009', 'jeunes'),
        ('80009', 'jeunes-16-26'),
        ('81018', 'femmes'),
        ('81019', 'personnes-en-situation-illettrisme'),
        ('81020', 'personnes-en-situation-de-handicap'),
        ('81021', 'personnes-en-situation-illettrisme'),
        ('81022', 'personnes-de-nationalite-etrangere'),
        ('81023', 'personnes-de-nationalite-etrangere'),
        ('81024', 'jeunes-16-26'),
        ('81025', 'personnes-en-situation-de-handicap'),
        ('81026', 'personnes-en-situation-de-handicap'),
        ('81027', 'handicaps-mentaux'),
        ('81027', 'personnes-en-situation-de-handicap'),
        ('81042', 'personnes-de-nationalite-etrangere'),
        ('82015', 'salaries'),
        ('82016', 'salaries'),
        ('82044', 'salaries'),
        ('82046', 'salaries'),
        ('82055', 'salaries'),
        ('82070', 'salaries'),
        ('82071', 'salaries'),
        ('82074', 'salaries'),
        ('82075', 'salaries'),
        ('83056', 'tous-publics')
    ) AS x (formacode_v13, profil)
),

profils AS (
    SELECT
        actions__publics.numero_action AS "numero_action",
        ARRAY_AGG(profil.value)        AS "profils"
    FROM actions__publics,
        LATERAL (
            SELECT profils_mapping.profil
            FROM profils_mapping
            WHERE profils_mapping.formacode_v13 = actions__publics.code_public_vise
        ) AS profil (value)
    GROUP BY actions__publics.numero_action
),

final AS (
    SELECT DISTINCT ON (actions.numero, organismes_formateurs.numero)
        actions.numero                                  AS "id",
        COALESCE(
            coordonnees_lieu_de_formation.hash_adresse,
            coordonnees_organisme_formateur.hash_adresse
        )                                               AS "adresse_id",
        NULL                                            AS "prise_rdv",
        actions.detail_conditions_prise_en_charge       AS "frais_autres",
        profils.profils                                 AS "profils",
        actions.info_public_vise                        AS "profils_precisions",
        organismes_formateurs.numero                    AS "structure_id",
        'carif-oref'                                    AS "source",
        TRUE                                            AS "cumulable",
        NULL                                            AS "formulaire_en_ligne",
        NULL                                            AS "recurrence",
        CAST(NULL AS DATE)                              AS "date_creation",
        CAST(NULL AS DATE)                              AS "date_suspension",
        COALESCE(
            coordonnees_organisme_formateur.telfixe[1],
            coordonnees_organisme_formateur.portable[1],
            coordonnees_lieu_de_formation.telfixe[1],
            coordonnees_lieu_de_formation.portable[1],
            coordonnees_formation.telfixe[1],
            coordonnees_formation.portable[1]
        )                                               AS "telephone",
        COALESCE(
            coordonnees_organisme_formateur.courriel,
            coordonnees_lieu_de_formation.courriel,
            coordonnees_formation.courriel
        )                                               AS "courriel",
        TRUE                                            AS "contact_public",
        NULL                                            AS "contact_nom_prenom",
        COALESCE(actions.date_maj, formations.date_maj) AS "date_maj",
        CASE actions.code_perimetre_recrutement
            WHEN '1' THEN 'commune'
            WHEN '2' THEN 'departement'
            WHEN '3' THEN 'region'
            WHEN '5' THEN 'pays'
            ELSE 'region'
        END                                             AS "zone_diffusion_type",
        CASE actions.code_perimetre_recrutement
            WHEN '1' THEN communes.code
            WHEN '2' THEN communes.code_departement
            WHEN '3' THEN communes.code_region
        END                                             AS "zone_diffusion_code",
        CASE actions.code_perimetre_recrutement
            WHEN '1' THEN communes.nom
            WHEN '2' THEN communes.nom_departement
            WHEN '3' THEN communes.nom_region
        END                                             AS "zone_diffusion_nom",
        ARRAY_REMOVE(
            ARRAY[
                CASE
                    WHEN coordonnees_organisme_formateur.courriel IS NOT NULL
                        THEN 'envoyer-un-mail'
                END,
                CASE
                    WHEN
                        coordonnees_organisme_formateur.telfixe[1] IS NOT NULL
                        OR coordonnees_organisme_formateur.portable[1] IS NOT NULL
                        THEN 'telephoner'
                END
            ],
            NULL
        )                                               AS "modes_orientation_accompagnateur",
        NULL                                            AS "modes_orientation_accompagnateur_autres",
        ARRAY_REMOVE(
            ARRAY[
                CASE
                    WHEN coordonnees_organisme_formateur.courriel IS NOT NULL
                        THEN 'envoyer-un-mail'
                END,
                CASE
                    WHEN
                        coordonnees_organisme_formateur.telfixe[1] IS NOT NULL
                        OR coordonnees_organisme_formateur.portable[1] IS NOT NULL
                        THEN 'telephoner'
                END
            ],
            NULL
        )                                               AS "modes_orientation_beneficiaire",
        NULL                                            AS "modes_orientation_beneficiaire_autres",
        ARRAY[actions.conditions_specifiques]           AS "pre_requis",
        CAST(NULL AS TEXT [])                           AS "justificatifs",
        formations.intitule_formation                   AS "nom",
        ARRAY[
            CASE
                WHEN actions.prix_total_ttc = 0 THEN 'gratuit'
                WHEN actions.prise_en_charge_frais_possible THEN 'gratuit-sous-conditions'
                ELSE 'payant'
            END
        ]                                               AS "frais",
        CASE
            WHEN '15043' = ANY(formations.domaine_formation__formacode)
                THEN ARRAY['illettrisme', 'apprendre-francais']
            ELSE ARRAY['apprendre-francais']
        END                                             AS "thematiques",
        NULL                                            AS "lien_source",
        ARRAY['formation']                              AS "types",
        CASE
            WHEN actions.modalites_enseignement = '0' THEN ARRAY['en-presentiel']
            WHEN actions.modalites_enseignement = '1' THEN ARRAY['a-distance']
            WHEN actions.modalites_enseignement = '2' THEN ARRAY['en-presentiel', 'a-distance']
        END                                             AS "modes_accueil",
        CASE
            WHEN LENGTH(formations.objectif_formation) <= 280 THEN formations.objectif_formation
            ELSE LEFT(formations.objectif_formation, 279) || '…'
        END                                             AS "presentation_resume",
        formations.objectif_formation                   AS "presentation_detail",
        COALESCE(
            actions.url_action[1],
            coordonnees_organisme_formateur.web[1],
            coordonnees_lieu_de_formation.web[1],
            formations.url_formation[1],
            coordonnees_formation.web[1]
        )                                               AS "page_web"
    FROM actions
    INNER JOIN formations
        ON actions.numero_formation = formations.numero
    LEFT JOIN organismes_formateurs
        ON actions.numero_organisme_formateur = organismes_formateurs.numero

    -- coordonnees organismes formateurs
    LEFT JOIN organismes_formateurs__contacts
        ON
            organismes_formateurs.numero = organismes_formateurs__contacts.numero_organisme_formateur
            AND actions.numero = organismes_formateurs__contacts.numero_action
    LEFT JOIN coordonnees AS coordonnees_organisme_formateur
        ON organismes_formateurs__contacts.hash_coordonnees = coordonnees_organisme_formateur.hash_
    LEFT JOIN adresses AS adresses_organisme_formateur
        ON coordonnees_organisme_formateur.hash_adresse = adresses_organisme_formateur.hash_

    -- coordonnees lieux de formation
    LEFT JOIN coordonnees AS coordonnees_lieu_de_formation
        ON actions.hash_coordonnees_lieu_de_formation_principal = coordonnees_lieu_de_formation.hash_
    LEFT JOIN adresses AS adresses_lieu_de_formation
        ON coordonnees_lieu_de_formation.hash_adresse = adresses_lieu_de_formation.hash_

    -- coordonnes formations
    LEFT JOIN formations__contacts
        ON formations.numero = formations__contacts.numero_formation
    LEFT JOIN coordonnees AS coordonnees_formation
        ON formations__contacts.hash_coordonnees = coordonnees_formation.hash_

    LEFT JOIN communes
        ON COALESCE(adresses_lieu_de_formation.code_insee_commune, adresses_organisme_formateur.code_insee_commune) = communes.code
    LEFT JOIN profils
        ON actions.numero = profils.numero_action
    ORDER BY
        actions.numero ASC,
        organismes_formateurs.numero ASC,
        organismes_formateurs__contacts.type_contact = '3' DESC, -- référent pédagogique
        organismes_formateurs__contacts.type_contact = '0' DESC, -- autre
        organismes_formateurs__contacts.type_contact = '4' DESC -- accueil
)

SELECT * FROM final
