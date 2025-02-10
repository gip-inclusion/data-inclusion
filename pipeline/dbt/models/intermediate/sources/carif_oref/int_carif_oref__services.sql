WITH actions AS (
    SELECT * FROM {{ ref('stg_carif_oref__actions') }}
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

coordonnees AS (
    SELECT * FROM {{ ref('stg_carif_oref__coordonnees') }}
),

adresses AS (
    SELECT * FROM {{ ref('stg_carif_oref__adresses') }}
),

communes AS (
    SELECT
        communes.*,
        departements.nom AS "nom_departement",
        regions.nom      AS "nom_region"
    FROM
        {{ ref('stg_decoupage_administratif__communes') }} AS communes
    INNER JOIN {{ ref('stg_decoupage_administratif__departements') }} AS departements
        ON communes.code_departement = departements.code
    INNER JOIN {{ ref('stg_decoupage_administratif__regions') }} AS regions
        ON communes.code_region = regions.code
),

final AS (
    SELECT DISTINCT ON (actions.numero, actions.numero_organisme_formateur)
        actions.numero                                            AS "id",
        coordonnees.hash_adresse                                  AS "adresse_id",
        NULL                                                      AS "prise_rdv",
        actions.detail_conditions_prise_en_charge                 AS "frais_autres",
        ARRAY['familles-enfants']                                 AS "profils",
        actions.info_public_vise                                  AS "profils_precisions",
        actions.numero_organisme_formateur                        AS "structure_id",
        'carif-oref'                                              AS "source",
        TRUE                                                      AS "cumulable",
        NULL                                                      AS "formulaire_en_ligne",
        NULL                                                      AS "recurrence",
        CAST(NULL AS DATE)                                        AS "date_creation",
        CAST(NULL AS DATE)                                        AS "date_suspension",
        COALESCE(coordonnees.telfixe[0], coordonnees.portable[0]) AS "telephone",
        coordonnees.courriel                                      AS "courriel",
        TRUE                                                      AS "contact_public",
        NULL                                                      AS "contact_nom_prenom",
        CURRENT_DATE                                              AS "date_maj",
        CASE actions.code_perimetre_recrutement
            WHEN '1' THEN 'commune'
            WHEN '2' THEN 'departement'
            WHEN '3' THEN 'region'
            WHEN '5' THEN 'pays'
        END                                                       AS "zone_diffusion_type",
        CASE actions.code_perimetre_recrutement
            WHEN '1' THEN communes.code
            WHEN '2' THEN communes.code_departement
            WHEN '3' THEN communes.code_region
        END                                                       AS "zone_diffusion_code",
        CASE actions.code_perimetre_recrutement
            WHEN '1' THEN communes.nom
            WHEN '2' THEN communes.nom_departement
            WHEN '3' THEN communes.nom_region
        END                                                       AS "zone_diffusion_nom",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN coordonnees.courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN coordonnees.telfixe[0] IS NOT NULL OR coordonnees.portable[0] IS NOT NULL THEN 'telephoner' END
            ],
            NULL
        )                                                         AS "modes_orientation_accompagnateur",
        NULL                                                      AS "modes_orientation_accompagnateur_autres",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN coordonnees.courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN coordonnees.telfixe[0] IS NOT NULL OR coordonnees.portable[0] IS NOT NULL THEN 'telephoner' END
            ],
            NULL
        )                                                         AS "modes_orientation_beneficiaire",
        NULL                                                      AS "modes_orientation_beneficiaire_autres",
        ARRAY[actions.conditions_specifiques]                     AS "pre_requis",
        CAST(NULL AS TEXT [])                                     AS "justificatifs",
        formations.intitule_formation                             AS "nom",
        ARRAY[
            CASE
                WHEN actions.prix_total_ttc = 0 THEN 'gratuit'
                WHEN actions.prise_en_charge_frais_possible THEN 'gratuit-sous-conditions'
                ELSE 'payant'
            END
        ]                                                         AS "frais",
        ARRAY['famille--garde-denfants']                          AS "thematiques",
        NULL                                                      AS "lien_source",
        ARRAY['formation']                                        AS "types",
        CASE
            WHEN actions.modalites_enseignement = '0' THEN ARRAY['en-presentiel']
            WHEN actions.modalites_enseignement = '1' THEN ARRAY['a-distance']
            WHEN actions.modalites_enseignement = '2' THEN ARRAY['en-presentiel', 'a-distance']
        END                                                       AS "modes_accueil",
        CASE
            WHEN LENGTH(formations.objectif_formation) <= 280 THEN formations.objectif_formation
            ELSE LEFT(formations.objectif_formation, 279) || '…'
        END                                                       AS "presentation_resume",
        formations.objectif_formation                             AS "presentation_detail",
        COALESCE(actions.url_action[0], coordonnees.web[0])       AS "page_web"
    FROM
        actions
    INNER JOIN formations
        ON actions.numero_formation = formations.numero
    LEFT JOIN organismes_formateurs
        ON actions.numero_organisme_formateur = organismes_formateurs.numero
    LEFT JOIN organismes_formateurs__contacts
        ON
            organismes_formateurs.numero = organismes_formateurs__contacts.numero_organisme_formateur
            AND actions.numero = organismes_formateurs__contacts.numero_action
    LEFT JOIN coordonnees
        ON organismes_formateurs__contacts.hash_coordonnees = coordonnees.hash_
    LEFT JOIN adresses
        ON coordonnees.hash_adresse = adresses.hash_
    LEFT JOIN communes
        ON adresses.code_insee_commune = communes.code
)

SELECT * FROM final
