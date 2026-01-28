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

publics_mapping AS (
    SELECT * FROM {{ ref('_map_carif_oref__publics') }}
),

thematiques_mapping AS (
    SELECT * FROM {{ ref('_map_carif_oref__thematiques') }}
),

departements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__departements') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

publics AS (
    SELECT
        actions__publics.numero_action   AS "numero_action",
        ARRAY_AGG(DISTINCT public.value) AS "publics"
    FROM actions__publics,
        LATERAL (
            SELECT publics_mapping.public
            FROM publics_mapping
            WHERE
                publics_mapping.formacode_v14 = actions__publics.code_public_vise
                AND publics_mapping.public IS NOT NULL
        ) AS public (value)    -- noqa: references.keywords
    GROUP BY actions__publics.numero_action
),

thematiques AS (
    SELECT
        formations.numero AS "numero_formation",
        ARRAY(
            SELECT DISTINCT thematiques_mapping.thematique
            FROM thematiques_mapping
            WHERE thematiques_mapping.formacode_v14 = ANY(formations.domaine_formation__formacode)
        )                 AS "thematiques"
    FROM formations
),

codes_public_vise AS (
    SELECT
        numero_action,
        ARRAY_AGG(DISTINCT code_public_vise ORDER BY code_public_vise) AS "codes_public_vise"
    FROM actions__publics
    GROUP BY numero_action
),

-- Formations can have many closely related actions with only a few differences, if not identical.
-- Here we remove actions that are closely related to another action of the same formation.
-- If all columns listed in the following DISTINCT ON match, then we consider the two actions as
-- closely related, and we keep only one of them. More columns means more actions.
deduplicated_actions AS (
    SELECT DISTINCT ON (
        actions.numero_formation,
        actions.prix_total_ttc,
        actions.detail_conditions_prise_en_charge,
        actions.info_public_vise,
        actions.modalites_recrutement,
        actions.modalites_enseignement,
        actions.code_perimetre_recrutement,
        actions.duree_hebdo,
        actions.numero_organisme_formateur,
        codes_public_vise.codes_public_vise,
        adresses_lieu_de_formation.ville
    ) actions.*
    FROM actions
    LEFT JOIN codes_public_vise
        ON actions.numero = codes_public_vise.numero_action
    LEFT JOIN coordonnees AS coordonnees_lieu_de_formation
        ON actions.hash_coordonnees_lieu_de_formation_principal = coordonnees_lieu_de_formation.hash_
    LEFT JOIN adresses AS adresses_lieu_de_formation
        ON coordonnees_lieu_de_formation.hash_adresse = adresses_lieu_de_formation.hash_
    ORDER BY
        actions.numero_formation ASC,
        actions.prix_total_ttc ASC,
        actions.detail_conditions_prise_en_charge ASC,
        actions.info_public_vise ASC,
        actions.modalites_recrutement ASC,
        actions.modalites_enseignement ASC,
        actions.code_perimetre_recrutement ASC,
        actions.duree_hebdo ASC,
        actions.numero_organisme_formateur ASC,
        codes_public_vise.codes_public_vise ASC,
        adresses_lieu_de_formation.ville ASC,

        -- the following criteria are used to select one action among closely related actions
        actions.date_maj DESC NULLS LAST, -- most recent action
        actions.numero ASC -- stability if same date_maj
),

final AS (
    SELECT DISTINCT ON (actions.numero)
        'carif-oref'                                                 AS "source",
        COALESCE(actions.date_maj, formations.date_maj)              AS "date_maj",
        'carif-oref--' || actions.numero                             AS "id",
        'carif-oref--' || coordonnees_lieu_de_formation.hash_adresse AS "adresse_id",
        'carif-oref--' || organismes_formateurs.numero               AS "structure_id",
        CASE
            WHEN LENGTH(formations.intitule_formation) > 150
                THEN LEFT(formations.intitule_formation, 149) || '…'
            ELSE formations.intitule_formation
        END                                                          AS "nom",
        ARRAY_TO_STRING(
            ARRAY_REMOVE(
                ARRAY[
                    '### Objectif de la formation' || E'\n\n' || formations.objectif_formation,
                    '### Contenu de la formation' || E'\n\n' || formations.contenu_formation
                ],
                NULL
            ),
            E'\n\n'
        )                                                            AS "description",
        COALESCE(formations.url_formation[1], FORMAT(
            'https://www.intercariforef.org/formations/%s/formation-%s_%s.html',
            SLUGIFY(formations.intitule_formation),
            formations.numero,
            SUBSTRING(actions.numero, 4)
        ))                                                           AS "lien_source",
        'formation'                                                  AS "type",
        thematiques.thematiques                                      AS "thematiques",
        CASE
            WHEN actions.prix_total_ttc = 0 THEN 'gratuit'
            WHEN actions.prix_total_ttc > 0 THEN 'payant'
        END                                                          AS "frais",
        CASE
            WHEN actions.detail_conditions_prise_en_charge IS NOT NULL
                THEN FORMAT('Prise en charge: %s', actions.detail_conditions_prise_en_charge)
        END                                                          AS "frais_autres",
        CASE
            WHEN publics.publics @> ARRAY['tous-publics']
                THEN ARRAY['tous-publics']
            ELSE publics.publics
        END                                                          AS "publics",
        actions.info_public_vise                                     AS "publics_precisions",
        NULLIF(actions.conditions_specifiques, '')                   AS "conditions_acces",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY[
                    CASE
                        WHEN
                            COALESCE(
                                coordonnees_organisme_formateur.telfixe[1],
                                coordonnees_organisme_formateur.portable[1],
                                coordonnees_lieu_de_formation.telfixe[1],
                                coordonnees_lieu_de_formation.portable[1],
                                coordonnees_formation.telfixe[1],
                                coordonnees_formation.portable[1]
                            ) IS NOT NULL
                            THEN 'telephoner'
                    END,
                    CASE
                        WHEN
                            COALESCE(
                                coordonnees_organisme_formateur.courriel,
                                coordonnees_lieu_de_formation.courriel,
                                coordonnees_formation.courriel
                            ) IS NOT NULL
                            THEN 'envoyer-un-courriel'
                    END
                ],
                NULL
            ),
            '{}'
        )                                                            AS "modes_mobilisation",
        actions.modalites_recrutement                                AS "mobilisation_precisions",
        ARRAY['professionnels']                                      AS "mobilisable_par",
        COALESCE(
            coordonnees_organisme_formateur.telfixe[1],
            coordonnees_organisme_formateur.portable[1],
            coordonnees_lieu_de_formation.telfixe[1],
            coordonnees_lieu_de_formation.portable[1],
            coordonnees_formation.telfixe[1],
            coordonnees_formation.portable[1]
        )                                                            AS "telephone",
        COALESCE(
            coordonnees_organisme_formateur.courriel,
            coordonnees_lieu_de_formation.courriel,
            coordonnees_formation.courriel
        )                                                            AS "courriel",
        NULL                                                         AS "contact_nom_prenom",
        CASE
            WHEN actions.modalites_enseignement = 0 THEN ARRAY['en-presentiel']
            WHEN actions.modalites_enseignement = 1 THEN ARRAY['a-distance']
            WHEN actions.modalites_enseignement = 2 THEN ARRAY['en-presentiel', 'a-distance']
        END                                                          AS "modes_accueil",
        CASE actions.code_perimetre_recrutement
            WHEN '1' THEN ARRAY[communes.code]
            WHEN '2' THEN ARRAY[communes.code_departement]
            ELSE NULLIF(
                ARRAY(
                    SELECT departements.code
                    FROM departements
                    WHERE departements.code_region = communes.code_region
                ),
                '{}'
            )
        END                                                          AS "zone_eligibilite",
        CAST(actions.duree_hebdo AS FLOAT)                           AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INTEGER)                                        AS "nombre_semaines",
        NULL                                                         AS "horaires_accueil",
        JSONB_BUILD_OBJECT(
            'formation', formations.raw,
            'action', actions.raw
        )                                                            AS "_extra"
    FROM deduplicated_actions AS actions
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
    LEFT JOIN publics
        ON actions.numero = publics.numero_action
    LEFT JOIN thematiques
        ON formations.numero = thematiques.numero_formation
    ORDER BY
        actions.numero ASC,
        organismes_formateurs__contacts.type_contact = 3 DESC, -- référent pédagogique
        organismes_formateurs__contacts.type_contact = 0 DESC, -- autre
        organismes_formateurs__contacts.type_contact = 4 DESC -- accueil
)

SELECT * FROM final
