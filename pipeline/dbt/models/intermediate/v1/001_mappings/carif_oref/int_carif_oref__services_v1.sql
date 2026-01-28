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

actions__sessions AS (
    SELECT * FROM {{ ref('stg_carif_oref__actions__sessions') }}
),

sessions_formatted AS (
    SELECT DISTINCT ON (numero_formation, numero_organisme_formateur, session_debut, session_fin)
        numero_formation,
        numero_organisme_formateur,
        session_debut,
        TO_CHAR(session_debut, 'DD/MM/YYYY') || ' - ' || TO_CHAR(session_fin, 'DD/MM/YYYY') AS session_periode
    FROM actions__sessions
    ORDER BY numero_formation, numero_organisme_formateur, session_debut, session_fin
),

sessions_aggregated AS (
    SELECT
        numero_formation,
        numero_organisme_formateur,
        ARRAY_AGG(session_periode ORDER BY session_debut) AS sessions
    FROM sessions_formatted
    GROUP BY numero_formation, numero_organisme_formateur
),

conditions_aggregated AS (
    SELECT
        numero_formation,
        numero_organisme_formateur,
        ARRAY_AGG(
            DISTINCT
            CASE WHEN conditions_specifiques = '-' THEN 'Aucune' ELSE conditions_specifiques END
        ) AS conditions_specifiques
    FROM actions
    WHERE conditions_specifiques IS NOT NULL
    GROUP BY numero_formation, numero_organisme_formateur
),

publics AS (
    SELECT
        actions.numero_formation           AS "numero_formation",
        actions.numero_organisme_formateur AS "numero_organisme_formateur",
        ARRAY_AGG(DISTINCT public.value)   AS "publics"
    FROM actions
    INNER JOIN actions__publics ON actions.numero = actions__publics.numero_action,
        LATERAL (
            SELECT publics_mapping.public
            FROM publics_mapping
            WHERE
                publics_mapping.formacode_v14 = actions__publics.code_public_vise
                AND publics_mapping.public IS NOT NULL
        ) AS public (value)    -- noqa: references.keywords
    GROUP BY actions.numero_formation, actions.numero_organisme_formateur
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

final AS (
    SELECT DISTINCT ON (actions.numero_formation, organismes_formateurs.numero)
        'carif-oref'                                                                AS "source",
        COALESCE(actions.date_maj, formations.date_maj)                             AS "date_maj",
        'carif-oref--' || organismes_formateurs.numero || '--' || formations.numero AS "id",
        COALESCE(
            coordonnees_lieu_de_formation.hash_adresse,
            coordonnees_organisme_formateur.hash_adresse
        )                                                                           AS "adresse_id",
        'carif-oref--' || organismes_formateurs.numero                              AS "structure_id",
        CASE
            WHEN LENGTH(formations.intitule_formation) > 150
                THEN LEFT(formations.intitule_formation, 149) || '…'
            ELSE formations.intitule_formation
        END                                                                         AS "nom",
        ARRAY_TO_STRING(
            ARRAY_REMOVE(
                ARRAY[
                    '### Objectif de la formation' || E'\n\n' || formations.objectif_formation,
                    '### Contenu de la formation' || E'\n\n' || formations.contenu_formation,
                    CASE
                        WHEN sessions_aggregated.sessions IS NOT NULL
                            THEN '### Sessions' || E'\n\n- ' || ARRAY_TO_STRING(sessions_aggregated.sessions, E'\n- ')
                    END
                ],
                NULL
            ),
            E'\n\n'
        )                                                                           AS "description",
        FORMAT(
            'https://www.intercariforef.org/formations/%s/formation-%s_%s.html',
            SLUGIFY('formations.intitule_formation'),
            formations.numero,
            SUBSTRING(actions.numero, 4)
        )                                                                           AS "lien_source",
        'formation'                                                                 AS "type",
        thematiques.thematiques                                                     AS "thematiques",
        CASE
            WHEN actions.prix_total_ttc = 0 THEN 'gratuit'
            WHEN actions.prix_total_ttc > 0 THEN 'payant'
        END                                                                         AS "frais",
        CASE
            WHEN actions.detail_conditions_prise_en_charge IS NOT NULL
                THEN FORMAT('Prise en charge: %s', actions.detail_conditions_prise_en_charge)
        END                                                                         AS "frais_autres",
        CASE
            WHEN publics.publics @> ARRAY['tous-publics']
                THEN ARRAY['tous-publics']
            ELSE publics.publics
        END                                                                         AS "publics",
        actions.info_public_vise                                                    AS "publics_precisions",
        NULLIF(
            ARRAY_TO_STRING(conditions_aggregated.conditions_specifiques, ', '),
            ''
        )                                                                           AS "conditions_acces",
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
        )                                                                           AS "modes_mobilisation",
        actions.modalites_recrutement                                               AS "mobilisation_precisions",
        ARRAY['professionnels']                                                     AS "mobilisable_par",
        COALESCE(
            coordonnees_organisme_formateur.telfixe[1],
            coordonnees_organisme_formateur.portable[1],
            coordonnees_lieu_de_formation.telfixe[1],
            coordonnees_lieu_de_formation.portable[1],
            coordonnees_formation.telfixe[1],
            coordonnees_formation.portable[1]
        )                                                                           AS "telephone",
        COALESCE(
            coordonnees_organisme_formateur.courriel,
            coordonnees_lieu_de_formation.courriel,
            coordonnees_formation.courriel
        )                                                                           AS "courriel",
        NULL                                                                        AS "contact_nom_prenom",
        CASE
            WHEN actions.modalites_enseignement = 0 THEN ARRAY['en-presentiel']
            WHEN actions.modalites_enseignement = 1 THEN ARRAY['a-distance']
            WHEN actions.modalites_enseignement = 2 THEN ARRAY['en-presentiel', 'a-distance']
        END                                                                         AS "modes_accueil",
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
        END                                                                         AS "zone_eligibilite",
        CAST(actions.duree_hebdo AS FLOAT)                                          AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INTEGER)                                                       AS "nombre_semaines",
        NULL                                                                        AS "horaires_accueil",
        JSONB_BUILD_OBJECT(
            'formation', formations.raw,
            'action', actions.raw
        )                                                                           AS "_extra"
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
    LEFT JOIN publics
        ON
            actions.numero_formation = publics.numero_formation
            AND actions.numero_organisme_formateur = publics.numero_organisme_formateur
    LEFT JOIN thematiques
        ON formations.numero = thematiques.numero_formation
    LEFT JOIN sessions_aggregated
        ON
            actions.numero_formation = sessions_aggregated.numero_formation
            AND actions.numero_organisme_formateur = sessions_aggregated.numero_organisme_formateur
    LEFT JOIN conditions_aggregated
        ON
            actions.numero_formation = conditions_aggregated.numero_formation
            AND actions.numero_organisme_formateur = conditions_aggregated.numero_organisme_formateur
    ORDER BY
        actions.numero_formation ASC,
        organismes_formateurs.numero ASC,
        organismes_formateurs__contacts.type_contact = 3 DESC, -- référent pédagogique
        organismes_formateurs__contacts.type_contact = 0 DESC, -- autre
        organismes_formateurs__contacts.type_contact = 4 DESC -- accueil
)

SELECT * FROM final
