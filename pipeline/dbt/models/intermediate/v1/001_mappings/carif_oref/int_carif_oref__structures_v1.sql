WITH organismes_formateurs AS (
    SELECT * FROM {{ ref('stg_carif_oref__organismes_formateurs') }}
),

organismes_formateurs__contacts AS (
    SELECT * FROM {{ ref('stg_carif_oref__organismes_formateurs__contacts') }}
),

coordonnees AS (
    SELECT * FROM {{ ref('stg_carif_oref__coordonnees') }}
),

actions AS (
    SELECT * FROM {{ ref('stg_carif_oref__actions') }}
),

formations AS (
    SELECT * FROM {{ ref('stg_carif_oref__formations') }}
),

date_maj AS (
    SELECT
        actions.numero_organisme_formateur,
        actions.hash_coordonnees_lieu_de_formation_principal,
        MAX(COALESCE(actions.date_maj, formations.date_maj)) AS date_maj
    FROM actions
    LEFT JOIN formations
        ON actions.numero_formation = formations.numero
    GROUP BY actions.numero_organisme_formateur, actions.hash_coordonnees_lieu_de_formation_principal
),

final AS (
    -- sort and distinct by numero to keep the first action with the same numero
    -- because there are several contacts available for the same organismes_formateurs.numero
    SELECT DISTINCT ON (organismes_formateurs.numero)
        'carif-oref'                                                                     AS "source",
        'carif-oref--' || organismes_formateurs.numero                                   AS "id",
        coordonnees.hash_                                                                AS "hash_id",
        'carif-oref--' || COALESCE(
            coordonnees.hash_adresse,
            coordonnees_lieu_de_formation_principal.hash_adresse
        )                                                                                AS "adresse_id",
        organismes_formateurs.raison_sociale_formateur                                   AS "nom",
        date_maj.date_maj                                                                AS "date_maj",
        FORMAT(
            'https://www.intercariforef.org/formations/%s/organisme-%s.html',
            SLUGIFY('organismes_formateurs.raison_sociale_formateur'),
            organismes_formateurs.numero
        )                                                                                AS "lien_source",
        organismes_formateurs.siret_formateur__siret                                     AS "siret",
        COALESCE(
            coordonnees.telfixe[1],
            coordonnees.portable[1],
            coordonnees_lieu_de_formation_principal.telfixe[1],
            coordonnees_lieu_de_formation_principal.portable[1]
        )                                                                                AS "telephone",
        COALESCE(coordonnees.courriel, coordonnees_lieu_de_formation_principal.courriel) AS "courriel",
        COALESCE(coordonnees.web[1], coordonnees_lieu_de_formation_principal.web[1])     AS "site_web",
        NULL                                                                             AS "description",
        NULL                                                                             AS "horaires_accueil",
        NULL                                                                             AS "accessibilite_lieu",
        CAST(NULL AS TEXT [])                                                            AS "reseaux_porteurs"
    FROM organismes_formateurs
    LEFT JOIN organismes_formateurs__contacts
        ON organismes_formateurs.numero = organismes_formateurs__contacts.numero_organisme_formateur
    LEFT JOIN coordonnees
        ON organismes_formateurs__contacts.hash_coordonnees = coordonnees.hash_
    LEFT JOIN date_maj
        ON organismes_formateurs.numero = date_maj.numero_organisme_formateur
    LEFT JOIN coordonnees AS coordonnees_lieu_de_formation_principal
        ON date_maj.hash_coordonnees_lieu_de_formation_principal = coordonnees_lieu_de_formation_principal.hash_
    ORDER BY organismes_formateurs.numero
)

SELECT * FROM final
