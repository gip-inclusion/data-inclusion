WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures_v1') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__adresses_v1') }}
),

sirets AS (
    SELECT * FROM {{ ref('int__sirets_v1') }}
),

urls AS (
    SELECT
        input_url,
        "url"
    FROM {{ ref('int__urls_v1') }}
    WHERE status_code > 0
)

SELECT
    structures.adresse_id                                 AS "adresse_id",
    structures.courriel                                   AS "courriel",
    structures.horaires_accueil                           AS "horaires_accueil",
    structures.id                                         AS "id",
    structures.lien_source                                AS "lien_source",
    CASE
        WHEN LENGTH(structures.description) > 10000
            THEN LEFT(structures.description, 9999) || '…'
        ELSE structures.description
    END                                                   AS "description",
    CASE
        WHEN sirets.statut = 'valide' THEN sirets.siret
        WHEN sirets.statut = 'successeur-ouvert' THEN sirets.siret_successeur
        WHEN sirets.statut = 'fermé-définitivement' THEN NULL
    END                                                   AS "siret",
    structures.source                                     AS "source",
    structures.date_maj                                   AS "date_maj",
    structures.reseaux_porteurs                           AS "reseaux_porteurs",
    processings.format_phone_number(structures.telephone) AS "telephone",
    urls_site_web.url                                     AS "site_web",
    urls_accessibilite_lieu.url                           AS "accessibilite_lieu",
    CASE
        WHEN LENGTH(structures.nom) <= 150 THEN structures.nom
        ELSE LEFT(structures.nom, 149) || '…'
    END                                                   AS "nom",
    adresses.longitude                                    AS "longitude",
    adresses.latitude                                     AS "latitude",
    adresses.complement_adresse                           AS "complement_adresse",
    adresses.commune                                      AS "commune",
    adresses.adresse                                      AS "adresse",
    adresses.code_postal                                  AS "code_postal",
    adresses.code_insee                                   AS "code_insee"
FROM structures
LEFT JOIN urls AS urls_site_web ON structures.site_web = urls_site_web.input_url
LEFT JOIN urls AS urls_accessibilite_lieu ON structures.accessibilite_lieu = urls_accessibilite_lieu.input_url
LEFT JOIN adresses ON structures.adresse_id = adresses.id
LEFT JOIN sirets ON structures.id = sirets.id
