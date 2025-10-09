WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures_v1') }}
),

structures_v0_to_v1 AS (
    SELECT *
    FROM {{ ref('int__structures_v0_to_v1') }}
    WHERE source NOT IN (
        SELECT DISTINCT structures.source FROM structures
    )
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
    structures.courriel                                   AS "courriel",
    structures.horaires_accueil                           AS "horaires_accueil",
    structures.id                                         AS "id",
    structures.lien_source                                AS "lien_source",
    structures.description                                AS "description",
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
UNION ALL
SELECT
    structures_v0_to_v1.courriel,
    structures_v0_to_v1.horaires_accueil,
    structures_v0_to_v1.id,
    structures_v0_to_v1.lien_source,
    structures_v0_to_v1.description,
    structures_v0_to_v1.siret,
    structures_v0_to_v1.source,
    structures_v0_to_v1.date_maj,
    structures_v0_to_v1.reseaux_porteurs,
    structures_v0_to_v1.telephone,
    structures_v0_to_v1.site_web,
    structures_v0_to_v1.accessibilite_lieu,
    structures_v0_to_v1.nom,
    structures_v0_to_v1.longitude,
    structures_v0_to_v1.latitude,
    structures_v0_to_v1.complement_adresse,
    structures_v0_to_v1.commune,
    structures_v0_to_v1.adresse,
    structures_v0_to_v1.code_postal,
    structures_v0_to_v1.code_insee
FROM structures_v0_to_v1
