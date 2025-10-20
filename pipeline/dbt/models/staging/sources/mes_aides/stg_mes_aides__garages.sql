WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

regions AS (
    SELECT DISTINCT ON (raw.garage_id, raw.region__nom)
        raw.garage_id,
        raw.region__nom,
        regions.code
    FROM (
        SELECT
            data ->> 'ID'     AS "garage_id",
            data ->> 'Région' AS "region__nom"
        FROM source
    ) AS "raw"
    LEFT JOIN {{ ref('stg_decoupage_administratif__regions') }} AS regions ON raw.region__nom % regions.nom
    ORDER BY
        raw.garage_id,
        raw.region__nom,
        WORD_SIMILARITY(raw.region__nom, regions.nom) DESC
),

villes AS (
    SELECT DISTINCT ON (raw.garage_id, raw.ville__nom, raw.ville__code_postal)
        raw.garage_id,
        raw.ville__nom,
        raw.ville__code_postal,
        COALESCE(communes.code, communes_associees.chef_lieu) AS "ville__code_insee"
    FROM (
        SELECT
            data ->> 'ID'                                    AS "garage_id",
            SUBSTRING(data ->> 'Ville' FROM '^(.*?) \(.*\)') AS "ville__nom",
            SUBSTRING(data ->> 'Ville' FROM '\((.*?)\)')     AS "ville__code_postal"
        FROM source
    ) AS "raw"
    LEFT JOIN {{ ref('stg_decoupage_administratif__communes') }} AS communes
        ON
            raw.ville__nom % communes.nom
            AND ARRAY[raw.ville__code_postal] && communes.codes_postaux
    LEFT JOIN {{ ref('stg_decoupage_administratif__communes_associees_deleguees') }} AS communes_associees
        ON
            raw.ville__nom = communes_associees.nom
    ORDER BY
        raw.garage_id,
        raw.ville__nom,
        raw.ville__code_postal,
        WORD_SIMILARITY(raw.ville__nom, communes.nom) DESC NULLS FIRST
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'Adresse'), '')                                        AS "adresse",
        CAST(source.data ->> 'Créé le' AS DATE)                                            AS "cree_le",
        source.data ->> 'Créé par'                                                         AS "cree_par",
        NULLIF(TRIM(source.data ->> 'Critères d''éligibilité'), '')                        AS "criteres_eligibilite",
        SPLIT_PART(source.data ->> 'Département', ' - ', 1)                                AS "departement__code",
        SPLIT_PART(source.data ->> 'Département', ' - ', 2)                                AS "departement__nom",
        NULLIF(TRIM(source.data ->> 'Email'), '')                                          AS "email",
        COALESCE(source.data ->> 'En Ligne' = 'checked', FALSE)                            AS "en_ligne",
        source.data ->> 'ID'                                                               AS "id",
        CAST(source.data ->> 'Mis à jour le' AS DATE)                                      AS "mis_a_jour_le",
        CAST(source.data ->> 'Modifié le' AS DATE)                                         AS "modifie_le",
        NULLIF(TRIM(source.data ->> 'Nom'), '')                                            AS "nom",
        NULLIF(TRIM(source.data ->> 'Partenaire'), '')                                     AS "partenaire",
        regions.region__nom                                                                AS "region__nom",
        regions.code                                                                       AS "region__code",
        SUBSTRING(NULLIF(TRIM(source.data ->> 'Téléphone'), '') FROM '\+?\d[\d\.\-\s]*\d') AS "telephone",
        NULLIF(TRIM(source.data ->> 'Type'), '')                                           AS "type",
        NULLIF(TRIM(source.data ->> 'Url'), '')                                            AS "url",
        villes.ville__nom                                                                  AS "ville__nom",
        villes.ville__code_postal                                                          AS "ville__code_postal",
        villes.ville__code_insee                                                           AS "ville__code_insee"
    FROM source
    LEFT JOIN regions ON source.data ->> 'ID' = regions.garage_id
    LEFT JOIN villes ON source.data ->> 'ID' = villes.garage_id
)

SELECT * FROM final
