WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

regions AS (
    SELECT DISTINCT ON (raw.garage_id, raw.nom)
        raw.garage_id,
        raw.nom,
        regions.code
    FROM (
        SELECT
            source.data ->> 'id'                       AS "garage_id",
            NULLIF(TRIM(source.data ->> 'region'), '') AS "nom"
        FROM source
    ) AS "raw"
    LEFT JOIN {{ ref('stg_decoupage_administratif__regions') }} AS regions ON raw.nom % regions.nom
    ORDER BY
        raw.garage_id,
        raw.region__nom,
        SIMILARITY(raw.region__nom, regions.nom) DESC
),

villes AS (
    SELECT DISTINCT ON (raw.garage_id, raw.nom, raw.code_postal)
        raw.garage_id,
        raw.nom,
        raw.code_postal,
        COALESCE(communes.code, communes_associees.chef_lieu) AS "code_insee"
    FROM (
        SELECT
            data ->> 'id'                      AS "garage_id",
            NULLIF(TRIM(data ->> 'ville'), '') AS "nom",
            NULLIF(TRIM(data ->> '???'), '')   AS "code_postal"  -- TODO
        FROM source
    ) AS "raw"
    LEFT JOIN {{ ref('stg_decoupage_administratif__communes') }} AS communes
        ON
            raw.nom % communes.nom
            AND ARRAY[raw.code_postal] && communes.codes_postaux
    LEFT JOIN {{ ref('stg_decoupage_administratif__communes_associees_deleguees') }} AS communes_associees
        ON
            raw.nom = communes_associees.nom
    ORDER BY
        raw.garage_id,
        raw.nom,
        raw.code_postal,
        SIMILARITY(raw.nom, communes.nom) DESC NULLS FIRST
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'adresse'), '')                                               AS "adresse",
        SUBSTRING(NULLIF(TRIM(source.data -> 'contact' ->> 'tel'), '') FROM '\+?\d[\d\.\-\s]*\d') AS "contact__telephone",
        NULLIF(TRIM(source.data -> 'contact' ->> 'email'), '')                                    AS "contact__email",
        CAST(source.data -> 'metadata' ->> 'dateCreation' AS DATE)                                AS "metadata__date_creation",
        CAST(source.data -> 'metadata' ->> 'dateMiseAJour' AS DATE)                               AS "metadata__date_mise_a_jour",
        CAST(source.data -> 'metadata' ->> 'dateModification' AS DATE)                            AS "metadata__date_modification",
        NULLIF(TRIM(source.data ->> 'criteresEligibilite'), '')                                   AS "criteres_eligibilite",
        CAST(source.data ->> 'enLigne' AS BOOLEAN)                                                AS "en_ligne",
        source.data ->> 'id'                                                                      AS "id",
        NULLIF(TRIM(source.data ->> 'departement'), '')                                           AS "departement",
        NULLIF(TRIM(source.data ->> 'nom'), '')                                                   AS "nom",
        NULLIF(TRIM(source.data ->> 'partenaire'), '')                                            AS "partenaire",
        NULLIF(TRIM(source.data ->> 'region'), '')                                                AS "region",
        NULLIF(TRIM(source.data ->> 'type'), '')                                                  AS "type",
        NULLIF(TRIM(source.data ->> 'url'), '')                                                   AS "url",
        NULLIF(TRIM(source.data ->> 'urlMesAides'), '')                                           AS "url_mes_aides",
        NULLIF(TRIM(source.data ->> 'ville'), '')                                                 AS "ville",
        CAST(source.data ->> 'longitude' AS FLOAT)                                                AS "longitude",
        CAST(source.data ->> 'latitude' AS FLOAT)                                                 AS "latitude",
        NULLIF(TRIM(source.data ->> 'typeStructure'), '')                                         AS "type_structure",
        regions.code                                                                              AS "region__code",
        regions.nom                                                                               AS "region__nom",
        villes.nom                                                                                AS "ville__nom",
        villes.code_postal                                                                        AS "ville__code_postal",
        villes.code_insee                                                                         AS "ville__code_insee"
    FROM source
    LEFT JOIN regions ON source.data ->> 'id' = regions.garage_id
    LEFT JOIN villes ON source.data ->> 'id' = villes.garage_id
)

SELECT * FROM final
