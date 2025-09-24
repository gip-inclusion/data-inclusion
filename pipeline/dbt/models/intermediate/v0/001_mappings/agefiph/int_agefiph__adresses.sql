WITH structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

code_insee AS (
    SELECT DISTINCT ON (1)
        structures.attributes__field_adresse__locality                  AS input_commune,
        structures.attributes__field_adresse__postal_code               AS input_code_postal,
        communes.nom,
        communes.codes_postaux,
        communes.nom <-> structures.attributes__field_adresse__locality AS distance,
        communes.code
    FROM structures
    LEFT JOIN communes
        ON (communes.nom || ' ' || LEFT(communes.code, 2)) % (REPLACE(LOWER(structures.attributes__field_adresse__locality), 'cedex', '') || ' ' || LEFT(structures.attributes__field_adresse__postal_code, 2))
    ORDER BY structures.attributes__field_adresse__locality, communes.nom <-> structures.attributes__field_adresse__locality ASC
)

SELECT
    'agefiph'                                           AS "source",
    structures.id                                       AS "id",
    structures.attributes__field_adresse__locality      AS "commune",
    structures.attributes__field_adresse__postal_code   AS "code_postal",
    code_insee.code                                     AS "code_insee",
    structures.attributes__field_adresse__address_line1 AS "adresse",
    structures.attributes__field_adresse__address_line2 AS "complement_adresse",
    structures.attributes__field_geolocalisation__lng   AS "longitude",
    structures.attributes__field_geolocalisation__lat   AS "latitude"
FROM structures
LEFT JOIN code_insee
    ON
        structures.attributes__field_adresse__locality = code_insee.input_commune
        AND structures.attributes__field_adresse__postal_code = code_insee.input_code_postal
        AND code_insee.distance < 0.6
