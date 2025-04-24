WITH structures AS (
    SELECT * FROM {{ ref('stg_agefiph__structures') }}
)

SELECT
    'agefiph'                                           AS "source",
    structures.id                                       AS "id",
    structures.attributes__field_adresse__locality      AS "commune",
    structures.attributes__field_adresse__postal_code   AS "code_postal",
    NULL                                                AS "code_insee",
    structures.attributes__field_adresse__address_line1 AS "adresse",
    structures.attributes__field_adresse__address_line2 AS "complement_adresse",
    structures.attributes__field_geolocalisation__lng   AS "longitude",
    structures.attributes__field_geolocalisation__lat   AS "latitude"
FROM structures
