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
),

attempted_addresses AS (
    SELECT
        structures.id                                                                                              AS structure_id,
        structures.attributes__field_adresse__address_line1 || structures.attributes__field_adresse__address_line2 AS full_address,
        -- Attempt 1 : extract address number and street from the concatenation of both address lines
        REGEXP_REPLACE(
            SUBSTRING(
                structures.attributes__field_adresse__address_line1
                || structures.attributes__field_adresse__address_line2
                FROM '\d{1,3}[,/]? .*'
            ),
            '( - |  | BP | CS ).*',
            ''
        )                                                                                                          AS "adresse_1",
        -- Attempt 2 : extract most probable address from each address line
        REGEXP_REPLACE(CASE
            WHEN structures.attributes__field_adresse__address_line1 ~ '^[0-9]'
                THEN structures.attributes__field_adresse__address_line1
            ELSE structures.attributes__field_adresse__address_line2
        END, '( - |  | BP | CS ).*', '')                                                                           AS "adresse_2"
    FROM structures
),

resolved_addresses AS (
    SELECT
        attempted_addresses.structure_id,
        attempted_addresses.full_address,
        -- Slightly favour attempt 2 if it starts with a number as it's goint to be shorter
        -- and not contain concatenation artifacts
        CASE
            WHEN attempted_addresses.adresse_2 ~ '^[0-9]'
                THEN attempted_addresses.adresse_2
            ELSE attempted_addresses.adresse_1
        END AS "adresse"
    FROM attempted_addresses
)

SELECT
    'agefiph'                                                                       AS "source",
    'agefiph--' || structures.id                                                    AS "id",
    structures.attributes__field_adresse__locality                                  AS "commune",
    structures.attributes__field_adresse__postal_code                               AS "code_postal",
    code_insee.code                                                                 AS "code_insee",
    resolved_addresses.adresse                                                      AS "adresse",
    REGEXP_REPLACE(resolved_addresses.full_address, resolved_addresses.adresse, '') AS "complement_adresse",
    structures.attributes__field_geolocalisation__lng                               AS "longitude",
    structures.attributes__field_geolocalisation__lat                               AS "latitude"
FROM structures
LEFT JOIN resolved_addresses
    ON structures.id = resolved_addresses.structure_id
LEFT JOIN code_insee
    ON
        structures.attributes__field_adresse__locality = code_insee.input_commune
        AND structures.attributes__field_adresse__postal_code = code_insee.input_code_postal
        AND code_insee.distance < 0.6
