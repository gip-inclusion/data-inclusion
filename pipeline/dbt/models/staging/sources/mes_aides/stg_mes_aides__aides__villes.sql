WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

raw AS (
    SELECT
        data ->> 'ID'                          AS "aide_id",
        SUBSTRING(villes FROM '^(.*) \(.*\)$') AS "nom",
        SUBSTRING(villes FROM '\((.*)\)$')     AS "code_postal"
    FROM source,
        UNNEST(STRING_TO_ARRAY(data ->> 'Villes', ',')) AS villes
),

final AS (
    SELECT DISTINCT ON (raw.aide_id, raw.nom)
        raw.aide_id,
        raw.nom,
        raw.code_postal,
        communes.code
    FROM raw
    LEFT JOIN {{ ref('stg_decoupage_administratif__communes') }} AS communes
        ON
            raw.nom % communes.nom
            AND ARRAY[raw.code_postal] && communes.codes_postaux
    ORDER BY
        raw.aide_id,
        raw.nom,
        WORD_SIMILARITY(raw.nom, communes.nom) DESC
)

SELECT
    aide_id,
    nom,
    code_postal,
    CASE
        WHEN code IS NOT NULL THEN code
        -- FIXME: hardcoded exceptions for bad postal codes provided by Mes Aides
        WHEN code IS NULL AND nom ~* 'paris' THEN '75056'
        WHEN code IS NULL AND nom ~* 'lyon' THEN '69123'
        WHEN code IS NULL AND nom ~* 'saint-victoret' THEN '13102'
    END AS code
FROM final
