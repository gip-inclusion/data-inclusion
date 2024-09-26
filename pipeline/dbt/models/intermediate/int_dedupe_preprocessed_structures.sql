WITH structures AS (
    SELECT
        _di_surrogate_id                                         AS id,
        source,
        TO_CHAR(date_maj, 'MM/DD/YYYY')                          AS date_maj,
        REGEXP_REPLACE(TRIM(LOWER(nom)), '[^ a-z0-9]', '')       AS nom,
        commune,
        adresse,
        ARRAY[COALESCE(latitude, 0.0), COALESCE(longitude, 0.0)] AS location,  -- noqa: references.keywords
        code_postal,
        code_insee,
        siret,
        SUBSTRING(siret, 0, 10)                                  AS siren,
        REPLACE(telephone, ' ', '')                              AS telephone,
        courriel
    FROM api__structures
    WHERE NOT antenne
),

final AS (
    SELECT * FROM structures
    WHERE source IN (
        'action-logement',
        'dora',
        'emplois-de-linclusion',
        'france-travail',
        'mes-aides',
        'soliguide'
    )
    AND code_insee IS NOT NULL
)

SELECT * FROM final
