WITH source AS (
    {{ stg_source_header('finess', 'etablissements') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'categetab'), '')             AS "categorie",
        NULLIF(TRIM(data ->> 'commune'), '')               AS "commune",
        NULLIF(TRIM(data ->> 'compldistrib'), '')          AS "complement_distribution",
        NULLIF(TRIM(data ->> 'complrs'), '')               AS "complement_raison_sociale",
        NULLIF(TRIM(data ->> 'compvoie'), '')              AS "complement_voie",
        CAST(NULLIF(TRIM(data ->> 'datemaj'), '') AS DATE) AS "date_maj_structure",
        NULLIF(TRIM(data ->> 'departement'), '')           AS "departement",
        NULLIF(TRIM(data ->> 'lieuditbp'), '')             AS "lieu_dit_bp",
        NULLIF(TRIM(data ->> 'ligneacheminement'), '')     AS "ligne_acheminement",
        NULLIF(TRIM(data ->> 'nofinesset'), '')            AS "numero_finess_et",
        NULLIF(TRIM(data ->> 'numvoie'), '')               AS "numero_voie",
        NULLIF(TRIM(data ->> 'rs'), '')                    AS "raison_sociale",
        NULLIF(TRIM(data ->> 'rslongue'), '')              AS "raison_sociale_longue",
        NULLIF(TRIM(data ->> 'siret'), '')                 AS "siret",
        NULLIF(TRIM(data ->> 'telephone'), '')             AS "telephone",
        NULLIF(TRIM(data ->> 'typeet'), '')                AS "type_etablissement",
        NULLIF(TRIM(data ->> 'typvoie'), '')               AS "type_voie",
        NULLIF(TRIM(data ->> 'voie'), '')                  AS "libelle_voie"
    FROM source
)

SELECT * FROM final
WHERE categorie IN ('156', '246')
