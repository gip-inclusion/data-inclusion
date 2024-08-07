WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses') }}
),

valid_adresses AS (
    SELECT adresses.*
    FROM adresses
    LEFT JOIN
        LATERAL
        -- noqa: disable=references.qualification
        LIST_ADRESSE_ERRORS(
            adresse,
            code_insee,
            code_postal,
            commune,
            complement_adresse,
            id,
            latitude,
            longitude,
            source
        ) AS errors ON TRUE
        -- noqa: enable=references.qualification
    WHERE errors.field IS NULL
),

geocoded_results AS (
    SELECT * FROM {{ ref('int_extra__geocoded_results') }}
),

geocoded_addresses AS (

    SELECT
        valid_adresses._di_surrogate_id,
        valid_adresses.id,
        valid_adresses.source,
        valid_adresses.complement_adresse,
        geocoded_results.result_name     AS adresse,
        geocoded_results.longitude,
        geocoded_results.latitude,
        geocoded_results.result_city     AS commune,
        geocoded_results.result_postcode AS code_postal,
        geocoded_results.result_citycode AS code_insee,
        geocoded_results.result_score
    FROM valid_adresses
    LEFT JOIN geocoded_results ON valid_adresses._di_surrogate_id = geocoded_results._di_surrogate_id
    WHERE geocoded_results.result_postcode != 'municipality' AND geocoded_results.result_score >= 0.8

),

geocoded_cities AS (

    SELECT
        valid_adresses._di_surrogate_id,
        valid_adresses.id,
        valid_adresses.source,
        valid_adresses.complement_adresse,
        valid_adresses.adresse,
        geocoded_results.longitude,
        geocoded_results.latitude,
        geocoded_results.result_city     AS commune,
        geocoded_results.result_postcode AS code_postal,
        geocoded_results.result_citycode AS code_insee,
        geocoded_results.result_score
    FROM valid_adresses
    LEFT JOIN geocoded_results ON valid_adresses._di_surrogate_id = geocoded_results._di_surrogate_id
    WHERE geocoded_results.result_postcode = 'municipality' AND geocoded_results.result_score >= 0.8

),

non_geocoded_addresses AS (
    SELECT
        valid_adresses._di_surrogate_id,
        valid_adresses.id,
        valid_adresses.source,
        valid_adresses.complement_adresse,
        valid_adresses.adresse,
        CAST(valid_adresses.longitude AS FLOAT)                               AS longitude,
        CAST(valid_adresses.latitude AS FLOAT)                                AS latitude,
        valid_adresses.commune,
        valid_adresses.code_postal,
        /*
        If there was a supplied INSEE code, keep it. If not, use the geocoded one,
        knowing that it might be of poor quality. We need it within the services to
        establish the diffusion zones.
        */
        COALESCE(valid_adresses.code_insee, geocoded_results.result_citycode) AS code_insee,
        geocoded_results.result_score
    FROM valid_adresses
    LEFT JOIN geocoded_results ON valid_adresses._di_surrogate_id = geocoded_results._di_surrogate_id
    WHERE geocoded_results.result_score IS NULL OR geocoded_results.result_score < 0.8
),

final AS (
    SELECT * FROM geocoded_addresses
    UNION ALL
    SELECT * FROM geocoded_cities
    UNION ALL
    SELECT * FROM non_geocoded_addresses
)

SELECT * FROM final
