WITH adresses_garage AS (
    SELECT * FROM {{ ref('int_mes_aides__adresses_garage') }}
),

adresses_permis_velo AS (
    SELECT * FROM {{ ref('int_mes_aides__adresses_permis_velo') }}
),

final AS (
    SELECT
        adresses_garage.id,
        adresses_garage.commune,
        adresses_garage.code_insee,
        adresses_garage.longitude,
        adresses_garage.latitude,
        adresses_garage.source,
        adresses_garage.code_postal,
        adresses_garage.adresse,
        adresses_garage.complement_adresse
    FROM adresses_garage
    UNION ALL
    SELECT
        adresses_permis_velo.id,
        adresses_permis_velo.commune,
        adresses_permis_velo.code_insee,
        adresses_permis_velo.longitude,
        adresses_permis_velo.latitude,
        adresses_permis_velo.source,
        adresses_permis_velo.code_postal,
        adresses_permis_velo.adresse,
        adresses_permis_velo.complement_adresse
    FROM adresses_permis_velo
)

SELECT * FROM final
