WITH structures AS (
    SELECT * FROM {{ ref('int__structures') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels') }}
),

doublons AS (
    SELECT * FROM {{ ref('int__doublons_structures') }}
),

erreurs AS (
    SELECT * FROM {{ ref('int__erreurs_validation') }}
    WHERE resource_type = 'structure'
),

sirets AS (
    SELECT * FROM {{ ref('int__sirets') }}
),

erreurs_v0 AS (
    SELECT DISTINCT _di_surrogate_id
    FROM erreurs
    WHERE schema_version = 'v0' AND resource_type = 'structure'
),

erreurs_v1 AS (
    SELECT DISTINCT _di_surrogate_id
    FROM erreurs
    WHERE schema_version = 'v1' AND resource_type = 'structure'
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__structures'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                ]
            )
        }},
        doublons.cluster_id                                                  AS "_cluster_id",
        courriels_personnels.courriel IS NOT NULL                            AS "_has_pii",
        structures.source NOT IN ('soliguide', 'agefiph')                    AS "_in_opendata",
        erreurs_v0._di_surrogate_id IS NULL                                  AS "_is_valid_v0",
        erreurs_v1._di_surrogate_id IS NULL                                  AS "_is_valid_v1",
        sirets.statut IS NOT NULL AND sirets.statut = 'fermé-définitivement' AS "_is_closed",
        -- the following fields will be removed in v1
        -- for now they are kept for compatibility, but without any value
        CAST(NULL AS BOOLEAN)                                                AS "antenne"
    FROM structures
    LEFT JOIN doublons ON structures._di_surrogate_id = doublons.structure_id
    LEFT JOIN courriels_personnels ON structures.courriel = courriels_personnels.courriel
    LEFT JOIN sirets ON structures._di_surrogate_id = sirets._di_surrogate_id
    LEFT JOIN erreurs_v0 ON structures._di_surrogate_id = erreurs_v0._di_surrogate_id
    LEFT JOIN erreurs_v1 ON structures._di_surrogate_id = erreurs_v1._di_surrogate_id
)

SELECT * FROM final
