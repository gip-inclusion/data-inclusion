WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels') }}
),

doublons AS (
    SELECT * FROM {{ ref('int__doublons_structures') }}
),

erreurs AS (
    SELECT DISTINCT _di_surrogate_id
    FROM {{ ref('int__erreurs_validation') }}
    WHERE resource_type = 'structure' AND schema_version = 'v1'
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__union_structures__enhanced'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                    'thematiques',
                ]
            )
        }},
        doublons.cluster_id                               AS "cluster_id",
        courriels_personnels.courriel IS NOT NULL         AS "_has_pii",
        structures.source NOT IN ('soliguide', 'agefiph') AS "_in_opendata"
    FROM structures
    LEFT JOIN doublons ON structures._di_surrogate_id = doublons.structure_id
    LEFT JOIN courriels_personnels ON structures.courriel = courriels_personnels.courriel
    LEFT JOIN erreurs ON structures._di_surrogate_id = erreurs._di_surrogate_id
    WHERE erreurs._di_surrogate_id IS NULL
)

SELECT * FROM final
