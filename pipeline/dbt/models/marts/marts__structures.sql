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
        doublons.cluster_id                               AS "cluster_id",
        courriels_personnels.courriel IS NOT NULL         AS "_has_pii",
        structures.source NOT IN ('soliguide', 'agefiph') AS "_in_opendata",
        NOT EXISTS (
            SELECT
            FROM erreurs
            WHERE
                erreurs._di_surrogate_id = structures._di_surrogate_id
                AND erreurs.schema_version = 'v0'
        )                                                 AS "_is_valid_v0",
        NOT EXISTS (
            SELECT
            FROM erreurs
            WHERE
                erreurs._di_surrogate_id = structures._di_surrogate_id
                AND erreurs.schema_version = 'v1'
        )                                                 AS "_is_valid_v1"
    FROM structures
    LEFT JOIN doublons ON structures._di_surrogate_id = doublons.structure_id
    LEFT JOIN courriels_personnels ON structures.courriel = courriels_personnels.courriel
)

SELECT * FROM final
