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
    SELECT DISTINCT _di_surrogate_id
    FROM {{ ref('int__erreurs_validation') }}
    WHERE resource_type = 'structure'
),

sirets AS (
    SELECT * FROM {{ ref('int__sirets') }}
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
                    "antenne"
                ]
            )
        }},
        doublons.cluster_id                                                  AS "_cluster_id",
        courriels_personnels.courriel IS NOT NULL                            AS "_has_pii",
        structures.source NOT IN ('soliguide', 'agefiph')                    AS "_in_opendata",
        erreurs._di_surrogate_id IS NULL                                     AS "_is_valid",
        sirets.statut IS NOT NULL AND sirets.statut = 'fermé-définitivement' AS "_is_closed",
        -- the following is kept for retrocompatibility
        CAST(NULL AS BOOLEAN)                                                AS "antenne"
    FROM structures
    LEFT JOIN doublons ON structures._di_surrogate_id = doublons.structure_id
    LEFT JOIN courriels_personnels ON structures.courriel = courriels_personnels.courriel
    LEFT JOIN sirets ON structures._di_surrogate_id = sirets._di_surrogate_id
    LEFT JOIN erreurs ON structures._di_surrogate_id = erreurs._di_surrogate_id
)

SELECT * FROM final
