WITH structures AS (
    SELECT * FROM {{ ref('int__structures_v1') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels_v1') }}
),

doublons AS (
    SELECT * FROM {{ ref('int__doublons_structures_v1') }}
),

erreurs AS (
    SELECT DISTINCT id
    FROM {{ ref('int__erreurs_validation_v1') }}
    WHERE resource_type = 'structure'
),

sirets AS (
    SELECT * FROM {{ ref('int__sirets_v1') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__structures_v1'),
                except=[
                    'adresse_id',
                ]
            )
        }},
        doublons.cluster_id                                                  AS "_cluster_id",
        courriels_personnels.courriel IS NOT NULL                            AS "_has_pii",
        structures.source NOT IN ('soliguide', 'agefiph')                    AS "_in_opendata",
        erreurs.id IS NULL                                                   AS "_is_valid",
        sirets.statut IS NOT NULL AND sirets.statut = 'fermé-définitivement' AS "_is_closed"
    FROM structures
    LEFT JOIN doublons ON structures.id = doublons.structure_id
    LEFT JOIN courriels_personnels ON structures.courriel = courriels_personnels.courriel
    LEFT JOIN sirets ON structures.id = sirets.id
    LEFT JOIN erreurs ON structures.id = erreurs.id
)

SELECT * FROM final
