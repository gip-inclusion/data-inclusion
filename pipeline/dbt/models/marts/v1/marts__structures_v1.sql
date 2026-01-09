WITH structures AS (
    SELECT * FROM {{ ref('int__structures_v1') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels_v1') }}
    UNION
    SELECT * FROM {{ ref('int__courriels_personnels') }}
),

doublons AS (
    SELECT * FROM {{ ref('int__doublons_structures_v1') }}
),

erreurs AS (
    SELECT DISTINCT id
    FROM {{ ref('int__erreurs_validation_v1') }}
    WHERE resource_type = 'structure'
),

sirets_v0 AS (
    SELECT
        source || '--' || id AS "id",
        siret,
        date_fermeture,
        siret_successeur,
        statut
    FROM {{ ref('int__sirets') }}
),

sirets AS (
    SELECT * FROM sirets_v0
    UNION
    SELECT * FROM {{ ref('int__sirets_v1') }} AS sirets_v1
    WHERE sirets_v1.id NOT IN (
        SELECT sirets_v0.id FROM sirets_v0
    )
),

geocodages AS (
    SELECT
        adresse_id,
        score
    FROM {{ ref('int__geocodages_v1') }}
    UNION ALL
    SELECT
        adresse_id,
        score
    FROM {{ ref('int__geocodages') }}
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
        geocodages.score IS NOT NULL AND geocodages.score >= 0.75            AS "_is_address_valid",
        courriels_personnels.courriel IS NOT NULL                            AS "_has_pii",
        structures.source NOT IN ('soliguide', 'agefiph')                    AS "_in_opendata",
        erreurs.id IS NULL                                                   AS "_is_valid",
        sirets.statut IS NOT NULL AND sirets.statut = 'fermé-définitivement' AS "_is_closed"
    FROM structures
    LEFT JOIN doublons ON structures.id = doublons.structure_id
    LEFT JOIN courriels_personnels ON structures.courriel = courriels_personnels.courriel
    LEFT JOIN sirets ON structures.id = sirets.id
    LEFT JOIN erreurs ON structures.id = erreurs.id
    LEFT JOIN geocodages ON structures.adresse_id = geocodages.adresse_id
)

SELECT * FROM final
