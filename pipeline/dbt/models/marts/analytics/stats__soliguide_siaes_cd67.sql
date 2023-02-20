WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

raw_soliguide_services AS (
    SELECT * FROM {{ ref('stg_soliguide__services') }}
),

structures_in_cd67 AS (
    SELECT *
    FROM structures
    WHERE LEFT(code_insee, 2) = '67' OR LEFT(_di_geocodage_code_insee, 2) = '67'
),

gip_structures_in_cd67 AS (
    SELECT *
    FROM structures_in_cd67
    WHERE source = 'emplois-de-linclusion' OR source = 'dora'
),

soliguide_siaes_in_cd67 AS (
    SELECT *
    FROM structures_in_cd67
    WHERE
        source = 'soliguide'
        -- use the raw source data: structures with a service in categorie '205' are SIAEs
        AND id IN (SELECT lieu_id FROM raw_soliguide_services WHERE categorie = '205')
),

final AS (
    SELECT
        CASE
            WHEN gip_structures_in_cd67.antenne THEN 'antenne'
            WHEN NOT gip_structures_in_cd67.antenne AND soliguide_siaes_in_cd67._di_annotated_siret = gip_structures_in_cd67.siret THEN 'exact match'
            WHEN NOT gip_structures_in_cd67.antenne AND LEFT(soliguide_siaes_in_cd67._di_annotated_siret, 9) = LEFT(gip_structures_in_cd67.siret, 9) THEN 'partial match'
            ELSE 'not referenced by soliguide'
        END AS "status",
        {{
            dbt_utils.star(
                from=ref('int__enhanced_structures'),
                relation_alias='soliguide_siaes_in_cd67',
                suffix='_soliguide'
            )
        }},
        {{
            dbt_utils.star(
                from=ref('int__enhanced_structures'),
                relation_alias='gip_structures_in_cd67',
                suffix='_gip'
            )
        }}
    FROM soliguide_siaes_in_cd67
    FULL OUTER JOIN gip_structures_in_cd67 ON LEFT(soliguide_siaes_in_cd67._di_annotated_siret, 9) = LEFT(gip_structures_in_cd67.siret, 9)
    ORDER BY
        soliguide_siaes_in_cd67.id,
        gip_structures_in_cd67.source,
        status,
        gip_structures_in_cd67.siret,
        gip_structures_in_cd67.id
)

SELECT * FROM final
