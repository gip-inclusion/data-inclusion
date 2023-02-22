WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('stg_odspep__res_partenariales') }}
),

adresses AS (
    SELECT * FROM {{ ref('stg_odspep__adresses') }}
),

contacts AS (
    SELECT * FROM {{ ref('stg_odspep__contacts') }}
),

aggregated_ressource AS (
    SELECT * FROM {{ ref('int_odspep__aggregated_ressource') }}
),


final AS (
    SELECT
        {{ dbt_utils.star(
                relation_alias='ressources_partenariales',
                from=ref('stg_odspep__res_partenariales'),
                except=['id_adr', 'id_ctc'])
        }},
        {{ dbt_utils.star(
                relation_alias='adresses',
                from=ref('stg_odspep__adresses'),
                except=['id', 'id_adr', 'id_res'])
        }},
        {{ dbt_utils.star(
                relation_alias='contacts',
                from=ref('stg_odspep__contacts'),
                except=['id', 'id_ctc', 'id_res'])
        }},
        aggregated_ressource."code",
        aggregated_ressource."type_code",
        aggregated_ressource."id" AS "unique_code"
    FROM ressources_partenariales
    LEFT JOIN adresses ON ressources_partenariales.id_adr = adresses.id
    LEFT JOIN contacts ON ressources_partenariales.id_ctc = contacts.id
    LEFT JOIN aggregated_ressource ON ressources_partenariales.id_res = aggregated_ressource.id_res
)

SELECT * FROM final
