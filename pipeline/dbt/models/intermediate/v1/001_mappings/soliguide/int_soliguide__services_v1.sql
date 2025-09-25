WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

services AS (
    SELECT * FROM {{ ref('stg_soliguide__services') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_soliguide__categories') }}
),

phones AS (
    SELECT * FROM {{ ref('stg_soliguide__phones') }}
),

publics AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__administrative') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__gender') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__familiale') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_soliguide__lieux__publics__other') }}
),

final AS (
    SELECT
    FROM services
)

SELECT * FROM final
