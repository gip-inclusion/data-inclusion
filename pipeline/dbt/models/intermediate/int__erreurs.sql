WITH services AS (
    SELECT * FROM {{ ref('int__union_services') }}
),

structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

services_errors AS (
    SELECT
        services._di_surrogate_id AS "service_id",
        NULL                      AS "structure_id",
        errors.type               AS "type",
        errors.loc                AS "loc",
        errors.msg                AS "msg",
        errors.input              AS "input"
    FROM
        services,
        LATERAL (SELECT * FROM VALIDATE('service', TO_JSONB(services))) AS errors
),

structures_errors AS (
    SELECT
        NULL                        AS "service_id",
        structures._di_surrogate_id AS "structure_id",
        errors.type                 AS "type",
        errors.loc                  AS "loc",
        errors.msg                  AS "msg",
        errors.input                AS "input"
    FROM
        structures,
        LATERAL (SELECT * FROM VALIDATE('structure', TO_JSONB(structures))) AS errors
),

final AS (
    SELECT * FROM services_errors
    UNION ALL
    SELECT * FROM structures_errors
)

SELECT * FROM final
