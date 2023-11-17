MODEL (
  name dora.int_dora__contacts,
  kind FULL,
  grain courriel,
  audits [
    not_empty_string(column=courriel),
    not_null(columns=[courriel]),
  ],
);

WITH

structure_contacts AS (
    SELECT
        courriel
    FROM public_staging.stg_dora__structures
    WHERE courriel != ''
),

service_contacts AS (
    SELECT
        courriel
    FROM public_staging.stg_dora__services
    WHERE courriel != ''
),

final AS (
    SELECT * FROM structure_contacts
    UNION
    SELECT * FROM service_contacts
)

SELECT DISTINCT courriel FROM final ORDER BY courriel
