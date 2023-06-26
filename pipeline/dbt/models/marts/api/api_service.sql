{{
    config(
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (_di_surrogate_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT services_structure_surrogate_id_fk FOREIGN KEY (_di_structure_surrogate_id) REFERENCES {{ ref('api_structure') }} (_di_surrogate_id)"
        ]
    )
}}

WITH services AS (
    SELECT * FROM {{ ref('int__validated_services') }}
),

final AS (
    SELECT *
    FROM services
)

SELECT * FROM final
