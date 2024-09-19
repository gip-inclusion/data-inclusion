WITH contacts_union AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_dora__contacts'),
                ref('int_mediation_numerique__contacts'),
                ref('int_mes_aides__contacts'),
            ],
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        source || '-' || id AS "_di_surrogate_id",
        id                  AS "id",
        source              AS "source",
        courriel            AS "courriel",
        telephone           AS "telephone",
        contact_nom_prenom  AS "contact_nom_prenom",
        contact_uid         AS "contact_uid"
    FROM contacts_union
)

SELECT * FROM final
