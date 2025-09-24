WITH services AS (
    {{
        dbt_utils.union_relations(
            relations=[
            ],
            include=[
                'source',
                'id',
                'courriel',
                'telephone',
                'contact_nom_prenom',
            ],
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        source             AS "source",
        id                 AS "id",
        courriel           AS "courriel",
        telephone          AS "telephone",
        contact_nom_prenom AS "contact_nom_prenom"
    FROM services
    WHERE
        courriel IS NOT NULL
        OR telephone IS NOT NULL
        OR contact_nom_prenom IS NOT NULL
)

SELECT * FROM final
