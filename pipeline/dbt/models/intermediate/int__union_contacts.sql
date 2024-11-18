WITH services AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_action_logement__services'),
                ref('int_agefiph__services'),
                ref('int_dora__services'),
                ref('int_france_travail__services'),
                ref('int_fredo__services'),
                ref('int_mediation_numerique__services'),
                ref('int_mes_aides__services'),
                ref('int_monenfant__services'),
                ref('int_odspep__services'),
                ref('int_reseau_alpha__services'),
                ref('int_soliguide__services'),
            ],
            include=[
                'id',
                'source',
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
        source || '-' || id AS "_di_surrogate_id",
        id                  AS "id",
        source              AS "source",
        courriel            AS "courriel",
        telephone           AS "telephone",
        contact_nom_prenom  AS "contact_nom_prenom"
    FROM services
    WHERE
        courriel IS NOT NULL
        OR telephone IS NOT NULL
        OR contact_nom_prenom IS NOT NULL
)

SELECT * FROM final
