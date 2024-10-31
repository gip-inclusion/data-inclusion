WITH adresses AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_action_logement__adresses'),
                ref('int_agefiph__adresses'),
                ref('int_dora__adresses'),
                ref('int_cd35__adresses'),
                ref('int_emplois_de_linclusion__adresses'),
                ref('int_finess__adresses'),
                ref('int_france_travail__adresses'),
                ref('int_fredo__adresses'),
                ref('int_mediation_numerique__adresses'),
                ref('int_mes_aides__adresses'),
                ref('int_monenfant__adresses'),
                ref('int_odspep__adresses'),
                ref('int_reseau_alpha__adresses'),
                ref('int_soliguide__adresses'),
            ],
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        *,
        source || '-' || id AS "_di_surrogate_id"
    FROM adresses
)

SELECT * FROM final
