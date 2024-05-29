WITH services AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_action_logement__services'),
                ref('int_agefiph__services'),
                ref('int_cd72__services'),
                ref('int_data_inclusion__services'),
                ref('int_dora__services'),
                ref('int_france_travail__services'),
                ref('int_mediation_numerique__services'),
                ref('int_mes_aides__services'),
                ref('int_monenfant__services'),
                ref('int_odspep__services'),
                ref('int_reseau_alpha__services'),
                ref('int_soliguide__services'),
            ],
            column_override={
                "contact_public": "BOOLEAN",
                "cumulable": "BOOLEAN",
                "date_creation": "DATE",
                "date_maj": "DATE",
                "date_suspension": "DATE",
                "frais": "TEXT[]",
                "justificatifs": "TEXT[]",
                "modes_accueil": "TEXT[]",
                "modes_orientation_accompagnateur": "TEXT[]",
                "modes_orientation_beneficiaire": "TEXT[]",
                "pre_requis": "TEXT[]",
                "profils": "TEXT[]",
                "thematiques": "TEXT[]",
                "types": "TEXT[]",
            },
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        *,
        source || '-' || id           AS "_di_surrogate_id",
        source || '-' || structure_id AS "_di_structure_surrogate_id",
        source || '-' || adresse_id   AS "_di_adresse_surrogate_id"
    FROM services
)

SELECT * FROM final
