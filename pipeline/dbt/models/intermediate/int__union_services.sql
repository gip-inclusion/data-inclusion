WITH services AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_action_logement__services'),
                ref('int_agefiph__services'),
                ref('int_data_inclusion__services'),
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

unnested_brevo_contacts AS (
    SELECT DISTINCT
        UNNEST(contact_uids) AS id,
        date_di_rgpd_opposition,
        est_interdit
    FROM {{ ref('int_brevo__contacts') }}
),

service_contacts AS (
    SELECT
        SPLIT_PART(id, ':', 1) AS source,
        SPLIT_PART(id, ':', 3) AS id,
        date_di_rgpd_opposition,
        est_interdit
    FROM unnested_brevo_contacts
    WHERE SPLIT_PART(id, ':', 2) = 'services'
),

contacts_opposes AS (
    SELECT
        id,
        source
    FROM service_contacts
    WHERE date_di_rgpd_opposition IS NOT NULL
),

contacts_hardbounced AS (
    SELECT
        id,
        source
    FROM service_contacts
    WHERE est_interdit
),

services_with_contacts AS (
    SELECT
        -- NOTE(vperron) : The int__dora__services below is a ruse, the FROM clause
        -- still refers to the `services` CTE. But we need a dbt_utils.star() and we need
        -- to reference "something" that has the same columns as int__union_services,
        -- but not itself as it would create a cycle.
        -- Also, directly referencing all the columns by name makes the tests super painful:
        -- suddenly all the columns have to be emulated in the fixtures.
        -- But we still need to remove some columns :'(
        {{ dbt_utils.star(from=ref('int_dora__services'), relation_alias='services', except=["contact_nom_prenom", "courriel", "telephone"]) }},
        CASE
            WHEN contacts_opposes.id IS NULL
                THEN services.contact_nom_prenom
        END                                 AS contact_nom_prenom,
        CASE
            WHEN contacts_opposes.id IS NULL OR contacts_hardbounced.id IS NULL
                THEN services.courriel
        END                                 AS courriel,
        CASE
            WHEN contacts_opposes.id IS NULL
                THEN services.telephone
        END                                 AS telephone,
        contacts_opposes.id IS NOT NULL     AS contact_opposes,
        contacts_hardbounced.id IS NOT NULL AS contact_hardbounced
    FROM services
    -- Enable USING instead of ON for the JOIN condition to avoid duplicate columns
    -- noqa: disable=structure.using
    LEFT JOIN contacts_opposes USING (id, source)
    LEFT JOIN contacts_hardbounced USING (id, source)
    -- noqa: enable=structure.using
),

final AS (
    SELECT
        services.*,
        services.source || '-' || services.id           AS _di_surrogate_id,
        services.source || '-' || services.structure_id AS _di_structure_surrogate_id,
        services.source || '-' || services.adresse_id   AS _di_adresse_surrogate_id
    FROM services_with_contacts AS services
)

SELECT * FROM final
