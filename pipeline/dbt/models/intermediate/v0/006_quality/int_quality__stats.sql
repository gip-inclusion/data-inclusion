/* Included : all the providers that do actually end up in the marts */

-- depends_on: {{ ref('stg_action_logement__services') }}
-- depends_on: {{ ref('stg_action_logement__structures') }}
-- depends_on: {{ ref('int_action_logement__services') }}
-- depends_on: {{ ref('int_action_logement__structures') }}
-- depends_on: {{ ref('stg_agefiph__services') }}
-- depends_on: {{ ref('stg_agefiph__structures') }}
-- depends_on: {{ ref('int_agefiph__services') }}
-- depends_on: {{ ref('int_agefiph__structures') }}
-- depends_on: {{ ref('stg_carif_oref__formations') }}
-- depends_on: {{ ref('int_carif_oref__services') }}
-- depends_on: {{ ref('stg_cd35__organisations') }}
-- depends_on: {{ ref('int_cd35__structures') }}
-- depends_on: {{ ref('stg_dora__services') }}
-- depends_on: {{ ref('stg_dora__structures') }}
-- depends_on: {{ ref('int_dora__services') }}
-- depends_on: {{ ref('int_dora__structures') }}
-- depends_on: {{ ref('stg_emplois_de_linclusion__siaes') }}
-- depends_on: {{ ref('stg_emplois_de_linclusion__organisations') }}
-- depends_on: {{ ref('int_emplois_de_linclusion__structures') }}
-- depends_on: {{ ref('stg_france_travail__agences') }}
-- depends_on: {{ ref('stg_france_travail__services') }}
-- depends_on: {{ ref('int_france_travail__services') }}
-- depends_on: {{ ref('int_france_travail__structures') }}
-- depends_on: {{ ref('stg_fredo__structures') }}
-- depends_on: {{ ref('int_fredo__structures') }}
-- depends_on: {{ ref('stg_ma_boussole_aidants__structures__services') }}
-- depends_on: {{ ref('stg_ma_boussole_aidants__structures') }}
-- depends_on: {{ ref('int_ma_boussole_aidants__services') }}
-- depends_on: {{ ref('int_ma_boussole_aidants__structures') }}
-- depends_on: {{ ref('stg_mission_locale__offres') }}
-- depends_on: {{ ref('stg_mission_locale__structures') }}
-- depends_on: {{ ref('int_mission_locale__services') }}
-- depends_on: {{ ref('int_mission_locale__structures') }}
-- depends_on: {{ ref('stg_mediation_numerique__services') }}
-- depends_on: {{ ref('stg_mediation_numerique__structures') }}
-- depends_on: {{ ref('int_mediation_numerique__services') }}
-- depends_on: {{ ref('int_mediation_numerique__structures') }}
-- depends_on: {{ ref('stg_mes_aides__permis_velo') }}
-- depends_on: {{ ref('stg_mes_aides__garages') }}
-- depends_on: {{ ref('int_mes_aides__services') }}
-- depends_on: {{ ref('int_mes_aides__structures') }}
-- depends_on: {{ ref('stg_monenfant__creches') }}
-- depends_on: {{ ref('int_monenfant__structures') }}
-- depends_on: {{ ref('stg_reseau_alpha__formations') }}
-- depends_on: {{ ref('stg_reseau_alpha__structures') }}
-- depends_on: {{ ref('int_reseau_alpha__services') }}
-- depends_on: {{ ref('int_reseau_alpha__structures') }}
-- depends_on: {{ ref('stg_soliguide__lieux') }}
-- depends_on: {{ ref('int_soliguide__structures') }}
-- depends_on: {{ ref('marts__services') }}
-- depends_on: {{ ref('marts__structures') }}

WITH

{% for source_node in graph.sources.values() if source_node.source_meta.is_provider %}
    {% if source_node.meta.kind %}
        {% set source_name = source_node.source_name %}
        {% set source_slug = source_name | replace("_", "-") %}
        {% set stream_name = source_node.name %}
        {% set staging_name = source_node.meta.staging_name or stream_name %}
        {% set stream_kind = source_node.meta.kind ~ "s" %}
        {{ source_name }}__{{ stream_name }}__tmp_marts AS (
            SELECT * FROM {{ ref('marts__' ~ stream_kind) }}
            WHERE source = '{{ source_slug }}'
        ),
        {{ source_name }}__{{ stream_name }}__tmp_api AS (
            SELECT * FROM public.api__{{ stream_kind }}
            WHERE source = '{{ source_slug }}'
        ),
        {{ source_name }}__{{ stream_name }}__tmp_api_contacts AS (
            SELECT * FROM {{ source_name }}__{{ stream_name }}__tmp_api
            WHERE courriel IS NOT NULL AND telephone IS NOT NULL
        ),
        {{ source_name }}__{{ stream_name }}__tmp_api_adresse AS (
            SELECT * FROM {{ source_name }}__{{ stream_name }}__tmp_api
            WHERE NOT (adresse IS NOT NULL AND code_insee IS NOT NULL)
        ),
        {{ source_name }}__{{ stream_name }}__stats AS (
            /*
            Manually handle the layout as the sqlfluff DBT templater can't handle spacing
            when the column expression contains macros
            */
            -- noqa: disable=layout.spacing
            SELECT
                CAST('{{ run_started_at.strftime("%Y-%m-%d") }}' AS DATE)                    AS date_day,
                '{{ source_name }}'                                                          AS source,
                '{{ stream_name }}'                                                          AS stream,  -- noqa: references.keywords
                (SELECT COUNT(*) FROM {{ source(source_name, stream_name) }})                AS count_raw,
                (SELECT COUNT(*) FROM {{ ref('stg_' ~ source_name ~ '__' ~ staging_name) }}) AS count_stg,
                (SELECT COUNT(*) FROM {{ ref('int_' ~ source_name ~ '__' ~ stream_kind) }})  AS count_int,
                (SELECT COUNT(*) FROM {{ source_name }}__{{ stream_name }}__tmp_marts)       AS count_marts,
                (SELECT COUNT(*) FROM {{ source_name }}__{{ stream_name }}__tmp_api)         AS count_api,
                (SELECT COUNT(*) FROM {{ source_name }}__{{ stream_name }}__tmp_api_contacts)AS count_contacts,
                (SELECT COUNT(*) FROM {{ source_name }}__{{ stream_name }}__tmp_api_adresse) AS count_addresses
        -- noqa: enable=layout.spacing
        ),
    {% endif %}
{% endfor %}

final AS (
    {% for source_node in graph.sources.values() if source_node.source_meta.is_provider %}
        {% if source_node.meta.kind %}
            {% if not loop.first %}
                UNION ALL
            {% endif %}
            {% set source_name = source_node.source_name %}
            {% set stream_name = source_node.name %}
            SELECT * FROM {{ source_name }}__{{ stream_name }}__stats
        {% endif %}
    {% endfor %}
)

SELECT * FROM final
