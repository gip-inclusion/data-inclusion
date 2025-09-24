{% for model, model_class in [
    ('int__structures_v1', 'data_inclusion.schema.v1.Structure'),
    ('int__services_v1', 'data_inclusion.schema.v1.Service')
] %}

    SELECT
        resources.source,
        resources.id,
        LOWER(SPLIT_PART('{{ model_class }}', '.', -1)) AS resource_type,
        SPLIT_PART('{{ model_class }}', '.', -2)        AS schema_version,
        errors.model_class,
        errors.type,
        errors.loc,
        errors.msg,
        errors.input
    FROM
        {{ ref(model) }} AS resources,
        processings.model_validate('{{ model_class }}', TO_JSONB(resources)) AS errors  -- noqa: RF02

    {% if not loop.last %}UNION ALL{% endif %}

{% endfor %}
