{% for model, model_class in [
    ('int__structures', 'data_inclusion.schema.v0.Structure'),
    ('int__services', 'data_inclusion.schema.v0.Service'),
] %}

    SELECT
        resources._di_surrogate_id,
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
