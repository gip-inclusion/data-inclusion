{# usage:

    models:
    - name: marts__structures
        data_tests:
        - model_validate:
            model_class: data_inclusion.schema.v0.Structure
#}

{% test model_validate(model, model_class) %}
    SELECT
        resources.id,
        resources.source,
        errors.*
    FROM
        {{ model }} AS resources,
        processings.model_validate('{{ model_class }}', TO_JSONB(resources)) AS errors
{% endtest %}
