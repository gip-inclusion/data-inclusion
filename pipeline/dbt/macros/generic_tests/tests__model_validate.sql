{# usage:

    models:
    - name: marts__structures
        data_tests:
        - model_validate:
            model_class: data_inclusion.schema.v0.Structure
#}

{% test model_validate(model, model_class, where) %}
    WITH filtered_model AS (
        SELECT *
        FROM {{ model }}
        {% if where %}
        WHERE {{ where }}
        {% endif %}
    )

    SELECT
        resources.id,
        resources.source,
        errors.*
    FROM
        filtered_model AS resources,
        processings.model_validate('{{ model_class }}', TO_JSONB(resources)) AS errors
{% endtest %}
