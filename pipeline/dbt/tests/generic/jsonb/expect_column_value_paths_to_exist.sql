{% test expect_column_value_paths_to_exist(model, column_name, path_list) %}

WITH validation AS (
    SELECT {{ column_name }} AS test_column
    FROM {{ model }}
),

validation_errors AS (
    {% for path in path_list %}
    SELECT test_column
    FROM validation
    WHERE NOT test_column @? '{{ path }}'
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT * FROM validation_errors

{% endtest %}
