{% macro create_udf__adresse_checks() %}
DROP FUNCTION IF EXISTS LIST_ADRESSE_ERRORS;
CREATE OR REPLACE FUNCTION LIST_ADRESSE_ERRORS(
        adresse TEXT,
        code_insee TEXT,
        code_postal TEXT,
        commune TEXT,
        complement_adresse TEXT,
        id TEXT,
        latitude FLOAT,
        longitude FLOAT,
        source TEXT
    )
RETURNS TABLE (field TEXT, value TEXT) AS $$
DECLARE
BEGIN

    {% set checks = [
            ('id', 'id IS NOT NULL'),
            ('code_insee', 'code_insee IS NULL OR CHECK_CODE_INSEE(code_insee)'),
            ('code_postal', 'code_postal IS NULL OR CHECK_CODE_POSTAL(code_postal)'),
        ]
    %}

    {% for field, expression in checks %}
    IF NOT ({{ expression }}) THEN
        field := '{{ field }}';
        value := CAST({{ field }} AS TEXT);
        RETURN NEXT;
    END IF;
    {% endfor %}

    RETURN;
END;
$$ LANGUAGE plpgsql;
{% endmacro %}


{% test check_adresse(model, include) %}
    {{ check_adresse_errors(model, include) }}
{% endtest %}


{% macro check_adresse_errors(model, include=[]) %}
WITH final AS (
    SELECT
        {% for extra_column in include %}
        {{ extra_column }} AS "{{ extra_column }}",
        {% endfor %}
        id                 AS "adresse_id",
        field              AS "field",
        value              AS "value"
    FROM
        {{ model }},
        LATERAL LIST_ADRESSE_ERRORS(
            adresse,
            code_insee,
            code_postal,
            commune,
            complement_adresse,
            id,
            latitude,
            longitude,
            source
        )
)

SELECT * FROM final
{% endmacro %}