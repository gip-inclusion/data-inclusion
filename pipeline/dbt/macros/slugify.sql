{% macro slugify(str) %}
REGEXP_REPLACE(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            LOWER(TRANSLATE(
                {{ str }},
                'àáâãäåèéêëìíîïòóôõöùúûüýÿçñ',
                'aaaaeeeeiiiioooouuuuyycn'
            )),
            '[^a-z0-9\s-]', '', 'g'
        ),
        '\s+', '-', 'g'
    ),
    '-+', '-', 'g'
)
{% endmacro %}
