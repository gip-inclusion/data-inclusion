{% macro create_udf__structure_checks() %}
DROP FUNCTION IF EXISTS LIST_STRUCTURE_ERRORS;
CREATE OR REPLACE FUNCTION LIST_STRUCTURE_ERRORS(
        accessibilite TEXT,
        antenne BOOLEAN,
        courriel TEXT,
        date_maj DATE,
        horaires_ouverture TEXT,
        id TEXT,
        labels_autres TEXT[],
        labels_nationaux TEXT[],
        lien_source TEXT,
        nom TEXT,
        presentation_detail TEXT,
        presentation_resume TEXT,
        rna TEXT,
        siret TEXT,
        site_web TEXT,
        source TEXT,
        telephone TEXT,
        thematiques TEXT[],
        typologie TEXT
    )
RETURNS TABLE (field TEXT, value TEXT) AS $$
DECLARE
BEGIN
    {% set checks = [
            ("id", "id IS NOT NULL"),
            ("source", "source IS NOT NULL"),
            ("siret", "siret IS NULL OR CHECK_SIRET(siret)"),
            ("rna", "rna IS NULL OR CHECK_RNA(rna)"),
            ("nom", "nom IS NOT NULL AND LENGTH(nom) <= 150 AND LENGTH(nom) >= 3 AND nom !~ '(?<!etc)\.$'"),
            ("date_maj", "date_maj IS NOT NULL"),
            ("typologie", "typologie IS NULL OR typologie IN (SELECT t.value FROM " ~ ref('typologies_de_structures') ~ " AS t)"),
            ("labels_nationaux", "labels_nationaux IS NULL OR labels_nationaux <@ ARRAY(SELECT l.value FROM " ~ ref('labels_nationaux') ~ " AS l)"),
            ("thematiques", "thematiques IS NULL OR thematiques <@ ARRAY(SELECT t.value FROM " ~ ref('thematiques') ~ "AS t)"),
            ("presentation_resume", "presentation_resume IS NULL OR LENGTH(presentation_resume) <= 280"),
            ("courriel", "courriel IS NULL OR CHECK_COURRIEL(courriel)"),
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


{% test check_structure(model, include) %}
    {{ check_structure_errors(model, include) }}
{% endtest %}


{% macro check_structure_errors(model, include=[]) %}
WITH final AS (
    SELECT
        {% for extra_column in include %}
        {{ extra_column }} AS "{{ extra_column }}",
        {% endfor %}
        id                 AS "structure_id",
        field              AS "field",
        value              AS "value"
    FROM
        {{ model }},
        LATERAL LIST_STRUCTURE_ERRORS(
            accessibilite,
            antenne,
            courriel,
            date_maj,
            horaires_ouverture,
            id,
            labels_autres,
            labels_nationaux,
            lien_source,
            nom,
            presentation_detail,
            presentation_resume,
            rna,
            siret,
            site_web,
            source,
            telephone,
            thematiques,
            typologie
        )
)

SELECT * FROM final
{% endmacro %}
