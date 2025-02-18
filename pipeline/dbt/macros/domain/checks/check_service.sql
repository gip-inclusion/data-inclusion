{% macro create_udf__service_checks() %}
DROP FUNCTION IF EXISTS LIST_SERVICE_ERRORS;
CREATE OR REPLACE FUNCTION LIST_SERVICE_ERRORS(
        contact_public BOOLEAN,
        contact_nom_prenom TEXT,
        courriel TEXT,
        cumulable BOOLEAN,
        date_creation DATE,
        date_maj DATE,
        date_suspension DATE,
        frais TEXT[],
        frais_autres TEXT,
        id TEXT,
        justificatifs TEXT[],
        lien_source TEXT,
        modes_accueil TEXT[],
        modes_orientation_accompagnateur TEXT[],
        modes_orientation_accompagnateur_autres TEXT,
        modes_orientation_beneficiaire TEXT[],
        modes_orientation_beneficiaire_autres TEXT,
        mobilisable_par TEXT[],
        nom TEXT,
        page_web TEXT,
        presentation_detail TEXT,
        presentation_resume TEXT,
        prise_rdv TEXT,
        profils TEXT[],
        profils_precisions TEXT,
        recurrence TEXT,
        source TEXT,
        structure_id TEXT,
        telephone TEXT,
        thematiques TEXT[],
        types TEXT[],
        zone_diffusion_code TEXT,
        zone_diffusion_nom TEXT,
        zone_diffusion_type TEXT,
        pre_requis TEXT[]
    )
RETURNS TABLE (field TEXT, value TEXT) AS $$
DECLARE
BEGIN
    {% set checks = [
            ("id", "id IS NOT NULL"),
            ("structure_id", "structure_id IS NOT NULL"),
            ("source", "source IS NOT NULL"),
            ("nom", "nom IS NOT NULL"),
            ("presentation_resume", "presentation_resume IS NULL OR LENGTH(presentation_resume) <= 280"),
            ("types", "types IS NULL OR types <@ ARRAY(SELECT t.value FROM " ~ ref('typologies_de_services') ~ "AS t)"),
            ("thematiques", "thematiques IS NULL OR thematiques <@ ARRAY(SELECT t.value FROM " ~ ref('thematiques') ~ "AS t)"),
            ("frais", "frais IS NULL OR frais <@ ARRAY(SELECT f.value FROM " ~ ref('frais') ~ "AS f)"),
            ("profils", "profils IS NULL OR profils <@ ARRAY(SELECT p.value FROM " ~ ref('profils') ~ "AS p)"),
            ("profils_precisions", "profils_precisions IS NULL OR LENGTH(profils_precisions) <= 500"),
            ("courriel", "courriel IS NULL OR CHECK_COURRIEL(courriel)"),
            ("modes_accueil", "modes_accueil IS NULL OR modes_accueil <@ ARRAY(SELECT m.value FROM " ~ ref('modes_accueil') ~ "AS m)"),
            ("modes_orientation_accompagnateur", "modes_orientation_accompagnateur IS NULL OR modes_orientation_accompagnateur <@ ARRAY(SELECT m.value FROM " ~ ref('modes_orientation_accompagnateur') ~ "AS m)"),
            ("modes_orientation_beneficiaire", "modes_orientation_beneficiaire IS NULL OR modes_orientation_beneficiaire <@ ARRAY(SELECT m.value FROM " ~ ref('modes_orientation_beneficiaire') ~ "AS m)"),
            ("mobilisable_par", "mobilisable_par IS NULL OR mobilisable_par <@ ARRAY(SELECT m.value FROM " ~ ref('mobilisable_par') ~ "AS m)"),
            ("zone_diffusion_code", "zone_diffusion_code IS NULL OR zone_diffusion_code ~ '^(\d{9}|\w{5}|\w{2,3}|\d{2})$'"),
            ("zone_diffusion_type", "zone_diffusion_type IS NULL OR zone_diffusion_type IN (SELECT t.value FROM " ~ ref('zones_de_diffusion_types') ~ "AS t)"),
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


{% test check_service(model, include) %}
    {{ check_service_errors(model, include) }}
{% endtest %}


{% macro check_service_errors(model, include=[]) %}
WITH final AS (
    SELECT
        {% for extra_column in include %}
        {{ extra_column }} AS "{{ extra_column }}",
        {% endfor %}
        id                 AS "service_id",
        field              AS "field",
        value              AS "value"
    FROM
        {{ model }},
        LATERAL LIST_SERVICE_ERRORS(
            contact_public,
            contact_nom_prenom,
            courriel,
            cumulable,
            date_creation,
            date_maj,
            date_suspension,
            frais,
            frais_autres,
            id,
            justificatifs,
            lien_source,
            modes_accueil,
            modes_orientation_accompagnateur,
            modes_orientation_accompagnateur_autres,
            modes_orientation_beneficiaire,
            modes_orientation_beneficiaire_autres,
            mobilisable_par,
            nom,
            page_web,
            presentation_detail,
            presentation_resume,
            prise_rdv,
            profils,
            profils_precisions,
            recurrence,
            source,
            structure_id,
            telephone,
            thematiques,
            types,
            zone_diffusion_code,
            zone_diffusion_nom,
            zone_diffusion_type,
            pre_requis
        )
)

SELECT * FROM final
{% endmacro %}
