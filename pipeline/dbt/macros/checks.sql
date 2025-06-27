{% test check_service(model, schema_version) %}
    {{ select_service_errors(model, schema_version) }}
{% endtest %}

{% test check_structure(model, schema_version) %}
    {{ select_structure_errors(model, schema_version) }}
{% endtest %}

{% test check_adresse(model, schema_version) %}
    {{ select_adresse_errors(model, schema_version) }}
{% endtest %}

{% macro check_id() %}
id IS NOT NULL
{% endmacro %}

{% macro check_structure_id() %}
structure_id IS NOT NULL
{% endmacro %}

{% macro check_source() %}
source IS NOT NULL
{% endmacro %}

{% macro check_nom() %}
nom IS NOT NULL AND LENGTH(nom) <= 150 AND LENGTH(nom) >= 3 AND nom !~ '(?<!etc)\.$'
{% endmacro %}

{% macro check_nombre_semaines() %}
nombre_semaines IS NULL OR nombre_semaines > 0
{% endmacro %}

{% macro check_presentation_resume() %}
presentation_resume IS NULL OR LENGTH(presentation_resume) <= 280
{% endmacro %}

{% macro check_description() %}
description IS NOT NULL AND LENGTH(description) >= 50 AND LENGTH(description) <= 2000
{% endmacro %}

{% macro check_types() %}
types IS NULL OR types <@ ARRAY(SELECT t.value FROM {{ ref('typologies_de_services') }} AS t)
{% endmacro %}

{% macro check_telephone() %}
telephone IS NULL OR processings.format_phone_number(telephone) IS NOT NULL
{% endmacro %}

{% macro check_thematiques() %}
thematiques IS NULL OR thematiques <@ ARRAY(SELECT t.value FROM {{ ref('thematiques') }} AS t)
{% endmacro %}

{% macro check_frais() %}
frais IS NULL OR frais <@ ARRAY(SELECT f.value FROM {{ ref('frais') }} AS f)
{% endmacro %}

{% macro check_profils() %}
profils IS NULL OR profils <@ ARRAY(SELECT p.value FROM {{ ref('profils') }} AS p)
{% endmacro %}

{% macro check_profils_precisions() %}
profils_precisions IS NULL OR LENGTH(profils_precisions) <= 500
{% endmacro %}

{% macro check_courriel() %}
{# RFC 5322 #}
courriel IS NULL OR courriel ~ '^[a-zA-Z0-9!#$%&''*+/=?^_`{|}~-]+[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$'
{% endmacro %}

{% macro check_modes_accueil() %}
modes_accueil IS NULL OR modes_accueil <@ ARRAY(SELECT m.value FROM {{ ref('modes_accueil') }} AS m)
{% endmacro %}

{% macro check_modes_orientation_accompagnateur() %}
modes_orientation_accompagnateur IS NULL OR modes_orientation_accompagnateur <@ ARRAY(SELECT m.value FROM {{ ref('modes_orientation_accompagnateur') }} AS m)
{% endmacro %}

{% macro check_modes_orientation_beneficiaire() %}
modes_orientation_beneficiaire IS NULL OR modes_orientation_beneficiaire <@ ARRAY(SELECT m.value FROM {{ ref('modes_orientation_beneficiaire') }} AS m)
{% endmacro %}

{% macro check_mobilisable_par() %}
mobilisable_par IS NULL OR mobilisable_par <@ ARRAY(SELECT m.value FROM {{ ref('mobilisable_par') }} AS m)
{% endmacro %}

{% macro check_modes_mobilisation() %}
modes_mobilisation IS NULL OR modes_mobilisation <@ ARRAY(SELECT m.value FROM {{ ref('modes_mobilisation') }} AS m)
{% endmacro %}

{% macro check_volume_horaire_hebdomadaire() %}
volume_horaire_hebdomadaire IS NULL OR volume_horaire_hebdomadaire >= 0
{% endmacro %}

{% macro check_zone_diffusion_code() %}
zone_diffusion_code IS NULL OR zone_diffusion_code ~ '^(\d{9}|\w{5}|\w{2,3}|\d{2})$'
{% endmacro %}

{% macro check_zone_diffusion_type() %}
zone_diffusion_type IS NULL OR zone_diffusion_type IN (SELECT t.value FROM {{ ref('zones_de_diffusion_types') }} AS t)
{% endmacro %}

{% macro check_date_maj() %}
date_maj IS NOT NULL
{% endmacro %}

{% macro check_siret() %}
siret IS NULL OR siret ~ '^\d{14}$'
{% endmacro %}

{% macro check_rna() %}
rna IS NULL OR rna ~ '^W\d{9}$'
{% endmacro %}

{% macro check_typologie() %}
typologie IS NULL OR typologie IN (SELECT t.value FROM {{ ref('typologies_de_structures') }} AS t)
{% endmacro %}

{% macro check_labels_nationaux() %}
labels_nationaux IS NULL OR labels_nationaux <@ ARRAY(SELECT l.value FROM {{ ref('labels_nationaux') }} AS l)
{% endmacro %}

{% macro check_code_postal() %}
code_postal IS NULL OR code_postal ~ '^\d{5}$'
{% endmacro %}

{% macro check_code_insee() %}
code_insee IS NULL OR code_insee ~ '^.{5}$'
{% endmacro %}

{% macro select_service_errors(model, schema_version) %}
{% set checks = [
        ('id', check_id()),
        ("source", check_source()),
        ("nom", check_nom()),
        ("nombre_semaines", check_nombre_semaines()),
        ("presentation_resume", check_presentation_resume()),
        ("telephone", check_telephone()),
        ("courriel", check_courriel()),
        ("date_maj", check_date_maj()),
        ('structure_id', check_structure_id()),
        ("types", check_types()),
        ("thematiques", check_thematiques()),
        ("frais", check_frais()),
        ("profils", check_profils()),
        ("profils_precisions", check_profils_precisions()),
        ("modes_accueil", check_modes_accueil()),
        ("modes_orientation_accompagnateur", check_modes_orientation_accompagnateur()),
        ("modes_orientation_beneficiaire", check_modes_orientation_beneficiaire()),
        ("volume_horaire_hebdomadaire", check_volume_horaire_hebdomadaire()),
        ("zone_diffusion_code", check_zone_diffusion_code()),
        ("zone_diffusion_type", check_zone_diffusion_type())
] %}
{% if schema_version == 'v0' %}
{% set checks = checks + [
        ("presentation_resume", check_presentation_resume()),
] %}
{% elif schema_version == 'v1' %}
{% set checks = checks + [
        ("mobilisable_par", check_mobilisable_par()),
        ("modes_mobilisation", check_modes_mobilisation()),
] %}
{% endif %}

{{ select_errors(model, checks, 'service', schema_version) }}
{% endmacro %}

{% macro select_structure_errors(model, schema_version) %}
{% set checks = [
        ('id', check_id()),
        ("source", check_source()),
        ("nom", check_nom()),
        ("telephone", check_telephone()),
        ("courriel", check_courriel()),
        ("date_maj", check_date_maj()),
        ("siret", check_siret()),
        ("rna", check_rna()),
        ("typologie", check_typologie()),
        ("labels_nationaux", check_labels_nationaux()),
] %}
{% if schema_version == 'v0' %}
{% set checks = checks + [
        ("thematiques", check_thematiques()),
        ("presentation_resume", check_presentation_resume()),
] %}
{% elif schema_version == 'v1' %}
{% set checks = checks + [
] %}
{% endif %}

{{ select_errors(model, checks, 'structure', schema_version) }}
{% endmacro %}

{% macro select_adresse_errors(model, schema_version) %}
{% set checks = [
        ('id', check_id()),
        ("code_postal", check_code_postal()),
        ("code_insee", check_code_insee()),
] %}

{{ select_errors(model, checks, 'adresse', schema_version) }}
{% endmacro %}


{% macro select_errors(model, checks, resource_type, schema_version) %}
{# generates, from a list of checks, a select query that lists all check violations #}

{% for field, expression in checks %}
SELECT
    source || '-' || id       AS "_di_surrogate_id",
    source                    AS "source",
    id                        AS "id",
    '{{ field }}'             AS "field",
    CAST({{ field }} AS TEXT) AS "value",
    '{{ schema_version }}'    AS "schema_version",
    '{{ resource_type }}'     AS "resource_type"
FROM {{ model }}
WHERE NOT ({{ expression }})
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}
