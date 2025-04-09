/*
Create user defined SQL functions.

This macro should be run with `dbt run-operation create_udfs`.
Another way would be to use the `on-run-start` hook, but it does not play nicely with concurrency.
*/

{% macro create_udfs() %}

{% set sql %}

CREATE SCHEMA IF NOT EXISTS processings;

{{ udf__deduplicate() }}
{{ udf__format_phone_number() }}
{{ udf__geocode() }}
{{ udf__score() }}

{{ create_udf_soliguide__new_hours_to_osm_opening_hours() }}
{{ create_udf__common_checks() }}
{{ create_udf__service_checks() }}
{{ create_udf__adresse_checks() }}
{{ create_udf__structure_checks() }}

{% endset %}

{% do run_query(sql) %}

{% endmacro %}
