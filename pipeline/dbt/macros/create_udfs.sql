/*
Create user defined SQL functions.

This macro should be run with `dbt run-operation create_udfs`.
Another way would be to use the `on-run-start` hook, but it does not play nicely with concurrency.
*/

{% macro create_udfs() %}

{% set sql %}

CREATE SCHEMA IF NOT EXISTS processings;

{{ udf__brevo_import_contacts() }}
{{ udf__check_urls() }}
{{ udf__deduplicate() }}
{{ udf__geocode() }}
{{ udf__format_phone_number() }}
{{ udf__score() }}
{{ udf__soliguide_to_osm_opening_hours() }}

{% endset %}

{% do run_query(sql) %}

{% endmacro %}
