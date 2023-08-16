{% macro create_udf_soliguide__new_hours_to_osm_opening_hours() %}
CREATE OR REPLACE FUNCTION udf_soliguide__new_hours_to_osm_opening_hours(data JSONB) RETURNS TEXT AS $$
DECLARE
    opening_hours TEXT := '';
    weekday_ TEXT;
BEGIN
    FOREACH weekday_ IN ARRAY ARRAY['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    LOOP
        IF NOT CAST(data#>>ARRAY[weekday_, 'open'] AS BOOLEAN) THEN
            CONTINUE;
        END IF;

        DECLARE
            weekday_selector TEXT := INITCAP(LEFT(weekday_, 2));
            time_selector TEXT := '';
            timeslot_data JSONB;
            sub_opening_hours TEXT;
        BEGIN
            FOREACH timeslot_data IN ARRAY ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS(data#>ARRAY[weekday_, 'timeslot']))
            LOOP
                time_selector := ARRAY_TO_STRING(
                    ARRAY[
                        time_selector,
                        ',',
                        LEFT(LPAD(timeslot_data->>'start', 4, '0'), 2),
                        ':',
                        RIGHT(LPAD(timeslot_data->>'start', 4, '0'), 2),
                        '-',
                        LEFT(LPAD(timeslot_data->>'end', 4, '0'), 2),
                        ':',
                        RIGHT(LPAD(timeslot_data->>'end', 4, '0'), 2)
                    ],
                    ''
                );
            END LOOP;

            time_selector := TRIM(LEADING ',' FROM time_selector);

            sub_opening_hours := weekday_selector || ' ' || time_selector || ';';
            opening_hours := opening_hours || sub_opening_hours || ' ';
        END;

    END LOOP;

    CASE data->>'closedHolidays'
        WHEN 'CLOSED' THEN
            opening_hours := opening_hours || 'PH closed;';
        WHEN 'OPEN' THEN
            opening_hours := opening_hours || 'PH open;';
        ELSE
    END CASE;

    opening_hours := TRIM(opening_hours);
    opening_hours := NULLIF(opening_hours, '');

    RETURN opening_hours;
END;
$$ LANGUAGE plpgsql;
{% endmacro %}