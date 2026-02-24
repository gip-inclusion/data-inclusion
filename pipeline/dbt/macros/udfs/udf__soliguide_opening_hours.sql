{% macro udf__soliguide_opening_hours() %}

DROP FUNCTION IF EXISTS processings.soliguide_opening_hours;

CREATE OR REPLACE FUNCTION processings.soliguide_opening_hours(data JSONB)
RETURNS TEXT
AS $$

    import difflib
    import enum
    from typing import Annotated, Any, Literal

    import pydantic

    def fuzzy_enum_validator(enum_class):
        def _validator(value: Any) -> Any:
            return next(iter(difflib.get_close_matches(value, enum_class, n=1)), value)
        return _validator

    class HolidaysStatus(enum.StrEnum):
        CLOSED = "CLOSED"
        OPEN = "OPEN"
        UNKNOWN = "UNKNOWN"

    class TimeSlotItem(pydantic.BaseModel):
        start: int
        end: int

    class WeekdaySchedule(pydantic.BaseModel):
        open: bool
        timeslot: list[TimeSlotItem]

    class Schedule(pydantic.BaseModel):
        monday: WeekdaySchedule
        tuesday: WeekdaySchedule
        wednesday: WeekdaySchedule
        thursday: WeekdaySchedule
        friday: WeekdaySchedule
        saturday: WeekdaySchedule
        sunday: WeekdaySchedule
        description: str | None = None
        closed_holidays: Annotated[
            HolidaysStatus,
            pydantic.Field(alias="closedHolidays"),
            pydantic.BeforeValidator(fuzzy_enum_validator(HolidaysStatus)),
        ] = HolidaysStatus.UNKNOWN

        def weekdays(self) -> list[tuple[str, WeekdaySchedule]]:
            return [
                ("monday", self.monday),
                ("tuesday", self.tuesday),
                ("wednesday", self.wednesday),
                ("thursday", self.thursday),
                ("friday", self.friday),
                ("saturday", self.saturday),
                ("sunday", self.sunday),
            ]

    def handle_timeslot_item(time_slot_item: TimeSlotItem) -> str:
        start_hour = str(time_slot_item.start).zfill(4)
        end_hour = str(time_slot_item.end).zfill(4)
        return f"{start_hour[:2]}:{start_hour[2:]}-{end_hour[:2]}:{end_hour[2:]}"

    def handle_weekday(weekday_str: str, weekday_schedule: WeekdaySchedule) -> str:
        weekday_abbr = weekday_str[:2].capitalize()

        if not weekday_schedule.open:
            return f"{weekday_abbr} closed"

        time_selector = ",".join(
            [handle_timeslot_item(ts) for ts in weekday_schedule.timeslot]
        )
        return f"{weekday_abbr} {time_selector} open"

    def handle_schedule(schedule: Schedule) -> str:
        result = ", ".join([handle_weekday(dw, sch) for dw, sch in schedule.weekdays()])

        if schedule.description is not None and len(schedule.description) > 0:
            result += f"; {schedule.description}"

        if schedule.closed_holidays == HolidaysStatus.CLOSED:
            result += "; PH closed"
        elif schedule.closed_holidays == HolidaysStatus.OPEN:
            result += "; PH open"

        return result

    schedule = Schedule.model_validate_json(data)

    return handle_schedule(schedule)

$$ LANGUAGE plpython3u;

{% endmacro %}
