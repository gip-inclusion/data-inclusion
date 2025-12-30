import difflib
import enum
import functools
from typing import Annotated, Any

import pydantic


def _fuzzy_enum_validator(enum_class):
    def _validator(value: Any) -> Any:
        return next(iter(difflib.get_close_matches(value, enum_class, n=1)), value)

    return _validator


class HolidaysStatus(str, enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    UNKNOWN = "UNKNOWN"


class TimeSlotItem(pydantic.BaseModel):
    start: int
    end: int


class WeekdaySchedule(pydantic.BaseModel):
    open: bool
    timeslot: list[TimeSlotItem] = []


class Schedule(pydantic.BaseModel):
    monday: WeekdaySchedule = WeekdaySchedule(open=False)
    tuesday: WeekdaySchedule = WeekdaySchedule(open=False)
    wednesday: WeekdaySchedule = WeekdaySchedule(open=False)
    thursday: WeekdaySchedule = WeekdaySchedule(open=False)
    friday: WeekdaySchedule = WeekdaySchedule(open=False)
    saturday: WeekdaySchedule = WeekdaySchedule(open=False)
    sunday: WeekdaySchedule = WeekdaySchedule(open=False)
    description: str | None = None
    closed_holidays: Annotated[
        HolidaysStatus,
        pydantic.Field(alias="closedHolidays"),
        pydantic.BeforeValidator(_fuzzy_enum_validator(HolidaysStatus)),
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


def _handle_timeslot_item(time_slot_item: TimeSlotItem) -> str:
    start_hour = str(time_slot_item.start).zfill(4)
    end_hour = str(time_slot_item.end).zfill(4)
    return f"{start_hour[:2]}:{start_hour[2:]}-{end_hour[:2]}:{end_hour[2:]}"


def _handle_weekday(weekday_str: str, weekday_schedule: WeekdaySchedule) -> str:
    weekday_abbr = weekday_str[:2].capitalize()
    if not weekday_schedule.open:
        return f"{weekday_abbr} closed"
    time_selector = ",".join(
        [_handle_timeslot_item(ts) for ts in weekday_schedule.timeslot]
    )
    return f"{weekday_abbr} {time_selector} open"


def _handle_schedule(schedule: Schedule) -> str:
    result = ", ".join([_handle_weekday(dw, sch) for dw, sch in schedule.weekdays()])
    if schedule.description is not None and len(schedule.description) > 0:
        result += f"; {schedule.description}"
    if schedule.closed_holidays == HolidaysStatus.CLOSED:
        result += "; PH closed"
    elif schedule.closed_holidays == HolidaysStatus.OPEN:
        result += "; PH open"
    return result


@functools.lru_cache(maxsize=1024)
def _cached_to_osm(data: str) -> str:
    schedule = Schedule.model_validate_json(data)
    return _handle_schedule(schedule)


def soliguide_opening_hours(data: str | list | dict | None) -> str | None:
    if data is None:
        return None
    try:
        if isinstance(data, str):
            return _cached_to_osm(data)
        elif isinstance(data, dict | list):
            schedule = Schedule.model_validate(data)
            return _handle_schedule(schedule)
        else:
            return None
    except Exception:
        return None
