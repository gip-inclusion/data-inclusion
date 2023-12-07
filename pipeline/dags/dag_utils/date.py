import pendulum

TIME_ZONE = "Europe/Paris"


def to_local(dt: pendulum.DateTime) -> pendulum.datetime:
    return pendulum.instance(dt.astimezone(pendulum.timezone(TIME_ZONE)))


def local_date_str(dt: pendulum.DateTime) -> str:
    return to_local(dt).date().to_date_string()


def local_day_datetime_str(dt: pendulum.DateTime) -> str:
    return to_local(dt).to_day_datetime_string()
