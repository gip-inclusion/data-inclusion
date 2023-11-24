import pendulum


def local_date_str(dt: pendulum.DateTime) -> str:
    import pendulum

    return (
        pendulum.instance(dt.astimezone(pendulum.timezone("Europe/Paris")))
        .date()
        .to_date_string()
    )
