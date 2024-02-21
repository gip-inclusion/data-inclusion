from datetime import datetime

import pytz


def now() -> datetime:
    return datetime.now(tz=pytz.UTC)
