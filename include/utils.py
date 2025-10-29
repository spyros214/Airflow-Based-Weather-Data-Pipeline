from datetime import datetime
from dateutil import tz


ATHENS_TZ = tz.gettz("Europe/Athens")


def to_athens(dt: datetime) -> datetime:
    return dt.astimezone(ATHENS_TZ)
