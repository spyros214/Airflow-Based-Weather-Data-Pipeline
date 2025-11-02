from datetime import datetime
from dateutil import tz


ATHENS_TZ = tz.gettz("Europe/Athens")


def to_athens(dt: datetime) -> datetime:
"""Μετατρέπει ένα datetime αντικείμενο στη ζώνη ώρας της Αθήνας."""    
    return dt.astimezone(ATHENS_TZ)
