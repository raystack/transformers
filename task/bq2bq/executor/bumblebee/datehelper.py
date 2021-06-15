
from datetime import datetime, date, timedelta
from pytimeparse import parse
import pytz

def parse_duration(time_str):
    """
    :param time_str: example 1d, 2h
    :return: timedelta object
    """
    if time_str == "" or time_str == "0":
        return timedelta(seconds=0)
    return timedelta(seconds=parse(time_str))

def localise_datetime(datetimeobj: datetime, tzname: str):
    """
    :param datetimeobj:
    :param tzname:
    :return:
    """
    local_timezone = pytz.timezone(tzname)
    if datetimeobj.tzinfo is None:
        return local_timezone.localize(datetimeobj)
    else:
        return datetimeobj.astimezone(local_timezone)
