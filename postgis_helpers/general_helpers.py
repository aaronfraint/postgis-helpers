import datetime
from pytz import timezone


def now(tz: str = None) -> datetime.datetime:
    """
    Return the current date/time. Optionally provide
    a ``pytz`` timezone, e.g.: 'US/Eastern'

    :param tz: timezone from the pytz list, defaults to False
    :type tz: Union[bool, str], optional
    :return: current datetime in the system or specified timezone
    :rtype: ``datetime.datetime``
    """
    if tz:
        return datetime.datetime.now(timezone(tz))
    else:
        return datetime.datetime.now()


def report_time_delta(
    start_time: datetime.datetime, end_time: datetime.datetime
) -> str:
    """
    Calculate a timedelta between two datetimes,
    and return a string with "h:mm:ss.ss"

    :param start_time: first timepoint
    :type start_time: datetime.datetime
    :param end_time: second timepoint
    :type end_time: datetime.datetime
    :return: text formatted as "h:mm:ss.ss"
    :rtype: str
    """

    hms, milisec = str(end_time - start_time).split(".")

    return "runtime = " + hms + "." + milisec[:2]


def dt_as_time(dt: datetime.datetime) -> str:
    h, m, s = dt.strftime("%H:%M:%S").split(":")

    return f"{h}:{m}:{s}"
