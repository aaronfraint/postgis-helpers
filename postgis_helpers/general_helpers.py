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


def report_time_delta(start_time: datetime.datetime,
                      end_time: datetime.datetime) -> str:
    """
    Calculate a timedelta between two datetimes,
    and return a string with "Runtime: h:mm:ss.sss"

    :param start_time: first timepoint
    :type start_time: datetime.datetime
    :param end_time: second timepoint
    :type end_time: datetime.datetime
    :return: text formatted as "Runtime: h:mm:ss.sss"
    :rtype: str
    """

    hms, milisec = str(end_time - start_time).split(".")

    return "Runtime = " + hms + "." + milisec[:3]
