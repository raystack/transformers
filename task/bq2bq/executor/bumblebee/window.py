import calendar
from abc import ABC
from abc import abstractmethod
from datetime import datetime, timedelta
from bumblebee.datehelper import parse_duration


class BaseWindow(ABC):

    @property
    @abstractmethod
    def start(self):
        pass

    @property
    @abstractmethod
    def end(self):
        pass

    @property
    @abstractmethod
    def size(self):
        pass

    @property
    @abstractmethod
    def offset(self):
        pass

    @property
    @abstractmethod
    def truncate_upto(self):
        pass


class Window(BaseWindow):
    """
    provide start and end property from window_type

    """
    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    @property
    def size(self):
        return self._size

    @property
    def offset(self):
        return self._offset

    @property
    def truncate_upto(self):
        return self._truncate_upto


class WindowFactory:
    """
    Generate window that determine start_time and end_time
    """
    @staticmethod
    def create_window(end_time: datetime, window_size: str, window_offset: str, window_truncate_upto: str):
        return XWindow(end_time, window_size.lower(), window_offset.lower(), window_truncate_upto.lower())

    @staticmethod
    def create_window_with_time(start_time: datetime, end_time: datetime):
        return CustomWindow(start_time, end_time)


class CustomWindow(Window):
    """
    Generate window based on already computed start and end time
    """
    def __init__(self, start_time: datetime, end_time: datetime):
        self._offset = parse_duration("0")
        self._size = timedelta(seconds=(end_time - start_time).total_seconds())

        self._start = start_time
        self._end = end_time
        self._truncate_upto = ""


class XWindow(Window):
    """
    Generate window based on user config inputs
    """
    def __init__(self, end_time: datetime, window_size: str, window_offset: str, window_truncate_upto: str):
        floating_end = end_time

        # apply truncation
        if window_truncate_upto == "h":
            # remove time upto hours
            floating_end = floating_end.replace(minute=0, second=0, microsecond=0)
        elif window_truncate_upto == "d":
            # remove time upto days
            floating_end = floating_end.replace(hour=0, minute=0, second=0, microsecond=0)
        elif window_truncate_upto == "w":
            # remove time upto days
            # get week lists for current month
            week_matrix_per_month = calendar.Calendar().monthdatescalendar(end_time.year, end_time.month)
            # find week where current day lies
            current_week = None
            for week in week_matrix_per_month:
                for day in week:
                    if day == end_time.date():
                        current_week = week

            floating_end = datetime.combine(current_week[6], end_time.min.time())
            floating_end = floating_end.replace(tzinfo=end_time.tzinfo)
        elif window_truncate_upto == "" or window_truncate_upto == "0":
            # do nothing
            floating_end = floating_end
        else:
            raise Exception("unsupported truncate method: {}".format(window_truncate_upto))

        # generate shift & length
        self._offset = parse_duration(window_offset)
        self._size = parse_duration(window_size)

        self._end = floating_end + self._offset
        self._start = self._end - self._size
        self._truncate_upto = window_truncate_upto
        pass


class MonthlyWindow(Window):
    """
    @Deprecated
    - Not Supported at the moment
    Monthly window returns start time of the month and end time of the month
    """

    def __init__(self,start_time):
        if start_time.date().day != 1:
            raise Exception("for {} start_time should be in the start date of the month".format(start_time))

        cal = calendar.Calendar()
        fullweekdates = cal.monthdatescalendar(start_time.year,start_time.month)

        dates = []
        for week in fullweekdates:
            for date in week:
                if date.month == start_time.month:
                    dates.append(date)

        self._start = self._to_datetime(dates[0])
        self._end = self._to_datetime(dates[len(dates)-1])

    def _to_datetime(self,date):
        return datetime.combine(date, datetime.min.time())
