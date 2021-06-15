from unittest import TestCase

from bumblebee.query import Query, WindowParameter, ExecutionParameter, DestinationParameter
from bumblebee.window import WindowFactory
from datetime import datetime, timedelta


class test_Query(TestCase):
    def setUp(self):
        self.scheduled_at = datetime(2020, 7, 8, 4)
        self.scheduled_next_at = datetime(2020, 7, 9, 4)

    def test_should_replace_dstart_and_dend_with_date(self):

        params = {
            '__dstart__': '2019-01-01',
            '__dend__': '2019-01-02'
        }

        query = Query("select * from table where date => '__dstart__' and date < '__dend__'")
        result = query.replace_param(params)

        self.assertEqual("select * from table where date => '2019-01-01' and date < '2019-01-02'",result)

    def test_should_replace_destination_table_and_execution_date(self):
        window_parameter = WindowParameter(WindowFactory.create_window_with_time(self.scheduled_at, self.scheduled_at + timedelta(days=1)))
        execution_parameter = ExecutionParameter(self.scheduled_at)
        destination_parameter = DestinationParameter("table")

        query = Query("select * from `__destination_table__` where date => '__execution_time__' and date < '__dend__'")
        result = query.apply_parameter(window_parameter).apply_parameter(execution_parameter)\
            .apply_parameter(destination_parameter)

        self.assertEqual("select * from `table` where date => '2020-07-08T04:00:00.000000' and date < '2020-07-09'", result)

    def test_apply_window(self):
        start_time = datetime(2019,1,1)
        window_parameter = WindowParameter(WindowFactory.create_window_with_time(start_time, start_time + timedelta(days=1)))

        query = Query("select * from table where date => '__dstart__' and date < '__dend__'")
        result = query.apply_parameter(window_parameter)
        self.assertEqual("select * from table where date => '2019-01-01' and date < '2019-01-02'", result)

        query = Query("select * from table where date => '__dstart__' and date < '__dend__'")
        result = query.apply_parameter(window_parameter)
        self.assertEqual("select * from table where date => '2019-01-01' and date < '2019-01-02'", result)

    def test_valid_hour_size_in_window_parameter(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "2h", "0", "h")
        window_parameter = WindowParameter(window)

        query = Query("select * from table where date => '__dstart__' and date < '__dend__' and tt < '__dstart__'")
        result = query.replace_param(window_parameter)

        self.assertEqual("select * from table where date => '2020-07-09 02:00:00' and date < '2020-07-09 04:00:00' and tt < '2020-07-09 02:00:00'",result)

    def test_dend_should_not_be_replaced_inside_the_word(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "2h", "0", "h")
        window_parameter = WindowParameter(window)

        query = Query("select * from table where date => adstarta && event > __execution_time__")
        result = query.replace_param(window_parameter)

        self.assertEqual("select * from table where date => adstarta && event > __execution_time__",result)
