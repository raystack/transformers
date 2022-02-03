from unittest import TestCase, mock
from unittest.mock import MagicMock, PropertyMock, call

from bumblebee.transformation import *
from bumblebee.config import TaskConfigFromEnv

from google.cloud.bigquery.job import WriteDisposition
from google.cloud.bigquery.table import TimePartitioningType
import os


def set_vars_with_default(
        tz="UTC",
        project="bq_project",
        dataset="playground_dev",
        table="abcd",
        load_method="REPLACE",
):
    os.environ['PROJECT'] = project
    os.environ['DATASET'] = dataset
    os.environ['TABLE'] = table
    os.environ['SQL_TYPE'] = "STANDARD"
    os.environ['LOAD_METHOD'] = load_method
    os.environ['TASK_WINDOW'] = "DAILY"
    os.environ['TIMEZONE'] = tz
    os.environ['USE_SPILLOVER'] = "false"
    os.environ['CONCURRENCY'] = "1"


class TestTransformationTask(TestCase):
    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_partition_transform_execute(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'
            """

        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_end_time = localise_datetime(datetime(2019, 1, 2), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        window = WindowFactory.create_window_with_time(localized_end_time - timedelta(days=1), localized_end_time)
        loader = PartitionLoader(bigquery_service, task_config.destination_table,
                                 task_config.load_method, window.start)
        task = PartitionTransformation(task_config, loader, query, window, False, localized_execution_time)
        task.execute()

        final_query = """select count(1) from table where date >= '2019-01-01' and date < '2019-01-02'
            """

        bigquery_service.transform_load.assert_called_with(query=final_query,
                                            write_disposition=WriteDisposition.WRITE_TRUNCATE,
                                            destination_table="bq_project.playground_dev.abcd$20190101")

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_table_transform(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'
                """

        properties = """[DESTINATION]
        PROJECT="bq_project"
        DATASET="playground_dev"
        TABLE="abcd"
        SQL_TYPE="STANDARD" #LEGACY/STANDARD

        [TRANSFORMATION]
        WINDOW_SIZE = 1d
        WINDOW_OFFSET = 1d
        WINDOW_TRUNCATE_UPTO = d
        TIMEZONE="UTC"

        [LOAD]
        LOAD_METHOD="REPLACE"
                """
        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_start_time = localise_datetime(datetime(2019, 1, 2), task_config.timezone)
        localized_end_time = localise_datetime(datetime(2019, 1, 3), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        task = TableTransformation(bigquery_service, task_config, query, localized_start_time, localized_end_time, False, localized_execution_time)
        task.transform()

        final_query = """select count(1) from table where date >= '2019-01-02' and date < '2019-01-03'
                """

        bigquery_service.transform_load.assert_called_with(query=final_query,
                                                           write_disposition=WriteDisposition.WRITE_TRUNCATE,
                                                           destination_table="bq_project.playground_dev.abcd")

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_single_partition_transform_1d_window_0_offset_without_spillover(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'
                    """
        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_start_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)
        localized_end_time = localise_datetime(datetime(2019, 1, 2), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        task = SinglePartitionTransformation(bigquery_service, task_config, query, localized_start_time,
                                             localized_end_time, False, localized_execution_time)
        task.transform()

        final_query = """select count(1) from table where date >= '2019-01-01' and date < '2019-01-02'
                    """

        bigquery_service.transform_load.assert_called_with(query=final_query,
                                                           write_disposition=WriteDisposition.WRITE_TRUNCATE,
                                                           destination_table="bq_project.playground_dev.abcd$20190101")

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_single_partition_transform_2d_window_24h_offset_without_spillover(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_start_time = localise_datetime(datetime(2019, 1, 4), task_config.timezone)
        localized_end_time = localise_datetime(datetime(2019, 1, 6), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 4), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        task = SinglePartitionTransformation(bigquery_service, task_config, query, localized_start_time,
                                             localized_end_time, False, localized_execution_time)
        task.transform()

        final_query = """select count(1) from table where date >= '2019-01-04' and date < '2019-01-06'"""

        bigquery_service.transform_load.assert_called_with(query=final_query,
                                                           write_disposition=WriteDisposition.WRITE_TRUNCATE,
                                                           destination_table="bq_project.playground_dev.abcd$20190104")

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_single_partition_transform_7d_window_without_spillover(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        properties = """
            [DESTINATION]
            PROJECT="bq_project"
            DATASET="playground_dev"
            TABLE="abcd"
            SQL_TYPE="STANDARD" #LEGACY/STANDARD

            [TRANSFORMATION]
            WINDOW_SIZE = 7d
            WINDOW_OFFSET = 0
            WINDOW_TRUNCATE_UPTO = d
            TIMEZONE="UTC"

            [LOAD]
            LOAD_METHOD="REPLACE"
            """
        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_start_time = localise_datetime(datetime(2019, 1, 3), task_config.timezone)
        localized_end_time = localise_datetime(datetime(2019, 1, 10), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 3), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        task = SinglePartitionTransformation(bigquery_service, task_config, query, localized_start_time,
                                             localized_end_time, False, localized_execution_time)
        task.transform()

        final_query = """select count(1) from table where date >= '2019-01-03' and date < '2019-01-10'"""

        bigquery_service.transform_load.assert_called_with(query=final_query,
                                                           write_disposition=WriteDisposition.WRITE_TRUNCATE,
                                                           destination_table="bq_project.playground_dev.abcd$20190103")

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_single_partition_transform_2d_with_spillover(self, BigqueryServiceMock):
        query = "select count(1) from table where date >= '2019-01-03' and date < '2019-01-04'\n" + OPTIMUS_QUERY_BREAK_MARKER+ "\n"
        query += "select count(1) from table where date >= '2019-01-04' and date < '2019-01-05'"

        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_start_time = localise_datetime(datetime(2019, 1, 3), task_config.timezone)
        localized_end_time = localise_datetime(datetime(2019, 1, 5), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 5), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        task = MultiPartitionTransformation(bigquery_service, task_config, query, localized_start_time,
                                            localized_end_time, False, localized_execution_time, timedelta(days=1), 1)
        task.transform()

        final_query_1 = """select count(1) from table where date >= '2019-01-03' and date < '2019-01-04'\n"""
        final_query_2 = """\nselect count(1) from table where date >= '2019-01-04' and date < '2019-01-05'"""

        calls = [call(query=final_query_1, write_disposition=WriteDisposition.WRITE_TRUNCATE,
                      destination_table="bq_project.playground_dev.abcd$20190103"),
                 call(query=final_query_2, write_disposition=WriteDisposition.WRITE_TRUNCATE,
                      destination_table="bq_project.playground_dev.abcd$20190104")]
        bigquery_service.transform_load.assert_has_calls(calls, any_order=True)
        self.assertEqual(len(bigquery_service.transform_load.call_args_list), len(calls))

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_dml_transform(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_start_time = localise_datetime(datetime(2019, 1, 2), task_config.timezone)
        localized_end_time = localise_datetime(datetime(2019, 1, 3), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        task = DMLBasedTransformation(bigquery_service, task_config, query, localized_start_time, localized_end_time, False,
                                   localized_execution_time)
        task.execute()

        final_query = """select count(1) from table where date >= '2019-01-02' and date < '2019-01-03'"""
        bigquery_service.execute_query.assert_called_with(final_query)

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_execute_dry_run(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default()
        task_config = TaskConfigFromEnv()
        localized_start_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)
        localized_end_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)
        localized_execution_time = localise_datetime(datetime(2019, 1, 1), task_config.timezone)

        bigquery_service = BigqueryServiceMock()
        dry_run = True
        task = TableTransformation(bigquery_service, task_config, query, localized_start_time,
                                   localized_end_time, dry_run, localized_execution_time)
        task.transform()
        bigquery_service.transform_load.assert_not_called()


class TestTransformation(TestCase):

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_should_run_dml_merge_statements(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default(load_method="MERGE")
        task_config = TaskConfigFromEnv()

        end_time = datetime(2019, 2, 2)
        execution_time = datetime(2019, 2, 2)

        bigquery_service = BigqueryServiceMock()
        transformation = Transformation(bigquery_service,
                                        task_config,
                                        query,
                                        None,
                                        end_time - timedelta(days=1),
                                        end_time,
                                        execution_time,
                                        False)
        transformation.transform()

        final_query = """select count(1) from table where date >= '2019-02-01' and date < '2019-02-02'"""
        bigquery_service.execute_query.assert_called_with(final_query)

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_should_run_table_task(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default(load_method="APPEND")
        task_config = TaskConfigFromEnv()

        end_time = datetime(2019, 2, 2)
        execution_time = datetime(2019, 2, 2)

        bigquery_service = BigqueryServiceMock()

        def get_table_mock(table_name):
            if table_name == 'bq_project.playground_dev.abcd':
                table_mock = MagicMock()
                type(table_mock).time_partitioning = None
                return table_mock

        bigquery_service.get_table = MagicMock(side_effect=get_table_mock)

        transformation = Transformation(bigquery_service,
                                        task_config,
                                        query,
                                        None,
                                        end_time - timedelta(days=1),
                                        end_time,
                                        execution_time,
                                        False)
        transformation.transform()

        final_query = """select count(1) from table where date >= '2019-02-01' and date < '2019-02-02'"""
        bigquery_service.transform_load.assert_called_with(query=final_query,
                                                   write_disposition=WriteDisposition.WRITE_APPEND,
                                                   destination_table="bq_project.playground_dev.abcd")

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_table_transform_with_merge_load_method_and_non_partitioned_destination(self, BigqueryServiceMock):
        query = "select count(1) from table where date >= '2019-01-03' and date < '2019-01-04'\n" + OPTIMUS_QUERY_BREAK_MARKER + "\n"
        query += "select count(1) from table where date >= '2019-01-04' and date < '2019-01-05'"

        set_vars_with_default()
        task_config = TaskConfigFromEnv()

        end_time = datetime(2019, 1, 5)
        execution_time = datetime(2019, 2, 2)

        bigquery_service = BigqueryServiceMock()

        def get_table_mock(table_name):
            if table_name == 'bq_project.playground_dev.abcd':
                table_mock = MagicMock()
                type(table_mock).time_partitioning = None
                return table_mock

        bigquery_service.get_table = MagicMock(side_effect=get_table_mock)

        transformation = Transformation(bigquery_service,
                                        task_config,
                                        query,
                                        None,
                                        end_time - timedelta(days=2),
                                        end_time,
                                        execution_time,
                                        False)
        transformation.transform()

        final_query = """select count(1) from table where date >= '2019-01-03' and date < '2019-01-04'\n"""
        bigquery_service.transform_load.assert_called_with(query=final_query,
                                                           write_disposition=WriteDisposition.WRITE_TRUNCATE,
                                                           destination_table="bq_project.playground_dev.abcd")

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_should_run_partition_task_on_field(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default(load_method="REPLACE_MERGE")
        task_config = TaskConfigFromEnv()

        end_time = datetime(2019, 2, 2)
        execution_time = datetime(2019, 2, 2)

        bigquery_service = BigqueryServiceMock()

        def get_table_mock(table_name):
            if table_name == 'bq_project.playground_dev.abcd':
                time_partitioning = MagicMock()
                type(time_partitioning).field = PropertyMock(return_value="event_timestamp")
                type(time_partitioning).field_type = PropertyMock(return_value="TIMESTAMP")
                type(time_partitioning).type_ = PropertyMock(return_value=TimePartitioningType.DAY)

                table_mock = MagicMock()
                type(table_mock).time_partitioning = PropertyMock(return_value=time_partitioning)
                type(table_mock).partitioning_type = "DAY"
                return table_mock

        bigquery_service.get_table = MagicMock(side_effect=get_table_mock)

        transformation = Transformation(bigquery_service,
                                        task_config,
                                        query,
                                        None,
                                        end_time - timedelta(days=1),
                                        end_time,
                                        execution_time,
                                        False)
        transformation.transform()

        final_query = """-- Optimus generated\nDECLARE partitions ARRAY<DATE>;\n\n\n\nCREATE TEMP TABLE `opt__partitions` AS (\n  select count(1) from table where date >= '__dstart__' and date < '__dend__'\n);\n\nSET (partitions) = (\n    SELECT AS STRUCT\n        array_agg(DISTINCT DATE(`event_timestamp`))\n    FROM opt__partitions\n);\n\nMERGE INTO\n  `bq_project.playground_dev.abcd` AS target\nUSING\n  (\n      Select * from `opt__partitions`\n  ) AS source\nON FALSE\nWHEN NOT MATCHED BY SOURCE AND DATE(`event_timestamp`) IN UNNEST(partitions)\nTHEN DELETE\nWHEN NOT MATCHED THEN INSERT\n  (\n     \n  )\nVALUES\n  (\n      \n  );\n"""
        bigquery_service.execute_query.assert_called_with(final_query)

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_should_fail_if_partition_task_for_ingestion_time_without_filter_in_REPLACE_MERGE(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default(load_method="REPLACE_MERGE")
        task_config = TaskConfigFromEnv()

        end_time = datetime(2019, 2, 2)
        execution_time = datetime(2019, 2, 2)

        bigquery_service = BigqueryServiceMock()

        def get_table_mock(table_name):
            if table_name == 'bq_project.playground_dev.abcd':
                time_partitioning = MagicMock()
                type(time_partitioning).field = PropertyMock(return_value=None)
                type(time_partitioning).type_ = PropertyMock(return_value=TimePartitioningType.DAY)

                table_mock = MagicMock()
                type(table_mock).time_partitioning = PropertyMock(return_value=time_partitioning)
                type(table_mock).partitioning_type = "DAY"
                return table_mock

        bigquery_service.get_table = MagicMock(side_effect=get_table_mock)
        transformation = Transformation(bigquery_service,
                                        task_config,
                                        query,
                                        None,
                                        end_time - timedelta(days=1),
                                        end_time,
                                        execution_time,
                                        False)
        with self.assertRaises(Exception) as ex:
            transformation.transform()
        self.assertTrue("partition filter is required" in str(ex.exception))

    @mock.patch("bumblebee.bigquery_service.BigqueryService")
    def test_execute_table_not_found_raise_exception(self, BigqueryServiceMock):
        query = """select count(1) from table where date >= '__dstart__' and date < '__dend__'"""

        set_vars_with_default()
        task_config = TaskConfigFromEnv()

        end_time = datetime(2019, 2, 2)
        execution_time = datetime(2019, 2, 2)

        bigquery_service = BigqueryServiceMock()

        def get_table_mock(table_name):
            raise Exception("{} table not found".format(table_name))
        bigquery_service.get_table = MagicMock(side_effect=get_table_mock)

        transformation = Transformation(bigquery_service,
                                        task_config,
                                        query,
                                        None,
                                        end_time - timedelta(days=1),
                                        end_time,
                                        execution_time,
                                        False)


        with self.assertRaises(Exception) as ex:
            transformation.transform()
        self.assertTrue("table not found" in str(ex.exception))

class TestBulkExecutor(TestCase):

    def test_bulk_executor(self):
        timezone = "Asia/Jakarta"
        concurrency = 10

        start_time = localise_datetime(datetime(2019, 1, 1), timezone)
        next_day = start_time + timedelta(days=1)

        datetime_ranges = [start_time, next_day]

        def execute_mock():
            print("a")

        task_mock = MagicMock()
        task_mock.execute = MagicMock(side_effect=execute_mock)

        tasks = [ task_mock for dt in datetime_ranges]

        executor = ConcurrentTaskExecutor(concurrency)
        executor.execute(tasks)

        task_mock.execute.assert_has_calls([call(), call()])