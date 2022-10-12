
import pytz
from pytz.exceptions import UnknownTimeZoneError

from unittest import TestCase
from unittest.mock import MagicMock,call

from datetime import datetime
import iso8601
from bumblebee.config import LoadMethod, TaskFiles, AppConfig, TaskConfigFromEnv, parse_date
from bumblebee.datehelper import localise_datetime
from bumblebee.filesystem import FileSystem

from google.cloud.bigquery.job import WriteDisposition
import os


class TestAppConfig(TestCase):
    def set_vars_with_default(self, dt):
        os.environ['DSTART'] = dt
        os.environ['DEND'] = dt
        os.environ['EXECUTION_TIME'] = dt
        os.environ['DRY_RUN'] = "true"
        os.environ['JOB_LABELS'] = "environment=integration,lifecycle=process"
        os.environ['JOB_DIR'] = "./tests/sample_config"
        os.environ['PROJECT'] = "project-id"
        os.environ['DATASET'] = "dataset"
        os.environ['TABLE'] = "sample_select"
        os.environ['SQL_TYPE'] = "STANDARD"
        os.environ['LOAD_METHOD'] = "APPEND"
        os.environ['TASK_WINDOW'] = "DAILY"
        os.environ['TIMEZONE'] = "UTC"
        os.environ['USE_SPILLOVER'] = "false"
        os.environ['CONCURRENCY'] = "1"

    def test_should_parse_args(self):
        dt = "2020-11-30T19:32:25.680530"
        self.set_vars_with_default(dt)

        opts = AppConfig()
        self.assertEqual(opts.dry_run, True)
        self.assertEqual(opts.dstart, parse_date(dt))
        self.assertEqual(opts.dend, parse_date(dt))
        self.assertEqual(opts.execution_time, parse_date(dt))
        self.assertEqual(opts.job_labels, {"environment": "integration", "lifecycle": "process"})
        self.assertEqual("./tests/sample_config/in/query.sql", opts.sql_file)
        self.assertEqual(None, opts.spillover_sql_file)

    def test_should_fail_for_invalid_date(self):
        dt = "2020-invalid-date"
        self.set_vars_with_default(dt)

        with self.assertRaises(iso8601.ParseError) as e:
            AppConfig()
        self.assertEqual("Unable to parse date string '2020-invalid-date'", str(e.exception))

    def test_should_fail_for_empty_dstart(self):
        dt = "2020-11-30T19:32:25.680530"
        self.set_vars_with_default(dt)
        os.environ['DSTART'] = ""

        with self.assertRaises(AssertionError) as e:
            AppConfig()
        self.assertEqual("config 'DSTART' must be provided", str(e.exception))

    def test_should_fail_for_missing_dstart(self):
        dt = "2020-11-30T19:32:25.680530"
        self.set_vars_with_default(dt)
        del os.environ['DSTART']

        with self.assertRaises(AssertionError) as e:
            AppConfig()
        self.assertEqual("config 'DSTART' must be provided", str(e.exception))

    def test_should_fail_for_missing_label_value(self):
        dt = "2020-11-30T19:32:25.680530"
        self.set_vars_with_default(dt)
        os.environ['JOB_LABELS'] = "environment=integration,lifecycle="

        with self.assertRaises(AssertionError) as e:
            AppConfig()
        self.assertEqual("label value cannot be empty in JOB_LABELS", str(e.exception))

    def test_should_fail_for_missing_label_key_name(self):
        dt = "2020-11-30T19:32:25.680530"
        self.set_vars_with_default(dt)
        os.environ['JOB_LABELS'] = "environment=integration,=some_value"

        with self.assertRaises(AssertionError) as e:
            AppConfig()
        self.assertEqual("label name cannot be empty in JOB_LABELS", str(e.exception))

    def test_dry_run_should_be_false_by_default(self):
        dt = "2020-11-30T19:32:25.680530"
        self.set_vars_with_default(dt)
        del os.environ['DRY_RUN']

        c = AppConfig()
        self.assertEqual(False, c.dry_run)


class TestConfig(TestCase):
    def set_vars_with_default(self, tz: str = "UTC"):
        os.environ['PROJECT'] = "project-id"
        os.environ['DATASET'] = "dataset"
        os.environ['TABLE'] = "sample_select"
        os.environ['SQL_TYPE'] = "STANDARD"
        os.environ['LOAD_METHOD'] = "APPEND"
        os.environ['TASK_WINDOW'] = "DAILY"
        os.environ['TIMEZONE'] = tz
        os.environ['USE_SPILLOVER'] = "false"
        os.environ['CONCURRENCY'] = "1"

    def setUp(self):
        self.timezone = pytz.timezone("Asia/Jakarta")

    def test_localise_datetime(self):
        tzname = "Asia/Jakarta"
        start_time = datetime(2019, 1, 1)
        localised_start_time = localise_datetime(start_time, tzname)

        expected_start_time = self.timezone.localize(datetime(year=2019, month=1, day=1))
        self.assertEqual(expected_start_time,localised_start_time)

    def test_localise_datetime_utc(self):
        utc = pytz.timezone("UTC")
        start_time = utc.localize(datetime(2019, 1, 1, 17))

        localized_start_time = localise_datetime(start_time, "Asia/Jakarta")

        expected = self.timezone.localize(datetime(year=2019, month=1, day=2))
        self.assertEqual(expected, localized_start_time)

    def test_timezone_config(self):
        tz = "Asia/Jakarta"
        self.set_vars_with_default(tz)

        task_config = TaskConfigFromEnv()

        self.assertEqual(task_config.timezone, tz)

    def test_invalid_timezone_trigger_exception(self):
        tz = "xxwdw"
        self.set_vars_with_default(tz)

        with self.assertRaises(UnknownTimeZoneError) as ex:
            TaskConfigFromEnv()

        self.assertTrue(tz in str(ex.exception))

    def test_concurrency(self):
        self.set_vars_with_default()
        os.environ['CONCURRENCY'] = "2"

        config = TaskConfigFromEnv()

        self.assertEqual(config.concurrency, 2)

    def test_concurrency_should_not_zero_exception(self):
        self.set_vars_with_default()
        os.environ['CONCURRENCY'] = "0"

        with self.assertRaises(Exception) as ex:
            TaskConfigFromEnv()

        self.assertTrue('value should be integer and greater than 0' in str(ex.exception))

    def test_empty_destination_exception(self):
        properties = """[DESTINATION]
        PROJECT=""
        DATASET=""
        TABLE=""
        SQL_TYPE="STANDARD" #LEGACY/STANDARD

        [TRANSFORMATION]        
        WINDOW_SIZE="1d"
        WINDOW_OFFSET=""
        WINDOW_TRUNCATE_UPTO="d"
        TIMEZONE="Asia/Jakarta"
        USE_SPILLOVER="TRUE"
        CONCURRENCY=0

        [LOAD]
        LOAD_METHOD="REPLACE"
                """

        self.set_vars_with_default()
        os.environ['PROJECT'] = ""
        os.environ['DATASET'] = ""
        os.environ['TABLE'] = ""

        with self.assertRaises(AssertionError) as ex:
            TaskConfigFromEnv()

        self.assertEqual("config 'PROJECT' must be provided", str(ex.exception))

    def test_allow_field_addition(self):
        self.set_vars_with_default()
        os.environ['ALLOW_FIELD_ADDITION'] = 'true'

        config = TaskConfigFromEnv()
        self.assertEqual(True, config.allow_field_addition)
        del os.environ['ALLOW_FIELD_ADDITION']

    def test_allow_field_addition_should_be_false_by_default(self):
        self.set_vars_with_default()

        config = TaskConfigFromEnv()
        self.assertEqual(False, config.allow_field_addition)


class TestTaskFiles(TestCase):

    def test_task_files_without_spillover_query(self):
        fs = FileSystem()
        fs.exist = MagicMock(return_value=True)
        fs.read = MagicMock(return_value="content")

        query_sql_file = "./booking/query.sql"
        files = [query_sql_file]

        task_files = TaskFiles(fs, files)

        fs.exist.assert_has_calls([call(query_sql_file)])
        fs.read.assert_has_calls([call(query_sql_file)])

        self.assertEqual(task_files.query, "content")
        self.assertEqual(task_files.spillover_query, None)

    def test_task_files_with_spillover_query(self):
        fs = FileSystem()
        fs.exist = MagicMock(return_value=True)
        fs.read = MagicMock(return_value="content")

        query_sql_file = "./booking/query.sql"
        spillover_sql_file = "./booking/spillover_date.sql"
        files = [query_sql_file, spillover_sql_file]

        task_files = TaskFiles(fs, files)

        fs.exist.assert_has_calls([call(query_sql_file)])
        fs.read.assert_has_calls([call(query_sql_file)])

        self.assertEqual(task_files.query, "content")
        self.assertEqual(task_files.spillover_query, "content")


class TestLoadMethod(TestCase):

    def test_write_disposition(self):
        load_method = LoadMethod.APPEND
        expected_write_disposition = WriteDisposition.WRITE_APPEND
        self.assertEqual(load_method.write_disposition, expected_write_disposition)
