import datetime
import os
from datetime import timedelta
from enum import Enum
from typing import List
from typing import Optional
import configparser
import iso8601
import pytz
from google.cloud.bigquery.job import WriteDisposition, QueryPriority
from abc import ABC
from abc import abstractmethod

from bumblebee.datehelper import parse_duration
from bumblebee.filesystem import FileSystem
from bumblebee.log import get_logger

logger = get_logger(__name__)


def get_env_config(name, default=None, raise_if_empty=False):
    val = os.environ.get(name, default=default)
    if not raise_if_empty:
        return val
    if val == "" or val is None:
        raise AssertionError("config '{}' must be provided".format(name))
    return val


def parse_date(date) -> datetime:
    return iso8601.parse_date(date)


class LoadMethod(Enum):
    """
    Bigquery Query load method
    """
    APPEND = "APPEND"

    REPLACE = "REPLACE"

    REPLACE_MERGE = "REPLACE_MERGE"

    REPLACE_ALL = "REPLACE_ALL"

    MERGE = "MERGE"

    @property
    def write_disposition(self):
        if self == LoadMethod.APPEND:
            return WriteDisposition.WRITE_APPEND
        elif self == LoadMethod.REPLACE or self == LoadMethod.REPLACE_MERGE or self == LoadMethod.REPLACE_ALL:
            return WriteDisposition.WRITE_TRUNCATE
        else:
            raise Exception("write disposition is only for APPEND and REPLACE load method")


class TaskConfig(ABC):

    @property
    @abstractmethod
    def destination_project(self) -> str:
        pass

    @property
    @abstractmethod
    def destination_dataset(self) -> str:
        pass

    @property
    @abstractmethod
    def destination_table_name(self) -> str:
        pass

    @property
    def destination_table(self) -> str:
        return "{}.{}.{}".format(self.destination_project, self.destination_dataset, self.destination_table_name)

    @property
    @abstractmethod
    def sql_type(self) -> str:
        pass

    @property
    @abstractmethod
    def load_method(self) -> LoadMethod:
        pass

    @property
    @abstractmethod
    def timezone(self):
        pass

    @property
    @abstractmethod
    def use_spillover(self) -> bool:
        pass

    @property
    @abstractmethod
    def concurrency(self) -> int:
        pass

    @property
    @abstractmethod
    def filter_expression(self) -> str:
        pass

    @abstractmethod
    def print(self):
        pass


class TaskConfigFromEnv(TaskConfig):

    def __init__(self):
        self._destination_project = get_env_config("PROJECT", raise_if_empty=True)
        self._execution_project = get_env_config("EXECUTION_PROJECT", default=self._destination_project)
        self._destination_dataset = get_env_config("DATASET", raise_if_empty=True)
        self._destination_table_name = get_env_config("TABLE", raise_if_empty=True)
        self._sql_type = get_env_config("SQL_TYPE", raise_if_empty=True)
        self._filter_expression = get_env_config("PARTITION_FILTER", default=None)
        self._query_priority = get_env_config("QUERY_PRIORITY", default="INTERACTIVE")
        self._load_method = LoadMethod[get_env_config("LOAD_METHOD", raise_if_empty=True)]
        self._timezone = _validate_timezone_exist(get_env_config("TIMEZONE", default="UTC"))
        self._use_spillover = _bool_from_str(get_env_config("USE_SPILLOVER", default="true"))
        self._concurrency = _validate_greater_than_zero(int(get_env_config("CONCURRENCY", default=1)))
        self._allow_field_addition = self._get_property_or_default("ALLOW_FIELD_ADDITION", False)

    @property
    def destination_project(self) -> str:
        return self._destination_project

    @property
    def execution_project(self) -> str:
        return self._execution_project

    @property
    def destination_dataset(self) -> str:
        return self._destination_dataset

    @property
    def allow_field_addition(self) -> str:
        return self._allow_field_addition

    @property
    def destination_table_name(self) -> str:
        return self._destination_table_name

    @property
    def filter_expression(self) -> int:
        return self._filter_expression

    @property
    def sql_type(self) -> str:
        return self._sql_type

    @property
    def query_priority(self):
        if self._query_priority == 'BATCH':
            return QueryPriority.BATCH
        else:
            return QueryPriority.INTERACTIVE

    @property
    def load_method(self):
        return self._load_method

    @property
    def use_spillover(self) -> bool:
        return self._use_spillover

    @property
    def timezone(self):
        return self._timezone

    @property
    def concurrency(self) -> int:
        return self._concurrency

    def print(self):
        logger.info("task config:\n{}".format(
            "\n".join([
                "destination: {}".format(self.destination_table),
                "load method: {}".format(self.load_method),
                "timezone: {}".format(self.timezone),
                "partition_filter: {}".format(self.filter_expression),
            ])
        ))

    def __str__(self) -> str:
        return str(self.__dict__)


class AppConfig:
    """generates config from environment variables for app"""

    DEFAULT_XCOM_PATH = "/airflow/xcom/return.json"
    DEFAULT_JOB_DIR = "/data"
    JOB_INPUT_SUBDIR = "in"
    JOB_OUTPUT_SUBDIR = "out"

    def __init__(self):
        self.sql_file: Optional[str] = None
        self.properties_file: Optional[str] = None
        self.spillover_sql_file: Optional[str] = None
        self.dstart: datetime = None
        self.dend: datetime = None
        self.execution_time: datetime = None
        self.dry_run = self._is_dry_run(get_env_config("DRY_RUN", "false"))
        self.job_labels = self._get_job_labels(get_env_config("JOB_LABELS", default="owner=optimus"))
        self.xcom_path = get_env_config("XCOM_PATH", self.DEFAULT_XCOM_PATH)

        self._parse_datetime_vars()
        self._parse_specs_dir()

    def _parse_datetime_vars(self):
        dstart = get_env_config("DSTART", raise_if_empty=True)
        dend = get_env_config("DEND", raise_if_empty=True)
        default_execution_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        try:
            self.execution_time = parse_date(get_env_config("EXECUTION_TIME", default_execution_time))
            self.dstart = parse_date(dstart)
            self.dend = parse_date(dend)
        except iso8601.ParseError:
            logger.error(
                "dstart/dend/execution-time should be YYYY-mm-dd or date time iso8601 format YYYY-mm-ddTHH:MM:SSZ")
            raise

    def _parse_specs_dir(self):
        dir = get_env_config("JOB_DIR", default=self.DEFAULT_JOB_DIR)
        dir = "{}/{}".format(dir, self.JOB_INPUT_SUBDIR)
        for dirpath, _, files in os.walk(dir):
            for filename in files:
                filepath = os.path.join(dirpath, filename)
                if filename == 'query.sql':
                    self.sql_file = filepath
                elif filename == 'spillover_date.sql':
                    self.spillover_sql_file = filepath

    def _is_dry_run(self, input_config) -> bool:
        if input_config.lower() in ["true", "1", "yes", "y"]:
            logger.info("Bumblebee is running in dry-run mode")
            return True
        else:
            return False

    def _get_job_labels(self, input_config) -> dict:
        job_labels_dict = {}
        assert input_config not in ["", None], "JOB_LABELS must be provided in k1=v1,k2=v2 format"

        label_sep = ","
        key_value_sep = "="
        for label_pair in input_config.split(label_sep):
            key_value = label_pair.split(key_value_sep)
            assert key_value[0] != "", "label name cannot be empty in JOB_LABELS"
            assert key_value[1] != "", "label value cannot be empty in JOB_LABELS"
            job_labels_dict[key_value[0]] = key_value[1]

        return job_labels_dict


class TaskFiles:
    def __init__(self, fs: FileSystem, files: List):
        self.fs = fs

        fileset = self._read_all_files(files)

        self.query = fileset['query.sql']

        self.properties_cfg = None
        if 'properties.cfg' in fileset.keys():
            self.properties_cfg = fileset['properties.cfg']

        self.spillover_query = None
        if 'spillover_date.sql' in fileset.keys():
            self.spillover_query = fileset['spillover_date.sql']

    def _read_all_files(self, files):
        fileset = {}
        for file in files:
            if file is not None and self.fs.exist(file):
                content = self.fs.read(file)
                filename = self.fs.basename(file)
                fileset[filename] = content
        return fileset


def _validate_greater_than_zero(val: int):
    if val > 0:
        return val
    raise Exception("value should be integer and greater than 0")


def _validate_timezone_exist(timezone_name: str):
    pytz.timezone(timezone_name)
    return timezone_name


def _validate_not_empty(val_str: str):
    if isinstance(val_str, str) and len(val_str) > 0:
        return val_str
    else:
        raise Exception("value should not be empty")


def _validate_window_size(val: str):
    if parse_duration(val) == timedelta(seconds=0):
        raise ValueError("invalid window size: {}".format(val))
    return val


def _bool_from_str(bool_str: str) -> bool:
    if bool_str.lower() == "true":
        return True
    elif bool_str.lower() == "false":
        return False
    raise Exception("value should be a string true or false")


class TaskConfigFromFile(TaskConfig):

    def __init__(self, raw_properties):

        config = configparser.ConfigParser(allow_no_value=True)
        config.optionxform = str
        config.read_string(raw_properties)

        self._properties = {}
        for section in config.sections():
            for key in config[section]:
                self._properties[key] = config[section][key]

        self._destination_table_name = _validate_not_empty(self._get_property("TABLE"))
        self._destination_project = _validate_not_empty(self._get_property("PROJECT"))
        self._execution_project = _validate_not_empty(self._get_property_or_default("EXECUTION_PROJECT", self._destination_project))
        self._destination_dataset = _validate_not_empty(self._get_property("DATASET"))

        self._window_size = _validate_window_size(self._get_property("WINDOW_SIZE"))
        self._window_offset = self._get_property("WINDOW_OFFSET")
        self._window_truncate_upto = self._get_property("WINDOW_TRUNCATE_UPTO")

        self._filter_expression = self._get_property_or_default("PARTITION_FILTER", None)
        self._query_priority = self._get_property_or_default("QUERY_PRIORITY", "INTERACTIVE")
        self._load_method = LoadMethod[self._get_property("LOAD_METHOD")]
        self._timezone = _validate_timezone_exist(self._get_property_or_default("TIMEZONE", "UTC"))

        self._use_spillover = _bool_from_str(self._get_property_or_default("USE_SPILLOVER", "true"))
        self._concurrency = _validate_greater_than_zero(int(self._get_property_or_default("CONCURRENCY", 1)))
        self._allow_field_addition = self._get_property_or_default("ALLOW_FIELD_ADDITION", False)

    @property
    def sql_type(self) -> str:
        return "STANDARD"

    @property
    def destination_dataset(self):
        return self._destination_dataset

    @property
    def destination_project(self):
        return self._destination_project

    @property
    def execution_project(self):
        return self._execution_project

    @property
    def destination_table_name(self):
        return self._destination_table_name

    @property
    def window_size(self):
        return self._window_size

    @property
    def window_offset(self):
        return self._window_offset

    @property
    def window_truncate_upto(self):
        return self._window_truncate_upto

    @property
    def timezone(self):
        return self._timezone
    
    @property
    def query_priority(self):
        if self._query_priority == 'BATCH':
            return QueryPriority.BATCH
        else:
            return QueryPriority.INTERACTIVE

    @property
    def load_method(self):
        return self._load_method

    @property
    def use_spillover(self) -> bool:
        return self._use_spillover

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def filter_expression(self) -> str:
        return self._filter_expression

    @property
    def allow_field_addition(self) -> str:
        return self._allow_field_addition

    def print(self):
        logger.info("task config:\n{}".format(
            "\n".join([
                "destination: {}".format(self.destination_table),
                "load method: {}".format(self.load_method),
                "timezone: {}".format(self.timezone),
                "partition_filter: {}".format(self.filter_expression),
                "spillover: {}".format(self.use_spillover),
            ])
        ))

    def _get_property(self, key):
        return self._properties[key].strip('"')

    def _get_property_or_default(self, key: str, default: str):
        if key in self._properties:
            return self._properties[key].strip('"')
        return default

    def __str__(self) -> str:
        return str(self.__dict__)
